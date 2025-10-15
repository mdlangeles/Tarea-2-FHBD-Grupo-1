import pendulum
from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

@dag(
    dag_id="medallion_pipeline_comments",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["dlt", "spark", "medallion"],
)
def medallion_pipeline_dag():
    """
    Pipeline completo: Bronze (DLT) -> Silver (Spark).
    """

    @task.virtualenv(
        task_id="ingest_to_bronze_dlt_limited",
        requirements=[
            "dlt", "pyarrow", "requests",
            "fsspec==2024.3.1", "s3fs==2024.3.1",
            "botocore>=1.34.41", "aiobotocore>=2.7.0", "boto3>=1.34.41"
        ],
        system_site_packages=False
    )
    def ingest_comments_to_bronze_task():
        import dlt, requests, pyarrow.parquet as pq, time, tempfile
        from typing import Iterator

        @dlt.resource(name="comments_2021", write_disposition="replace")
        def comments_2021_arrow() -> Iterator["pyarrow.Table"]:
            URL = "https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/comments/2021.parquet"
            print(f"[DLT] Descargando... {URL}")
            with requests.get(URL, stream=True, timeout=(10, 60)) as r:
                r.raise_for_status()
                with tempfile.NamedTemporaryFile() as tmp:
                    for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                        if chunk: tmp.write(chunk)
                    tmp.flush()
                    pf = pq.ParquetFile(tmp.name)
                    for i in range(2): # Limitado a 2 row-groups para pruebas
                        yield pf.read_row_group(i)
        
        pipeline = dlt.pipeline(
            pipeline_name="bronze_comments_2021_pipeline",
            destination="filesystem",
            dataset_name="comments_2021",
        )
        print("=== Iniciando ingesta DLT a Bronze (MODO PRUEBA) ===")
        load_info = pipeline.run(comments_2021_arrow(), loader_file_format="parquet")
        print("=== DLT a Bronze finalizado ===")
        print(load_info)

    # --- TAREA 2: SILVER LAYER ---
    transform_to_silver_task = SparkSubmitOperator(
        task_id="transform_to_silver_spark",
        conn_id="spark_default", # <-- AHORA SÍ USARÁ ESTA CONEXIÓN
        # master="spark://spark-master:7077", # <-- ESTA LÍNEA SE ELIMINA
        application="/opt/airflow/dags/silver_transformation.py",
        packages=(
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
            "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.96.1,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "org.apache.iceberg:iceberg-aws-bundle:1.5.2"
        ),
        conf={
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
            "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.spark_catalog.uri": "http://nessie:19120/api/v1",
            "spark.sql.catalog.spark_catalog.ref": "main",
            "spark.sql.catalog.spark_catalog.authentication.type": "NONE",
            "spark.sql.catalog.spark_catalog.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
            "spark.sql.catalog.spark_catalog.warehouse": "s3a://lakehouse/",
            "spark.sql.catalog.spark_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            "spark.sql.catalog.spark_catalog.s3.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minioadmin",
            "spark.hadoop.fs.s3a.secret.key": "minioadmin",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        }
    )

    # --- DEPENDENCIAS ---
    bronze_task_instance = ingest_comments_to_bronze_task()
    bronze_task_instance >> transform_to_silver_task

# Instancia el DAG
medallion_pipeline_dag()