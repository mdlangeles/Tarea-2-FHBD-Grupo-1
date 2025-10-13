import pendulum
from airflow.decorators import dag, task

@dag(
    dag_id="dlt_comments_2021_bronze_virtualenv",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["dlt", "bronze"],
)
def dlt_comments_bronze_dag():
    """
    DAG que ingesta los comentarios de StackOverflow 2021 usando un entorno virtual.
    """

    @task.virtualenv(
        task_id="load_comments_to_bronze_virtual",
        requirements=[
            "dlt",
            "pyarrow",
            "requests",
            "fsspec==2024.3.1",
            "s3fs==2024.3.1",
            "botocore>=1.34.41",
            "aiobotocore>=2.7.0",
            "boto3>=1.34.41"
        ],
        system_site_packages=False
    )
    def load_comments_in_virtualenv():
        """
        Esta función se ejecuta dentro de un entorno virtual autocontenido.
        """
        import dlt
        import requests
        import pyarrow.parquet as pq
        import time
        import tempfile
        from typing import Iterator

        PIPELINE_NAME = "bronze_ingest_dlt_comments2021_virtual"
        DATASET_NAME = "comments_2021"

        @dlt.resource(name="comments_2021", write_disposition="replace")
        def comments_2021_arrow() -> Iterator["pyarrow.Table"]:
            URL = "https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/comments/2021.parquet"
            print(f"[DLT] Descargando (stream) → {URL}")
            with requests.get(URL, stream=True, timeout=(10, 60)) as r:
                r.raise_for_status()
                with tempfile.NamedTemporaryFile() as tmp:
                    for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                        if chunk:
                            tmp.write(chunk)
                    tmp.flush()

                    pf = pq.ParquetFile(tmp.name)
                    print(f"[DLT] ParquetFile: {pf.metadata.num_rows} filas, {pf.num_row_groups} row-groups")
                    for i in range(pf.num_row_groups):
                        tbl = pf.read_row_group(i)
                        print(f"[DLT] Row-group {i+1}/{pf.num_row_groups}: {tbl.num_rows} filas → yield")
                        yield tbl
        
        pipeline = dlt.pipeline(
            pipeline_name=PIPELINE_NAME,
            destination="filesystem",
            dataset_name=DATASET_NAME,
        )

        print("=== Iniciando ingesta DLT → filesystem en entorno virtual ===")
        t0 = time.time()
        
        load_info = pipeline.run(
            comments_2021_arrow(),
            loader_file_format="parquet",
        )

        print("=== DLT finalizado en %.2fs ===" % (time.time() - t0))
        print(load_info)

    load_comments_in_virtualenv()

dlt_comments_bronze_dag()