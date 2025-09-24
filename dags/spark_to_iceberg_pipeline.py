from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="spark_to_iceberg_pipeline",
    default_args=default_args,
    description="Pipeline: parquet en Bronze -> Iceberg (Nessie)",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["spark", "iceberg", "minio"],
) as dag:

    # 1. Validar que MinIO está accesible
    check_minio = BashOperator(
        task_id="check_minio",
        bash_command="curl -f http://minio:9000/minio/health/live"
    )

    # 2. Ejecutar job de Spark (carga parquet en Iceberg)
    spark_job = SparkSubmitOperator(
        task_id="run_spark_job",
        application="/opt/airflow/dags/Spark-load-to-Iceberg.py",
        conn_id="spark_default",
        packages="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
                 "org.apache.iceberg:iceberg-aws-bundle:1.5.0,"
                 "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,"
                 "org.apache.hadoop:hadoop-aws:3.3.4",
        executor_memory="2g",
        driver_memory="1g",
        env_vars={
            "AWS_ACCESS_KEY_ID": "minioadmin",
            "AWS_SECRET_ACCESS_KEY": "minioadmin",
            "AWS_REGION": "us-east-1",
            "S3_ENDPOINT": "http://minio:9000",
            "NESSIE_URI": "http://nessie:19120/api/v1",
        }
    )

    # 3. Validar que la tabla existe en Iceberg
    validate_iceberg = SparkSubmitOperator(
        task_id="validate_iceberg",
        application="/opt/airflow/dags/validate_iceberg.py",
        conn_id="spark_default",
        packages="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
                 "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,"
                 "org.apache.hadoop:hadoop-aws:3.3.2",
        executor_memory="1g",
        driver_memory="1g",
        env_vars={
            "AWS_ACCESS_KEY_ID": "minioadmin",
            "AWS_SECRET_ACCESS_KEY": "minioadmin",
            "S3_ENDPOINT": "http://minio:9000",
            "NESSIE_URI": "http://nessie:19120/api/v1",
        }
    )

    # Dependencias
    check_minio >> spark_job >> validate_iceberg
