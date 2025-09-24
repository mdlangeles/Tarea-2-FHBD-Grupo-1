from pyspark.sql import SparkSession
import pyspark

CATALOG_URI = "http://nessie:19120/api/v1"
WAREHOUSE = "s3a://mybucket/gold/"   # apunta al directorio que contiene las tablas
STORAGE_URI = "http://minio:9000"
AWS_ACCESS_KEY = "minioadmin"
AWS_SECRET_KEY = "minioadmin"

# Configuración de Spark para leer desde Iceberg
conf = (
    pyspark.SparkConf()
        .setAppName("validate_iceberg")
        .set("spark.jars.packages", ",".join([
            "org.postgresql:postgresql:42.7.3",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
            "org.apache.iceberg:iceberg-aws-bundle:1.5.0",
            "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1",
            #"org.apache.hadoop:hadoop-aws:3.3.4"                    
        ]))
        .set("spark.sql.extensions",
             "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
             "org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
        # Catálogo Nessie/Iceberg
        .set("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.catalog.nessie.uri", CATALOG_URI)
        .set("spark.sql.catalog.nessie.ref", "main")
        .set("spark.sql.catalog.nessie.authentication.type", "NONE")
        .set("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .set("spark.sql.catalog.nessie.s3.endpoint", STORAGE_URI)
        .set("spark.sql.catalog.nessie.s3.path-style-access", "true")  # importante para MinIO
        .set("spark.sql.catalog.nessie.warehouse", WAREHOUSE)
        #.set("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        # Configuración Hadoop/S3A para MinIO
        .set("spark.hadoop.fs.s3a.endpoint", STORAGE_URI)
        .set("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
        .set("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
        .set("spark.hadoop.fs.s3a.path.style.access", "true")
        .set("spark.hadoop.fs.s3a.aws.credentials.provider",
             "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
)

spark = SparkSession.builder.config(conf=conf).getOrCreate()
print("✅ Spark Session for validation started")

# Leer tabla en Iceberg
try:
    df = spark.table("nessie.gold.yellowtrip")
    print("✅ La tabla fue encontrada. Primeras filas:")
    df.show(5)
    print(df.limit(5).toPandas())

except Exception as e:
    print("❌ Error: no se pudo leer la tabla en Iceberg")
    print(str(e))
