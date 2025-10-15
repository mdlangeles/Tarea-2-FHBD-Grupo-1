import re
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, month, dayofmonth, to_timestamp, when, length, udf, year, current_date, datediff
from pyspark.sql.types import StringType

def standardize_column_names(df):
    """Renombra columnas de PascalCase a snake_case para unificar schemas."""
    new_df = df
    for c in df.columns:
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', c)
        new_col = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
        new_df = new_df.withColumnRenamed(c, new_col)
    return new_df

def read_bronze_data(spark, dataset_name, year=None):
    """Lee datos de la capa Bronze."""
    base_bronze_path = "s3a://lakehouse/bronze"
    path = ""
    # Asegúrate de que tus archivos se llamen así (ej: comments_2020.parquet)
    if dataset_name == "comments" and year == 2021:
        # 👇 AQUÍ ESTÁ LA CORRECCIÓN FINAL 👇
        # La tarea DLT crea una subcarpeta con el mismo nombre del dataset
        path = f"{base_bronze_path}/comments_2021/comments_2021/*.parquet"
    elif dataset_name in ["comments", "posts"] and year:
        path = f"{base_bronze_path}/{dataset_name}_{year}.parquet"
    elif dataset_name in ["badges", "users"]:
        path = f"{base_bronze_path}/{dataset_name}.parquet"
    else:
        raise ValueError(f"Dataset '{dataset_name}' no soportado.")
    
    print(f"Leyendo de: {path}")
    df = spark.read.parquet(path)
    return standardize_column_names(df)

def transform_comments_to_silver(df, year, load_timestamp):
    """Transforma el DataFrame de comments a la capa Silver."""
    binary_to_utf8_udf = udf(lambda b: b.decode('utf-8', 'ignore') if b is not None else None, StringType())
    
    return (
        df
        .withColumn("load_date", lit(load_timestamp).cast("timestamp"))
        .withColumn("comment_year", lit(year))
        .withColumn("comment_month", month(col("creation_date")))
        .withColumn("comment_day", dayofmonth(col("creation_date")))
        .withColumn("comment_text_decoded", binary_to_utf8_udf(col("text")))
        .withColumn("user_display_name_decoded", binary_to_utf8_udf(col("user_display_name")))
        .withColumn("text_length", when(col("comment_text_decoded").isNotNull(), length(col("comment_text_decoded"))).otherwise(0))
        .withColumn("has_user_display_name", when(col("user_display_name_decoded").isNotNull() & (col("user_display_name_decoded") != ""), True).otherwise(False))
        .withColumn("score_category",
                    when(col("score") >= 5, "high")
                    .when(col("score") >= 1, "medium")
                    .otherwise("low"))
        .select(
            col("id").alias("comment_id"), "post_id", "score", "score_category",
            col("comment_text_decoded").alias("comment_text"), "text_length", "creation_date",
            "comment_year", "comment_month", "comment_day",
            "user_id", "user_display_name_decoded", "has_user_display_name",
            "load_date"
        )
    )

def write_to_silver(spark, df, table_name, partition_cols=[], key_cols=None):
    """Escribe un DataFrame en una tabla Iceberg, creándola o haciendo MERGE."""
    target_table = f"silver.{table_name}"
    try:
        df.createOrReplaceTempView(f"source_view_{table_name}")
        # Intenta un MERGE si la tabla ya existe y se proporcionan claves
        if key_cols and spark.catalog.tableExists(target_table):
            print(f"Realizando MERGE en tabla existente: {target_table}")
            match_condition = " AND ".join([f"target.{col} = source.{col}" for col in key_cols])
            spark.sql(f"""
                MERGE INTO {target_table} AS target
                USING source_view_{table_name} AS source ON {match_condition}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
            print(f"✅ MERGE en {target_table} completado.")
        else: # Si no hay claves o la tabla no existe, la crea/sobrescribe
            print(f"Creando o sobrescribiendo tabla: {target_table}")
            writer = df.write.format("iceberg").mode("overwrite")
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            writer.saveAsTable(target_table)
            print(f"✅ Tabla {target_table} creada/sobrescrita.")
            
    except Exception as e:
        print(f"❌ Error escribiendo en {target_table}: {e}")
        raise e

def main(spark):
    """Función principal que orquesta la transformación Silver."""
    print("🚀 === INICIANDO PROCESO SILVER === 🚀")
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS silver")
    print("✅ Namespace 'silver' en Nessie asegurado.")
    
    print("\n--- 📝 PROCESANDO COMMENTS ---")
    try:
        df_comments_dlt = read_bronze_data(spark, "comments", year=2021)
        if df_comments_dlt:
            silver_comments = transform_comments_to_silver(df_comments_dlt, 2021, current_timestamp)
            write_to_silver(spark, silver_comments, "comments", partition_cols=["comment_year"], key_cols=["comment_id"])
    except Exception as e:
        print(f"❌ Error procesando Comments: {e}")
    
    print("\n🏁 === PROCESO SILVER COMPLETADO === 🏁")

if _name_ == "_main_":
    spark_session = SparkSession.builder.appName("SilverLayerFromAirflow").getOrCreate()
    main(spark_session)