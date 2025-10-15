# **Proyecto de Arquitectura Lakehouse y Pipeline de Datos**

## **Descripción**

Este proyecto implementa una arquitectura **Lakehouse** con un pipeline de procesamiento de datos utilizando herramientas y frameworks de big data. El pipeline procesa los datos a través de tres etapas: **Bronze**, **Silver** y **Gold**, asegurando calidad de los datos, transformación y agregación.

La arquitectura emplea las siguientes tecnologías:

- **MinIO** para almacenamiento de objetos
- **Apache Iceberg** para formatos de tablas
- **Apache Spark** (PySpark) para procesamiento de datos
- **Nessie** como catálogo de metadatos
- **Apache Airflow** para orquestación de pipelines
- **DLT (Data Load Tool)** para ingesta de datos
- **Dremio/Trino** para consultas SQL
- **Jupyter Notebooks** para ejecución manual

## **Estructura del Proyecto**

```plaintext
.
├── dags/                         # DAGs de Airflow para las tareas del pipeline
├── notebooks/                    # Notebooks de Jupyter para ejecución manual
├── .gitignore                    # Archivo de exclusión para Git
├── Dockerfile.airflow            # Dockerfile para la configuración de Airflow
├── Dockerfile.jupyter            # Dockerfile para la configuración de Jupyter
├── docker-compose.yml            # Archivo Docker Compose para desplegar los contenedores
├── requirements.txt              # Dependencias de Python necesarias

```

# **Flujo del Proyecto**

### **1. Bronze**

- **Objetivo**: Ingesta de datos sin transformar y almacenamiento en formato Parquet.
- **Ejecución**: Se cargan los datos de las tablas **posts** y **users** de 2020 y 2021 utilizando DLT. Las demás tablas deben cargarse manualmente.
- **Tecnologías**: DLT, MinIO, formato Parquet.

### **2. Silver**

- **Objetivo**: Transformación y normalización de los datos históricos utilizando el formato Iceberg.
- **Ejecución**: Los datos históricos de las tablas **posts** y **users** de 2020 y 2021 se procesan y se fusionan.
- **Tecnologías**: Apache Spark (PySpark), Apache Iceberg.

### **3. Gold**

- **Objetivo**: Agregación de datos y generación de métricas y KPIs.
- **Ejecución**: Se generan métricas como `post_counts_by_user` y `vote_stats_per_post`.
- **Tecnologías**: Apache Spark (PySpark), Apache Iceberg.

## **Cómo Ejecutar el Proyecto**

Para ejecutar el proyecto en tu máquina local, sigue estos pasos:

1. **Construir los contenedores de Docker**:
   ```bash
   docker-compose build

2. **Inicializar la base de datos de Airflow:**
   ```bash
   docker-compose run airflow-init

3. **Iniciar los servicios:**
    ```bash
    docker-compose up -d

# Acceder a Minio

Abre tu navegagor y navega a [http://localhost:9001](http://localhost:9001)  para acceder a la interfaz de **Minio** y ver Bucket.

# 🚀 Acceder a Airflow

Abre tu navegador y navega a [http://localhost:8080](http://localhost:8080) para acceder a la interfaz de **Airflow** y ejecutar el DAG.

##  Configuración en **Airflow**

### En **Admin → Variables**

**Key:** `dlt_secrets_toml`  
**Value:**

```toml
[destination.filesystem]
bucket_url = "s3://lakehouse/bronze"

[destination.filesystem.credentials]
aws_access_key_id = "minioadmin"
aws_secret_access_key = "minioadmin"
endpoint_url = "http://minio:9000"
region_name = "us-east-1"
addressing_style = "path"
```

### En **Admin → Connections**

**Connection Id:** `spark_default`  
**Connection Type:** `Spark`  
**Host:** `spark://spark-master`  
**Port:** `7077`

Listo, ahora ejecuta el pipeline en airflow.

---

## ⚙️ Ejecución Manual

Si el DAG de Airflow falla, puedes ejecutar manualmente los siguientes notebooks para cada etapa:

- **Ingesta Bronze:** `notebooks/bronze_ingest.ipynb`
  Para este notebook, debes tener una carpeta en tu directorio llamada **StackOverflowData**
  Ahí debes tener tus archivos.parquet para la ejecución.
  
  <img width="300" height="300" alt="image" src="https://github.com/user-attachments/assets/21cac00d-d333-4f28-a5e9-64467e15acb8" />



  También puedes verificar en Dremio [http://localhost:9047](http://localhost:9047). Donde:

   - Le das en añadir **Data Source**.
   - Después en **Amazon S3**
   - En "name" le das **minio_lakehouse**
   - En AWS Access Key le das "minioadmin"
   - En AWS Access Secret le das "minioadmin" también
   - Desactivas la opción "Encrypt connection"
   - Le das a la opción "Add bucket":
      1. lakehouse
   - Pasas a "Advanced Options" y activas la opción "Enable compatibility mode"
   - Ya en **Connection Properties** veras columnas "Name" y "Value".
   - Añaderás 2, y en cada una respectivamente harás:
      1. Name = fs.f3a.endpoint / Value = minio:9000
      2. Name = fs.f3a.path.style.access / Value = true
   - Una vez **Guardes** podrás consultar las tablas.
  
  
- **Transformación Silver:** `notebooks/silver_transform.ipynb`.
  Al ejecutar este notebook, ingresa a minio para verificar que se ejecutó de manera correcta y el bucket llamado "silver" se creó, ahí estárán nuestros datos.
- **Agregación Gold:** `notebooks/gold_agg.ipynb`.
  Ya en este punto, ingresa a trino [http://localhost:8181](http://localhost:8181)

  ### **Explorar el Catálogo y los Esquemas**

```sql
SHOW CATALOGS;

SHOW SCHEMAS FROM nessie;

SHOW TABLES FROM nessie.silver;
```

### **Tabla posts**
```sql
SELECT * FROM nessie.silver.posts LIMIT 1;

SELECT * 
FROM nessie.silver.posts 
WHERE owner_user_id = 12487679 
LIMIT 10;
```

### **Tabla users**
```sql
SELECT * FROM nessie.silver.users LIMIT 1;

SELECT * 
FROM nessie.silver.users 
WHERE user_id = 12487679 
LIMIT 1;

```

### **Tabla badges**
```sql
SELECT * FROM nessie.silver.badges LIMIT 1;

```

### **Tabla badges**
```sql
SELECT * FROM nessie.silver.badges LIMIT 1;

```

## **Consultas Analíticas**
### **Número de preguntas y respuestas por usuario**
```sql
SELECT 
    p.owner_user_id AS user_id,
    COUNT(CASE WHEN p.post_type_id = 1 THEN 1 END) AS question_count,
    COUNT(CASE WHEN p.post_type_id = 2 THEN 1 END) AS answer_count
FROM nessie.silver.posts AS p
WHERE p.owner_user_id IS NOT NULL
GROUP BY p.owner_user_id
ORDER BY question_count DESC, answer_count DESC
LIMIT 20;

```


### **Número de preguntas y respuestas con nombres de usuario**
```sql
SELECT 
    u.user_id,
    u.display_name,
    COUNT(CASE WHEN p.post_type_id = 1 THEN 1 END) AS question_count,
    COUNT(CASE WHEN p.post_type_id = 2 THEN 1 END) AS answer_count
FROM nessie.silver.posts AS p
JOIN nessie.silver.users AS u
  ON p.owner_user_id = u.user_id
GROUP BY u.user_id, u.display_name
ORDER BY question_count DESC, answer_count DESC
LIMIT 20;

```

### **Estadísticas de Insignias (Badges)**
```sql
SELECT 
    b.user_id,
    COUNT(*) AS total_badges,
    COUNT(DISTINCT b.badge_name) AS distinct_badge_types,
    SUM(CASE WHEN b.badge_class = 1 THEN 1 ELSE 0 END) AS gold_badges,
    SUM(CASE WHEN b.badge_class = 2 THEN 1 ELSE 0 END) AS silver_badges,
    SUM(CASE WHEN b.badge_class = 3 THEN 1 ELSE 0 END) AS bronze_badges,
    MAX(b.load_date) AS fecha_cargue
FROM nessie.silver.badges AS b
WHERE b.user_id IS NOT NULL
GROUP BY b.user_id
ORDER BY total_badges DESC;


```

### **Cálculo del Engagement de Usuario**
```sql
SELECT 
    u.user_id,
    u.display_name,
    COALESCE(p.total_posts, 0) AS total_posts,
    COALESCE(c.total_comments, 0) AS total_comments,
    COALESCE(b.total_badges, 0) AS total_badges,
    (COALESCE(p.total_posts, 0) +
     COALESCE(c.total_comments, 0) +
     COALESCE(b.total_badges, 0)) AS engagement_score,
    MAX(u.load_date) AS fecha_cargue
FROM nessie.silver.users AS u
LEFT JOIN (
    SELECT owner_user_id AS user_id, COUNT(*) AS total_posts
    FROM nessie.silver.posts
    WHERE owner_user_id IS NOT NULL
    GROUP BY owner_user_id
) AS p ON u.user_id = p.user_id
LEFT JOIN (
    SELECT user_id, COUNT(*) AS total_comments
    FROM nessie.silver.comments
    WHERE user_id IS NOT NULL
    GROUP BY user_id
) AS c ON u.user_id = c.user_id
LEFT JOIN (
    SELECT user_id, COUNT(*) AS total_badges
    FROM nessie.silver.badges
    WHERE user_id IS NOT NULL
    GROUP BY user_id
) AS b ON u.user_id = b.user_id
WHERE u.user_id IS NOT NULL
GROUP BY 
    u.user_id, 
    u.display_name, 
    p.total_posts, 
    c.total_comments, 
    b.total_badges
ORDER BY engagement_score DESC;

```

---

## 🧠 Consultas de Datos

- **Trino:** Accede a las tablas *Silver* y replica los datos en *Gold* usando consultas SQL.  
- **Dremio:** Consulta las tablas *Gold* para realizar análisis.

---

## 🌐 Puertos y Servicios

| Servicio                          | URL / Puerto                                        | Descripción                                  |
|-----------------------------------|-----------------------------------------------------|----------------------------------------------|
| **Airflow Webserver**             | [http://localhost:8080](http://localhost:8080)      | Interfaz principal de Airflow                |
| **MinIO (Console)**               | [http://localhost:9001](http://localhost:9001)      | Administración avanzada                      |
| **Jupyter Notebooks**             | [http://localhost:8888](http://localhost:8888)      | Ejecución manual de notebooks                |
| **Dremio**                        | [http://localhost:9047](http://localhost:9047)      | Consultas y análisis de datos                |
| **Trino**                         | [http://localhost:8181](http://localhost:8181)      | Motor de consultas SQL distribuidas          |


---

## 🔑 Acceso a los Servicios

- **Airflow Webserver:** Gestiona y ejecuta tus DAGs → [http://localhost:8080](http://localhost:8080)  
- **MinIO:** Interfaz de administración → [http://localhost:9000](http://localhost:9000)  
- **Jupyter Notebooks:** Ejecuta scripts manualmente → [http://localhost:8888](http://localhost:8888)  
- **Dremio:** Analiza datos con SQL → [http://localhost:9047](http://localhost:9047)  
- **Trino:** Ejecuta consultas distribuidas → [http://localhost:8181](http://localhost:8181)

---

## 🧩 Servicios del Proyecto

Este proyecto utiliza los siguientes servicios dentro de Docker:

- **MinIO:** Almacenamiento de objetos para datos sin procesar.  
- **Airflow:** Orquestador de tareas del pipeline de datos.  
- **Apache Spark:** Procesamiento de datos en paralelo.  
- **Nessie:** Catálogo de metadatos para gestión de tablas.  
- **Dremio:** Plataforma para consultas SQL.  
- **Trino:** Motor de consultas SQL distribuido.  

---

## 🧭 Diagrama de Arquitectura

<img width="1171" height="1334" alt="image" src="https://github.com/user-attachments/assets/d91a8036-9588-4590-b71b-3fbfd904fe7b" />
