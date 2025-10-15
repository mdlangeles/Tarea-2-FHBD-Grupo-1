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

# 🚀 Acceder a Airflow

Abre tu navegador y navega a [http://localhost:8080](http://localhost:8080) para acceder a la interfaz de **Airflow** y ejecutar el DAG.

---

## ⚙️ Ejecución Manual

Si el DAG de Airflow falla, puedes ejecutar manualmente los siguientes notebooks para cada etapa:

- **Ingesta Bronze:** `notebooks/bronze_ingest.ipynb`  
- **Transformación Silver:** `notebooks/silver_transform.ipynb`  
- **Agregación Gold:** `notebooks/gold_agg.ipynb`

---

## 🧠 Consultas de Datos

- **Trino:** Accede a las tablas *Silver* y replica los datos en *Gold* usando consultas SQL.  
- **Dremio:** Consulta las tablas *Gold* para realizar análisis.

---

## 🌐 Puertos y Servicios

| Servicio                          | URL / Puerto                                        | Descripción                                  |
|-----------------------------------|-----------------------------------------------------|----------------------------------------------|
| **Airflow Webserver**             | [http://localhost:8080](http://localhost:8080)      | Interfaz principal de Airflow                |
| **MinIO (Interfaz)**              | [http://localhost:9000](http://localhost:9000)      | Consola de MinIO                             |
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


