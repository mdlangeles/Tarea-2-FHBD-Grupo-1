# -*- coding: utf-8 -*-
"""
Funciones reutilizables para cargar el Parquet de StackOverflow comments/2021 a MinIO con DLT.
Este archivo no tiene nada de Airflow, solo la lógica.
"""

import time
import tempfile
from typing import Iterator

import requests
import pyarrow.parquet as pq
import dlt

# === Config global ===
URL = "https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/comments/2021.parquet"
PIPELINE_NAME = "bronze_ingest_dlt_comments2021"
DATASET_NAME = "comments_2021"
PIPELINES_DIR = "/opt/airflow/.dlt"  # donde Airflow monta secrets.toml


@dlt.resource(name="comments_2021", write_disposition="replace")
def comments_2021_arrow() -> Iterator["pyarrow.Table"]:
    """Descarga el Parquet remoto en streaming y lo entrega por row-group como tablas Arrow."""
    t0 = time.time()
    print(f"[DLT] Descargando (stream) → {URL}")
    with requests.get(URL, stream=True, timeout=(10, 60)) as r:
        r.raise_for_status()
        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
            chunk_bytes = 0
            for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):  # 8 MB
                if chunk:
                    tmp.write(chunk)
                    chunk_bytes += len(chunk)
            tmp.flush()
            t_dl = time.time()
            print(f"[DLT] Descarga OK: ~{chunk_bytes/1024/1024:.1f} MiB en {t_dl - t0:.2f}s")

            pf = pq.ParquetFile(tmp.name)
            print(f"[DLT] ParquetFile: {pf.metadata.num_rows} filas, "
                  f"{pf.metadata.num_columns} columnas, {pf.num_row_groups} row-groups")

            for i in range(pf.num_row_groups):
                tg0 = time.time()
                tbl = pf.read_row_group(i)
                tg1 = time.time()
                print(f"[DLT] Row-group {i+1}/{pf.num_row_groups}: {tbl.num_rows} filas en {tg1 - tg0:.2f}s → yield")
                yield tbl

            print(f"[DLT] Lectura por row-groups completada en {time.time() - t_dl:.2f}s (total {time.time() - t0:.2f}s).")


def run_dlt_load():
    """Ejecuta la carga completa a MinIO (filesystem) usando DLT."""
    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination="filesystem",      # credentials y bucket_url desde secrets.toml
        dataset_name=DATASET_NAME,
        pipelines_dir=PIPELINES_DIR,
        dev_mode=False,                # en Airflow, mejor usar False
    )

    print("=== Iniciando ingesta DLT → filesystem ===")
    t0 = time.time()

    load_info = pipeline.run(
        comments_2021_arrow(),
        loader_file_format="parquet",
    )

    print("=== DLT finalizado en %.2fs ===" % (time.time() - t0))
    try:
        print(load_info)
    except Exception:
        pass

    try:
        dataset = pipeline.dataset()
        print("[DLT] Conteo de filas:\n", dataset.row_counts().df())
    except Exception:
        print("[DLT] Row counts no disponible (ok).")

    print("[DLT] filesystem root: s3://lakehouse/bronze")
    print("[DLT] dataset lógico:", DATASET_NAME)
