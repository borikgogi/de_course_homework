"""Bronze stage — read the raw NDJSON and flatten it to one wide table.

TODO (Завдання 1): реалізуйте build_bronze().
Контракт колонок та типів: див. CONTRACTS.md → "bronze".

Підказки:
  * читайте NDJSON ліниво: pl.scan_ndjson(config.LANDING_FILE, schema=config.LANDING_SCHEMA)
  * розгортайте вкладені структури через .struct.field("...")
  * created_at -> datetime: .str.to_datetime("%Y-%m-%dT%H:%M:%SZ", time_zone="UTC")
  * commit_count: довжина списку payload.commits; для не-PushEvent коміти
    відсутні -> заповніть 0 (.list.len().fill_null(0))
  * запишіть результат у config.BRONZE_FILE (Parquet) і поверніть DataFrame
"""

from __future__ import annotations
import os
import polars as pl
from . import config

#Функція для зберігання паркету
def Save_to_parquet(df: pl.DataFrame, s_path: str):
    output_dir = os.path.dirname(s_path)

    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    df.write_parquet(s_path, compression="zstd")



def build_bronze() -> pl.DataFrame:
  file_path = config.LANDING_FILE

  #читаємо файл
  df = pl.scan_ndjson(file_path, schema=config.LANDING_SCHEMA)
  
  #читаємо DataFrame по кожному рядку та записуємо нову структуру
  df_res = df.select([
      pl.col("id").alias("event_id"),
      pl.col("type").alias("event_type"),
      pl.col("actor").struct.field("id").cast(pl.Int64).alias("actor_id"),
      pl.col("actor").struct.field("login").alias("actor_login"),
      pl.col("repo").struct.field("id").cast(pl.Int64).alias("repo_id"),
      pl.col("repo").struct.field("name").alias("repo_name"),
      pl.col("created_at")
        .str.to_datetime("%Y-%m-%dT%H:%M:%SZ", time_zone="UTC")
        .alias("created_at"),
      pl.col("public").cast(pl.Boolean).alias("public"),
      pl.col("payload").struct.field("action").alias("action"),
      pl.col("payload")
        .struct.field("commits")
        .list.len()
        .fill_null(0)
        .cast(pl.Int64)
        .alias("commit_count")
  ]).collect()
  
  print("\n*** 1 CHECKPOINT BRONZE ***")
  #перевіряємо показники задачі
  rows=df_res.height
  event_types = df_res.select(pl.col("event_type").n_unique()).item()
  null_dates = df_res.select(pl.col("created_at").null_count()).item()
  print(f"Кількість пядків: {rows}; унікальних типів подій: {event_types}; Creates at NULLs: {null_dates}")
  Save_to_parquet(df_res, config.BRONZE_FILE)
  
  return df_res