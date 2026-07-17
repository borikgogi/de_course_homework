"""Silver stage — clean, filter and de-duplicate the bronze events.

TODO (Завдання 2 і 3): реалізуйте build_silver() і write_silver_partitioned().
Контракт: див. CONTRACTS.md → "silver" і "silver partitioned".

build_silver():
  * залиште тільки типи з config.TARGET_EVENT_TYPES
  * приберіть рядки з порожнім/відсутнім repo_name, відсутнім event_id чи created_at
  * гарантуйте унікальність по event_id (.unique(subset=["event_id"]))
  * запишіть у config.SILVER_FILE і поверніть DataFrame

write_silver_partitioned():
  * запишіть silver як Hive-партиціонований датасет за event_type
  * директорія: config.SILVER_PARTITIONED_DIR
  * підказка: df.write_parquet(dir, partition_by="event_type")
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

#зчитуємо паркет
df=pl.scan_parquet(config.BRONZE_FILE)


def build_silver(bronze: pl.DataFrame) -> pl.DataFrame:

  #для себе робимо список для фільтації null
  filter_null_col = ["repo_name", "event_id", "created_at"]

  #фільтруємо данні згідно задач
  df_res = (df.drop_nulls(subset=filter_null_col)
            .filter(pl.col("event_type")
            .is_in(config.TARGET_EVENT_TYPES) & 
              (pl.col("repo_name") !="")).unique(subset="event_id")
            .collect()
  )
  #вивід рузультатів для перевірки
  print("\n*** 2 CHECKPOINT SILVER ***")
  print(f"Кількість рядків: {df_res.height}")

  types_count = df_res["event_type"].n_unique()
  print(f"кількість типів подій:  {types_count}")

  nullcount = df_res.select(pl.col(filter_null_col).null_count())
  print(f"Кількість null в ключах: {nullcount}")

  if df_res["event_id"].n_unique() == df_res.height:
    print("всі event_id унікальні")  
  else:
    print("event_id НЕ унікальні")       

  print("Кількість по кожному з типу:")
  count = df_res.group_by(pl.col("event_type")).len()
  print(count) 
  Save_to_parquet(df_res, config.SILVER_FILE)
  
  return df_res

#-------------------------------------------------------------#
def write_silver_partitioned(silver: pl.DataFrame) -> None:
  
  print("\n*** 3 CHECKPOINT SILVER ***")
  df = pl.scan_parquet(config.SILVER_FILE).collect()

  output_dir = os.path.dirname(config.SILVER_PARTITIONED_DIR)
  if output_dir:
    os.makedirs(config.SILVER_PARTITIONED_DIR, exist_ok=True)
  
  df.write_parquet(config.SILVER_PARTITIONED_DIR, partition_by="event_type", compression="zstd")

  print("Перевірка партицій:")
  df_check = pl.scan_parquet(f"{config.SILVER_PARTITIONED_DIR}/**/*.parquet", hive_partitioning=True).collect()
  print(f"кількість партицій: {df_check["event_type"].n_unique()}; кількість рядків: {df_check.height}")
  

