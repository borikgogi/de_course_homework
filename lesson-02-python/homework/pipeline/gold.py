"""Gold stage — three analytics tables built from silver.

TODO (Завдання 4, 5, 6): реалізуйте три функції нижче.
Контракт: див. CONTRACTS.md → "gold repo_activity", "gold activity_per_minute",
"gold push_commits_by_repo". Усі лічильники приводьте до Int64 (.cast(pl.Int64)),
щоб схема результату була стабільною.

  * build_repo_activity:        кількість подій + кількість унікальних типів на repo
  * build_activity_per_minute:  кількість подій по хвилинах (.dt.truncate("1m"))
  * build_push_commits_by_repo: тільки PushEvent — кількість пушів і сума commit_count на repo
"""

from __future__ import annotations

import polars as pl
import os
from . import config

#Функція для зберігання паркету
def Save_to_parquet(df: pl.DataFrame, s_path: str):
    output_dir = os.path.dirname(s_path)

    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    df.write_parquet(s_path, compression="zstd")

#зчитуємо паркет
df=pl.scan_parquet(config.SILVER_FILE)


def build_repo_activity(silver: pl.DataFrame) -> pl.DataFrame:
    df_res4=(df
        .group_by("repo_name")
        .agg(
            # Загальна кількість подій
            pl.len().cast(pl.Int64).alias("event_count"),
            # Кількість унікальних типів подій
            pl.col("event_type").n_unique().cast(pl.Int64).alias("distinct_event_types"), 
        )
        .sort("event_count", descending=True)
        .collect()
    )

    print("\n*** 4 CHECKPOINT GOLD ***")
    print(df_res4.head(5))

    print(f"Всого рядків: {df_res4.height}; сумма: {df_res4["event_count"].sum()}")
    Save_to_parquet(df_res4, config.GOLD_REPO_ACTIVITY)
    return df_res4


def build_activity_per_minute(silver: pl.DataFrame) -> pl.DataFrame:
    df_res5=(df
            .with_columns(
                pl.col("created_at")
                .cast(pl.Datetime("us", "UTC"))
                .dt.truncate("1m")
                .alias("minute")
                )
                .group_by("minute")
                .agg(pl.len().cast(pl.Int64).alias("event_count"))
                .sort("minute")
                .collect()
            )
    print("\n*** 5 CHECKPOINT GOLD ***")
    print(f"Всого рядків: {df_res5.height}; сумма: {df_res5["event_count"].sum()}")
    Save_to_parquet(df_res5, config.GOLD_ACTIVITY_PER_MINUTE)
    return df_res5



def build_push_commits_by_repo(silver: pl.DataFrame) -> pl.DataFrame:
    df_res6=(df.lazy()
            .filter(pl.col("event_type") == "PushEvent")
            .group_by("repo_name")
            .agg(
                pl.len().cast(pl.Int64).alias("push_events"),
                pl.col("commit_count").sum().cast(pl.Int64).alias("total_commits")
                )
            .collect()
            )
    print("\n*** 6 CHECKPOINT GOLD ***")
    print(f"Всого рядків: {df_res6.height}; сумма: {df_res6["total_commits"].sum()}")
    Save_to_parquet(df_res6, config.GOLD_PUSH_COMMITS)
    return df_res6