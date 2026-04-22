from __future__ import annotations

from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text


DB_PATH = Path("data/pricing_intel.db")
DB_URI = f"sqlite:///{DB_PATH}"


def get_engine():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return create_engine(DB_URI, future=True)


def init_db() -> None:
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS raw_pricing (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company TEXT,
                    plan_name TEXT,
                    price_text TEXT,
                    billing_cycle TEXT,
                    features TEXT,
                    source_url TEXT,
                    scraped_at TEXT
                );
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS processed_pricing (
                    plan_id INTEGER PRIMARY KEY,
                    company TEXT,
                    plan_name TEXT,
                    price REAL,
                    tags TEXT,
                    feature_score REAL,
                    value_score REAL,
                    segment TEXT,
                    scraped_at TEXT
                );
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS similarity_scores (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    plan_id_a INTEGER,
                    plan_id_b INTEGER,
                    score REAL
                );
                """
            )
        )


def write_raw(df_raw: pd.DataFrame) -> None:
    engine = get_engine()
    to_store = df_raw.copy()
    if "tags" in to_store.columns:
        to_store["tags"] = to_store["tags"].apply(
            lambda x: " | ".join(x) if isinstance(x, list) else str(x)
        )
    if "features" in to_store.columns:
        to_store["features"] = to_store["features"].apply(
            lambda x: " | ".join(x) if isinstance(x, list) else ""
        )
    if "raw_features" in to_store.columns:
        to_store["raw_features"] = to_store["raw_features"].apply(
            lambda x: " | ".join(x) if isinstance(x, list) else ""
        )
    elif "tags" in to_store.columns:
        to_store["features"] = to_store["tags"].apply(
            lambda x: " | ".join(x) if isinstance(x, list) else str(x)
        )
    for required in ["price_text", "billing_cycle"]:
        if required not in to_store.columns:
            to_store[required] = ""
    to_store.to_sql("raw_pricing", engine, if_exists="replace", index=False)


def write_processed(df_processed: pd.DataFrame) -> None:
    engine = get_engine()
    to_store = df_processed.copy()
    to_store["tags"] = to_store["tags"].apply(lambda x: " | ".join(x) if isinstance(x, list) else "")
    cols = [
        "plan_id",
        "company",
        "plan_name",
        "price",
        "tags",
        "feature_score",
        "value_score",
        "segment",
        "scraped_at",
    ]
    to_store[cols].to_sql("processed_pricing", engine, if_exists="replace", index=False)


def write_similarity(df_similarity: pd.DataFrame) -> None:
    engine = get_engine()
    df_similarity.to_sql("similarity_scores", engine, if_exists="replace", index=False)


def read_table(name: str) -> pd.DataFrame:
    engine = get_engine()
    return pd.read_sql_table(name, engine)
