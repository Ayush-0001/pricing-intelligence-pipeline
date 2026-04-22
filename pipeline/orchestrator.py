import logging
from pathlib import Path
from typing import Dict

import pandas as pd

from db.database import init_db, write_processed, write_raw, write_similarity
from pipeline.cleaning import standardize_records
from pipeline.feature_engineering import add_features
from pipeline.ml import add_ml_outputs, build_similarity_table
from scraper.base import write_raw_json
from scraper.sites import run_all_scrapers


LOGGER = logging.getLogger(__name__)


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def run_pipeline(raw_json_path: str = "data/raw_pricing.json") -> Dict[str, int]:
    setup_logging()
    init_db()

    Path("data").mkdir(parents=True, exist_ok=True)

    LOGGER.info("Starting scraping stage")
    raw_records = run_all_scrapers()
    write_raw_json(raw_records, raw_json_path)

    LOGGER.info("Starting cleaning stage")
    cleaned_df = standardize_records(raw_records)
    if cleaned_df.empty:
        raise RuntimeError("Pipeline failed: no scraped records available")

    LOGGER.info("Starting feature engineering stage")
    featured_df = add_features(cleaned_df)

    LOGGER.info("Starting ML stage")
    ml_df = add_ml_outputs(featured_df)
    similarity_matrix = ml_df.attrs["similarity_matrix"]
    similarity_df = build_similarity_table(ml_df, similarity_matrix)

    LOGGER.info("Persisting data")
    write_raw(cleaned_df)
    write_processed(ml_df)
    write_similarity(similarity_df if not similarity_df.empty else pd.DataFrame(columns=["plan_id_a", "plan_id_b", "score"]))

    result = {
        "raw_records": len(cleaned_df),
        "processed_records": len(ml_df),
        "similarity_pairs": len(similarity_df),
    }
    LOGGER.info("Pipeline completed: %s", result)
    return result
