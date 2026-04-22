import re
from typing import Any, Dict, List

import pandas as pd

TAGS = {
    "collaboration": ["team", "collaborat", "shared"],
    "automation": ["automation", "workflow"],
    "integrations": ["integration", "api"],
    "storage": ["storage", "file"],
    "security": ["sso", "permissions", "security"],
    "analytics": ["analytics", "reports", "charts"],
}


def normalize_price(price: Any) -> float:
    if price is None:
        return float("nan")
    if isinstance(price, (float, int)):
        return float(price)
    text = str(price).strip().lower()
    if not text:
        return float("nan")
    if "custom pricing" in text or "contact sales" in text:
        return float("nan")
    if "free" in text:
        return 0.0
    match = re.search(r"(\d+(?:[.,]\d+)?)", text)
    if not match:
        return float("nan")
    return float(match.group(1).replace(",", "."))


def extract_tags(text: str) -> List[str]:
    low = str(text).lower()
    out: List[str] = []
    for tag, keywords in TAGS.items():
        if any(keyword in low for keyword in keywords):
            out.append(tag)
    return list(dict.fromkeys(out))


def extract_plan_tags(raw_features: Any) -> List[str]:
    if not isinstance(raw_features, list):
        return []
    tags: List[str] = []
    for feature in raw_features:
        tags.extend(extract_tags(feature))
    return list(dict.fromkeys(tags))[:8]


def standardize_records(raw_records: List[Dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(raw_records)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "company",
                "plan_name",
                "price",
                "tags",
                "source_url",
                "scraped_at",
            ]
        )

    df["company"] = df["company"].fillna("Unknown").astype(str)
    df["plan_name"] = df["plan_name"].fillna("Unknown").astype(str)
    if "raw_features" not in df.columns and "features" in df.columns:
        df["raw_features"] = df["features"]
    if "price" not in df.columns and "price_text" in df.columns:
        df["price"] = df["price_text"]
    df["raw_features"] = df["raw_features"].apply(lambda x: x if isinstance(x, list) else [])
    df["source_url"] = df["source_url"].fillna("").astype(str)
    df["scraped_at"] = df["scraped_at"].fillna("").astype(str)
    df["tags"] = df["raw_features"].apply(extract_plan_tags)
    df["price"] = df["price"].apply(normalize_price)
    df["plan_name"] = df["plan_name"].astype(str).str.strip().str.title()
    df.loc[df["plan_name"].str.contains("free", case=False, na=False), "price"] = 0.0

    # Data sanity gates
    df = df[df["plan_name"].str.len().between(3, 40)]
    df = df[df["tags"].apply(lambda tags: len(tags) > 0)]
    df = df[df["tags"].apply(lambda tags: len(tags) <= 8)]
    df = df[(df["price"].isna()) | (df["price"] >= 0.0)]
    df = df[~((~df["plan_name"].str.contains("free", case=False, na=False)) & (df["price"] == 0.0))]

    return df[
        [
            "company",
            "plan_name",
            "price",
            "raw_features",
            "tags",
            "source_url",
            "scraped_at",
        ]
    ]
