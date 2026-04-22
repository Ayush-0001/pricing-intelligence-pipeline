from __future__ import annotations

from typing import List

import pandas as pd


def add_ml_outputs(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["plan_id"] = out.index.astype(int)
    out.attrs["similarity_matrix"] = []
    return out


def build_similarity_table(df: pd.DataFrame, similarity_matrix) -> pd.DataFrame:
    return pd.DataFrame(columns=["plan_id_a", "plan_id_b", "score"])


def recommend(df: pd.DataFrame, budget: float, required_tags: List[str]) -> pd.DataFrame:
    d = df[df["price"] <= budget].copy()
    d["tag_match"] = d["tags"].apply(
        lambda t: len(set(t) & set(required_tags))
    )
    d["price_weight"] = d["price"].apply(
        lambda x: 0.8 if x == 0 else 1.0
    )
    d["final_score"] = (
        0.5 * d["value_score"] +
        0.3 * d["tag_match"] +
        0.2 * d["feature_score"]
    ) * d["price_weight"]
    return d.sort_values("final_score", ascending=False).head(3)


def recommend_plans(
    df: pd.DataFrame,
    budget: float,
    required_tags: List[str],
    top_n: int = 3,
) -> pd.DataFrame:
    tags = [tag.lower().strip() for tag in required_tags if tag.strip()]
    recommendations = recommend(df, budget=budget, required_tags=tags).head(top_n)
    recommendations["recommendation_reason"] = recommendations.apply(
        lambda row: (
            f"{row['company']} {row['plan_name']} matches {int(row['tag_match'])} required tags "
            f"and ranks high on value per price."
        ),
        axis=1,
    )
    return recommendations
