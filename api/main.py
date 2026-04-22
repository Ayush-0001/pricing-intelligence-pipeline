from typing import List

import pandas as pd
from fastapi import FastAPI, HTTPException, Query

from db.database import read_table
from pipeline.ml import recommend_plans


app = FastAPI(title="Competitor Pricing Intelligence API", version="1.0.0")


def _processed_df() -> pd.DataFrame:
    try:
        df = read_table("processed_pricing")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Could not read processed data: {exc}") from exc
    if "tags" in df.columns:
        df["tags"] = df["tags"].fillna("").apply(
            lambda x: [t.strip() for t in str(x).split("|") if t.strip()]
        )
    return df


@app.get("/companies")
def companies():
    df = _processed_df()
    return sorted(df["company"].dropna().unique().tolist())


@app.get("/pricing")
def pricing(company: str | None = None):
    df = _processed_df()
    if company:
        df = df[df["company"].str.lower() == company.lower()]
    return df.to_dict(orient="records")


@app.get("/compare")
def compare(plans: List[str] = Query(...)):
    df = _processed_df()
    out = df[df["plan_name"].isin(plans)]
    return out.to_dict(orient="records")


@app.get("/best-value")
def best_value(top_n: int = 5):
    df = _processed_df().sort_values("value_score", ascending=False).head(top_n)
    return df.to_dict(orient="records")


@app.get("/recommend")
def recommend(budget: float, required_tags: str = ""):
    df = _processed_df()
    rec_df = recommend_plans(df, budget=budget, required_tags=required_tags.split(","), top_n=3)
    return rec_df.to_dict(orient="records")


@app.get("/similar")
def similar(plan_id: int, top_n: int = 3):
    try:
        sim_df = read_table("similarity_scores")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Could not read similarity data: {exc}") from exc

    if sim_df.empty:
        return []

    target = sim_df[(sim_df["plan_id_a"] == plan_id) | (sim_df["plan_id_b"] == plan_id)].copy()
    if target.empty:
        return []

    target["other_plan_id"] = target.apply(
        lambda row: row["plan_id_b"] if row["plan_id_a"] == plan_id else row["plan_id_a"],
        axis=1,
    )
    target = target.sort_values("score", ascending=False).head(top_n)

    plans = _processed_df()[["plan_id", "company", "plan_name", "price", "segment"]]
    merged = target.merge(plans, left_on="other_plan_id", right_on="plan_id", how="left")
    return merged[
        ["other_plan_id", "score", "company", "plan_name", "price", "segment"]
    ].to_dict(orient="records")
