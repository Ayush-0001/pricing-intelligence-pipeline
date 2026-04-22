import pandas as pd

WEIGHTS = {
    "collaboration": 1.2,
    "automation": 1.5,
    "integrations": 1.3,
    "analytics": 1.2,
    "security": 1.1,
    "storage": 1.0,
}


def feature_score(tags):
    return sum(WEIGHTS.get(tag, 1.0) for tag in tags)


def compute_value_score(row):
    price = row["price"]
    score = row["feature_score"]
    if price == 0:
        return score * 0.8
    return score / price


def add_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["price"] = (
        out["price"]
        .astype(str)
        .str.replace("$", "", regex=False)
        .str.replace(",", "", regex=False)
    )
    out["price"] = pd.to_numeric(out["price"], errors="coerce")
    out = out[out["price"].notna()].copy()

    out["feature_score"] = out["tags"].apply(
        lambda tags: feature_score(tags if isinstance(tags, list) else [])
    )
    out["value_score"] = out.apply(compute_value_score, axis=1)
    out["segment"] = out["price"].apply(
        lambda x: "free" if x == 0 else ("mid" if x <= 20 else "enterprise")
    )
    return out
