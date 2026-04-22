import os
import sys

import pandas as pd
import plotly.express as px
import streamlit as st

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from db.database import read_table
from pipeline.ml import recommend


st.set_page_config(page_title="Business Decision Dashboard", layout="wide")
st.title("Business Decision Dashboard")


@st.cache_data(ttl=300)
def load_data():
    return read_table("processed_pricing")


try:
    df = load_data()
except Exception as exc:
    st.error(f"Data unavailable. Run `python run_pipeline.py` first. Details: {exc}")
    st.stop()

if df.empty:
    st.warning("No processed pricing data available. Run `python run_pipeline.py` to populate data.")
    st.stop()

df["tags"] = df["tags"].fillna("").apply(lambda x: [t.strip() for t in str(x).split("|") if t.strip()])
df["price"] = (
    df["price"]
    .astype(str)
    .str.replace("$", "", regex=False)
    .str.replace(",", "", regex=False)
)
df["price"] = pd.to_numeric(df["price"], errors="coerce")
df = df[df["price"].notna()].copy()
df = df[df["tags"].apply(lambda t: isinstance(t, list) and len(t) > 0)].copy()
df["value_score"] = df["feature_score"] / (df["price"] + 5)
df["adjusted_score"] = df["value_score"] * df["price"].apply(
    lambda x: 0.7 if x == 0 else 1.0
)

st.subheader("Master Table (Full Data)")
st.dataframe(
    df[["company", "plan_name", "price", "tags", "adjusted_score"]],
    use_container_width=True
)

st.subheader("Key Market Insights")
paid_df = df[df["price"] > 0].sort_values("adjusted_score", ascending=False)
free_df = df[df["price"] == 0].sort_values("adjusted_score", ascending=False)
mid_df = df[df["segment"] == "mid"].sort_values("adjusted_score", ascending=False)
if not paid_df.empty:
    paid = paid_df.iloc[0]
    st.markdown(
        f"- Best paid plan: **{paid['company']} {paid['plan_name']}** delivers strong feature-to-price value for paid tiers."
    )
if not free_df.empty:
    free = free_df.iloc[0]
    st.markdown(
        f"- Best free plan: **{free['company']} {free['plan_name']}** is best when zero cost is mandatory but still includes usable capabilities."
    )
if not mid_df.empty:
    mid = mid_df.iloc[0]
    st.markdown(
        f"- Mid-tier insight: **{mid['company']} {mid['plan_name']}** is the best balance option around the common <=$20 budget range."
    )

st.subheader("Best Plan by Budget")
budget = st.slider("Maximum monthly budget", min_value=0, max_value=200, value=25, step=1)
df_filtered = df[df.price <= budget].copy()
best_budget = df_filtered.sort_values(
    ["adjusted_score", "feature_score"],
    ascending=False
).head(3)
if best_budget.empty:
    st.info("No plans fit this budget.")
else:
    st.dataframe(best_budget[["company", "plan_name", "price", "tags", "feature_score", "value_score", "adjusted_score"]], use_container_width=True)

st.subheader("Side-by-Side Comparison")
plan_labels = df.apply(lambda row: f"{row['company']} - {row['plan_name']}", axis=1).tolist()
plan_map = dict(zip(plan_labels, df.index))
col1, col2 = st.columns(2)
with col1:
    left_pick = st.selectbox("Plan A", options=plan_labels, index=0)
with col2:
    right_pick = st.selectbox("Plan B", options=plan_labels, index=min(1, len(plan_labels) - 1))
compare = df.loc[[plan_map[left_pick], plan_map[right_pick]], ["company", "plan_name", "price", "tags", "value_score"]]
st.dataframe(compare, use_container_width=True)


def compare_insight(a, b):
    if a["value_score"] > b["value_score"]:
        return f"{a['plan_name']} offers better value due to stronger features per dollar."
    elif a["value_score"] < b["value_score"]:
        return f"{b['plan_name']} offers better value due to better pricing efficiency."
    else:
        return "Both plans provide similar value."


planA = df.loc[plan_map[left_pick]]
planB = df.loc[plan_map[right_pick]]
st.info(compare_insight(planA, planB))

st.subheader("Price vs Value Chart")
fig = px.scatter(
    df,
    x="price",
    y="value_score",
    color="company",
    hover_data=["plan_name", "tags"],
)
fig.update_xaxes(type="linear")
top = df.sort_values("adjusted_score", ascending=False).head(3)
for _, row in top.iterrows():
    fig.add_annotation(
        x=row["price"],
        y=row["value_score"],
        text=row["plan_name"],
        showarrow=True
    )
fig.update_layout(xaxis_title="Price", yaxis_title="Value Score")
st.plotly_chart(fig, use_container_width=True)

st.subheader("Recommendation Section")
required_tags = st.multiselect(
    "Required features",
    options=["collaboration", "automation", "integrations", "analytics", "security", "storage"],
    default=["collaboration", "integrations"],
)
if st.button("Get Recommendations"):
    result = recommend(df, budget=float(budget), required_tags=required_tags)
    if result.empty:
        st.warning("No recommendations match the selected budget and features.")
    else:
        result = result.copy()
        result = result.sort_values("adjusted_score", ascending=False).head(3)
        result["explanation"] = result.apply(
            lambda row: (
                f"{row['company']} {row['plan_name']} is recommended because it fits the budget, "
                f"matches {int(row['tag_match'])} required tags, and has strong value at its price."
            ),
            axis=1,
        )
        st.dataframe(
            result[
                ["company", "plan_name", "price", "tags", "feature_score", "value_score", "tag_match", "final_score", "explanation"]
            ],
            use_container_width=True,
        )
        best_free = result[result["price"] == 0].sort_values("adjusted_score", ascending=False)
        best_paid = result[result["price"] > 0].sort_values("adjusted_score", ascending=False)
        if not best_free.empty:
            free_plan = best_free.iloc[0]
            st.success(f"Best Free Plan: {free_plan['company']} {free_plan['plan_name']}")
        if not best_paid.empty:
            paid_plan = best_paid.iloc[0]
            st.success(f"Best Paid Plan: {paid_plan['company']} {paid_plan['plan_name']}")

        top_plan = result.iloc[0]
        st.success(
            f"Recommended Plan: {top_plan['company']} {top_plan['plan_name']} - "
            f"Best balance of features and pricing."
        )
