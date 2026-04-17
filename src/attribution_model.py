"""

--------------------------------
Two attribution models:
  1. Last-Touch Attribution  — 100% credit to the last campaign seen
  2. Linear Multi-Touch (MTA) — equal credit split across all campaigns seen
Outputs a CSV report: data/outputs/attribution_results.csv
"""

import pandas as pd
from sqlalchemy import create_engine
import os

DB_URL = "postgresql://retail_user:retail_pass@localhost:5433/retail_media"
engine = create_engine(DB_URL)
os.makedirs("data/outputs", exist_ok=True)

print("Loading data from PostgreSQL...")
query = """
    SELECT
        f.transaction_id,
        f.customer_sk,
        f.total_amount,
        f.ad_exposed,
        f.date_sk,
        c.campaign_id,
        c.campaign_name,
        c.channel,
        c.target_category,
        c.budget_usd
    FROM fact_transactions f
    LEFT JOIN dim_campaign c ON f.campaign_sk = c.campaign_sk
"""
df = pd.read_sql(query, engine)
print(f"  Loaded {len(df):,} transactions")

# ── Exposed transactions only ─────────────────────────────────────────────────
exposed = df[df["ad_exposed"] == True].copy()
print(f"  Ad-exposed transactions: {len(exposed):,}")

# ── 1. Last-Touch Attribution ─────────────────────────────────────────────────
# Each transaction gets 100% credit to the one campaign it was exposed to
print("\n[1/2] Last-Touch Attribution...")
last_touch = (
    exposed.groupby(["campaign_id", "campaign_name", "channel", "target_category", "budget_usd"])
    .agg(
        attributed_transactions = ("transaction_id", "count"),
        attributed_revenue      = ("total_amount", "sum"),
    )
    .reset_index()
)
last_touch["avg_order_value"]  = (last_touch["attributed_revenue"] / last_touch["attributed_transactions"]).round(2)
last_touch["roas"]             = (last_touch["attributed_revenue"] / last_touch["budget_usd"]).round(4)
last_touch["attribution_model"] = "last_touch"

# ── 2. Linear Multi-Touch Attribution (MTA) ───────────────────────────────────
# Each customer may appear in multiple campaigns — split credit equally
print("[2/2] Linear Multi-Touch Attribution (MTA)...")

# Count how many campaigns each customer was exposed to
customer_campaign_counts = (
    exposed.groupby("customer_sk")["campaign_id"]
    .nunique()
    .reset_index()
    .rename(columns={"campaign_id": "num_campaigns"})
)
exposed_mta = exposed.merge(customer_campaign_counts, on="customer_sk")

# Each transaction contributes 1/num_campaigns credit
exposed_mta["credit_txn"]     = 1 / exposed_mta["num_campaigns"]
exposed_mta["credit_revenue"] = exposed_mta["total_amount"] / exposed_mta["num_campaigns"]

mta = (
    exposed_mta.groupby(["campaign_id", "campaign_name", "channel", "target_category", "budget_usd"])
    .agg(
        attributed_transactions = ("credit_txn", "sum"),
        attributed_revenue      = ("credit_revenue", "sum"),
    )
    .reset_index()
)
mta["attributed_transactions"] = mta["attributed_transactions"].round(1)
mta["attributed_revenue"]      = mta["attributed_revenue"].round(2)
mta["avg_order_value"]         = (mta["attributed_revenue"] / mta["attributed_transactions"]).round(2)
mta["roas"]                    = (mta["attributed_revenue"] / mta["budget_usd"]).round(4)
mta["attribution_model"]       = "linear_mta"

# ── Combine and save ──────────────────────────────────────────────────────────
results = pd.concat([last_touch, mta], ignore_index=True)
results = results.sort_values(["attribution_model", "attributed_revenue"], ascending=[True, False])
results.to_csv("data/outputs/attribution_results.csv", index=False)

# ── Print summary ─────────────────────────────────────────────────────────────
print("\n── Last-Touch: Top 5 Campaigns by Revenue ──────────────")
top_lt = last_touch.sort_values("attributed_revenue", ascending=False).head(5)
print(f"  {'Campaign':<35} {'Channel':<20} {'Revenue':>12} {'ROAS':>8}")
print(f"  {'─'*78}")
for _, row in top_lt.iterrows():
    print(f"  {row['campaign_name'][:34]:<35} {row['channel']:<20} ${row['attributed_revenue']:>11,.2f} {row['roas']:>8.2f}")

print("\n── MTA: Top 5 Campaigns by Revenue ─────────────────────")
top_mta = mta.sort_values("attributed_revenue", ascending=False).head(5)
print(f"  {'Campaign':<35} {'Channel':<20} {'Revenue':>12} {'ROAS':>8}")
print(f"  {'─'*78}")
for _, row in top_mta.iterrows():
    print(f"  {row['campaign_name'][:34]:<35} {row['channel']:<20} ${row['attributed_revenue']:>11,.2f} {row['roas']:>8.2f}")

print(f"\n✅ Attribution complete! Saved to data/outputs/attribution_results.csv")