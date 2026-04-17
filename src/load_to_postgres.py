"""
Phase 2: ETL — Load CSVs into PostgreSQL Star Schema
------------------------------------------------------
Reads the 4 raw CSVs from data/raw/, transforms them,
and loads them into the star schema tables in order:
  1. dim_date        (generated, not from CSV)
  2. dim_customer
  3. dim_product
  4. dim_campaign
  5. fact_transactions  (joins all dims to resolve surrogate keys)
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import date, timedelta

# ── Database connection ───────────────────────────────────────────────────────
DB_URL = "postgresql://retail_user:retail_pass@localhost:5433/retail_media"
engine = create_engine(DB_URL)

def log(msg):
    print(f"  {msg}")

# ── 1. dim_date — generate all dates for 2024 ────────────────────────────────
print("\n[1/5] Loading dim_date...")
date_rows = []
current = date(2024, 1, 1)
end     = date(2024, 12, 31)
while current <= end:
    date_rows.append({
        "date_sk":     int(current.strftime("%Y%m%d")),
        "full_date":   current,
        "year":        current.year,
        "quarter":     (current.month - 1) // 3 + 1,
        "month":       current.month,
        "month_name":  current.strftime("%B"),
        "week":        current.isocalendar()[1],
        "day_of_week": current.weekday(),
        "day_name":    current.strftime("%A"),
        "is_weekend":  current.weekday() >= 5,
    })
    current += timedelta(days=1)

dim_date_df = pd.DataFrame(date_rows)
dim_date_df.to_sql("dim_date", engine, if_exists="append", index=False)
log(f"Inserted {len(dim_date_df):,} date rows")

# ── 2. dim_customer ───────────────────────────────────────────────────────────
print("\n[2/5] Loading dim_customer...")
customers_df = pd.read_csv("data/raw/customers.csv")
customers_df.to_sql("dim_customer", engine, if_exists="append", index=False)
log(f"Inserted {len(customers_df):,} customer rows")

# ── 3. dim_product ────────────────────────────────────────────────────────────
print("\n[3/5] Loading dim_product...")
products_df = pd.read_csv("data/raw/products.csv")
products_df.to_sql("dim_product", engine, if_exists="append", index=False)
log(f"Inserted {len(products_df):,} product rows")

# ── 4. dim_campaign ───────────────────────────────────────────────────────────
print("\n[4/5] Loading dim_campaign...")
campaigns_df = pd.read_csv("data/raw/campaigns.csv")
campaigns_df.to_sql("dim_campaign", engine, if_exists="append", index=False)
log(f"Inserted {len(campaigns_df):,} campaign rows")

# ── 5. fact_transactions ──────────────────────────────────────────────────────
print("\n[5/5] Loading fact_transactions...")
txn_df = pd.read_csv("data/raw/transactions.csv", parse_dates=["transaction_date"])

# ── Resolve surrogate keys ────────────────────────────────────────────────────
# Read back the auto-assigned surrogate keys from each dim table
with engine.connect() as conn:
    cust_map = pd.read_sql("SELECT customer_sk, customer_id FROM dim_customer", conn)
    prod_map = pd.read_sql("SELECT product_sk, product_id FROM dim_product", conn)
    camp_map = pd.read_sql("SELECT campaign_sk, campaign_id FROM dim_campaign", conn)

# Join surrogate keys onto transactions
txn_df = txn_df.merge(cust_map, on="customer_id", how="left")
txn_df = txn_df.merge(prod_map, on="product_id", how="left")
txn_df = txn_df.merge(camp_map, on="campaign_id", how="left")

# Build date_sk as integer YYYYMMDD
txn_df["date_sk"] = txn_df["transaction_date"].dt.strftime("%Y%m%d").astype(int)

# Select only the fact table columns
fact_df = txn_df[[
    "transaction_id",
    "customer_sk",
    "product_sk",
    "campaign_sk",
    "date_sk",
    "quantity",
    "unit_price",
    "discount_pct",
    "total_amount",
    "channel",
    "ad_exposed",
]].copy()

# campaign_sk will be NaN for unexposed rows — that's correct (NULL FK)
fact_df["campaign_sk"] = fact_df["campaign_sk"].where(fact_df["campaign_sk"].notna(), other=None)

# Load in chunks to avoid memory issues
CHUNK = 10_000
for i in range(0, len(fact_df), CHUNK):
    chunk = fact_df.iloc[i:i + CHUNK]
    chunk.to_sql("fact_transactions", engine, if_exists="append", index=False)
    log(f"  Loaded rows {i:,} – {min(i+CHUNK, len(fact_df)):,}")

# ── Validation queries ────────────────────────────────────────────────────────
print("\n── Validation ──────────────────────────────────────────")
with engine.connect() as conn:
    counts = {
        "dim_customer":     conn.execute(text("SELECT COUNT(*) FROM dim_customer")).scalar(),
        "dim_product":      conn.execute(text("SELECT COUNT(*) FROM dim_product")).scalar(),
        "dim_campaign":     conn.execute(text("SELECT COUNT(*) FROM dim_campaign")).scalar(),
        "dim_date":         conn.execute(text("SELECT COUNT(*) FROM dim_date")).scalar(),
        "fact_transactions":conn.execute(text("SELECT COUNT(*) FROM fact_transactions")).scalar(),
    }
    for table, count in counts.items():
        log(f"{table:<22}: {count:>7,} rows")

    # Quick sanity check — exposed vs unexposed AOV
    aov = conn.execute(text("""
        SELECT
            ad_exposed,
            COUNT(*)                          AS txn_count,
            ROUND(AVG(total_amount)::numeric, 2) AS avg_order_value,
            ROUND(SUM(total_amount)::numeric, 2) AS total_revenue
        FROM fact_transactions
        GROUP BY ad_exposed
        ORDER BY ad_exposed DESC
    """)).fetchall()

    print("\n── Exposed vs Unexposed (preview of Phase 3 lift) ──────")
    print(f"  {'Exposed':<12} {'Transactions':>14} {'Avg Order Value':>16} {'Total Revenue':>16}")
    print(f"  {'─'*60}")
    for row in aov:
        label = "Yes (ad)" if row[0] else "No (organic)"
        print(f"  {label:<12} {row[1]:>14,} {str(row[2]):>16} {str(row[3]):>16}")

print("\n✅ Phase 2 complete! Star schema loaded.")
print("Next step → Phase 3: ML models (attribution + conversion prediction + A/B tests)")