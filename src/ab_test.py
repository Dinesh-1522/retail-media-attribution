"""
-------------------------------------------
Tests whether ad-exposed customers show statistically
significant differences vs organic (unexposed) customers.

Tests run:
  1. t-test         — Is avg order value significantly different?
  2. Chi-square     — Is purchase frequency significantly different?
  3. Lift analysis  — Revenue lift % by campaign channel

Output:
  - data/outputs/ab_test_results.csv
  - data/outputs/lift_by_channel.csv
  - data/outputs/ab_test_chart.png
"""

import pandas as pd
import numpy as np
from scipy import stats
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import os

DB_URL = "postgresql://retail_user:retail_pass@localhost:5433/retail_media"
engine = create_engine(DB_URL)
os.makedirs("data/outputs", exist_ok=True)

# ── Load data ─────────────────────────────────────────────────────────────────
print("Loading data...")
query = """
    SELECT
        f.transaction_id,
        f.customer_sk,
        f.total_amount,
        f.ad_exposed,
        f.quantity,
        f.discount_pct,
        COALESCE(c.channel, 'organic') AS channel
    FROM fact_transactions f
    LEFT JOIN dim_campaign c ON f.campaign_sk = c.campaign_sk
"""
df = pd.read_sql(query, engine)
print(f"  Loaded {len(df):,} transactions\n")

exposed  = df[df["ad_exposed"] == True]["total_amount"]
organic  = df[df["ad_exposed"] == False]["total_amount"]

# ── Test 1: Welch's t-test on Average Order Value ────────────────────────────
print("── Test 1: Avg Order Value (Welch's t-test) ─────────────")
t_stat, p_value = stats.ttest_ind(exposed, organic, equal_var=False)
mean_exposed = exposed.mean()
mean_organic = organic.mean()
lift_pct     = ((mean_exposed - mean_organic) / mean_organic) * 100

print(f"  Exposed  AOV : ${mean_exposed:.2f}  (n={len(exposed):,})")
print(f"  Organic  AOV : ${mean_organic:.2f}  (n={len(organic):,})")
print(f"  Lift         : {lift_pct:+.2f}%")
print(f"  t-statistic  : {t_stat:.4f}")
print(f"  p-value      : {p_value:.4f}")
print(f"  Significant  : {'✅ YES (p < 0.05)' if p_value < 0.05 else '❌ NO (p >= 0.05)'}")

# ── Test 2: Chi-square on purchase frequency per customer ─────────────────────
print("\n── Test 2: Purchase Frequency (Chi-square) ──────────────")
customer_freq = df.groupby(["customer_sk", "ad_exposed"]).size().reset_index(name="txn_count")

freq_exposed = customer_freq[customer_freq["ad_exposed"] == True]["txn_count"]
freq_organic = customer_freq[customer_freq["ad_exposed"] == False]["txn_count"]

# Build contingency table: high frequency (>= median) vs low frequency
median_freq = customer_freq["txn_count"].median()
customer_freq["high_freq"] = customer_freq["txn_count"] >= median_freq

contingency = pd.crosstab(
    customer_freq["ad_exposed"],
    customer_freq["high_freq"]
)
chi2, p_chi2, dof, expected = stats.chi2_contingency(contingency)

print(f"  Median txns/customer : {median_freq:.0f}")
print(f"  Exposed  avg freq    : {freq_exposed.mean():.2f} txns/customer")
print(f"  Organic  avg freq    : {freq_organic.mean():.2f} txns/customer")
print(f"  Chi2 statistic       : {chi2:.4f}")
print(f"  p-value              : {p_chi2:.4f}")
print(f"  Degrees of freedom   : {dof}")
print(f"  Significant          : {'✅ YES (p < 0.05)' if p_chi2 < 0.05 else '❌ NO (p >= 0.05)'}")

# ── Test 3: Revenue Lift by Channel ──────────────────────────────────────────
print("\n── Test 3: Revenue Lift by Channel ──────────────────────")
channel_stats = (
    df.groupby("channel")
    .agg(
        txn_count     = ("transaction_id", "count"),
        total_revenue = ("total_amount", "sum"),
        avg_aov       = ("total_amount", "mean"),
    )
    .reset_index()
)

# Compute lift vs organic baseline
organic_aov = channel_stats.loc[channel_stats["channel"] == "organic", "avg_aov"].values[0]
channel_stats["lift_vs_organic_pct"] = ((channel_stats["avg_aov"] - organic_aov) / organic_aov * 100).round(2)
channel_stats = channel_stats.sort_values("lift_vs_organic_pct", ascending=False)
channel_stats.to_csv("data/outputs/lift_by_channel.csv", index=False)

print(f"  {'Channel':<22} {'Transactions':>13} {'Avg AOV':>10} {'Lift vs Organic':>17}")
print(f"  {'─'*65}")
for _, row in channel_stats.iterrows():
    lift_str = f"{row['lift_vs_organic_pct']:+.2f}%"
    print(f"  {row['channel']:<22} {row['txn_count']:>13,} {row['avg_aov']:>9.2f}  {lift_str:>16}")

# ── Save A/B test summary ─────────────────────────────────────────────────────
ab_results = pd.DataFrame([
    {
        "test": "Welch t-test (AOV)",
        "metric": "Avg Order Value",
        "exposed_value": round(mean_exposed, 2),
        "organic_value": round(mean_organic, 2),
        "lift_pct": round(lift_pct, 2),
        "statistic": round(t_stat, 4),
        "p_value": round(p_value, 4),
        "significant": p_value < 0.05,
    },
    {
        "test": "Chi-square (purchase frequency)",
        "metric": "Txns per Customer",
        "exposed_value": round(freq_exposed.mean(), 2),
        "organic_value": round(freq_organic.mean(), 2),
        "lift_pct": round((freq_exposed.mean() - freq_organic.mean()) / freq_organic.mean() * 100, 2),
        "statistic": round(chi2, 4),
        "p_value": round(p_chi2, 4),
        "significant": p_chi2 < 0.05,
    },
])
ab_results.to_csv("data/outputs/ab_test_results.csv", index=False)

# ── Chart: AOV by channel ─────────────────────────────────────────────────────
colors = ["#1F4E79" if c != "organic" else "#AAAAAA" for c in channel_stats["channel"]]
plt.figure(figsize=(9, 5))
bars = plt.bar(channel_stats["channel"], channel_stats["avg_aov"], color=colors, edgecolor="white")
plt.axhline(y=organic_aov, color="#CC0000", linestyle="--", linewidth=1.2, label=f"Organic baseline (${organic_aov:.2f})")
plt.title("Average Order Value by Channel vs Organic Baseline", fontsize=13)
plt.xlabel("Channel")
plt.ylabel("Avg Order Value ($)")
plt.legend()
plt.tight_layout()
plt.savefig("data/outputs/ab_test_chart.png", dpi=150)
plt.close()

print(f"\n✅ A/B test analysis complete!")
print(f"   Saved: data/outputs/ab_test_results.csv")
print(f"   Saved: data/outputs/lift_by_channel.csv")
print(f"   Saved: data/outputs/ab_test_chart.png")
print(f"\nNext step → Phase 4: Airflow DAG to orchestrate all scripts")