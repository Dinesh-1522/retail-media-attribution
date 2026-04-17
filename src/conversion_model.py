"""

---------------------------------------
Predicts which customers are most likely to convert (make a purchase)
when exposed to an ad campaign.

Models trained:
  1. Logistic Regression  — interpretable baseline
  2. Gradient Boosting    — higher accuracy

Features used:
  - Customer: age, loyalty_tier, email_opt_in
  - Date: day_of_week, quarter, is_weekend, month
  - Product: category, base_price
  - Transaction: discount_pct, quantity

Output:
  - data/outputs/model_results.csv
  - data/outputs/feature_importance.csv
  - data/outputs/feature_importance.png
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import roc_auc_score, classification_report
import matplotlib.pyplot as plt
import os

DB_URL = "postgresql://retail_user:retail_pass@localhost:5433/retail_media"
engine = create_engine(DB_URL)
os.makedirs("data/outputs", exist_ok=True)

# NOTE: We exclude campaign features (channel, budget, cpm) because they
# are direct proxies for ad_exposed=True and would cause data leakage (AUC=1.0).
print("Loading data...")
query = """
    SELECT
        f.transaction_id,
        f.ad_exposed,
        f.discount_pct,
        f.quantity,
        f.total_amount,
        cu.age,
        cu.loyalty_tier,
        cu.email_opt_in,
        d.day_of_week,
        d.quarter,
        d.is_weekend,
        d.month,
        p.category  AS product_category,
        p.base_price
    FROM fact_transactions f
    JOIN dim_customer cu ON f.customer_sk = cu.customer_sk
    JOIN dim_date     d  ON f.date_sk     = d.date_sk
    JOIN dim_product  p  ON f.product_sk  = p.product_sk
"""
df = pd.read_sql(query, engine)
print(f"  Loaded {len(df):,} rows")

print("\nEngineering features...")
le_loyalty  = LabelEncoder()
le_category = LabelEncoder()
df["loyalty_tier_enc"]     = le_loyalty.fit_transform(df["loyalty_tier"].astype(str))
df["product_category_enc"] = le_category.fit_transform(df["product_category"].astype(str))
df["email_opt_in_int"]     = df["email_opt_in"].astype(int)
df["is_weekend_int"]       = df["is_weekend"].astype(int)

FEATURES = [
    "age", "loyalty_tier_enc", "email_opt_in_int",
    "day_of_week", "quarter", "is_weekend_int", "month",
    "product_category_enc", "base_price", "discount_pct", "quantity",
]
TARGET = "ad_exposed"
X = df[FEATURES]
y = df[TARGET].astype(int)

print(f"  Features : {len(FEATURES)}")
print(f"  Exposed  : {y.sum():,} ({y.mean()*100:.1f}%)")
print(f"  Organic  : {(~y.astype(bool)).sum():,} ({(1-y.mean())*100:.1f}%)")

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)
print(f"\n  Train: {len(X_train):,}  |  Test: {len(X_test):,}")

print("\n[1/2] Training Logistic Regression...")
lr = LogisticRegression(max_iter=1000, random_state=42)
lr.fit(X_train, y_train)
lr_probs = lr.predict_proba(X_test)[:, 1]
lr_auc   = roc_auc_score(y_test, lr_probs)
lr_preds = lr.predict(X_test)
print(f"  AUC: {lr_auc:.4f}")
print("  Classification Report:")
for line in classification_report(y_test, lr_preds, target_names=["organic", "ad_exposed"]).splitlines():
    print(f"    {line}")

print("\n[2/2] Training Gradient Boosting Classifier...")
gb = GradientBoostingClassifier(n_estimators=100, max_depth=4, learning_rate=0.1, random_state=42)
gb.fit(X_train, y_train)
gb_probs = gb.predict_proba(X_test)[:, 1]
gb_auc   = roc_auc_score(y_test, gb_probs)
gb_preds = gb.predict(X_test)
print(f"  AUC: {gb_auc:.4f}")
print("  Classification Report:")
for line in classification_report(y_test, gb_preds, target_names=["organic", "ad_exposed"]).splitlines():
    print(f"    {line}")

fi_df = pd.DataFrame({
    "feature":    FEATURES,
    "importance": gb.feature_importances_
}).sort_values("importance", ascending=False)
fi_df.to_csv("data/outputs/feature_importance.csv", index=False)

print("\n── Top Feature Importances (Gradient Boosting) ──────────")
print(f"  {'Feature':<25} {'Importance':>12}")
print(f"  {'─'*40}")
for _, row in fi_df.iterrows():
    bar = "█" * int(row["importance"] * 200)
    print(f"  {row['feature']:<25} {row['importance']:>10.4f}  {bar}")

plt.figure(figsize=(8, 5))
plt.barh(fi_df["feature"][::-1], fi_df["importance"][::-1], color="#1F4E79")
plt.xlabel("Importance")
plt.title("Feature Importances — Gradient Boosting")
plt.tight_layout()
plt.savefig("data/outputs/feature_importance.png", dpi=150)
plt.close()

results_df = X_test.copy()
results_df["actual"]       = y_test.values
results_df["lr_pred_prob"] = lr_probs.round(4)
results_df["gb_pred_prob"] = gb_probs.round(4)
results_df["gb_predicted"] = gb_preds
results_df.to_csv("data/outputs/model_results.csv", index=False)

print(f"\n── Model Comparison ─────────────────────────────────────")
print(f"  {'Model':<28} {'AUC':>8}")
print(f"  {'─'*38}")
print(f"  {'Logistic Regression':<28} {lr_auc:>8.4f}")
print(f"  {'Gradient Boosting':<28} {gb_auc:>8.4f}")
print(f"\n✅ Model training complete!")
print(f"   Saved: data/outputs/model_results.csv")
print(f"   Saved: data/outputs/feature_importance.csv")
print(f"   Saved: data/outputs/feature_importance.png")