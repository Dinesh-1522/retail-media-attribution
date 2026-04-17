
""""
Retail Media Campaign Attribution & Sales Lift Analysis
-------------------------------------------------------
Generates 4 CSV files that simulate a retail media ecosystem:
  - customers.csv       : 2,000 customer profiles
  - products.csv        : 200 products across 6 categories
  - campaigns.csv       : 20 ad campaigns with budgets and channels
  - transactions.csv    : 50,000 purchase events with ad exposure flags
"""
 
import random
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta
 
fake = Faker()
random.seed(42)
Faker.seed(42)
 
# ── Config ────────────────────────────────────────────────────────────────────
NUM_CUSTOMERS    = 2_000
NUM_PRODUCTS     = 200
NUM_CAMPAIGNS    = 20
NUM_TRANSACTIONS = 50_000
START_DATE       = datetime(2024, 1, 1)
END_DATE         = datetime(2024, 12, 31)
 
CATEGORIES = ["Electronics", "Apparel", "Grocery", "Home & Garden", "Toys", "Beauty"]
CHANNELS   = ["display", "search", "social", "email", "sponsored_product"]
 
# ── Helper ────────────────────────────────────────────────────────────────────
def random_date(start, end):
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days),
                             hours=random.randint(0, 23),
                             minutes=random.randint(0, 59))
 
# ── 1. Customers ──────────────────────────────────────────────────────────────
print("Generating customers...")
customers = []
for i in range(1, NUM_CUSTOMERS + 1):
    customers.append({
        "customer_id":    f"C{i:05d}",
        "age":            random.randint(18, 70),
        "gender":         random.choice(["M", "F", "Other"]),
        "region":         fake.state(),
        "loyalty_tier":   random.choice(["Bronze", "Silver", "Gold", "Platinum"]),
        "signup_date":    fake.date_between(start_date="-5y", end_date="-1y"),
        "email_opt_in":   random.choice([True, False]),
    })
customers_df = pd.DataFrame(customers)
 
# ── 2. Products ───────────────────────────────────────────────────────────────
print("Generating products...")
products = []
for i in range(1, NUM_PRODUCTS + 1):
    category = random.choice(CATEGORIES)
    base_price = round(random.uniform(5.0, 500.0), 2)
    products.append({
        "product_id":   f"P{i:04d}",
        "product_name": fake.catch_phrase(),
        "category":     category,
        "base_price":   base_price,
        "margin_pct":   round(random.uniform(0.10, 0.60), 2),  # gross margin
        "brand":        fake.company(),
    })
products_df = pd.DataFrame(products)
 
# ── 3. Campaigns ──────────────────────────────────────────────────────────────
print("Generating campaigns...")
campaigns = []
for i in range(1, NUM_CAMPAIGNS + 1):
    start = random_date(START_DATE, END_DATE - timedelta(days=30))
    end   = start + timedelta(days=random.randint(7, 60))
    channel = random.choice(CHANNELS)
    budget  = round(random.uniform(5_000, 100_000), 2)
    campaigns.append({
        "campaign_id":      f"CAM{i:03d}",
        "campaign_name":    f"{fake.bs().title()} Campaign",
        "channel":          channel,
        "target_category":  random.choice(CATEGORIES),
        "start_date":       start.date(),
        "end_date":         min(end, END_DATE).date(),
        "budget_usd":       budget,
        # CPM = cost per 1,000 impressions (varies by channel)
        "cpm":              round(random.uniform(1.5, 12.0), 2),
        "target_audience":  random.choice(["all", "loyalty_gold", "new_customers", "high_value"]),
    })
campaigns_df = pd.DataFrame(campaigns)
 
# ── 4. Transactions ───────────────────────────────────────────────────────────
print("Generating transactions (this takes ~10 seconds)...")
 
# Pre-build lookup sets for fast access
campaign_list = campaigns_df.to_dict("records")
customer_ids  = customers_df["customer_id"].tolist()
product_ids   = products_df["product_id"].tolist()
product_price = dict(zip(products_df["product_id"], products_df["base_price"]))
 
transactions = []
for i in range(1, NUM_TRANSACTIONS + 1):
    customer_id  = random.choice(customer_ids)
    product_id   = random.choice(product_ids)
    txn_date     = random_date(START_DATE, END_DATE)
    base_price   = product_price[product_id]
    discount_pct = random.choice([0, 0, 0, 0.05, 0.10, 0.15, 0.20])  # 60% no discount
    quantity     = random.choices([1, 2, 3, 4, 5], weights=[60, 20, 10, 6, 4])[0]
    unit_price   = round(base_price * (1 - discount_pct), 2)
    total_amount = round(unit_price * quantity, 2)
 
    # Was this customer exposed to an active campaign on this date?
    exposed_campaign = None
    for camp in campaign_list:
        if camp["start_date"] <= txn_date.date() <= camp["end_date"]:
            # 30% chance of exposure if campaign is active
            if random.random() < 0.30:
                exposed_campaign = camp["campaign_id"]
                break  # one campaign per transaction (simplified)
 
    # Conversion uplift: exposed customers convert at higher rate
    # (this is the "lift" we'll measure in Phase 3)
    converted = True  # every row IS a transaction, so by definition converted
    # We'll compare exposed vs unexposed AOV and frequency in analysis
 
    transactions.append({
        "transaction_id":  f"T{i:07d}",
        "customer_id":     customer_id,
        "product_id":      product_id,
        "transaction_date": txn_date,
        "quantity":        quantity,
        "unit_price":      unit_price,
        "discount_pct":    discount_pct,
        "total_amount":    total_amount,
        "channel":         random.choice(["online", "in_store", "mobile_app"]),
        "campaign_id":     exposed_campaign,          # NULL = no ad exposure
        "ad_exposed":      exposed_campaign is not None,
    })
 
    if i % 10_000 == 0:
        print(f"  {i:,} / {NUM_TRANSACTIONS:,} transactions generated...")
 
transactions_df = pd.DataFrame(transactions)
 
# ── Save to CSV ───────────────────────────────────────────────────────────────
print("\nSaving CSVs to data/raw/...")
customers_df.to_csv("data/raw/customers.csv",     index=False)
products_df.to_csv("data/raw/products.csv",       index=False)
campaigns_df.to_csv("data/raw/campaigns.csv",     index=False)
transactions_df.to_csv("data/raw/transactions.csv", index=False)
 
# ── Summary ───────────────────────────────────────────────────────────────────
print("\n✅ Data generation complete!")
print("─" * 45)
print(f"  customers.csv     : {len(customers_df):,} rows")
print(f"  products.csv      : {len(products_df):,} rows")
print(f"  campaigns.csv     : {len(campaigns_df):,} rows")
print(f"  transactions.csv  : {len(transactions_df):,} rows")
print(f"\n  Ad-exposed txns   : {transactions_df['ad_exposed'].sum():,} "
      f"({transactions_df['ad_exposed'].mean()*100:.1f}%)")
print(f"  Avg order value   : ${transactions_df['total_amount'].mean():.2f}")
print(f"  Avg order (exposed)  : ${transactions_df[transactions_df.ad_exposed]['total_amount'].mean():.2f}")
print(f"  Avg order (unexposed): ${transactions_df[~transactions_df.ad_exposed]['total_amount'].mean():.2f}")
print("\nNext step → Phase 2: run docker-compose up and load these CSVs into PostgreSQL")