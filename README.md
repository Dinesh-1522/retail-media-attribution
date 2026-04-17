Retail Media Campaign Attribution & Sales Lift Analysis

End-to-end data science project simulating a retail media measurement platform —
built to demonstrate skills in data engineering, machine learning, statistical testing,
and business intelligence relevant to roles in retail media analytics.


Project Overview
This project replicates the core analytical work done at retail media networks like
Walmart Connect, Amazon Ads, and Kroger Precision Marketing — measuring whether
ad campaigns actually drive incremental sales, and which customers are most likely
to respond to advertising.
The business question: Do customers who are exposed to ad campaigns spend more,
buy more often, and generate more revenue than customers who find products organically?

Key Results
MetricResultTransactions analyzed50,000Ad-exposed transactions18,383 (36.8%)Organic transactions31,617 (63.2%)AOV — Ad-exposed$397.85AOV — Organic$400.09AOV lift-0.56% (not significant, p = 0.52)Purchase frequency liftSignificant ✅ (chi-square p < 0.0001)Conversion model AUC (Logistic Regression)0.6467Conversion model AUC (Gradient Boosting)0.7706Top predictive featureMonth (seasonality drives ad exposure)
Key finding: Ad campaigns did not significantly lift average order value,
but they did significantly increase purchase frequency — exposed customers bought
more often than organic customers (chi-square p < 0.0001). This is consistent
with real-world retail media findings where ads drive repeat visits rather than
larger basket sizes.

Architecture
Raw Data (Faker)
      ↓
PostgreSQL Star Schema (Docker)
      ↓
ETL Pipeline (pandas + SQLAlchemy)
      ↓
┌─────────────────────────────────┐
│  Attribution Models (MTA)       │
│  Conversion Model (GBM, LR)     │  ──→ data/outputs/
│  A/B Test Framework             │
└─────────────────────────────────┘
      ↓
Airflow DAG (orchestration)
      ↓
Tableau Dashboard (KPI reporting)

Star Schema
                    ┌──────────────┐
                    │  dim_date    │
                    └──────┬───────┘
                           │
┌──────────────┐    ┌──────┴──────────────┐    ┌──────────────┐
│ dim_customer ├────┤  fact_transactions  ├────┤  dim_product │
└──────────────┘    └──────┬──────────────┘    └──────────────┘
                           │
                    ┌──────┴───────┐
                    │ dim_campaign │
                    └──────────────┘
Tables:

fact_transactions — 50,000 purchase events with ad exposure flags
dim_customer — 2,000 customers with loyalty tier, region, demographics
dim_product — 200 products across 6 categories with pricing and margin
dim_campaign — 20 ad campaigns with channel, budget, CPM, targeting
dim_date — 366 dates for 2024 with week, quarter, weekend flags


Project Structure
retail-media-attribution/
├── dags/
│   └── retail_media_dag.py       # Airflow DAG — full pipeline orchestration
├── src/
│   ├── generate_data.py          # Phase 1: synthetic data generation (Faker)
│   ├── load_to_postgres.py       # Phase 2: ETL into PostgreSQL star schema
│   ├── attribution_model.py      # Phase 3a: last-touch + MTA attribution
│   ├── conversion_model.py       # Phase 3b: LR + GBM conversion prediction
│   └── ab_test.py                # Phase 3c: t-test + chi-square + lift analysis
├── sql/
│   └── schema.sql                # Star schema DDL
├── data/
│   ├── raw/                      # Generated CSVs (gitignored)
│   └── outputs/                  # Model outputs and charts (gitignored)
├── docker-compose.yml            # PostgreSQL container
├── requirements.txt
├── .env.example                  # Environment variable template
└── .gitignore

Tech Stack
LayerToolsData generationPython, Faker, pandasDatabasePostgreSQL 15, DockerETLpandas, SQLAlchemyMachine learningscikit-learn (Logistic Regression, Gradient Boosting)Statisticsscipy (t-test, chi-square)Visualizationmatplotlib, TableauOrchestrationApache AirflowSchema designKimball star schema, dbt-readyVersion controlGit, GitHub

Setup & Running
Prerequisites

Python 3.9+
Docker Desktop
Git

1. Clone the repo
bashgit clone https://github.com/Dinesh-1522/retail-media-attribution.git
cd retail-media-attribution
2. Install dependencies
bashpip install -r requirements.txt
3. Configure environment
bashcp .env.example .env
# Edit .env if needed — defaults work out of the box
4. Start PostgreSQL
bashdocker-compose up -d
# Wait 10 seconds for container to become healthy
docker exec -i retail_media_db psql -U retail_user -d retail_media < sql/schema.sql
5. Generate data & run pipeline
bashpython src/generate_data.py       # ~10 seconds
python src/load_to_postgres.py    # loads star schema
python src/attribution_model.py   # MTA + last-touch
python src/conversion_model.py    # LR + GBM models
python src/ab_test.py             # statistical tests
6. View outputs
All results are saved to data/outputs/:

attribution_results.csv — ROAS and revenue by campaign and model
model_results.csv — per-transaction conversion predictions
feature_importance.csv / .png — top predictive features
ab_test_results.csv — t-test and chi-square results
lift_by_channel.csv — revenue lift by ad channel
ab_test_chart.png — AOV by channel vs organic baseline


ML Models
Attribution Modeling
Two models measure which campaigns deserve credit for a conversion:

Last-Touch Attribution — 100% credit to the last campaign the customer
was exposed to before purchasing. Simple but over-credits retargeting campaigns.
Linear Multi-Touch (MTA) — Equal credit split across all campaigns a
customer was exposed to. Fairer for upper-funnel awareness campaigns.

Conversion Prediction
Predicts which customers are most likely to be ad-responsive using:
ModelAUCLogistic Regression0.6467Gradient Boosting0.7706
Top features: month (seasonality), quarter, base_price, age
A/B Testing Framework
TestMetricResultWelch t-testAvg order valuep = 0.52 — not significantChi-squarePurchase frequencyp < 0.0001 — significant ✅

Airflow Pipeline
The DAG retail_media_attribution_pipeline runs daily at 6am:
validate_data
      ↓
load_to_postgres
      ↓
attribution_model ──┐
conversion_model  ──┼──→ notify_success
ab_test           ──┘
To run locally:
bashpip install apache-airflow
export AIRFLOW_HOME=$(pwd)
airflow db init
airflow standalone
# Open http://localhost:8080

Tableau Dashboard
Five dashboards built on top of the PostgreSQL star schema:

Campaign Performance — ROAS, CTR, attributed revenue by campaign
Attribution Comparison — Last-touch vs MTA side by side
Sales Lift Analysis — Exposed vs organic AOV and frequency trends
Customer Segments — Conversion rates by loyalty tier and region
Channel Efficiency — Revenue lift by ad channel vs organic baseline

Live Dashboard: https://public.tableau.com/app/profile/dinesh.gudapati/viz/RetailMediaAttributionDashboardDineshGudapati/Sheet1

