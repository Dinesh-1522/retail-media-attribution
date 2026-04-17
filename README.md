# Retail Media Campaign Attribution & Sales Lift Analysis

> End-to-end data science project simulating a retail media measurement platform.

**Live Dashboard:** [Tableau Public](https://public.tableau.com/app/profile/dinesh.gudapati/viz/RetailMediaAttributionDashboardDineshGudapati/Sheet1)

---

## The Business Question

Do customers exposed to ad campaigns spend more, buy more often, and generate more revenue than organic customers? This project answers that using the same measurement frameworks used at Walmart Connect and Amazon Ads.

---

## Key Results

| Metric | Value |
|--------|-------|
| Transactions analyzed | 50,000 |
| Ad-exposed transactions | 18,383 (36.8%) |
| Organic transactions | 31,617 (63.2%) |
| AOV — Ad-exposed | $397.85 |
| AOV — Organic | $400.09 |
| Purchase frequency lift | **Significant (p < 0.0001)** |
| Conversion model AUC (GBM) | **0.7706** |
| Top predictive feature | Month (seasonality) |

---

## Architecture
Raw Data (Faker) → PostgreSQL Star Schema (Docker) → ETL Pipeline
→ Attribution Models (MTA + Last-Touch)
→ Conversion Model (GBM + Logistic Regression)
→ A/B Test Framework (t-test + chi-square)
→ Airflow DAG → Tableau Dashboard

---

## Star Schema

| Table | Rows | Description |
|-------|------|-------------|
| fact_transactions | 50,000 | Purchase events with ad exposure flags |
| dim_customer | 2,000 | Loyalty tier, region, demographics |
| dim_product | 200 | 6 categories, pricing, margin |
| dim_campaign | 20 | Channel, budget, CPM, targeting |
| dim_date | 366 | Week, quarter, weekend flags |

---

## Project Structure
retail-media-attribution/
├── dags/
│   └── retail_media_dag.py
├── src/
│   ├── generate_data.py
│   ├── load_to_postgres.py
│   ├── attribution_model.py
│   ├── conversion_model.py
│   └── ab_test.py
├── sql/
│   └── schema.sql
├── docker-compose.yml
├── requirements.txt
└── .env.example

---

## Tech Stack

| Layer | Tools |
|-------|-------|
| Data generation | Python, Faker, pandas |
| Database | PostgreSQL 15, Docker |
| ETL | pandas, SQLAlchemy |
| Machine learning | scikit-learn (LR, Gradient Boosting) |
| Statistics | scipy (t-test, chi-square) |
| Visualization | matplotlib, Tableau |
| Orchestration | Apache Airflow |
| Schema design | Kimball star schema |

---

## ML Models

| Model | AUC |
|-------|-----|
| Logistic Regression | 0.6467 |
| Gradient Boosting | **0.7706** |

### A/B Testing

| Test | Metric | Result |
|------|--------|--------|
| Welch t-test | Avg order value | Not significant (p = 0.52) |
| Chi-square | Purchase frequency | **Significant (p < 0.0001)** |

---

## Quickstart

```bash
git clone https://github.com/Dinesh-1522/retail-media-attribution.git
cd retail-media-attribution
pip install -r requirements.txt
docker-compose up -d
docker exec -i retail_media_db psql -U retail_user -d retail_media < sql/schema.sql
python src/generate_data.py
python src/load_to_postgres.py
python src/attribution_model.py
python src/conversion_model.py
python src/ab_test.py
```

---

## About

Built by **Dinesh Gudapati**

- LinkedIn: [linkedin.com/in/gudapati-dinesh](https://linkedin.com/in/gudapati-dinesh)
- GitHub: [github.com/Dinesh-1522](https://github.com/Dinesh-1522)
- Email: dineshgudapati18@gmail.com
