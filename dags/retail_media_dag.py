from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import os
import pandas as pd

default_args = {
    "owner":            "dinesh",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

dag = DAG(
    dag_id="retail_media_attribution_pipeline",
    default_args=default_args,
    description="End-to-end retail media attribution pipeline",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["retail", "media", "attribution", "ml"],
)

PROJECT = "/Users/dini/retail-media-attribution"

def validate_data(**context):
    files = {
        "data/raw/customers.csv":    1000,
        "data/raw/products.csv":      100,
        "data/raw/campaigns.csv":       5,
        "data/raw/transactions.csv": 10000,
    }
    errors = []
    for rel_path, min_rows in files.items():
        full_path = os.path.join(PROJECT, rel_path)
        if not os.path.exists(full_path):
            errors.append(f"MISSING: {rel_path}")
            continue
        df = pd.read_csv(full_path, nrows=min_rows + 1)
        if len(df) < min_rows:
            errors.append(f"TOO FEW ROWS: {rel_path}")
    if errors:
        raise ValueError("Validation failed:\n" + "\n".join(errors))
    print("All data files validated successfully")

def notify_success(**context):
    out_dir = os.path.join(PROJECT, "data/outputs")
    outputs = [
        "attribution_results.csv", "model_results.csv",
        "feature_importance.csv",  "feature_importance.png",
        "ab_test_results.csv",     "lift_by_channel.csv",
        "ab_test_chart.png",
    ]
    print(f"\nPipeline completed: {context['ds']}")
    for f in outputs:
        path   = os.path.join(out_dir, f)
        status = "OK" if os.path.exists(path) else "MISSING"
        print(f"  [{status}] {f}")

task_validate    = PythonOperator(task_id="validate_data",     python_callable=validate_data,    dag=dag)
task_load        = BashOperator(task_id="load_to_postgres",    bash_command=f"cd {PROJECT} && python src/load_to_postgres.py",  dag=dag)
task_attribution = BashOperator(task_id="attribution_model",   bash_command=f"cd {PROJECT} && python src/attribution_model.py", dag=dag)
task_conversion  = BashOperator(task_id="conversion_model",    bash_command=f"cd {PROJECT} && python src/conversion_model.py",  dag=dag)
task_abtest      = BashOperator(task_id="ab_test",             bash_command=f"cd {PROJECT} && python src/ab_test.py",           dag=dag)
task_notify      = PythonOperator(task_id="notify_success",    python_callable=notify_success,   dag=dag)

task_validate >> task_load >> [task_attribution, task_conversion, task_abtest] >> task_notify
