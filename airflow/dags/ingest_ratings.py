from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="ingest_ratings",
    start_date=datetime.now(),
    schedule_interval=None,
    catchup=False,
) as dag:

    load_ratings = BashOperator(
        task_id="load_ratings",
        bash_command="docker exec de_ingestion python load_ratings.py",
    )
