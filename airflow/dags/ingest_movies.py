from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="ingest_movies",
    start_date=datetime.now(),
    schedule_interval=None,
    catchup=False,
) as dag:

    load_movies = BashOperator(
        task_id="load_movies",
        bash_command="docker exec de_ingestion python load_movies.py",
    )
