from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id="full_data_transform",
    start_date=datetime.now(),
    schedule_interval=None,
    catchup=False,
    tags=["orchestrator"],
) as dag:

    # 1. Очистка таблиц
    trigger_cleanup = TriggerDagRunOperator(
        task_id="trigger_cleanup_tables",
        trigger_dag_id="cleanup_tables",
        wait_for_completion=True,
        poke_interval = 5,
    )

    # 2. Загрузка фильмов
    trigger_load_movies = TriggerDagRunOperator(
        task_id="trigger_load_movies",
        trigger_dag_id="ingest_movies",
        wait_for_completion=True,
        poke_interval = 5,
    )

    # 3. Загрузка рейтингов
    trigger_load_ratings = TriggerDagRunOperator(
        task_id="trigger_load_ratings",
        trigger_dag_id="ingest_ratings",
        wait_for_completion=True,
        poke_interval = 5,
    )

    # 4. Стаджинг
    trigger_staging = TriggerDagRunOperator(
        task_id="trigger_transform_staging",
        trigger_dag_id="transform_staging",
        wait_for_completion=True,
        poke_interval = 5,
    )

    # 5. Построение marts
    trigger_marts = TriggerDagRunOperator(
        task_id="trigger_build_marts",
        trigger_dag_id="build_marts",
        wait_for_completion=True,
        poke_interval = 5,
    )

    # Последовательность выполнения
    trigger_cleanup >> trigger_load_movies >> trigger_load_ratings >> trigger_staging >> trigger_marts