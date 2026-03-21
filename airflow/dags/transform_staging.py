from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="transform_staging",
    start_date=datetime.now(),
    schedule_interval=None,
    catchup=False,
    tags=["staging"],
) as dag:

    check_raw_tables = BashOperator(
        task_id="check_raw_tables",
        bash_command="""
        set -e
        TABLES=("raw_movies" "raw_ratings")
        for t in "${TABLES[@]}"; do
            if ! docker exec de_postgres psql -U de_user -d movies -tAc "SELECT 1 FROM information_schema.tables WHERE table_name='$t'" | grep -q 1; then
                echo "Table $t does not exist!"
                exit 1
            fi
        done
        echo "All raw tables exist."
        """,
    )

    run_staging = BashOperator(
        task_id="run_dbt_staging",
        bash_command="docker exec de_dbt dbt run --models staging",
    )

    check_raw_tables >> run_staging
