from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="build_marts",
    start_date=datetime.now(),
    schedule_interval=None,
    catchup=False,
    tags=["marts"],
) as dag:

    check_stg_tables = BashOperator(
        task_id="check_stg_tables",
        bash_command="""
        set -e
        TABLES=("stg_movies" "stg_ratings")
        for t in "${TABLES[@]}"; do
            if ! docker exec de_postgres psql -U de_user -d movies -tAc "SELECT 1 FROM information_schema.tables WHERE table_name='$t'" | grep -q 1; then
                echo "Table $t does not exist!"
                exit 1
            fi
        done
        echo "All staging tables exist."
        """
    )

    run_marts = BashOperator(
        task_id="run_dbt_marts",
        bash_command="docker exec de_dbt dbt run --models marts",
    )

    check_stg_tables >> run_marts