from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2

POSTGRES_CONN = {
    "host": "postgres",
    "database": "movies",
    "user": "de_user",
    "password": "de_pass",
    "port": 5432,
}

def cleanup_tables():
    conn = psycopg2.connect(**POSTGRES_CONN)
    cur = conn.cursor()

    tables = [
        ("rw", "raw_movies"),
        ("rw", "raw_ratings"),
        ("stg", "stg_movies"),
        ("stg", "stg_ratings"),
        ("mrt", "top_movies"),
        ("mrt", "average_ratings"),
        ("mrt", "popular_genres"),
    ]

    for schema, table in tables:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_name = %s
            );
            """,
            (schema, table),
        )

        exists = cur.fetchone()[0]

        if exists:
            cur.execute(f"TRUNCATE TABLE {schema}.{table} RESTART IDENTITY CASCADE;")
            print(f"Table {schema}.{table} cleared.")
        else:
            print(f"Table {schema}.{table} does not exist, skipping.")

    conn.commit()
    cur.close()
    conn.close()


with DAG(
    dag_id="cleanup_tables",
    start_date=datetime.now(),
    schedule_interval=None,
    catchup=False,
    tags=["maintenance"],
) as dag:

    clear_tables = PythonOperator(
        task_id="clear_all_tables",
        python_callable=cleanup_tables,
    )
