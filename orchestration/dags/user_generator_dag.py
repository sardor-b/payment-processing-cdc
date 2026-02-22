import asyncio

from airflow.sdk import DAG, Connection, task
from datetime import datetime, timedelta
from src.producers import generate_users

default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='generate_users',
    schedule='* * * * *',
    start_date=datetime(2026, 2, 22),
    catchup=False,
    default_args=default_args,
    tags=['oltp', 'users']
) as dag:
    @task
    def generate():
        conn = Connection.get('transactions_db')

        return asyncio.run(
            generate_users(
                host=conn.host,
                port=conn.port,
                database=conn.schema,
                user=conn.login,
                password=conn.password,
                n=1_000
            )
        )

    generate()
