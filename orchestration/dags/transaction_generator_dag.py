import asyncio

from airflow.sdk import DAG, Connection, task
from datetime import datetime, timedelta
from src.producers import generate_transactions

default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='generate_transactions',
    schedule='* * * * *',
    start_date=datetime(2026, 2, 22),
    catchup=False,
    default_args=default_args,
    tags=['oltp', 'transactions']
) as dag:
    @task
    def generate():
        conn = Connection.get('transactions_db')

        return asyncio.run(
            generate_transactions(
                host=conn.host,
                port=conn.port,
                database=conn.schema,
                user=conn.login,
                password=conn.password,
                n=60_000
            )
        )

    generate()
