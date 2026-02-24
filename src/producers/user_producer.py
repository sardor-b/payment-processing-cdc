import os
import faker
import random
import asyncio

from dotenv import load_dotenv
from src.connectors import AsyncPostgresConnector
from src.producers import account_producer


async def generate_users(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    n: int = random.randint(1, 100)
):
    db = AsyncPostgresConnector(
        host=host,
        port=port,
        db=database,
        user=user,
        password=password
    )

    generate = faker.Faker(use_weighting=True)
    params = []

    for _ in range(1, n):
        user_name, user_surname = (generate.name().split())[:2]
        params.append({
            'user_name': user_name,
            'user_surname': user_surname,
            'status': 'ACTIVE'
        })

    user_ids = [row['id'] for row in await db.fetchmany(
        query="""
              INSERT INTO main.users (user_name, user_surname, status) 
              VALUES (%(user_name)s, %(user_surname)s, %(status)s)
              RETURNING id;
        """,
        params_seq=params
    )]

    await account_producer.generate_accounts(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
        user_ids=user_ids
    )


if __name__ == '__main__':
    load_dotenv(dotenv_path='/Users/sardorb/Documents/pprocessing_cdc/config/.env')

    HOST = 'localhost'
    PORT = 5432
    USER = os.getenv('PSQL_USER')
    PASSWORD = os.getenv('PSQL_PASSWORD')
    DATABASE = os.getenv('PSQL_TRANSACTIONS_DB')

    asyncio.run(generate_users(
        host = HOST,
        port = PORT,
        database = DATABASE,
        user = USER,
        password = PASSWORD,
        n = 10
    ))