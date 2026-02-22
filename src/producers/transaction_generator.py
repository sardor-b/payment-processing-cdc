import os
import random
import asyncio

from dotenv import load_dotenv
from src.connectors import AsyncPostgresConnector


async def generate_transactions(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    n: int = random.randint(500, 1_000)
):
    db = AsyncPostgresConnector(
        host=host,
        port=port,
        db=database,
        user=user,
        password=password
    )

    originator = await db.fetch(
        query="""
            SELECT
                id,
                balance,
                RANDOM(0::numeric, balance) AS transaction_amount
            FROM main.bank_accounts
            ORDER BY RANDOM()
            LIMIT %s;
        """,
        params=(n,)
    )

    beneficiary = await db.fetch(
        query="""
            SELECT
                id,
                balance
            FROM main.bank_accounts
            ORDER BY RANDOM()
            LIMIT %s;
        """,
        params=(n,)
    )

    originator_update_params = [
        {
            'id': o['id'],
            'transaction_amount': o['transaction_amount']
        }
        for o in originator
    ]

    beneficiary_update_params = [
        {
            'id': b['id'],
            'transaction_amount': o['transaction_amount']
        }
        for o, b in zip(originator, beneficiary)
    ]

    transaction_update_params = [
        {
            'originator': o['id'],
            'beneficiary': b['id'],
            'amount': o['transaction_amount'],
            'status': 'SUCCESSFUL'
        }
        for o, b in zip(originator, beneficiary)
    ]

    await db.executemany(
        query="""
            update main.bank_accounts
            set 
                balance = balance - %(transaction_amount)s,
                update_dt = now()
            where id = %(id)s;
        """,
        params_seq=originator_update_params
    )

    await db.executemany(
        query="""
            update main.bank_accounts
            set 
                balance = balance + %(transaction_amount)s,
                update_dt = now()
            where id = %(id)s;
        """,
        params_seq=beneficiary_update_params
    )

    await db.executemany(
        query="""
            insert into main.transactions (originator, beneficiary, amount, status)
            values (%(originator)s, %(beneficiary)s, %(amount)s, %(status)s);
        """,
        params_seq=transaction_update_params
    )

if __name__ == '__main__':
    load_dotenv(dotenv_path='/Users/sardorb/Documents/pprocessing_cdc/config/.env')

    HOST = 'localhost'
    PORT = 5432
    USER = os.getenv('PSQL_USER')
    PASSWORD = os.getenv('PSQL_PASSWORD')
    DATABASE = os.getenv('PSQL_TRANSACTIONS_DB')

    asyncio.run(generate_transactions(
        host = HOST,
        port = PORT,
        database = DATABASE,
        user = USER,
        password = PASSWORD,
        n = 60_000
    ))

