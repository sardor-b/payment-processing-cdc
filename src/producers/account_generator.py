import random
from src.connectors import AsyncPostgresConnector

async def generate_accounts(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    user_ids: list,
):
    db = AsyncPostgresConnector(
        host=host,
        port=port,
        db=database,
        user=user,
        password=password
    )

    params = []

    mfos = [row["mfo"] for row in await db.fetch("SELECT mfo FROM main.banks ORDER BY RANDOM()")]
    sample_mfos = random.choices(mfos, k=len(user_ids))

    for user_id, mfo in zip(user_ids, sample_mfos):
        bank_account_seed = random.randint(100000000000, 999999999999)
        account = f'20208{str(bank_account_seed)}001'
        balance = round(random.uniform(10_000.0, 20_000_000.0),2)

        params.append(
            {
                'bank_id': mfo,
                'user_id': user_id,
                'currency_code': 'UZS',
                'account': account,
                'balance': balance,
                'status': 'ACTIVE'
            }
        )

    await db.executemany(
        query="""
            insert into main.bank_accounts (bank_id, user_id, currency_code, account, balance, status)
            values (%(bank_id)s, %(user_id)s, %(currency_code)s, %(account)s, %(balance)s, %(status)s)
        """,
        params_seq=params
    )
