import os
from dotenv import load_dotenv
from src.connectors import PostgresConnector

load_dotenv(dotenv_path='/config/.env')

HOST = 'localhost'
PORT = 5432
USER = os.getenv('PSQL_USER')
PASSWORD = os.getenv('PSQL_PASSWORD')
DATABASE = os.getenv('PSQL_TRANSACTIONS_DB')

db = PostgresConnector(
    host=HOST,
    port=PORT,
    db=DATABASE,
    user=USER,
    password=PASSWORD
)

result = db.execute('SELECT * FROM main.banks')
print(result)