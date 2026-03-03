import os
import uuid
import json
import aiorun
import asyncio
import logging
import clickhouse_connect

from datetime import datetime, timezone
from dotenv import load_dotenv
from decimal import Decimal, getcontext
from kstreams import create_engine, Stream

stream_engine = create_engine(
    title='transaction_consumer'
)

getcontext().prec = 4
load_dotenv(dotenv_path='../../config/.env')


CH_HOST = os.getenv('CH_HOST')
CH_USER = os.getenv('CH_USER')
CH_DB = os.getenv('CH_DB')
CH_PASSWORD = os.getenv('CH_PASSWORD')
BATCH_SIZE = 10_000
FLUSH_INTERVAL = 5

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/transcaction_consumer.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class TransactionBatcher:
    def __init__(
        self,
        ch_password: str,
        ch_host: str = 'localhost',
        ch_database: str = 'warehouse',
        ch_user: str = 'default',
        batch_size: int = BATCH_SIZE
    ):
        self.ch_host = ch_host
        self.ch_password = ch_password
        self.ch_user = ch_user
        self.ch_database = ch_database
        self.batch_size = batch_size
        self.batch = []
        self.last_flush = datetime.now()
        self.client = None


    async def connect(self):
        self.client = await clickhouse_connect.get_async_client(
            host=self.ch_host,
            username=self.ch_user,
            password=self.ch_password,
            database=self.ch_database
        )
        logger.info(f"Clickhouse client connected to {self.ch_database}")

    async def disconnect(self):
        await self.flush()
        if self.client():
            await self.client.close()
            logger.info(f"Clickhouse client disconnected from {self.ch_database}")


    async def __aenter__(self):
        await self.connect()
        return self


    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()


    async def add(self, data: dict) -> None:
        """Add message to batch"""
        self.batch.append(data)

        if len(self.batch) >= self.batch_size:
            await self.flush()
        elif (datetime.now() - self.last_flush).total_seconds() > FLUSH_INTERVAL:
            await self.flush()


    async def flush(self) -> None:
        if not self.batch:
            return

        try:
            rows = [
                (
                    data['id'],
                    data['originator'],
                    data['beneficiary'],
                    data['amount'],
                    data['status'],
                    data['transaction_dt']
                )
                for data in self.batch
            ]

            await self.client.insert(
                'warehouse.transactions',
                rows,
                column_names=[
                    'id',
                    'originator',
                    'beneficiary',
                    'amount',
                    'status',
                    'transaction_dt'
                ]
            )

            logger.info(f"Inserted {len(self.batch)} rows")
            self.batch = []
            self.last_flush = datetime.now()

        except Exception as e:
            logger.warning(f"Error in inserting batch {e}")


@stream_engine.stream('postgres.main.transactions')
async def main(stream: Stream):
    async for cr in stream:
        val = json.loads(cr.value.decode('utf-8'))
        data = {
            "id": val['payload']['after']['id'],
            "originator": val['payload']['after']['originator'],
            "beneficiary": val['payload']['after']['beneficiary'],
            "amount": Decimal(val['payload']['after']['amount']),
            "status": val['payload']['after']['status'],
            "transaction_dt": datetime.strptime(
                val['payload']['after']['transaction_dt'],
                '%Y-%m-%dT%H:%M:%S.%fZ'
            ).replace(tzinfo=timezone.utc)
        }

        await batcher.add(data)


async def periodic_flush():
    while True:
        await asyncio.sleep(FLUSH_INTERVAL)
        await batcher.flush()


async def start():
    global batcher

    batcher = TransactionBatcher(
        ch_host=CH_HOST,
        ch_user=CH_USER,
        ch_password=CH_PASSWORD
    )

    await batcher.connect()
    await stream_engine.start()


async def shutdown():
    global batcher

    await stream_engine.stop()

    if batcher:
        await batcher.disconnect()


async def run_app():
    try:
        await asyncio.gather(
            start(),
            periodic_flush()
        )
    except Exception as e:
        logger.error(f'Application error: {e}', exc_info=True)
    finally:
        await shutdown()


if __name__ == '__main__':
    aiorun.run(run_app(), stop_on_unhandled_errors=True)

