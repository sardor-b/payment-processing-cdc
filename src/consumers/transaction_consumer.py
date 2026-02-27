# pyspark==3.5.4
import os
import pyspark.sql.functions as F

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DecimalType

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_TOPIC = os.getenv('TRANSACTIONS_TOPIC')

CH_USER = os.getenv('CH_USER')
CH_PASSWORD = os.getenv('CH_PASSWORD')
CH_DB = os.getenv('CH_DB')
CH_HOST = os.getenv('CH_HOST')

CLICKHOUSE_URL = f'jdbc:clickhouse://{CH_HOST}/{CH_DB}'
CLICKHOUSE_PROPS = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "user": CH_USER,
    "password": CH_PASSWORD,
    "isolationLevel": "NONE"
}

def ch_write(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    print(f"Writing batch {batch_id} - {batch_df.count()} rows")

    (
        batch_df
        .write
        .format('jdbc')
        .option('url', CLICKHOUSE_URL)
        .option('dbtable', 'transactions')
        .options(**CLICKHOUSE_PROPS)
        .mode('append')
        .save()
    )

def main(spark: SparkSession):
    msg_schema = StructType([
        StructField('id', StringType(), False),
        StructField('originator', StringType(), False),
        StructField('beneficiary', StringType(), False),
        StructField('amount', DecimalType(19, 4), False),
        StructField('status', StringType(), False),
        StructField('transaction_dt', TimestampType(), False)
    ])

    raw_stream = (
        spark
        .readStream
        .format('kafka')
        .option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP_SERVERS)
        .option('subscribe', KAFKA_TOPIC)
        .option('startingOffsets', 'latest')
        .option('failOnDataLoss', 'true')
        .load()
    )

    parsed_stream = (
        raw_stream
        .selectExpr('CAST(value AS STRING) as raw')
        .withColumn('after', F.get_json_object(F.col('raw'), '$.payload.after'))
        .withColumn('data', F.from_json(F.col('after'), msg_schema))
        .select('data.*')
    )

    query = (
        parsed_stream
        .writeStream
        .foreachBatch(ch_write)
        .option('checkpointLocation', '/tmp/spark_checkpoint/transactions')
        .trigger(processingTime='10 seconds')
        .start()
    )

    query.awaitTermination()


if __name__ == '__main__':
    spark_session = (
        SparkSession
        .builder
        .appName('TransactionConsumer')
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,"
            "com.clickhouse:clickhouse-jdbc:0.8.1"
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .getOrCreate()
    )
    spark_session.sparkContext.setLogLevel("ERROR")
    main(spark_session)
