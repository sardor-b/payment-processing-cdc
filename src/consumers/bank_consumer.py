# pyspark==4.1.1
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DecimalType

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'postgres.main.banks'

CLICKHOUSE_URL = 'jdbc:clickhouse://localhost:8123/warehouse'
CLICKHOUSE_PROPS = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "user": "ch_user",
    "password": "ch_password",
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
        .option('dbtable', 'banks')
        .options(**CLICKHOUSE_PROPS)
        .mode('append')
        .save()
    )

def main(spark: SparkSession):
    msg_schema = StructType([
        StructField('mfo', StringType(), False),
        StructField('name', StringType(), False),
        StructField('branch', StringType(), False)
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
        .option('checkpointLocation', '/tmp/spark_checkpoint/banks')
        .trigger(processingTime='10 seconds')
        .start()
    )

    # query = (
    #     parsed_stream
    #     .writeStream
    #     .format('console')
    #     .option('truncate', 'false')
    #     .outputMode('append')
    #     .start()
    # )

    query.awaitTermination()


if __name__ == '__main__':
    spark_session = (
        SparkSession
        .builder
        .appName('BankConsumer')
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,"
            "com.clickhouse:clickhouse-jdbc:0.9.6"
        )
        .getOrCreate()
    )
    spark_session.sparkContext.setLogLevel("INFO")
    main(spark_session)