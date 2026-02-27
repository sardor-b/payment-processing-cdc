#!/bin/bash
set -e

spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,com.clickhouse:clickhouse-jdbc:0.8.1 \
  consumers/${CONSUMER_SCRIPT}