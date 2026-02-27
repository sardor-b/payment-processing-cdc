create table if not exists stg.q_transactions (
    raw_message String
)
engine = Kafka
settings
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'postgres.main.transactions',
    kafka_group_name = 'transactions_group',
    kafka_format = 'JSONAsString'
;