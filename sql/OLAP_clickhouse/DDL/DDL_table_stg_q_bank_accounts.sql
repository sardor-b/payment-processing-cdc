create table if not exists stg.q_bank_accounts (
    raw_message String
)
engine = Kafka
settings
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'postgres.main.bank_accounts',
    kafka_group_name = 'bank_accounts_group',
    kafka_format = 'JSONAsString'
;