create table if not exists stg.q_users (
    raw_message String
)
engine = Kafka
settings
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'postgres.main.users',
    kafka_group_name = 'users_group',
    kafka_format = 'JSONAsString'
;