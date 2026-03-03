create materialized view if not exists stg.mv_users to warehouse.users as
select
    JSONExtractString(raw_message, 'payload', 'after','id') as id,
    JSONExtractString(raw_message, 'payload', 'after','user_name') as user_name,
    JSONExtractString(raw_message, 'payload', 'after','user_surname') as user_surname,
    JSONExtractString(raw_message, 'payload', 'after','status') as status,
    parseDateTime64BestEffort(
        JSONExtractString(raw_message, 'payload', 'after', 'registration_dt'),
        6,
        'UTC'
    ) as registration_dt
from stg.q_users
where
    JSONExtractString(raw_message, 'payload', 'op') != 'd'
    and JSONHas(raw_message, 'payload')
;