create materialized view if not exists stg.mv_bank_accounts to warehouse.bank_accounts as
select
    JSONExtractString(raw_message, 'payload', 'after','id') as id,
    JSONExtractString(raw_message, 'payload', 'after','bank_id') as bank_id,
    JSONExtractString(raw_message, 'payload', 'after','user_id') as user_id,
    JSONExtractString(raw_message, 'payload', 'after','currency_code') as currency_code,
    JSONExtractString(raw_message, 'payload', 'after','account') as account,
    JSONExtractString(raw_message, 'payload', 'after','balance') as balance,
    JSONExtractString(raw_message, 'payload', 'after','status') as status,
    parseDateTime64BestEffort(
        JSONExtractString(raw_message, 'payload', 'after', 'update_dt'),
        6,
        'UTC'
    ) as update_dt,
    parseDateTime64BestEffort(
        JSONExtractString(raw_message, 'payload', 'after', 'registration_dt'),
        6,
        'UTC'
    ) as registration_dt
from stg.q_bank_accounts
where
    JSONExtractString(raw_message, 'payload', 'op') != 'd'
    and JSONHas(raw_message, 'payload')
;