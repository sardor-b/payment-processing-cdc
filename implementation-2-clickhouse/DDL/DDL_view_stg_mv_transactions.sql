create materialized view if not exists stg.mv_transactions to warehouse.transactions as
select
    JSONExtractString(raw_message, 'payload', 'after', 'id') as id,
    JSONExtractString(raw_message, 'payload', 'after', 'originator') as originator,
    JSONExtractString(raw_message, 'payload', 'after', 'beneficiary') as beneficiary,
    JSONExtractString(raw_message, 'payload', 'after', 'amount') as amount,
    JSONExtractString(raw_message, 'payload', 'after', 'status') as status,
    parseDateTime64BestEffort(
        JSONExtractString(raw_message, 'payload', 'after', 'transaction_dt'),
        6,
        'UTC'
    ) as transaction_dt
from stg.q_transactions
where
    JSONExtractString(raw_message, 'payload', 'op') != 'd'
    and JSONHas(raw_message, 'payload')
;