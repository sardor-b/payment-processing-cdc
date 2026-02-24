create table if not exists warehouse.transactions (
    id FixedString(36), -- used this instead of UUID because spark jdbc couldn't work with UUID
    originator FixedString(36),
    beneficiary FixedString(36),
    amount decimal(19, 4),
    status Enum('SUCCESSFUL', 'FAILED'),
    transaction_dt DateTime64(6, 'UTC')
)
engine = ReplacingMergeTree -- needed for deduplication
order by (transaction_dt, id)
partition by toYYYYMM(transaction_dt)
primary key (transaction_dt, id);