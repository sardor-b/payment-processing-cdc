create table if not exists warehouse.bank_accounts (
    id FixedString(36), -- used this instead of UUID because spark jdbc couldn't work with UUID
    bank_id FixedString(5),
    user_id FixedString(36),
    currency_code FixedString(3),
    account FixedString(20),
    balance decimal(19, 4),
    status Enum('ACTIVE', 'BLOCKED', 'TERMINATED'),
    update_dt DATETIME64(6, 'UTC'),
    registration_dt DATETIME64(6, 'UTC')
)
engine = ReplacingMergeTree -- used for UPSERT logic
order by (registration_dt, id)
partition by toYYYYMM(registration_dt)
primary key (registration_dt, id);
