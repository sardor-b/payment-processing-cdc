create table if not exists main.bank_accounts (
    id uuid not null primary key default gen_random_uuid(),
    bank_id char(5) not null references main.banks(mfo),
    user_id uuid not null references main.users(id),
    currency_code char(3) not null references main.currencies(alpha_code),
    account char(20) not null unique,
    balance numeric(19, 4) not null default 0,
    status bank_accounts_enum not null,
    update_dt timestamptz not null default now(),
    registration_dt timestamptz not null default now()
);