create table if not exists main.transactions (
    id uuid not null primary key default uuidv7(),
    originator uuid not null references main.bank_accounts(id),
    beneficiary uuid not null references main.bank_accounts(id),
    amount numeric(19, 4) not null check ( amount >= 0 ),
    status transactions_enum not null,
    transaction_dt timestamptz not null default now()
);
