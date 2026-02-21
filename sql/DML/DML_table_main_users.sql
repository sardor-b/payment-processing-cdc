create table if not exists main.users (
    id uuid not null primary key default uuidv7(),
    user_name varchar not null,
    user_surname varchar not null,
    status users_enum not null,
    registration_dt timestamptz not null default now()
);
