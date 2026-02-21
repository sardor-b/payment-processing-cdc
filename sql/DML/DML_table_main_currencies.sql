create table if not exists main.currencies (
    alpha_code char(3) not null primary key,
    numeric_code char(3) not null,
    currency varchar not null
);