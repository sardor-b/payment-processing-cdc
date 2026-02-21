create table if not exists main.banks (
    mfo char(5) not null primary key,
    name varchar not null,
    branch varchar not null
);