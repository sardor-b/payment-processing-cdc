create table if not exists warehouse.users
(
    id FixedString(36),
    user_name String,
    user_surname String,
    status Enum('ACTIVE', 'BLOCKED', 'TERMINATED'),
    registration_dt DateTime64(6, 'UTC')
)
engine = ReplacingMergeTree
order by (registration_dt, id)
partition by toYYYYMM(registration_dt)
primary key (registration_dt, id);