create table if not exists warehouse.banks (
    mfo FixedString(5),
    name String,
    branch String
)
engine = ReplacingMergeTree
order by (mfo, name)
primary key (mfo, name);