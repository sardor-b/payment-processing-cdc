create table if not exists warehouse.currencies
(
    alpha_code FixedString(3),
    numeric_code FixedString(3),
    currency String
)
engine = ReplacingMergeTree
order by (alpha_code, numeric_code)
primary key (alpha_code);