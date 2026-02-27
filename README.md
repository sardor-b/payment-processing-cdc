Overall logic:
PostgreSQL (OLTP)
      ↓  (Debezium CDC)
    Kafka
      ↓  (Python consumer or Spark)
  Data Warehouse (BigQuery / Snowflake / DuckDB)
      ↓  (dbt)
  Analytical Models
      ↓  (Clickhouse)
  Dashboard
--------------------------------------
Project structure mockup:
my_project/
├── data/
│   ├── raw/              # Original, immutable source data
│   ├── interim/          # Partially processed data
│   ├── processed/        # Final, clean datasets
│   └── external/         # Data from third-party sources
│
├── pipelines/            # ETL/ELT pipeline definitions
│   ├── ingestion/
│   ├── transformation/
│   └── loading/
│
├── dbt/                  # If using dbt for transformations
│   ├── models/
│   ├── tests/
│   └── macros/
│
├── orchestration/        # Airflow DAGs, Prefect flows, etc.
│   └── dags/
│
├── src/                  # Reusable Python source code
│   ├── __init__.py
│   ├── connectors/       # DB, API, cloud connectors
│   ├── transformers/     # Data transformation logic
│   ├── validators/       # Data quality checks
│   └── utils/            # Shared helpers
│
├── tests/
│   ├── unit/
│   └── integration/
│
├── config/
│   ├── dev.yaml
│   ├── staging.yaml
│   └── prod.yaml
│
├── infra/                # IaC (Terraform, Pulumi, etc.)
│   ├── terraform/
│   └── docker/
│
├── notebooks/            # Exploration only, not production
│
├── docs/
│
├── .env.example          # Template for secrets (never commit .env)
├── pyproject.toml        # or requirements.txt / setup.py
├── Makefile              # Common commands (run, test, lint)
└── README.md

--------------------------------------
Running services:
airflow: localhost:8080
postgesql - localhost:5432
kafka - localhost:9092 
      - localhost:8080 (UI)
grafana - localhost:3000
clickhouse - localhost:8123 (HTTP)
           - localhost:9000 (TCP)
postgresql - localhost:5432 

--------------------------------------
CDC engine: Kafka Connect + Debezium

--------------------------------------

DB TABLE ENUMs:
- public.users:
  - status:
    - ACTIVE
    - BLOCKED
    - TERMINATED
- public.bank_accounts
  - status:
    - ACTIVE
    - BLOCKED
    - TERMINATED
- public.transactions:
  - status:
    - SUCCESSFUL
    - FAILED

--------------------------------------
- CDC Streaming options:
  - Clickhouse tables (stg.q_*, stg.mv_*)
  - KStreams
  - Spark jobs (the most inefficient)
