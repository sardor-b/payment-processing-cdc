curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "postgres-cdc-connector-1",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "postgres",
      "database.port": "5432",
      "database.user": "oltp_user",
      "database.password": "monrise-hoover-malignant-joker",
      "database.dbname": "transactions_db",
      "topic.prefix": "postgres",
      "table.include.list": "main.transactions,main.users,main.bank_accounts",
      "plugin.name": "pgoutput",
      "slot.name": "debezium_slot",
      "publication.name": "debezium_publication",
      "decimal.handling.mode": "double",
      "snapshot.mode": "initial"
    }
  }'