import requests
import os

connector_config = {
    "name": "postgres-cdc-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": os.getenv("PG_HOST", "postgres"),
        "database.port": os.getenv("PG_PORT", "5432"),
        "database.user": os.getenv("PG_USER", "cdc_user"),
        "database.password": os.getenv("PG_PASSWORD", "cdc_pass"),
        "database.dbname": os.getenv("PG_DB", "sourcedb"),
        "database.server.name": "dbserver1",
        "table.include.list": "public.customers,public.drivers",
        "plugin.name": "pgoutput",
        "slot.name": "debezium_slot",
        "publication.name": "db_publication",
        "snapshot.mode": "initial",
        "topic.prefix": "dbserver1",
    }
}

resp = requests.post("http://connect:8083/connectors", json=connector_config)
print(resp.status_code, resp.text)