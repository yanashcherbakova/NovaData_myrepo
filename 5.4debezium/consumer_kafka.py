from kafka import KafkaConsumer
import json
import requests
from datetime import datetime

CLICKHOUSE_URL = "http://localhost:8123"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "clickhousepass"

def create_clickhouse_table():
    query = """
    CREATE TABLE IF NOT EXISTS changes (
        id UInt32,
        name String,
        created_at DateTime,
        op String,
        ts DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    ORDER BY (id, ts);
    """
    r = requests.post(
        CLICKHOUSE_URL,
        data=query,
        auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD)
    )

def insert_to_clickhouse(batch):
    values = []
    for row in batch:
        id = row.get("id", 0)
        name = row.get("name", "")
        created_at = row.get("created_at", datetime.now().isoformat())
        op = row.get("op", "u")
        values.append(f"({id}, '{name}', toDateTime('{created_at}'), '{op}')")

    if values:
        query = (
            "INSERT INTO changes (id, name, created_at, op) VALUES " +
            ", ".join(values)
        )
        r = requests.post(
            CLICKHOUSE_URL,
            data=query,
            auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD)
        )

def select_changes():
    query = "SELECT * FROM changes ORDER BY ts DESC LIMIT 10"
    r = requests.post(
        CLICKHOUSE_URL,
        data=query,
        auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD)
    )
    if r.status_code == 200:
        print("📥 Last events from Clickhouse:")
        print(r.text)
    else:
        print(f"❌ Error when SELECT: {r.text}")


def consume_kafka_once():
    consumer = KafkaConsumer(
        'pgserver1.public.my_table',
        bootstrap_servers='localhost:29092',
        auto_offset_reset='latest',
        enable_auto_commit=False,
        group_id='pg-consumer-once',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=20000
    )

    print("🟢 Listeing to updates in Kafka...\n")

    buffer = []

    for message in consumer:
        payload = message.value
        op = payload.get("op")

        if op == "u":
            after = payload.get("after")
            after["op"] = "u"
            print(f"🔁 Update: {after}")
            buffer.append(after)
        else:
            print(f"ℹ️ Missed (op = {op}):", payload)

    consumer.close()

    if buffer:
        insert_to_clickhouse(buffer)
    else:
        print("No events for logging in ClickHouse")


if __name__ == '__main__':
    create_clickhouse_table()
    consume_kafka_once()
    select_changes()