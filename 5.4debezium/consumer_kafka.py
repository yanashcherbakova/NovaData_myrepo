from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'pgserver1.public.my_table',  #topic name
    bootstrap_servers='localhost:29092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='pg-consumer-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("🟢 Listening to changes in Kafka...\n")

for message in consumer:
    payload = message.value

    if payload.get("op") == "c":
        after = payload.get("after")
        print(f"🆕 Insert: id={after['id']}, name={after['name']}, created_at={after['created_at']}")
    elif payload.get("op") == "u":
        print("🔁 Update:", payload)
    elif payload.get("op") == "d":
        print("❌ Deletion:", payload)
    else:
        print("ℹ️ Other event:", payload)