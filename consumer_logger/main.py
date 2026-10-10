import asyncio
import json
from aiokafka import AIOKafkaConsumer

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "call-events"
GROUP_ID = "logger-group"


async def consume():
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",  # on first run read from beginning
    )
    await consumer.start()
    print(f"[LOGGER] Connected to Kafka | topic: {TOPIC} | group: {GROUP_ID}")
    try:
        async for msg in consumer:
            event = msg.value
            print(
                f"[LOGGER] "
                f"partition={msg.partition} | "
                f"offset={msg.offset} | "
                f"event_type={event['event_type']} | "
                f"call_id={event['call_id']} | "
                f"bot={event['bot_id']} | "
                f"phone={event['customer_phone']} | "
                f"timestamp={event['timestamp']}"
            )
    finally:
        await consumer.stop()
        print("[LOGGER] Disconnected")


if __name__ == "__main__":
    asyncio.run(consume())