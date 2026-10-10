import asyncio
import json
from collections import defaultdict
from aiokafka import AIOKafkaConsumer

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "call-events"
GROUP_ID = "aggregator-group"

# in-memory counts — in production this would go to Redis/Postgres
disposition_counts: dict = defaultdict(lambda: defaultdict(int))
call_counts: dict = defaultdict(int)


async def consume():
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
    )
    await consumer.start()
    print(f"[AGGREGATOR] Connected to Kafka | topic: {TOPIC} | group: {GROUP_ID}")
    try:
        async for msg in consumer:
            event = msg.value
            event_type = event["event_type"]
            bot_id = event["bot_id"]

            # count every call_started per bot
            if event_type == "call_started":
                call_counts[bot_id] += 1
                print(f"[AGGREGATOR] call_started | bot={bot_id} | total_calls={call_counts[bot_id]}")

            # only process disposition details on disposition_captured
            elif event_type == "disposition_captured":
                disposition = event.get("disposition", "unknown")
                disposition_counts[bot_id][disposition] += 1
                print(
                    f"[AGGREGATOR] disposition_captured | "
                    f"bot={bot_id} | "
                    f"disposition={disposition} | "
                    f"counts={dict(disposition_counts[bot_id])}"
                )

            # log call_ended
            elif event_type == "call_ended":
                duration = event.get("call_duration", "N/A")
                print(f"[AGGREGATOR] call_ended | bot={bot_id} | duration={duration}s")

    finally:
        await consumer.stop()
        print("[AGGREGATOR] Disconnected")


if __name__ == "__main__":
    asyncio.run(consume())