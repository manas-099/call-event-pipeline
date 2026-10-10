
import json
from aiokafka import AIOKafkaProducer

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "call-events"

_producer = None


async def get_producer() -> AIOKafkaProducer:
    global _producer
    if _producer is None:
        _producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
        )
        await _producer.start()
        print("[PRODUCER] Connected to Kafka broker")
    return _producer


async def publish_event(event: dict):
    producer = await get_producer()
    await producer.send_and_wait(
        TOPIC,
        key=event["call_id"],
        value=event,
    )
    print(f"[PRODUCER] Published → event_type={event['event_type']} | call_id={event['call_id']}")


async def stop_producer():
    global _producer
    if _producer:
        await _producer.stop()
        _producer = None
        print("[PRODUCER] Disconnected from Kafka broker")