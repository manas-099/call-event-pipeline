from contextlib import asynccontextmanager
from fastapi import FastAPI
from schemas.call_event import CallEvent
from producer.kafka_producer import publish_event, stop_producer


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("[APP] Producer started")
    yield
    await stop_producer()
    print("[APP] Producer stopped")


app = FastAPI(title="Call Event Producer", lifespan=lifespan)


@app.post("/event")
async def receive_event(event: CallEvent):
    await publish_event(event.model_dump())
    return {
        "status": "published",
        "call_id": event.call_id,
        "event_type": event.event_type
    }


@app.get("/health")
async def health():
    return {"status": "ok"}