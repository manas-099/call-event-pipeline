from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime, timezone
import uuid


class CallEvent(BaseModel):
    call_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str  # call_started | call_ended | disposition_captured
    bot_id: str
    customer_phone: str
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    # only for disposition_captured events
    disposition: Optional[str] = None           # promise_to_pay | refused | not_reachable | callback_requested
    call_duration: Optional[int] = None         # total call seconds
    talk_time: Optional[int] = None             # seconds customer spoke
    hangup_by: Optional[str] = None             # customer | bot
    language_detected: Optional[str] = None     # hindi | hinglish | english