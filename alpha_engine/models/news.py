"""
alpha_engine.models.news — News & Social Signal Event Schemas
============================================================
Multi-Chain Paper Trading & Alpha Analytics Engine
Python 3.11+ | Pydantic v2
"""

from __future__ import annotations

import time
import uuid
from typing import Annotated, Optional

from pydantic import BaseModel, Field

from alpha_engine.models.base import _STRICT_MODEL_CFG
from alpha_engine.models.enums import ChainIdentifier, NewsSignalStatus


class TelegramMessage(BaseModel):
    """
    Normalized raw message captured from Telegram MTProto client.
    """

    model_config = _STRICT_MODEL_CFG

    channel_id: int
    channel_title: str
    message_id: int
    text: str
    timestamp: float
    is_edit: bool = False
    extracted_cas: list[str] = Field(default_factory=list)
    chain: Optional[ChainIdentifier] = None


class NewsSignalEvent(BaseModel):
    """
    Validated social / news alpha signal parsed and passed to the event bus.
    """

    model_config = _STRICT_MODEL_CFG

    signal_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp_ns: Annotated[int, Field(gt=0)] = Field(default_factory=lambda: time.time_ns())
    token_address: Annotated[str, Field(min_length=32, max_length=66)]
    chain: ChainIdentifier
    originating_channel: str
    channel_id: int
    message_id: int
    sybil_channel_count: Annotated[int, Field(ge=1)] = 1
    status: NewsSignalStatus = NewsSignalStatus.VALID
    raw_text: str
    is_edit_honeypot: bool = False
    rejection_reason: Optional[str] = None
