from pydantic import BaseModel
from typing import Any


class ChatMessage(BaseModel):
    role: str  # "user" | "assistant"
    content: str


class ChatRequest(BaseModel):
    message: str
    history: list[ChatMessage] = []


class HighlightedNode(BaseModel):
    type: str
    id: str


class ChatResponse(BaseModel):
    response: str
    sql: str | None = None
    highlighted_nodes: list[HighlightedNode] = []
    error: str | None = None
