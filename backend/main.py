"""
main.py — FastAPI application entry point.
"""
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from routers import graph, chat

load_dotenv()

app = FastAPI(
    title="SAP O2C Graph API",
    description="Graph-based SAP Order-to-Cash data explorer with LLM query interface",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(graph.router)
app.include_router(chat.router)


@app.get("/")
def root():
    return {"status": "ok", "message": "SAP O2C Graph API is running"}


@app.get("/health")
def health():
    return {"status": "ok"}
