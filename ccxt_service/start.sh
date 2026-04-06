#!/bin/bash
if [ ! -d ".venv" ]; then
    uv venv
fi
uv add uvicorn fastapi ccxt
source .venv/bin/activate
uvicorn backend.backend:app --reload --host 127.0.0.1 --port 3737
