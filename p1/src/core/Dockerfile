FROM python:3.11-slim

WORKDIR /app

# System deps (SQLite comes bundled with python:3.11-slim)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY common /app/common
COPY core /app/core

# SQLite DB directory (will be a mounted volume)
RUN mkdir -p /data

ENV PYTHONUNBUFFERED=1
ENV DB_PATH=/data/wm_central.db

WORKDIR /app/core

# Default: use env vars
CMD ["sh", "-c", "python central.py ${WM_SOCKET_PORT:-5000} ${KAFKA_BROKER:-kafka:9092}"]
