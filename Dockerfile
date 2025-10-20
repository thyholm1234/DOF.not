FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Tidszonepakke til korrekt DK-tid
RUN apt-get update && apt-get install -y --no-install-recommends tzdata \
  && rm -rf /var/lib/apt/lists/*

# Installér Python-krav
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App-kode
COPY . .

# Entrypoint
RUN chmod +x /app/entrypoint.sh