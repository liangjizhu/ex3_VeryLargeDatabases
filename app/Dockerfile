FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos código (también montamos volumen en compose para dev)
COPY . .

ENV PYTHONUNBUFFERED=1
