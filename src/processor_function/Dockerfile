FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Ensure we have the base requirements + pubsub
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install google-cloud-pubsub

COPY . .

# Run the local subscriber script
CMD ["python", "-u", "local_subscriber.py"]
