# ============================================
# Bridge Application Dockerfile
# ============================================
# Production-ready Python container with
# security hardening and minimal attack surface.
# ============================================

FROM python:3.11-slim

# Prevent Python from writing .pyc files and enable unbuffered output
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install curl for healthcheck and procps for pgrep
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl procps && \
    rm -rf /var/lib/apt/lists/*

# Install dependencies first (layer caching optimization)
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY bridge.py .

# Create a non-root user for security
RUN adduser --disabled-password --gecos "" --uid 1000 appuser
USER appuser

# Health check: verify the bridge process is running
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD pgrep -f bridge.py || exit 1

# Run the bridge application
CMD ["python", "-u", "bridge.py"]
