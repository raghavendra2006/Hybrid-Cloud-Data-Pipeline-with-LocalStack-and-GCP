# ============================================
# Bridge Application Dockerfile
# ============================================
# Multi-stage build for a lean production image.
# ============================================

FROM python:3.11-slim AS base

# Prevent Python from writing .pyc files and enable unbuffered output
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY bridge.py .

# Create a non-root user for security
RUN adduser --disabled-password --gecos "" --uid 1000 appuser
USER appuser

# Health check: verify the process is running
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD pgrep -f bridge.py || exit 1

# Run the bridge application
CMD ["python", "-u", "bridge.py"]
