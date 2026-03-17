# ── Build stage ──────────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential git \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml ./
COPY src/ ./src/

RUN pip install --upgrade pip && \
    pip install --no-cache-dir ".[dev]" && \
    pip install --no-cache-dir boto3

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

WORKDIR /app

# Runtime system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy app source
COPY src/ ./src/
COPY config/ ./config/

# Create necessary directories (S3 modunda volume mount gerekmez)
RUN mkdir -p /app/data /app/archive /app/logs

# Non-root user
RUN useradd -m -u 1000 tessera && chown -R tessera:tessera /app
USER tessera

ENV PYTHONPATH=/app/src
ENV TESSERA_CONFIG=/app/config/default.yaml
ENV PYTHONUNBUFFERED=1

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8000/ || exit 1

CMD ["python", "-c", "\
import sys; sys.path.insert(0, 'src'); \
import uvicorn; \
from tessera.web.app import create_app; \
app = create_app(); \
uvicorn.run(app, host='0.0.0.0', port=8000, proxy_headers=True, forwarded_allow_ips='*') \
"]
