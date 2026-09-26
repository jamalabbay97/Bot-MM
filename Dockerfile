# ==============================================================================
# Dockerfile — Bot-MM Multi-Chain Paper Trading Engine
# Multi-Stage Minimal Python 3.11 Build | Non-Root User | Production Hardened
# ==============================================================================

# ------------------------------------------------------------------------------
# Stage 1: Build Dependencies & Wheels
# ------------------------------------------------------------------------------
FROM python:3.11-slim AS builder

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Create isolated virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY pyproject.toml README.md ./
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir .

# ------------------------------------------------------------------------------
# Stage 2: Minimal Distroless-like Runtime
# ------------------------------------------------------------------------------
FROM python:3.11-slim AS runner

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONPATH="/app"

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

# Copy pre-compiled virtual environment from builder stage
COPY --from=builder /opt/venv /opt/venv

# Create dedicated non-root service account with UID/GID 10001
RUN groupadd -g 10001 botmm && \
    useradd -u 10001 -g botmm -d /app -s /sbin/nologin -c "Bot-MM Engine Service Account" botmm

# Create persistent state and log directories with appropriate ownership
RUN mkdir -p /app/logs /app/data && \
    chown -R botmm:botmm /app

# Copy application source code
COPY --chown=botmm:botmm . /app

# Switch to non-privileged user
USER botmm

# Healthcheck validating basic Python runtime and engine module health
HEALTHCHECK --interval=60s --timeout=10s --start-period=10s --retries=3 \
    CMD python -c "import alpha_engine; sys.exit(0)" || exit 1

# Default execution entrypoint (24/7 Self-Healing Supervisor)
CMD ["python", "-m", "alpha_engine.engine.runner"]
