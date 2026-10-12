# ==============================================================================
# Dockerfile — Bot-MM Multi-Chain Paper Trading Engine
# Multi-Stage Minimal Python Build | Non-Root User | Production Hardened
# ==============================================================================

ARG PYTHON_VERSION=3.12

# ------------------------------------------------------------------------------
# Stage 1: Build Dependencies, Wheels & Engine Package
# ------------------------------------------------------------------------------
FROM python:${PYTHON_VERSION}-slim AS builder

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    g++ \
    libffi-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Create isolated virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy dependency manifest first for optimal Docker layer caching
COPY requirements.txt ./
RUN pip install --upgrade pip setuptools wheel && \
    pip install -r requirements.txt

# Copy package definition and source code to install alpha-engine CLI
COPY pyproject.toml README.md ./
COPY alpha_engine ./alpha_engine
RUN pip install --no-deps .

# ------------------------------------------------------------------------------
# Stage 2: Minimal Distroless-like Hardened Runtime
# ------------------------------------------------------------------------------
FROM python:${PYTHON_VERSION}-slim AS runner

LABEL org.opencontainers.image.title="Bot-MM Alpha Engine" \
      org.opencontainers.image.description="Multi-Chain Paper Trading & Alpha Analytics Engine" \
      org.opencontainers.image.version="0.1.0" \
      org.opencontainers.image.authors="Principal Quantitative Systems Team" \
      org.opencontainers.image.source="https://github.com/jamalabbay97/Bot-MM" \
      org.opencontainers.image.licenses="Proprietary"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1 \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONPATH="/app"

WORKDIR /app

# Install minimal runtime system dependencies (sqlite3, ca-certificates, tini init system, curl)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    sqlite3 \
    tini \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy pre-compiled virtual environment from builder stage
COPY --from=builder /opt/venv /opt/venv

# Create dedicated non-root service account with UID/GID 10001
RUN groupadd -g 10001 botmm && \
    useradd -u 10001 -g botmm -d /app -s /usr/sbin/nologin -c "Bot-MM Engine Service Account" botmm

# Create persistent state and log directories with appropriate ownership
RUN mkdir -p /app/logs /app/data && \
    chown -R botmm:botmm /app

# Copy application source code with non-root ownership
COPY --chown=botmm:botmm . /app

# Switch to non-privileged user
USER botmm

# Healthcheck validating basic Python runtime and engine module health
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD python -c "import sys, alpha_engine; sys.exit(0)" || exit 1

# Process supervisor init system to properly handle OS signals & prevent zombies
ENTRYPOINT ["/usr/bin/tini", "--"]

# Default execution entrypoint (24/7 Self-Healing Supervisor)
CMD ["python", "-m", "alpha_engine.engine.runner"]
