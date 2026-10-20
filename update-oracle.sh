#!/usr/bin/env bash
set -Eeuo pipefail

SERVER="ubuntu@51.170.132.142"
SSH_KEY="$HOME/Downloads/ssh-key-2026-10-06 (1).key"
LOCAL_DIR="$HOME/Desktop/Alpha Bot"
ARCHIVE="/tmp/alpha-bot-deploy.tar.gz"
REMOTE_DIR="/home/ubuntu/alpha-bot"
REMOTE_ARCHIVE="/home/ubuntu/alpha-bot-deploy.tar.gz"

log() {
    printf '\n[%s] %s\n' "$(date '+%F %T')" "$*"
}

fail() {
    log "ERROR: $*"
    exit 1
}

command -v ssh >/dev/null || fail "ssh is not installed"
command -v scp >/dev/null || fail "scp is not installed"
command -v tar >/dev/null || fail "tar is not installed"

[[ -f "$SSH_KEY" ]] || fail "SSH key not found: $SSH_KEY"
[[ -d "$LOCAL_DIR" ]] || fail "Project directory not found"
[[ -f "$LOCAL_DIR/Dockerfile" ]] || fail "Dockerfile not found"
[[ -f "$LOCAL_DIR/docker-compose.yml" ]] || fail "docker-compose.yml not found"

log "Creating a fresh deployment archive..."

tar -C "$LOCAL_DIR" \
    --exclude='./.git' \
    --exclude='./.venv' \
    --exclude='./venv' \
    --exclude='./.env' \
    --exclude='./*.env' \
    --exclude='./*.session*' \
    --exclude='./data' \
    --exclude='./logs' \
    --exclude='./backup' \
    --exclude='./*.db' \
    --exclude='./*.db-wal' \
    --exclude='./*.db-shm' \
    --exclude='./*.sqlite*' \
    --exclude='./*.jsonl' \
    --exclude='./__pycache__' \
    --exclude='./.pytest_cache' \
    --exclude='./.mypy_cache' \
    --exclude='./.ruff_cache' \
    --exclude='./*.egg-info' \
    -czf "$ARCHIVE" .

log "Checking SSH connection..."
ssh -i "$SSH_KEY" \
    -o BatchMode=yes \
    -o ConnectTimeout=15 \
    "$SERVER" \
    'test -f /home/ubuntu/alpha-bot/.env &&
     test -f /home/ubuntu/alpha-bot/docker-compose.yml &&
     sudo docker compose -f /home/ubuntu/alpha-bot/docker-compose.yml ps -a'

log "Uploading source archive..."
scp -i "$SSH_KEY" "$ARCHIVE" "$SERVER:$REMOTE_ARCHIVE"

log "Validating archive before extraction..."
ssh -i "$SSH_KEY" "$SERVER" \
    "tar -tzf '$REMOTE_ARCHIVE' >/dev/null"

log "Extracting updated source files..."
ssh -i "$SSH_KEY" "$SERVER" \
    "tar -xzf '$REMOTE_ARCHIVE' -C '$REMOTE_DIR' &&
     rm -f '$REMOTE_ARCHIVE'"

log "Validating Compose configuration..."
ssh -i "$SSH_KEY" "$SERVER" \
    "cd '$REMOTE_DIR' &&
     sudo docker compose config --quiet"

log "Building updated image..."
ssh -i "$SSH_KEY" "$SERVER" \
    "cd '$REMOTE_DIR' &&
     sudo docker compose build bot-mm"

log "Checking application import in the new image..."
ssh -i "$SSH_KEY" "$SERVER" \
    "cd '$REMOTE_DIR' &&
     sudo docker compose run --rm --no-deps bot-mm python -c 'import alpha_engine; print(\"Import OK\")'"

log "Replacing the existing service with the new version..."
ssh -i "$SSH_KEY" "$SERVER" \
    "cd '$REMOTE_DIR' &&
     sudo docker compose up -d --no-deps --force-recreate bot-mm"

log "Waiting for the service to become healthy..."
healthy=0

for attempt in $(seq 1 30); do
    status=$(ssh -i "$SSH_KEY" "$SERVER" \
        "sudo docker inspect --format '{{.State.Health.Status}}' bot-mm-engine 2>/dev/null || true") || true

    if [[ "$status" == "healthy" ]]; then
        healthy=1
        break
    fi

    if [[ "$status" == "unhealthy" || "$status" == "exited" ]]; then
        break
    fi

    sleep 5
done

log "Final service status:"
ssh -i "$SSH_KEY" "$SERVER" \
    "cd '$REMOTE_DIR' &&
     sudo docker compose ps -a &&
     sudo docker compose logs --tail=50 bot-mm"

if [[ "$healthy" -ne 1 ]]; then
    fail "The service did not become healthy. Inspect the logs before attempting another update."
fi

log "Update completed successfully."
