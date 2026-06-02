#!/usr/bin/env bash
# run_stress_test.sh — run iceberg-cache stress tests locally
#
# Usage:
#   ./run_stress_test.sh                   # full suite (starts MinIO automatically)
#   ./run_stress_test.sh --skip-docker     # standalone scenarios only, no Docker
#   ./run_stress_test.sh --no-docker-mgmt  # MinIO must already be running
#   ./run_stress_test.sh --only NAME       # one specific scenario
#   ./run_stress_test.sh --list            # print scenario names and exit
#   ./run_stress_test.sh --keep-docker     # leave MinIO running after the suite

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker/docker-compose.yml"

# ── Defaults ──────────────────────────────────────────────────────────────────
SKIP_DOCKER=0
MANAGE_DOCKER=1     # start/stop Docker services automatically
KEEP_DOCKER=0       # leave services running after the suite
RUNNER_ARGS=()

# ── Parse args ────────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-docker)     SKIP_DOCKER=1;     RUNNER_ARGS+=("--skip-docker"); shift;;
    --no-docker-mgmt)  MANAGE_DOCKER=0;   shift;;
    --keep-docker)     KEEP_DOCKER=1;     shift;;
    --only)            RUNNER_ARGS+=("--only" "$2"); shift 2;;
    --list)            RUNNER_ARGS+=("--list"); shift;;
    --no-color)        RUNNER_ARGS+=("--no-color"); shift;;
    --verbose)         RUNNER_ARGS+=("--verbose"); shift;;
    --cache-mb)        export STRESS_CACHE_MB="$2"; shift 2;;
    *) echo "Unknown option: $1" >&2; exit 1;;
  esac
done

# ── Helpers ───────────────────────────────────────────────────────────────────
_green()  { printf '\033[32m%s\033[0m\n' "$*"; }
_yellow() { printf '\033[33m%s\033[0m\n' "$*"; }
_red()    { printf '\033[31m%s\033[0m\n' "$*"; }
_bold()   { printf '\033[1m%s\033[0m\n' "$*"; }

_require_cmd() {
  if ! command -v "$1" &>/dev/null; then
    _red "ERROR: '$1' not found. Please install it first."
    exit 1
  fi
}

_minio_ready() {
  curl -sf http://localhost:9000/minio/health/live &>/dev/null
}

# ── Pre-flight checks ─────────────────────────────────────────────────────────
_require_cmd python3

# Verify we can import from src/
if ! PYTHONPATH="$SCRIPT_DIR/src" python3 -c "import core.lru_cache" 2>/dev/null; then
  _red "ERROR: cannot import from src/. Run 'uv sync' or 'pip install -e .' first."
  exit 1
fi

# ── Docker lifecycle ──────────────────────────────────────────────────────────
DOCKER_STARTED=0

if [[ $SKIP_DOCKER -eq 0 && $MANAGE_DOCKER -eq 1 ]]; then
  if ! command -v docker &>/dev/null; then
    _yellow "WARNING: docker not found — skipping integration scenarios."
    RUNNER_ARGS+=("--skip-docker")
    SKIP_DOCKER=1
  elif ! docker info &>/dev/null 2>&1; then
    _yellow "WARNING: Docker daemon not running — skipping integration scenarios."
    RUNNER_ARGS+=("--skip-docker")
    SKIP_DOCKER=1
  else
    if _minio_ready; then
      _yellow "  MinIO already running on :9000 — skipping docker compose up."
    else
      _bold "Starting MinIO via docker compose ..."
      docker compose -f "$COMPOSE_FILE" up -d --wait 2>&1 | sed 's/^/  /'
      DOCKER_STARTED=1

      # Wait for health (up to 30 s)
      WAIT=0
      until _minio_ready || [[ $WAIT -ge 30 ]]; do
        sleep 1; WAIT=$((WAIT + 1))
      done
      if ! _minio_ready; then
        _red "ERROR: MinIO did not become healthy within 30 s."
        docker compose -f "$COMPOSE_FILE" logs minio | tail -20
        docker compose -f "$COMPOSE_FILE" down
        exit 1
      fi
      _green "  MinIO ready."
    fi
  fi
fi

# ── Cleanup trap ──────────────────────────────────────────────────────────────
_cleanup() {
  if [[ $DOCKER_STARTED -eq 1 && $KEEP_DOCKER -eq 0 ]]; then
    echo ""
    _bold "Stopping MinIO ..."
    docker compose -f "$COMPOSE_FILE" down 2>&1 | sed 's/^/  /'
  fi
}
trap _cleanup EXIT

# ── Run the stress suite ──────────────────────────────────────────────────────
echo ""
_bold "Running stress tests ..."
echo ""

PYTHONPATH="$SCRIPT_DIR/src:$SCRIPT_DIR" \
  python3 -m stress_tests.runner "${RUNNER_ARGS[@]+"${RUNNER_ARGS[@]}"}"

EXIT_CODE=$?

if [[ $EXIT_CODE -eq 0 ]]; then
  _green "All scenarios passed."
else
  _red "One or more scenarios failed."
fi

exit $EXIT_CODE
