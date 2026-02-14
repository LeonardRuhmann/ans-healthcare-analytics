#!/bin/bash
set -e

# ============================================================
# IntuitiveCare ETL + SQL Analytics — Full Pipeline
# ============================================================
# Usage:
#   ./run.sh          → Runs everything (ETL + Docker + Analytics)
#   ./run.sh etl      → Runs only the Python ETL pipeline
#   ./run.sh docker   → Runs only the Docker analytics (requires ETL output)
#   ./run.sh down     → Stops and removes the Docker container
# ============================================================

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'
BOLD='\033[1m'

log()  { echo -e "${GREEN}[✔]${NC} $1"; }
warn() { echo -e "${YELLOW}[!]${NC} $1"; }
fail() { echo -e "${RED}[✘]${NC} $1"; exit 1; }
header() { echo -e "\n${BOLD}═══════════════════════════════════════${NC}"; echo -e "${BOLD}  $1${NC}"; echo -e "${BOLD}═══════════════════════════════════════${NC}\n"; }

DOCKER_CMD="docker"
COMPOSE_CMD="docker-compose"

# Detect compose v2 plugin vs v1 standalone
if docker compose version &>/dev/null; then
    COMPOSE_CMD="docker compose"
elif ! command -v docker-compose &>/dev/null; then
    fail "Neither 'docker compose' nor 'docker-compose' found. Install Docker Compose."
fi

# Use sudo if user is not in docker group
if ! docker info &>/dev/null; then
    DOCKER_CMD="sudo docker"
    if [ "$COMPOSE_CMD" = "docker compose" ]; then
        COMPOSE_CMD="sudo docker compose"
    else
        COMPOSE_CMD="sudo docker-compose"
    fi
    warn "Docker requires sudo on this system."
fi

# ============================================================
# Phase 1: Python ETL Pipeline
# ============================================================
run_etl() {
    header "Phase 1: Python ETL Pipeline"

    if [ ! -d "venv" ]; then
        log "Creating virtual environment..."
        python3 -m venv venv
    fi

    source venv/bin/activate
    log "Virtual environment activated."

    log "Installing dependencies..."
    pip install -q -r requirements.txt

    log "Running ETL pipeline..."
    python -m src.main

    log "ETL completed. Output files in output/"
}

# ============================================================
# Phase 2: Docker — Start, Wait, Validate, Analyze
# ============================================================
wait_for_mysql() {
    local max_attempts=30
    local attempt=1

    echo -n "  Waiting for MySQL"
    while [ $attempt -le $max_attempts ]; do
        if $DOCKER_CMD exec mysql-ans mysql -uroot -proot -e "SELECT 1" &>/dev/null; then
            echo ""
            log "MySQL is ready."
            return 0
        fi
        echo -n "."
        sleep 2
        attempt=$((attempt + 1))
    done

    echo ""
    fail "MySQL did not become ready in time. Check: $DOCKER_CMD logs mysql-ans"
}

wait_for_init() {
    local max_attempts=30
    local attempt=1

    echo -n "  Waiting for data import"
    while [ $attempt -le $max_attempts ]; do
        local count=$($DOCKER_CMD exec mysql-ans mysql -uroot -proot -N -e "SELECT COUNT(*) FROM ans_test.fact_despesas_eventos" 2>/dev/null || echo "0")
        if [ "$count" -gt 0 ] 2>/dev/null; then
            echo ""
            log "Data imported: $count rows in fact_despesas_eventos."
            return 0
        fi
        echo -n "."
        sleep 2
        attempt=$((attempt + 1))
    done

    echo ""
    fail "Data import did not complete. Check: $DOCKER_CMD logs mysql-ans"
}

run_docker() {
    header "Phase 2: Docker SQL Analytics"

    # Pre-flight checks
    [ -f "output/consolidado_despesas.zip" ] || fail "output/consolidado_despesas.zip not found. Run ETL first: ./run.sh etl"
    [ -f "downloads/Relatorio_Cadop.csv" ]   || fail "downloads/Relatorio_Cadop.csv not found. Run ETL first: ./run.sh etl"

    # Extract CSV from ZIP (Docker mounts the CSV directly)
    log "Extracting consolidado_despesas.csv from ZIP..."
    # Clean up if Docker previously created a directory with this name
    [ -d "output/consolidado_despesas.csv" ] && rm -rf "output/consolidado_despesas.csv"
    unzip -o output/consolidado_despesas.zip -d output/ > /dev/null

    log "Starting container..."
    $COMPOSE_CMD up -d 2>&1 | tail -5

    wait_for_mysql
    wait_for_init

    header "Validation (sql/validate.sql)"
    $DOCKER_CMD exec -i mysql-ans mysql -uroot -proot ans_test < sql/validate.sql 2>/dev/null

    header "Analytics (sql/queries_analytics.sql)"
    $DOCKER_CMD exec -i mysql-ans mysql -uroot -proot ans_test < sql/queries_analytics.sql 2>/dev/null

    warn "Done! Container is still running."
    echo -e "  To stop: ${BOLD}./run.sh down${NC}"
}

# ============================================================
# Teardown
# ============================================================
run_down() {
    header "Stopping Docker"
    $COMPOSE_CMD down -v 2>&1
    log "Container removed."
}

# ============================================================
# Entrypoint
# ============================================================
case "${1:-all}" in
    etl)    run_etl ;;
    docker) run_docker ;;
    down)   run_down ;;
    all)    run_etl; run_docker ;;
    *)      echo "Usage: ./run.sh [etl|docker|down|all]"; exit 1 ;;
esac
