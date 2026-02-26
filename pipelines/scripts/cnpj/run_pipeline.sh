#!/bin/bash
# =============================================================================
# pipelines/scripts/cnpj/run_pipeline.sh  —  Dispara o pipeline CNPJ completo
#
# A partir de cnpj_transform, cada DAG aciona a próxima automaticamente:
#   cnpj_transform → cnpj_load_postgres → cnpj_matview_refresh → cnpj_load_neo4j
#
# Uso (a partir da raiz do projeto):
#   ./pipelines/scripts/cnpj/run_pipeline.sh [reference_month] [force_reprocess]
#   ./pipelines/scripts/cnpj/run_pipeline.sh 2026-02 true
#   ./pipelines/scripts/cnpj/run_pipeline.sh   # detecta o mês mais recente automaticamente
# =============================================================================

set -euo pipefail

AIRFLOW_CONTAINER="osint-platform-airflow-webserver-1"
RAW_DATA_DIR="/media/bigdata/osint-platform/data/cnpj/raw"
POLL_INTERVAL=30

REFERENCE_MONTH="${1:-$(ls "$RAW_DATA_DIR" | grep -E '^[0-9]{4}-[0-9]{2}$' | sort | tail -1)}"
FORCE_REPROCESS="${2:-false}"

# ---------------------------------------------------------------------------
RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

log()  { echo -e "${CYAN}[$(date '+%H:%M:%S')]${RESET} $*"; }
ok()   { echo -e "${GREEN}[$(date '+%H:%M:%S')] ✅ $*${RESET}"; }
fail() { echo -e "${RED}[$(date '+%H:%M:%S')] ❌ $*${RESET}"; exit 1; }
hr()   { echo -e "${BOLD}$(printf '─%.0s' {1..70})${RESET}"; }

fmt_duration() {
    local s=$1
    printf "%02dh%02dm%02ds" $(( s/3600 )) $(( (s%3600)/60 )) $(( s%60 ))
}

# ---------------------------------------------------------------------------

hr
echo -e "${BOLD}  🚀 CNPJ PIPELINE COMPLETO${RESET}"
echo -e "  Mês            : ${BOLD}${REFERENCE_MONTH}${RESET}"
echo -e "  Force reprocess: ${BOLD}${FORCE_REPROCESS}${RESET}"
echo -e "  Início         : $(date '+%Y-%m-%d %H:%M:%S')"
echo -e "  Cadeia         : transform → load_postgres → matview → neo4j"
hr
echo ""

# Dispara cnpj_transform
log "Disparando cnpj_transform..."
docker exec "$AIRFLOW_CONTAINER" airflow dags trigger cnpj_transform \
    --conf "{\"reference_month\": \"${REFERENCE_MONTH}\", \"process_empresas\": true, \"process_estabelecimentos\": true, \"force_reprocess\": ${FORCE_REPROCESS}}" \
    > /dev/null 2>&1 || true

# Aguarda até 30s o run aparecer no banco (evita race condition)
sleep 3
run_id=$(docker exec osint_postgres psql -U osint_admin -d osint_metadata -t -c \
    "SELECT run_id FROM dag_run WHERE dag_id='cnpj_transform' ORDER BY execution_date DESC LIMIT 1;" \
    | tr -d ' \n')

[[ -z "$run_id" ]] && fail "Não foi possível obter o run_id da cnpj_transform"

log "Triggerado → run_id: ${run_id}"
echo ""
log "Acompanhe na UI: http://localhost:8080"
log "Monitorando conclusão do pipeline (poll a cada ${POLL_INTERVAL}s)..."
echo ""

# cnpj_transform só marca success quando o pipeline inteiro termina
# (cada DAG usa wait_for_completion=True para o próximo)
t0=$(date +%s)
while true; do
    state=$(docker exec osint_postgres psql -U osint_admin -d osint_metadata -t -c \
        "SELECT state FROM dag_run WHERE dag_id='cnpj_transform' AND run_id='${run_id}';" \
        | tr -d ' \n')

    [[ -z "$state" ]] && state="unknown"

    elapsed=$(( $(date +%s) - t0 ))
    printf "\r  estado: %-12s  total: %s" "$state" "$(fmt_duration $elapsed)"

    case "$state" in
        success)
            echo ""
            ok "Pipeline completo em $(fmt_duration $elapsed)"
            break
            ;;
        failed)
            echo ""
            fail "Pipeline FALHOU — verifique a UI do Airflow"
            ;;
    esac

    sleep "$POLL_INTERVAL"
done

hr
echo -e "${BOLD}  ⏱  TEMPO TOTAL: $(fmt_duration $elapsed)${RESET}"
echo -e "  Concluído em : $(date '+%Y-%m-%d %H:%M:%S')"
hr
echo ""
