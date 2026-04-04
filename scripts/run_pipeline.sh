#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# LogiLake — Pipeline completo Medallion
# Uso desde WSL:        bash /mnt/c/logilake/scripts/run_pipeline.sh
# Uso desde Git Bash:   make run   (o: MSYS_NO_PATHCONV=1 wsl -e bash /mnt/c/logilake/scripts/run_pipeline.sh)
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

ROOT="/mnt/c/logilake"
CONDA_ROOT="/home/devcontainers/miniconda3"
NB_TIMEOUT=600   # segundos por notebook

GREEN='\033[0;32m'; RED='\033[0;31m'; CYAN='\033[0;36m'; NC='\033[0m'; BOLD='\033[1m'

# ── Activar conda ─────────────────────────────────────────────────────────────
source "$CONDA_ROOT/etc/profile.d/conda.sh"
conda activate base

cd "$ROOT"

echo ""
echo -e "${BOLD}LogiLake Pipeline — $(date '+%Y-%m-%d %H:%M:%S')${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

TOTAL_START=$SECONDS
ERRORS=0

run_notebook() {
    local label="$1"
    local nb_path="$ROOT/$2"
    local t_start=$SECONDS

    printf "  %-10s %s " "$label" "$(basename $nb_path)"

    if jupyter nbconvert \
        --execute \
        --to notebook \
        --inplace \
        --ExecutePreprocessor.timeout=$NB_TIMEOUT \
        --ExecutePreprocessor.kernel_name=python3 \
        "$nb_path" > /tmp/logilake_nb.log 2>&1; then

        local elapsed=$(( SECONDS - t_start ))
        echo -e "${GREEN}OK${NC} (${elapsed}s)"
    else
        echo -e "${RED}FAILED${NC}"
        echo ""
        echo "─── Error log ───────────────────────────────────"
        tail -20 /tmp/logilake_nb.log
        echo "─────────────────────────────────────────────────"
        ERRORS=$(( ERRORS + 1 ))
        return 1
    fi
}

echo ""
echo -e "${CYAN}Bronze${NC}  · Ingesta raw CSV → Delta"
run_notebook "Bronze" "notebooks/bronze/01_bronze_ingest.ipynb"

echo ""
echo -e "${CYAN}Silver${NC}  · Limpieza + Quality checks"
run_notebook "Silver" "notebooks/silver/02_silver_transform.ipynb"

echo ""
echo -e "${CYAN}Gold${NC}    · KPIs agregados"
run_notebook "Gold" "notebooks/gold/03_gold_kpis.ipynb"

echo ""
echo -e "${CYAN}Serving${NC} · Export CSV + sync dashboard"
run_notebook "Serving" "notebooks/serving/04_export_serving.ipynb"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
TOTAL=$(( SECONDS - TOTAL_START ))

if [ $ERRORS -eq 0 ]; then
    echo -e "${GREEN}${BOLD}Pipeline completado en ${TOTAL}s${NC}"
    echo ""
    echo "  Datos disponibles en:"
    echo "    Bronze  →  data/bronze/"
    echo "    Silver  →  data/silver/"
    echo "    Gold    →  data/gold/"
    echo "    CSVs    →  data/serving/   y   dashboard/data/"
    echo ""
    echo -e "  ${CYAN}Refresca el browser para ver el dashboard actualizado.${NC}"
else
    echo -e "${RED}${BOLD}Pipeline FAILED — $ERRORS etapa(s) con error${NC}"
    exit 1
fi
