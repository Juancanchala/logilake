# LogiLake — comandos principales
# Uso: make <target>

.PHONY: run bronze silver gold serving sync help

## Corre el pipeline completo Bronze→Silver→Gold→Serving + sync dashboard
run:
	MSYS_NO_PATHCONV=1 wsl -e bash /mnt/c/logilake/scripts/run_pipeline.sh

## Solo Bronze (ingesta raw CSV → Delta)
bronze:
	MSYS_NO_PATHCONV=1 wsl -e bash -c "\
		source /home/devcontainers/miniconda3/etc/profile.d/conda.sh && conda activate base && \
		jupyter nbconvert --execute --to notebook --inplace \
		--ExecutePreprocessor.timeout=600 \
		/mnt/c/logilake/notebooks/bronze/01_bronze_ingest.ipynb"

## Solo Silver (transformación + DQ)
silver:
	MSYS_NO_PATHCONV=1 wsl -e bash -c "\
		source /home/devcontainers/miniconda3/etc/profile.d/conda.sh && conda activate base && \
		jupyter nbconvert --execute --to notebook --inplace \
		--ExecutePreprocessor.timeout=600 \
		/mnt/c/logilake/notebooks/silver/02_silver_transform.ipynb"

## Solo Gold (KPIs)
gold:
	MSYS_NO_PATHCONV=1 wsl -e bash -c "\
		source /home/devcontainers/miniconda3/etc/profile.d/conda.sh && conda activate base && \
		jupyter nbconvert --execute --to notebook --inplace \
		--ExecutePreprocessor.timeout=600 \
		/mnt/c/logilake/notebooks/gold/03_gold_kpis.ipynb"

## Solo Serving (export CSV + sync dashboard)
serving:
	MSYS_NO_PATHCONV=1 wsl -e bash -c "\
		source /home/devcontainers/miniconda3/etc/profile.d/conda.sh && conda activate base && \
		jupyter nbconvert --execute --to notebook --inplace \
		--ExecutePreprocessor.timeout=600 \
		/mnt/c/logilake/notebooks/serving/04_export_serving.ipynb"

## Solo copia los CSVs de serving a dashboard/data (sin recorrer el pipeline)
sync:
	MSYS_NO_PATHCONV=1 wsl -e bash -c "\
		cp /mnt/c/logilake/data/serving/*.csv /mnt/c/logilake/dashboard/data/ && \
		echo 'CSVs sincronizados en dashboard/data/'"

## Muestra este menú
help:
	@echo ""
	@echo "  make run     — Pipeline completo (recomendado)"
	@echo "  make bronze  — Solo ingesta Bronze"
	@echo "  make silver  — Solo Silver"
	@echo "  make gold    — Solo Gold"
	@echo "  make serving — Solo Serving + sync"
	@echo "  make sync    — Solo copia CSVs a dashboard/data"
	@echo ""
