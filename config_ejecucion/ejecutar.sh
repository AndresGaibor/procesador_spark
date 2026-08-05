#!/usr/bin/env bash

# Script de ejecución para Bash (Linux / Mac) con todos los parámetros habilitados

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.." || exit 1

SCRIPT_QLIK_PATH="config_ejecucion/script_qlik.qvs"
CONEXIONES_PATH="config_ejecucion/conexiones.json"
BASE_DESTINO_PATH="config_ejecucion/base_destino.json"
SECRETOS_PATH="config_ejecucion/secretos.json"

export MOTOR_SECRETOS_JSON="$(cat "$SECRETOS_PATH")"
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PWD/.venv/bin/python"

./.venv/bin/spark-submit \
  --packages org.postgresql:postgresql:42.7.7 \
  motor.py \
  --dataflow-script "$SCRIPT_QLIK_PATH" \
  --conexiones "$CONEXIONES_PATH" \
  --base-destino "$BASE_DESTINO_PATH" \
  --ejecucion-id ejec-bancolombia-completo-001
