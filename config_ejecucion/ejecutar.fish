#!/usr/bin/env fish

# Script de ejecución para Fish shell con todos los parámetros habilitados

set -l BASE_DIR (dirname (status filename))/..
cd "$BASE_DIR"

set -l SCRIPT_QLIK_PATH "config_ejecucion/script_qlik.qvs"
set -l CONEXIONES_PATH "config_ejecucion/conexiones.json"
set -l BASE_DESTINO_PATH "config_ejecucion/base_destino.json"
set -l SECRETOS_PATH "config_ejecucion/secretos.json"

set -x MOTOR_SECRETOS_JSON (cat "$SECRETOS_PATH")
set -x PYSPARK_PYTHON "$PWD/.venv/bin/python"
set -x PYSPARK_DRIVER_PYTHON "$PWD/.venv/bin/python"

./.venv/bin/spark-submit \
  --packages org.postgresql:postgresql:42.7.7 \
  motor.py \
  --dataflow-script "$SCRIPT_QLIK_PATH" \
  --conexiones "$CONEXIONES_PATH" \
  --base-destino "$BASE_DESTINO_PATH" \
  --ejecucion-id ejec-bancolombia-completo-001
