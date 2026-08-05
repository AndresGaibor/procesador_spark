# Script PowerShell para Windows con todos los parámetros habilitados

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location "$ScriptDir\.."

$ScriptQlikPath = "config_ejecucion/script_qlik.qvs"
$ConexionesPath = "config_ejecucion/conexiones.json"
$BaseDestinoPath = "config_ejecucion/base_destino.json"
$SecretosPath = "config_ejecucion/secretos.json"

$env:MOTOR_SECRETOS_JSON = Get-Content -Raw $SecretosPath

$PythonExec = if (Test-Path "$ScriptDir\..\.venv\Scripts\python.exe") { "$ScriptDir\..\.venv\Scripts\python.exe" } else { (Get-Command python).Source }

$env:PYSPARK_PYTHON = $PythonExec
$env:PYSPARK_DRIVER_PYTHON = $PythonExec

& $PythonExec motor.py `
  --dataflow-script $ScriptQlikPath `
  --conexiones $ConexionesPath `
  --base-destino $BaseDestinoPath `
  --ejecucion-id ejec-bancolombia-completo-001


