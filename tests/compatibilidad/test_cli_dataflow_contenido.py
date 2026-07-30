from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def test_cli_compila_contenido_multilinea_como_un_argumento(tmp_path: Path) -> None:
    script = """[Ventas]:
LOAD
    [id],
    Trim([nombre]) AS [nombre]
FROM 'ventas.csv';
"""
    conexiones = tmp_path / "conexiones.json"
    conexiones.write_text("{}", encoding="utf-8")
    plan = tmp_path / "plan.json"
    resultado = tmp_path / "resultado.json"

    proceso = subprocess.run(
        [
            sys.executable,
            "motor.py",
            "--dataflow-script-contenido",
            script,
            "--conexiones",
            str(conexiones),
            "--ejecucion-id",
            "cli-contenido-1",
            "--solo-compilar",
            "--plan-salida",
            str(plan),
            "--resultado",
            str(resultado),
        ],
        cwd=Path(__file__).resolve().parents[2],
        text=True,
        capture_output=True,
        check=False,
    )

    assert proceso.returncode == 0, proceso.stderr
    payload = json.loads(resultado.read_text(encoding="utf-8"))
    assert payload["estado"] == "COMPILADO"
    assert payload["origen_script"] == "parametro"
    assert payload["referencia_script"] is None
    assert payload["hash_script"]
    assert script not in resultado.read_text(encoding="utf-8")
    assert plan.is_file()
