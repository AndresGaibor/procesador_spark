from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
if shutil.which("java") is None:
    pytest.skip("Java no está instalado", allow_module_level=True)

pytestmark = pytest.mark.spark

RAIZ = Path(__file__).resolve().parents[2]
ESQUEMA = (
    "id:entero|fecha:texto|cliente:texto|ciudad:texto|producto:texto|"
    "categoria:texto|cantidad:entero|precio:decimal(12,2)|"
    "descuento:decimal(12,2)|total:decimal(12,2)|"
    "metodo_pago:texto|vendedor:texto"
)
EVENTOS = {
    "LECTURA_COMPLETADA",
    "ESCRITURA_INICIO",
    "ESCRITURA_FIN",
    "EJECUCION_INICIO",
    "MODO_ESQUEMA_ENTRADA",
    "ESQUEMA_ENTRADA_DECLARADO",
    "ESQUEMA_ENTRADA_REAL",
    "PASO_INICIO numero",
    "PASO_FIN numero",
    "RESULTADO_MOTOR",
    "SALIDA_VALIDADA",
}


def _ejecutar(script: Path, salida: Path, resultado: Path):
    return subprocess.run(
        [
            sys.executable,
            str(script),
            "--receta",
            str(RAIZ / "tests/recursos/recetas/estricta.json"),
            "--entrada",
            str(RAIZ / "tests/recursos/datos/ventas.csv"),
            "--salida",
            salida.as_uri(),
            "--esquema",
            ESQUEMA,
            "--resultado",
            str(resultado),
            "--ejecucion-id",
            "diferencial-1",
        ],
        cwd=RAIZ,
        text=True,
        capture_output=True,
        check=False,
    )


def _nombres_eventos(stdout: str) -> list[str]:
    nombres: list[str] = []
    for linea in stdout.splitlines():
        prefijo = linea.split("=", 1)[0]
        if prefijo in EVENTOS:
            nombres.append(prefijo)
        elif linea.startswith("PASO_INICIO numero="):
            nombres.append("PASO_INICIO numero")
        elif linea.startswith("PASO_FIN numero="):
            nombres.append("PASO_FIN numero")
    return nombres


def test_motor_nuevo_equivale_al_original_en_flujo_estricto(tmp_path):
    original = _ejecutar(
        RAIZ / "legacy/motor_original.py",
        tmp_path / "salida-original",
        tmp_path / "resultado-original.json",
    )
    nuevo = _ejecutar(
        RAIZ / "motor.py",
        tmp_path / "salida-nueva",
        tmp_path / "resultado-nuevo.json",
    )

    assert original.returncode == nuevo.returncode == 0, (
        original.stderr,
        nuevo.stderr,
    )
    resultado_original = json.loads(
        (tmp_path / "resultado-original.json").read_text(encoding="utf-8")
    )
    resultado_nuevo = json.loads(
        (tmp_path / "resultado-nuevo.json").read_text(encoding="utf-8")
    )

    for clave in (
        "estado",
        "ejecucion_id",
        "receta",
        "version_receta",
        "modo_esquema_entrada",
        "columnas_entrada",
        "esquema_entrada_simple",
        "columnas_salida",
        "esquema_salida_simple",
        "total_registros",
        "archivo_success",
        "esquema_almacenamiento",
    ):
        assert resultado_nuevo[clave] == resultado_original[clave]

    assert resultado_original["cantidad_archivos_parquet"] >= 1
    assert resultado_nuevo["cantidad_archivos_parquet"] >= 1
    assert resultado_original["bytes_parquet"] > 0
    assert resultado_nuevo["bytes_parquet"] > 0
    assert _nombres_eventos(nuevo.stdout) == _nombres_eventos(original.stdout)
