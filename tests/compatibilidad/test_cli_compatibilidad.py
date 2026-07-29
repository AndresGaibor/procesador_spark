import json
import os
import re
import subprocess
import sys
from pathlib import Path


RAIZ = Path(__file__).resolve().parents[2]


def _crear_stub_pyspark(tmp_path: Path) -> Path:
    paquete = tmp_path / "pyspark" / "sql"
    paquete.mkdir(parents=True)
    (tmp_path / "pyspark" / "__init__.py").write_text("", encoding="utf-8")
    (paquete / "__init__.py").write_text(
        "class DataFrame: pass\nclass SparkSession: pass\n",
        encoding="utf-8",
    )
    (paquete / "functions.py").write_text("", encoding="utf-8")
    (paquete / "types.py").write_text(
        "\n".join(
            f"class {nombre}: pass"
            for nombre in (
                "BooleanType",
                "DateType",
                "DecimalType",
                "DoubleType",
                "FloatType",
                "IntegerType",
                "LongType",
                "NullType",
                "StringType",
                "StructField",
                "StructType",
                "TimestampType",
            )
        ),
        encoding="utf-8",
    )
    return tmp_path


def _normalizar_programa(texto: str) -> str:
    uso, resto = texto.split("\n\n", 1)
    uso = re.sub(r"^usage: \S+", "usage: motor.py", uso)
    uso = " ".join(uso.split())
    return uso + "\n\n" + resto


def test_help_es_identico_al_motor_original(tmp_path):
    stub = _crear_stub_pyspark(tmp_path)
    entorno = os.environ.copy()
    entorno["PYTHONPATH"] = os.pathsep.join(
        [str(stub), str(RAIZ), entorno.get("PYTHONPATH", "")]
    )

    original = subprocess.run(
        [sys.executable, str(RAIZ / "legacy" / "motor_original.py"), "--help"],
        cwd=RAIZ,
        env=entorno,
        text=True,
        capture_output=True,
        check=False,
    )
    nuevo = subprocess.run(
        [sys.executable, str(RAIZ / "motor.py"), "--help"],
        cwd=RAIZ,
        env=entorno,
        text=True,
        capture_output=True,
        check=False,
    )

    assert original.returncode == nuevo.returncode == 0
    assert _normalizar_programa(original.stdout) == _normalizar_programa(
        nuevo.stdout
    )
    assert original.stderr == nuevo.stderr == ""


def test_error_previo_a_spark_conserva_json_y_codigo_uno(tmp_path):
    resultado = tmp_path / "resultado.json"
    proceso = subprocess.run(
        [
            sys.executable,
            str(RAIZ / "motor.py"),
            "--receta",
            '{"entrada": {}}',
            "--entrada",
            "entrada.csv",
            "--salida",
            "salida",
            "--resultado",
            str(resultado),
            "--ejecucion-id",
            "e-cli-1",
        ],
        cwd=RAIZ,
        text=True,
        capture_output=True,
        check=False,
    )
    assert proceso.returncode == 1
    contenido = json.loads(resultado.read_text(encoding="utf-8"))
    assert contenido["estado"] == "ERROR"
    assert contenido["ejecucion_id"] == "e-cli-1"
    assert contenido["tipo_error"] == "ErrorReceta"
    assert proceso.stderr.splitlines()[0].startswith("RESULTADO_MOTOR=")
