from __future__ import annotations

import csv
import hashlib
import json
import os
from pathlib import Path

import pytest

GOLDEN_DIR_ENV = os.environ.get("QLIK_GOLDEN_DIR")

pytestmark = pytest.mark.skipif(
    GOLDEN_DIR_ENV is None,
    reason="QLIK_GOLDEN_DIR no esta definido",
)

GOLDEN_ARTEFACTS = [
    "manifest.json",
    "schema.json",
    "output.csv",
    "output_golden.csv",
]


def _golden_dir() -> Path:
    assert GOLDEN_DIR_ENV is not None, "QLIK_GOLDEN_DIR debe estar definido"
    return Path(GOLDEN_DIR_ENV)


class TestQlikGoldenArtefacts:
    def test_qlik_golden_dir_existe(self):
        golden_dir = _golden_dir()
        if not golden_dir.exists():
            pytest.fail(f"QLIK_GOLDEN_DIR={golden_dir} pero el directorio no existe")

    def test_manifest_sha256_valido(self):
        golden_dir = _golden_dir()
        manifest_path = golden_dir / "manifest.json"
        if not manifest_path.exists():
            pytest.fail(
                f"QLIK_GOLDEN_DIR={golden_dir} pero falta manifest.json. "
                f"Artefactos esperados: {GOLDEN_ARTEFACTS}"
            )

        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

        assert "sha256" in manifest, "manifest.json debe contener clave 'sha256'"
        sha = manifest["sha256"]
        assert len(sha) == 64, f"SHA-256 debe tener 64 caracteres, tiene {len(sha)}"

        output_csv = golden_dir / "output.csv"
        if output_csv.exists():
            hasher = hashlib.sha256()
            with open(output_csv, "rb") as f:
                for chunk in iter(lambda: f.read(65536), b""):
                    hasher.update(chunk)
            computed = hasher.hexdigest()
            assert sha == computed, (
                f"SHA-256 del manifest={sha} no coincide con output.csv={computed}"
            )

    def test_schema_json_valido(self):
        golden_dir = _golden_dir()
        schema_path = golden_dir / "schema.json"
        if not schema_path.exists():
            pytest.fail(
                f"QLIK_GOLDEN_DIR={golden_dir} pero falta schema.json. "
                f"Artefactos esperados: {GOLDEN_ARTEFACTS}"
            )

        schema = json.loads(schema_path.read_text(encoding="utf-8"))

        assert "columnas" in schema, "schema.json debe contener 'columnas'"
        assert isinstance(schema["columnas"], list), "'columnas' debe ser lista"
        assert len(schema["columnas"]) > 0, "'columnas' no puede estar vacia"

    def test_output_csv_byte_exacto(self):
        golden_dir = _golden_dir()
        output_csv = golden_dir / "output.csv"
        golden_csv = golden_dir / "output_golden.csv"

        if not golden_csv.exists():
            pytest.fail(
                f"QLIK_GOLDEN_DIR={golden_dir} pero falta output_golden.csv. "
                f"Artefactos esperados: {GOLDEN_ARTEFACTS}"
            )

        if not output_csv.exists():
            pytest.fail(
                f"QLIK_GOLDEN_DIR={golden_dir} pero falta output.csv. "
                f"Artefactos esperados: {GOLDEN_ARTEFACTS}"
            )

        output_bytes = output_csv.read_bytes()
        golden_bytes = golden_csv.read_bytes()

        assert output_bytes == golden_bytes, (
            "output.csv difiere byte-a-byte de output_golden.csv"
        )

    def test_output_csv_semantica_mismas_filas(self):
        golden_dir = _golden_dir()
        output_csv = golden_dir / "output.csv"
        golden_csv = golden_dir / "output_golden.csv"

        if not golden_csv.exists() or not output_csv.exists():
            pytest.skip("Faltan artefactos CSV para prueba semantica")

        def leer_csv_ordenado(path: Path) -> list[dict[str, str]]:
            with open(path, "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                return sorted(
                    [dict(row) for row in reader], key=lambda r: tuple(r.items())
                )

        filas_out = leer_csv_ordenado(output_csv)
        filas_golden = leer_csv_ordenado(golden_csv)

        assert len(filas_out) == len(filas_golden), (
            f"output.csv tiene {len(filas_out)} filas, golden tiene {len(filas_golden)}"
        )

        assert filas_out == filas_golden, (
            "output.csv y output_golden.csv tienen contenido diferente (orden-independent)"
        )

    def test_manifest_consistente_con_output(self):
        golden_dir = _golden_dir()
        manifest_path = golden_dir / "manifest.json"
        output_csv = golden_dir / "output.csv"

        if not manifest_path.exists() or not output_csv.exists():
            pytest.skip("Faltan artefactos para validar consistencia")

        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

        if "filas" in manifest:
            with open(output_csv, "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.reader(f)
                next(reader, None)
                filas_output = sum(1 for _ in reader)
            assert filas_output == manifest["filas"], (
                f"manifest.json indica {manifest['filas']} filas, "
                f"output.csv tiene {filas_output}"
            )
