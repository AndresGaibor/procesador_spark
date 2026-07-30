import json

import pytest

from motor_spark.conexiones.cargador import cargar_catalogo_contenido


def test_cargar_catalogo_contenido_construye_modelo_sin_archivo():
    contenido = json.dumps(
        {
            "version": 1,
            "descripcion": "inline",
            "jdbc": [],
            "locales": [
                {
                    "nombre": "Landing",
                    "ruta_base": "/srv/landing",
                    "allowlist": [{"esquema": "", "tabla": "ventas.csv", "campos": []}],
                }
            ],
            "sftp": [],
        }
    )

    catalogo = cargar_catalogo_contenido(contenido)

    assert catalogo.descripcion == "inline"
    assert catalogo.buscar_local("Landing") is not None
    assert catalogo.buscar_local("Landing").ruta_base == "/srv/landing"


def test_cargar_catalogo_contenido_rechaza_vacio():
    with pytest.raises(ValueError, match="vacío|vacio"):
        cargar_catalogo_contenido("   ")


def test_cargar_catalogo_contenido_rechaza_json_malformado():
    with pytest.raises(json.JSONDecodeError):
        cargar_catalogo_contenido('{"jdbc": [}')


def test_cargar_catalogo_contenido_rechaza_raiz_no_objeto():
    with pytest.raises(TypeError, match="objeto JSON"):
        cargar_catalogo_contenido("[]")


def test_cargar_catalogo_contenido_aplica_limite_en_bytes_utf8(monkeypatch):
    from motor_spark.conexiones import cargador

    monkeypatch.setattr(cargador, "LIMITE_TAMANIO_CATALOGO", 3)
    with pytest.raises(ValueError, match="excede"):
        cargar_catalogo_contenido("áá")
