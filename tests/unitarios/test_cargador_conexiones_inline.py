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


def test_cargar_catalogo_sftp_con_clave_privada():
    contenido = json.dumps(
        {
            "sftp": [
                {
                    "nombre": "Banco:SFTP",
                    "host": "209.50.245.140",
                    "puerto": 22,
                    "usuario": "sftpqlik",
                    "clave_privada": "/Users/demo/.ssh/sftp_debian",
                    "secreto_passphrase_nombre": "SFTP_KEY_PASSPHRASE",
                    "ruta_base": "/upload",
                    "allowlist": [{"esquema": "", "tabla": "salida.csv", "campos": []}],
                }
            ]
        }
    )

    catalogo = cargar_catalogo_contenido(contenido)
    conexion = catalogo.buscar_sftp("Banco:SFTP")

    assert conexion is not None
    assert conexion.usuario == "sftpqlik"
    assert conexion.clave_privada == "/Users/demo/.ssh/sftp_debian"
    assert conexion.secreto_passphrase_nombre == "SFTP_KEY_PASSPHRASE"
    assert conexion.secreto_nombre is None


def test_cargar_catalogo_sftp_con_contenido_clave_en_secreto():
    contenido = json.dumps(
        {
            "sftp": [
                {
                    "nombre": "Banco:SFTP",
                    "host": "209.50.245.140",
                    "puerto": 22,
                    "usuario": "sftpqlik",
                    "secreto_clave_privada_nombre": "SFTP_PRIVATE_KEY_B64",
                    "ruta_base": "/upload",
                    "allowlist": [{"esquema": "", "tabla": "salida.csv", "campos": []}],
                }
            ]
        }
    )

    conexion = cargar_catalogo_contenido(contenido).buscar_sftp("Banco:SFTP")

    assert conexion is not None
    assert conexion.usuario == "sftpqlik"
    assert conexion.secreto_clave_privada_nombre == "SFTP_PRIVATE_KEY_B64"
    assert conexion.clave_privada is None
    assert conexion.secreto_nombre is None


def test_catalogo_sftp_rechaza_password_y_contenido_clave_simultaneos():
    contenido = json.dumps(
        {
            "sftp": [
                {
                    "nombre": "Banco:SFTP",
                    "host": "209.50.245.140",
                    "secreto_nombre": "SFTP_PASSWORD",
                    "usuario": "sftpqlik",
                    "secreto_clave_privada_nombre": "SFTP_PRIVATE_KEY_B64",
                }
            ]
        }
    )

    with pytest.raises(ValueError, match="exactamente uno"):
        cargar_catalogo_contenido(contenido)


def test_catalogo_sftp_con_clave_inline_requiere_usuario():
    contenido = json.dumps(
        {
            "sftp": [
                {
                    "nombre": "Banco:SFTP",
                    "host": "209.50.245.140",
                    "secreto_clave_privada_nombre": "SFTP_PRIVATE_KEY_B64",
                }
            ]
        }
    )

    with pytest.raises(ValueError, match="requiere usuario"):
        cargar_catalogo_contenido(contenido)
