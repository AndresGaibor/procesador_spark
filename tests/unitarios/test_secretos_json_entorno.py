import json

import pytest

from motor_spark.conexiones.secretos import (
    LIMITE_SECRETOS_JSON_BYTES,
    cargar_secretos_json_entorno,
    combinar_secretos,
)


def test_cargar_secretos_json_entorno_devuelve_objeto_validado():
    entorno = {
        "MOTOR_SECRETOS_JSON": json.dumps(
            {
                "POSTGRES_BANCOLOMBIA": "usuario:clave",
                "SFTP_PRIVATE_KEY_B64": "LS0tLS1CRUdJTi0tLS0t",
            }
        )
    }

    secretos = cargar_secretos_json_entorno(entorno)

    assert secretos == {
        "POSTGRES_BANCOLOMBIA": "usuario:clave",
        "SFTP_PRIVATE_KEY_B64": "LS0tLS1CRUdJTi0tLS0t",
    }


def test_cargar_secretos_json_entorno_es_opcional():
    assert cargar_secretos_json_entorno({}) == {}
    assert cargar_secretos_json_entorno({"MOTOR_SECRETOS_JSON": "   "}) == {}


@pytest.mark.parametrize(
    ("contenido", "tipo_error", "fragmento_error"),
    [
        ("{invalido", ValueError, "JSON válido"),
        ("[]", TypeError, "objeto JSON"),
        ('{"NOMBRE-INVALIDO":"valor"}', ValueError, "nombre inválido"),
        ('{"SECRETO":123}', TypeError, "cadena de texto"),
        ('{"SECRETO":""}', ValueError, "vacío"),
        ('{"SECRETO":"linea\\notra"}', ValueError, "saltos de línea"),
    ],
)
def test_cargar_secretos_json_entorno_rechaza_contratos_invalidos(
    contenido, tipo_error, fragmento_error
):
    with pytest.raises(tipo_error, match=fragmento_error):
        cargar_secretos_json_entorno({"MOTOR_SECRETOS_JSON": contenido})


def test_cargar_secretos_json_entorno_rechaza_nombres_duplicados():
    contenido = '{"SECRETO":"primero","SECRETO":"segundo"}'

    with pytest.raises(ValueError, match="duplicado"):
        cargar_secretos_json_entorno({"MOTOR_SECRETOS_JSON": contenido})


def test_cargar_secretos_json_entorno_rechaza_exceso_de_tamano():
    contenido = '{"SECRETO":"' + ("x" * LIMITE_SECRETOS_JSON_BYTES) + '"}'

    with pytest.raises(ValueError, match="tamaño máximo"):
        cargar_secretos_json_entorno({"MOTOR_SECRETOS_JSON": contenido})


def test_combinar_secretos_da_prioridad_a_parametros_explicitos():
    secretos = combinar_secretos(
        desde_json={"COMPARTIDO": "json", "SOLO_JSON": "valor-json"},
        explicitos={"COMPARTIDO": "cli", "SOLO_CLI": "valor-cli"},
    )

    assert secretos == {
        "COMPARTIDO": "cli",
        "SOLO_JSON": "valor-json",
        "SOLO_CLI": "valor-cli",
    }
