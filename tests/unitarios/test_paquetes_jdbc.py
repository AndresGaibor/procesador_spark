import json

import pytest

from motor_spark.configuracion.paquetes_jdbc import resolver_paquetes_jdbc


def _catalogo(*drivers: str) -> str:
    return json.dumps({"jdbc": [{"driver": driver} for driver in drivers]})


def test_resuelve_postgresql_desde_catalogo_inline():
    paquetes = resolver_paquetes_jdbc(
        [
            "--conexiones-contenido",
            _catalogo("org.postgresql.Driver"),
        ]
    )

    assert paquetes == ("org.postgresql:postgresql:42.7.7",)


def test_resuelve_un_paquete_por_driver_repetido():
    paquetes = resolver_paquetes_jdbc(
        [
            "--conexiones-contenido",
            _catalogo("org.postgresql.Driver", "org.postgresql.Driver"),
        ]
    )

    assert paquetes == ("org.postgresql:postgresql:42.7.7",)


def test_catalogo_sin_jdbc_no_requiere_paquetes():
    assert resolver_paquetes_jdbc(["--conexiones-contenido", "{}"] ) == ()


def test_rechaza_driver_no_registrado():
    with pytest.raises(ValueError, match="no soportado"):
        resolver_paquetes_jdbc(
            [
                "--conexiones-contenido",
                _catalogo("com.ejemplo.DriverDesconocido"),
            ]
        )


def test_rechaza_catalogo_invalido_sin_mostrar_su_contenido():
    with pytest.raises(ValueError, match="JSON válido"):
        resolver_paquetes_jdbc(["--conexiones-contenido", "{dato-secreto"])
