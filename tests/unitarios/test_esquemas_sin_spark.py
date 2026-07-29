import pytest

from motor_spark.configuracion.modelos.entrada import EntradaConfig
from motor_spark.dominio.errores import ErrorReceta
from motor_spark.dominio.esquemas import obtener_modo_esquema


def test_obtener_modo_esquema_usa_modelo_normalizado():
    config = EntradaConfig(modo_esquema="infer")
    assert obtener_modo_esquema(config) == "inferir"


def test_entrada_rechaza_modo_desconocido():
    with pytest.raises(ValueError, match="modo_esquema"):
        EntradaConfig(modo_esquema="automatico")
