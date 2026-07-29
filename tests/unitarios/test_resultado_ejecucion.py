from types import SimpleNamespace

from motor_spark.aplicacion.resultado_ejecucion import (
    construir_resultado_error,
    construir_resultado_exito,
)
from motor_spark.configuracion.argumentos import ArgumentosEjecucion
from motor_spark.configuracion.modelos.receta import RecetaConfig
from motor_spark.dominio.errores import ErrorReceta


class EsquemaFalso:
    def jsonValue(self):
        return {"type": "struct", "fields": []}

    def simpleString(self):
        return "struct<>"


class DataFrameFalso:
    columns = ["id"]
    schema = EsquemaFalso()


def test_resultado_error_conserva_claves():
    argumentos = ArgumentosEjecucion("{}", "in", "out", "", None, "e-1")
    resultado = construir_resultado_error(argumentos, ErrorReceta("falló"))
    assert resultado == {
        "estado": "ERROR",
        "ejecucion_id": "e-1",
        "entrada": "in",
        "salida": "out",
        "tipo_error": "ErrorReceta",
        "mensaje": "falló",
    }


def test_resultado_exito_conserva_contrato_actual():
    argumentos = ArgumentosEjecucion("{}", "in", "out", "", None, "e-1")
    receta = RecetaConfig.model_validate({"nombre": "Ventas", "version": 2, "salida": {}})
    resultado = construir_resultado_exito(
        argumentos=argumentos,
        receta=receta,
        modo_esquema="estricto",
        datos_entrada=DataFrameFalso(),
        datos_salida=DataFrameFalso(),
        total_registros=3,
        metricas_incrementales={"modo_carga": "incremental"},
        metricas_salida={"archivo_success": True},
    )
    assert resultado["estado"] == "COMPLETADO"
    assert resultado["receta"] == "Ventas"
    assert resultado["version_receta"] == 2
    assert resultado["total_registros"] == 3
    assert resultado["modo_carga"] == "incremental"
    assert resultado["archivo_success"] is True
    assert resultado["esquema_salida_simple"] == "struct<>"
