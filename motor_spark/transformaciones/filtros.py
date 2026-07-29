from __future__ import annotations

from typing import Any

from motor_spark.configuracion.modelos.pasos import FiltrarPaso


def aplicar_filtro(
    datos: Any,
    paso: FiltrarPaso,
    numero_paso: int,
) -> Any:
    from pyspark.sql import functions as F

    del numero_paso
    return datos.filter(F.expr(paso.expresion))
