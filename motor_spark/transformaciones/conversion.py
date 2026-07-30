from __future__ import annotations

from typing import Any

from motor_spark.configuracion.modelos.pasos import ConvertirTipoPaso
from motor_spark.dominio.columnas import exigir_columnas
from motor_spark.dominio.tipos_spark import convertir_tipo_spark


def aplicar_conversion_tipo(
    datos: Any,
    paso: ConvertirTipoPaso,
    numero_paso: int,
) -> Any:
    from pyspark.sql import functions as F

    columna = paso.columna
    destino = paso.destino.strip().lower()
    formato = paso.formato
    exigir_columnas(datos, [columna], numero_paso)

    if destino in {"date", "fecha"}:
        expresion = (
            F.to_date(F.col(columna), formato) if formato else F.to_date(F.col(columna))
        )
    elif destino == "timestamp":
        expresion = (
            F.to_timestamp(F.col(columna), formato)
            if formato
            else F.to_timestamp(F.col(columna))
        )
    else:
        tipo_spark = convertir_tipo_spark(destino)
        expresion = F.col(columna).cast(tipo_spark.simpleString())

    return datos.withColumn(columna, expresion)
