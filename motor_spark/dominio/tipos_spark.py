from __future__ import annotations

import re
from typing import Any

from motor_spark.dominio.errores import ErrorReceta

PATRON_DECIMAL = re.compile(
    r"^decimal\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)$",
    re.IGNORECASE,
)


def convertir_tipo_spark(tipo_crudo: str) -> Any:
    from pyspark.sql.types import (
        BooleanType,
        DateType,
        DecimalType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        StringType,
        TimestampType,
    )

    tipo = tipo_crudo.strip().lower()
    tipos_simples = {
        "string": StringType(),
        "texto": StringType(),
        "int": IntegerType(),
        "integer": IntegerType(),
        "entero": IntegerType(),
        "long": LongType(),
        "bigint": LongType(),
        "double": DoubleType(),
        "float": FloatType(),
        "boolean": BooleanType(),
        "bool": BooleanType(),
        "date": DateType(),
        "fecha": DateType(),
        "timestamp": TimestampType(),
    }

    if tipo in tipos_simples:
        return tipos_simples[tipo]

    coincidencia = PATRON_DECIMAL.fullmatch(tipo)
    if coincidencia:
        precision = int(coincidencia.group(1))
        escala = int(coincidencia.group(2))
        if precision < 1 or precision > 38:
            raise ErrorReceta(
                f"Precisión decimal fuera del rango 1-38: {tipo}"
            )
        if escala < 0 or escala > precision:
            raise ErrorReceta(f"Escala decimal inválida: {tipo}")
        return DecimalType(precision, escala)

    raise ErrorReceta(f"Tipo no soportado: {tipo_crudo}")
