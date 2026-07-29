from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from motor_spark.compartido.eventos_consola import emitir
from motor_spark.configuracion.modelos.pasos import PasoConfig
from motor_spark.dominio.errores import ErrorReceta
from motor_spark.transformaciones.registro import REGISTRO_TRANSFORMACIONES


def aplicar_pasos(
    datos: Any,
    pasos: Sequence[PasoConfig],
) -> Any:
    resultado = datos

    for numero, paso in enumerate(pasos, start=1):
        tipo = paso.tipo
        emitir(f"PASO_INICIO numero={numero} tipo={tipo}")
        manejador = REGISTRO_TRANSFORMACIONES.get(tipo)
        if manejador is None:
            raise ErrorReceta(
                f"Operación no soportada en el paso {numero}: {tipo}"
            )
        resultado = manejador(resultado, paso, numero)
        emitir(
            f"PASO_FIN numero={numero} "
            f"tipo={tipo} columnas={resultado.columns}"
        )

    return resultado
