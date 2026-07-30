#!/usr/bin/env python3
from __future__ import annotations

from typing import Sequence

from motor_spark.configuracion.argumentos import analizar_argumentos


def main(argv: Sequence[str] | None = None) -> int:
    argumentos = analizar_argumentos(argv) # receta, entrada, salida, esquema, resultado, ejecucion_id
    from motor_spark.aplicacion.ejecutor_motor import ejecutar_motor
    return ejecutar_motor(argumentos)


if __name__ == "__main__":
    raise SystemExit(main())
