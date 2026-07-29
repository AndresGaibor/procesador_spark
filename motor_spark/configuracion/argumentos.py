from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Sequence


@dataclass(frozen=True, slots=True)
class ArgumentosEjecucion:
    receta: str
    entrada: str
    salida: str
    esquema: str
    resultado: str | None
    ejecucion_id: str


def crear_argumentos() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Motor Spark mecánico dirigido por recetas JSON"
    )

    parser.add_argument("--receta", required=True)
    parser.add_argument("--entrada", required=True)
    parser.add_argument("--salida", required=True)
    parser.add_argument(
        "--esquema",
        required=False,
        default="",
        help=(
            "Esquema columna:tipo separado por |. "
            "Solo se usa cuando entrada.modo_esquema=estricto."
        ),
    )
    parser.add_argument("--resultado", default=None)
    parser.add_argument("--ejecucion-id", required=True)

    return parser


def analizar_argumentos(
    argv: Sequence[str] | None = None,
) -> ArgumentosEjecucion:
    valores = crear_argumentos().parse_args(argv)

    return ArgumentosEjecucion(
        receta=valores.receta,
        entrada=valores.entrada,
        salida=valores.salida,
        esquema=valores.esquema,
        resultado=valores.resultado,
        ejecucion_id=valores.ejecucion_id,
    )
