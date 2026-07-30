from __future__ import annotations

import argparse
import re
from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ArgumentosEjecucion:
    receta: str
    entrada: str
    salida: str
    esquema: str
    resultado: str | None
    ejecucion_id: str


@dataclass(frozen=True, slots=True)
class ArgumentosDataflowScript:
    conexiones: str | None
    ejecucion_id: str
    dataflow_script: str | None = None
    dataflow_script_contenido: str | None = None
    conexiones_contenido: str | None = None
    resultado: str | None = None
    solo_compilar: bool = False
    plan_salida: str | None = None
    secretos: tuple[tuple[str, str], ...] = ()

    @property
    def origen_script(self) -> str:
        """Indica si el script provino de un archivo o del propio parámetro CLI."""
        return "parametro" if self.dataflow_script_contenido is not None else "archivo"

    @property
    def origen_conexiones(self) -> str:
        """Identifica el origen del catálogo sin exponer su contenido."""
        return "parametro" if self.conexiones_contenido is not None else "archivo"


def crear_argumentos() -> argparse.ArgumentParser:
    return _crear_parser_receta()


def _crear_parser_receta() -> argparse.ArgumentParser:
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


def _parsear_secretos(
    valores: Sequence[str], parser: argparse.ArgumentParser
) -> tuple[tuple[str, str], ...]:
    secretos: dict[str, str] = {}
    for valor in valores:
        nombre, separador, secreto = valor.partition("=")
        if (
            not separador
            or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", nombre)
            or not secreto
            or "\x00" in secreto
            or "\n" in secreto
            or "\r" in secreto
        ):
            parser.error("--secreto debe tener el formato NOMBRE=VALOR valido")
        if nombre in secretos:
            parser.error("--secreto no puede repetir un nombre")
        secretos[nombre] = secreto
    return tuple(secretos.items())


def _crear_parser_dataflow_script() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Motor Spark con scripts dataflow")

    grupo = parser.add_mutually_exclusive_group(required=True)
    grupo.add_argument(
        "--receta",
        help="Ruta a archivo de receta JSON",
    )
    grupo.add_argument(
        "--dataflow-script",
        dest="dataflow_script",
        help="Ruta a archivo de script dataflow",
    )
    grupo.add_argument(
        "-dataflowscript",
        dest="dataflow_script",
        help="Atajo para --dataflow-script",
    )
    grupo.add_argument(
        "--dataflow-script-contenido",
        dest="dataflow_script_contenido",
        help=(
            "Contenido Qlik completo enviado directamente. "
            "Es mutuamente excluyente con --dataflow-script."
        ),
    )

    grupo_conexiones = parser.add_mutually_exclusive_group(required=True)
    grupo_conexiones.add_argument(
        "--conexiones",
        help="Ruta al catálogo JSON de conexiones",
    )
    grupo_conexiones.add_argument(
        "--conexiones-contenido",
        dest="conexiones_contenido",
        help=(
            "Catálogo JSON completo enviado directamente. "
            "Es mutuamente excluyente con --conexiones."
        ),
    )
    parser.add_argument("--ejecucion-id", required=False)
    parser.add_argument("--resultado", default=None)
    parser.add_argument(
        "--secreto",
        action="append",
        default=[],
        metavar="NOMBRE=VALOR",
        help="Secreto inyectado en memoria para una referencia del catalogo.",
    )
    parser.add_argument(
        "--solo-compilar",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--plan-salida",
        default=None,
    )

    parser.add_argument("--entrada", required=False)
    parser.add_argument("--salida", required=False)
    parser.add_argument("--esquema", required=False)

    return parser


def _detectar_modo(argv: Sequence[str] | None) -> str:
    if argv is None:
        import sys

        argv = sys.argv[1:]

    for arg in argv:
        if arg in (
            "--receta",
            "--dataflow-script",
            "-dataflowscript",
            "--dataflow-script-contenido",
        ):
            return arg.lstrip("-")
    return "receta"


def analizar_argumentos(
    argv: Sequence[str] | None = None,
) -> ArgumentosEjecucion | ArgumentosDataflowScript:
    modo = _detectar_modo(argv)

    if modo == "receta":
        valores_receta = _crear_parser_receta().parse_args(argv)
        return ArgumentosEjecucion(
            receta=valores_receta.receta,
            entrada=valores_receta.entrada,
            salida=valores_receta.salida,
            esquema=valores_receta.esquema,
            resultado=valores_receta.resultado,
            ejecucion_id=valores_receta.ejecucion_id,
        )

    parser_dataflow = _crear_parser_dataflow_script()
    valores_df = parser_dataflow.parse_args(argv)

    if valores_df.entrada or valores_df.salida or valores_df.esquema:
        parser_dataflow.error(
            "--entrada, --salida y --esquema no son validos en modo dataflow-script"
        )

    if (
        valores_df.conexiones_contenido is not None
        and not valores_df.conexiones_contenido.strip()
    ):
        parser_dataflow.error("--conexiones-contenido no puede estar vacío")

    if not valores_df.ejecucion_id:
        parser_dataflow.error("--ejecucion-id es requerido en modo dataflow-script")

    if not valores_df.dataflow_script and not valores_df.dataflow_script_contenido:
        parser_dataflow.error(
            "--dataflow-script o --dataflow-script-contenido es requerido "
            "en modo dataflow-script"
        )

    if (
        valores_df.dataflow_script_contenido is not None
        and not valores_df.dataflow_script_contenido.strip()
    ):
        parser_dataflow.error("--dataflow-script-contenido no puede estar vacío")

    if valores_df.solo_compilar and not valores_df.plan_salida:
        parser_dataflow.error("--solo-compilar requiere --plan-salida")

    secretos = _parsear_secretos(valores_df.secreto, parser_dataflow)

    return ArgumentosDataflowScript(
        dataflow_script=valores_df.dataflow_script,
        dataflow_script_contenido=valores_df.dataflow_script_contenido,
        conexiones=valores_df.conexiones,
        conexiones_contenido=valores_df.conexiones_contenido,
        ejecucion_id=valores_df.ejecucion_id,
        resultado=valores_df.resultado,
        solo_compilar=valores_df.solo_compilar,
        plan_salida=valores_df.plan_salida,
        secretos=secretos,
    )
