from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from motor_spark.configuracion.modelos.entrada import normalizar_modo_esquema
from motor_spark.configuracion.modelos.pasos import TIPOS_PASO_SOPORTADOS
from motor_spark.configuracion.modelos.receta import RecetaConfig
from motor_spark.dominio.errores import ErrorReceta


def _validar_entrada_compatible(receta: dict[str, Any]) -> None:
    entrada = receta.get("entrada", {})
    if not isinstance(entrada, dict):
        raise ErrorReceta("receta.entrada debe ser un objeto")

    try:
        normalizar_modo_esquema(entrada.get("modo_esquema", "estricto"))
    except ValueError as excepcion:
        raise ErrorReceta(str(excepcion)) from excepcion

    tipos_forzados = entrada.get("tipos_forzados", {})
    if tipos_forzados is not None and not isinstance(tipos_forzados, dict):
        raise ErrorReceta("entrada.tipos_forzados debe ser un objeto")


def _validar_tipos_paso(receta: dict[str, Any]) -> None:
    pasos = receta.get("pasos", [])
    if pasos is None:
        return
    if not isinstance(pasos, list):
        return

    for numero, paso in enumerate(pasos, start=1):
        if not isinstance(paso, dict):
            continue
        tipo = str(paso.get("tipo", "")).strip().lower()
        if not tipo:
            raise ErrorReceta(f"El paso {numero} no tiene tipo")
        if tipo not in TIPOS_PASO_SOPORTADOS:
            raise ErrorReceta(f"Operación no soportada en el paso {numero}: {tipo}")


def _mensaje_validacion(error: ValidationError) -> str:
    partes: list[str] = []
    for detalle in error.errors(include_url=False, include_context=False):
        ubicacion = ".".join(str(valor) for valor in detalle["loc"])
        mensaje = str(detalle["msg"])
        partes.append(f"{ubicacion}: {mensaje}" if ubicacion else mensaje)
    return "Receta inválida: " + "; ".join(partes)


def cargar_receta(valor: str) -> RecetaConfig:
    """Carga una receta desde JSON directo o desde una ruta de archivo."""
    texto = str(valor or "").strip()

    if not texto:
        raise ErrorReceta("La receta está vacía")

    try:
        if texto.startswith("{"):
            receta_cruda = json.loads(texto)
        else:
            ruta = Path(texto)
            if not ruta.is_file():
                raise ErrorReceta(f"No existe el archivo de receta: {ruta}")
            receta_cruda = json.loads(ruta.read_text(encoding="utf-8"))
    except json.JSONDecodeError as excepcion:
        raise ErrorReceta(
            "JSON de receta inválido: "
            f"línea={excepcion.lineno}, "
            f"columna={excepcion.colno}, "
            f"detalle={excepcion.msg}"
        ) from excepcion

    if not isinstance(receta_cruda, dict):
        raise ErrorReceta("La receta debe ser un objeto JSON")

    _validar_entrada_compatible(receta_cruda)
    _validar_tipos_paso(receta_cruda)

    try:
        return RecetaConfig.model_validate(receta_cruda)
    except ValidationError as excepcion:
        raise ErrorReceta(_mensaje_validacion(excepcion)) from excepcion
