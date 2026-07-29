from __future__ import annotations

from typing import Any

from motor_spark.dominio.errores import ErrorReceta


def convertir_booleano(valor: Any, predeterminado: bool) -> bool:
    if valor is None:
        return predeterminado

    if isinstance(valor, bool):
        return valor

    normalizado = str(valor).strip().lower()

    if normalizado in {"1", "true", "si", "sí", "yes"}:
        return True

    if normalizado in {"0", "false", "no"}:
        return False

    raise ErrorReceta(f"Valor booleano inválido: {valor!r}")
