from __future__ import annotations

import os
import re
from typing import Any, Mapping


class AdministradorSecretos:
    def __init__(self, inyectados: Mapping[str, str] | None = None) -> None:
        self._cache: dict[str, str | None] = {}
        self._inyectados = dict(inyectados or {})

    def obtener(self, nombre_env: str) -> str | None:
        if nombre_env not in self._cache:
            self._cache[nombre_env] = self._inyectados.get(
                nombre_env, os.environ.get(nombre_env)
            )
        return self._cache[nombre_env]

    def obtener_obligatorio(self, nombre_env: str) -> str:
        valor = self.obtener(nombre_env)
        if valor is None:
            raise ValueError(f"Secreto '{nombre_env}' no encontrado en entorno")
        return valor

    def contiene(self, nombre_env: str) -> bool:
        return self.obtener(nombre_env) is not None

    def _mask_value(self, valor: str) -> str:
        if len(valor) <= 4:
            return "****"
        return valor[:2] + "*" * (len(valor) - 4) + valor[-2:]


class SecureError(Exception):
    pass


class ValidadorSecretos:
    PATRON_SENSIBLE = re.compile(
        r"(password|secret|token|api_key|apikey|auth|bearer|credential)",
        re.IGNORECASE,
    )

    def __init__(self, admin: AdministradorSecretos) -> None:
        self._admin = admin

    def validar_no_exponer(self, datos: dict[str, Any]) -> list[str]:
        secretos_encontrados: list[str] = []
        for clave, valor in datos.items():
            if self.PATRON_SENSIBLE.search(clave):
                secretos_encontrados.append(clave)
        return secretos_encontrados

    def formatear_para_log(self, datos: dict[str, Any]) -> dict[str, Any]:
        resultado: dict[str, Any] = {}
        for clave, valor in datos.items():
            if self.PATRON_SENSIBLE.search(clave):
                if isinstance(valor, str) and len(valor) > 4:
                    resultado[clave] = valor[:2] + "****" + valor[-2:]
                else:
                    resultado[clave] = "****"
            else:
                resultado[clave] = valor
        return resultado


_admin_global: AdministradorSecretos | None = None


def admin_secretos() -> AdministradorSecretos:
    global _admin_global
    if _admin_global is None:
        _admin_global = AdministradorSecretos()
    return _admin_global
