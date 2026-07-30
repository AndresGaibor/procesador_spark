from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping
from typing import Any

NOMBRE_ENV_SECRETOS_JSON = "MOTOR_SECRETOS_JSON"
LIMITE_SECRETOS_JSON_BYTES = 10 * 1024 * 1024
_PATRON_NOMBRE_SECRETO = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def cargar_secretos_json_entorno(
    entorno: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Carga secretos agrupados desde ``MOTOR_SECRETOS_JSON``.

    Talend puede exponer un contexto de tipo Password como una única variable
    de entorno. El motor valida todo el objeto antes de devolver valores, para
    que una configuración parcial o ambigua falle antes de crear Spark.

    El texto original nunca se incorpora a mensajes de error: puede contener
    contraseñas y una clave privada completa codificada en Base64.
    """
    origen = os.environ if entorno is None else entorno
    contenido = origen.get(NOMBRE_ENV_SECRETOS_JSON)
    if contenido is None or not contenido.strip():
        return {}

    if len(contenido.encode("utf-8")) > LIMITE_SECRETOS_JSON_BYTES:
        raise ValueError(
            f"{NOMBRE_ENV_SECRETOS_JSON} supera el tamaño máximo permitido"
        )

    def construir_objeto_sin_duplicados(
        pares: list[tuple[str, Any]],
    ) -> dict[str, Any]:
        objeto: dict[str, Any] = {}
        for nombre, valor in pares:
            if nombre in objeto:
                raise ValueError(
                    f"{NOMBRE_ENV_SECRETOS_JSON} contiene un nombre duplicado"
                )
            objeto[nombre] = valor
        return objeto

    try:
        datos = json.loads(
            contenido,
            object_pairs_hook=construir_objeto_sin_duplicados,
        )
    except json.JSONDecodeError as error:
        raise ValueError(
            f"{NOMBRE_ENV_SECRETOS_JSON} no contiene JSON válido"
        ) from error

    if not isinstance(datos, dict):
        raise TypeError(f"{NOMBRE_ENV_SECRETOS_JSON} debe contener un objeto JSON")

    secretos: dict[str, str] = {}
    for nombre, valor in datos.items():
        if not isinstance(nombre, str) or not _PATRON_NOMBRE_SECRETO.fullmatch(nombre):
            raise ValueError(f"{NOMBRE_ENV_SECRETOS_JSON} contiene un nombre inválido")
        if not isinstance(valor, str):
            raise TypeError(f"El secreto '{nombre}' debe ser una cadena de texto")
        if not valor.strip():
            raise ValueError(f"El secreto '{nombre}' está vacío")
        if "\x00" in valor:
            raise ValueError(f"El secreto '{nombre}' contiene un carácter NUL")
        if "\n" in valor or "\r" in valor:
            raise ValueError(f"El secreto '{nombre}' contiene saltos de línea")
        secretos[nombre] = valor

    return secretos


def combinar_secretos(
    *,
    desde_json: Mapping[str, str],
    explicitos: Mapping[str, str],
) -> dict[str, str]:
    """Combina orígenes con precedencia explícita y reproducible.

    ``--secreto`` conserva la prioridad máxima por ser la forma más específica.
    Después se consulta ``MOTOR_SECRETOS_JSON`` y, finalmente, el administrador
    mantiene su fallback histórico a variables de entorno individuales.
    """
    combinados = dict(desde_json)
    combinados.update(explicitos)
    return combinados


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
        """Indica si un secreto está disponible sin revelar su valor."""
        return self.obtener(nombre_env) is not None

    def redactar_texto(self, texto: str) -> str:
        """Sustituye secretos conocidos antes de escribir errores o trazas.

        Se ordenan por longitud para que un secreto corto contenido dentro de
        otro no deje visible el resto del valor más largo.
        """
        resultado = str(texto)
        valores = {
            valor
            for valor in (*self._inyectados.values(), *self._cache.values())
            if valor
        }
        for valor in sorted(valores, key=len, reverse=True):
            resultado = resultado.replace(valor, "****")
        return resultado

    def _mask_value(self, valor: str) -> str:
        if len(valor) <= 4:
            return "****"
        return valor[:2] + "*" * (len(valor) - 4) + valor[-2:]


class SecureError(Exception):
    pass


class ValidadorSecretos:
    PATRON_SENSIBLE = re.compile(
        r"(password|passphrase|private_key|secret|token|api_key|apikey|auth|bearer|credential)",
        re.IGNORECASE,
    )

    def __init__(self, admin: AdministradorSecretos) -> None:
        self._admin = admin

    def validar_no_exponer(self, datos: dict[str, Any]) -> list[str]:
        secretos_encontrados: list[str] = []
        for clave in datos:
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
