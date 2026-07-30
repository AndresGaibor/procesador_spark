from __future__ import annotations

import re


class SanitizadorInput:
    PATRON_IDENTIFICADOR = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
    PATRON_NOMBRE_TABLA = re.compile(r"^[A-Za-z_][A-Za-z0-9_\.\-]*$")
    PATRON_LIB_PATH = re.compile(r"^lib://[A-Za-z0-9_\-]+(/.*)?$")

    @classmethod
    def sanitizar_identificador(cls, valor: str) -> str:
        if not cls.PATRON_IDENTIFICADOR.match(valor):
            raise ValueError(f"Identificador invalido: {valor}")
        return valor

    @classmethod
    def sanitizar_nombre_tabla(cls, valor: str) -> str:
        if not cls.PATRON_NOMBRE_TABLA.match(valor):
            raise ValueError(f"Nombre de tabla invalido: {valor}")
        return valor

    @classmethod
    def sanitizar_lib_path(cls, valor: str) -> str:
        if not cls.PATRON_LIB_PATH.match(valor):
            raise ValueError(f"Path lib:// invalido: {valor}")
        return valor

    @classmethod
    def sanitizar_ruta_local(cls, valor: str, ruta_base: str | None = None) -> str:
        import os

        if valor.startswith("~"):
            valor = os.path.expanduser(valor)

        if ruta_base and not valor.startswith("/"):
            valor = os.path.join(ruta_base, valor)

        real = os.path.realpath(valor)

        if ruta_base:
            real_base = os.path.realpath(ruta_base)
            if not real.startswith(real_base):
                raise ValueError(f"Ruta fuera de directorio base: {valor}")

        return real

    @classmethod
    def es_seguro_para_sql(cls, valor: str) -> bool:
        patrones_peligrosos = [
            r"(\bOR\b|\bAND\b).*=.*",
            r";\s*",
            r"--",
            r"/\*.*\*/",
            r"\bUNION\b",
            r"\bSELECT\b",
            r"\bINSERT\b",
            r"\bUPDATE\b",
            r"\bDELETE\b",
            r"\bDROP\b",
            r"\bEXEC\b",
            r"\bEXECUTE\b",
        ]
        for patron in patrones_peligrosos:
            if re.search(patron, valor, re.IGNORECASE):
                return False
        return True

    @classmethod
    def sanitizar_para_sql(cls, valor: str) -> str:
        if not cls.es_seguro_para_sql(valor):
            raise ValueError(f"Valor no seguro para SQL: {valor}")
        return valor


class ValidadorCatalogos:
    @staticmethod
    def validar_esquema_tabla(esquema: str, tabla: str) -> tuple[bool, str]:
        if not esquema or not tabla:
            return False, "Esquema y tabla son requeridos"

        if len(esquema) > 256 or len(tabla) > 256:
            return False, "Esquema o tabla demasiado largos"

        return True, ""

    @staticmethod
    def validar_campos(campos: tuple[str, ...]) -> list[str]:
        errores = []
        for campo in campos:
            if not SanitizadorInput.PATRON_IDENTIFICADOR.match(campo):
                errores.append(f"Campo invalido: {campo}")
        return errores
