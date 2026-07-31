"""Resuelve paquetes Maven JDBC aprobados antes de iniciar Spark."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

PAQUETES_POR_DRIVER = {
    "org.postgresql.Driver": "org.postgresql:postgresql:42.7.7",
    "com.microsoft.sqlserver.jdbc.SQLServerDriver": (
        "com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11"
    ),
    "com.mysql.cj.jdbc.Driver": "com.mysql:mysql-connector-j:8.4.0",
    "org.mariadb.jdbc.Driver": "org.mariadb.jdbc:mariadb-java-client:3.5.2",
}


def _obtener_catalogo(argv: Sequence[str]) -> str | None:
    parser = argparse.ArgumentParser(add_help=False)
    grupo = parser.add_mutually_exclusive_group()
    grupo.add_argument("--conexiones")
    grupo.add_argument("--conexiones-contenido")
    argumentos, _ = parser.parse_known_args(argv)

    if argumentos.conexiones_contenido is not None:
        return argumentos.conexiones_contenido
    if argumentos.conexiones is not None:
        return Path(argumentos.conexiones).read_text(encoding="utf-8")
    return None


def resolver_paquetes_jdbc(argv: Sequence[str]) -> tuple[str, ...]:
    """Devuelve paquetes Maven para los drivers JDBC referenciados por el catálogo.

    Las coordenadas se definen exclusivamente en ``PAQUETES_POR_DRIVER``. El
    catálogo nunca puede indicar un artefacto Maven arbitrario.
    """
    contenido = _obtener_catalogo(argv)
    if contenido is None:
        return ()

    try:
        catalogo: Any = json.loads(contenido)
    except json.JSONDecodeError as error:
        raise ValueError("El catálogo de conexiones no contiene JSON válido") from error

    if not isinstance(catalogo, dict):
        raise TypeError("El catálogo de conexiones debe ser un objeto JSON")

    paquetes: list[str] = []
    for conexion in catalogo.get("jdbc", []):
        if not isinstance(conexion, dict):
            raise TypeError("Cada conexión JDBC debe ser un objeto JSON")
        driver = conexion.get("driver")
        if not isinstance(driver, str) or not driver.strip():
            raise ValueError("Cada conexión JDBC debe declarar un driver válido")
        paquete = PAQUETES_POR_DRIVER.get(driver)
        if paquete is None:
            raise ValueError(
                f"Driver JDBC no soportado para instalación automática: {driver!r}"
            )
        if paquete not in paquetes:
            paquetes.append(paquete)
    return tuple(paquetes)


def main(argv: Sequence[str] | None = None) -> int:
    import sys

    argumentos = tuple(sys.argv[1:] if argv is None else argv)
    try:
        print(",".join(resolver_paquetes_jdbc(argumentos)))
    except (OSError, TypeError, ValueError) as error:
        print(f"Error resolviendo drivers JDBC: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
