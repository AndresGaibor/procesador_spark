from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from motor_spark.conexiones.modelos import (
    CampoAllowlist,
    CatalogoConexiones,
    ConexionJdbc,
    ConexionLocal,
    ConexionSftp,
)


class CargadorConexiones:
    def __init__(self, ruta_catalogo: str | Path) -> None:
        self._ruta = Path(ruta_catalogo)

    def cargar(self) -> CatalogoConexiones:
        if not self._ruta.exists():
            raise FileNotFoundError(
                f"Catalogo de conexiones no encontrado: {self._ruta}"
            )

        contenido = self._ruta.read_text(encoding="utf-8")
        datos = json.loads(contenido)

        jdbc = []
        for j in datos.get("jdbc", []):
            jdbc.append(
                ConexionJdbc(
                    tipo=j.get("tipo", "jdbc"),
                    nombre=j["nombre"],
                    url=j["url"],
                    driver=j["driver"],
                    secreto_nombre=j["secreto_nombre"],
                    allowlist=tuple(
                        CampoAllowlist(
                            esquema=a["esquema"],
                            tabla=a["tabla"],
                            campos=tuple(a.get("campos", [])),
                        )
                        for a in j.get("allowlist", [])
                    ),
                    propiedades=j.get("propiedades", {}),
                )
            )

        locales = []
        for l in datos.get("locales", []):
            locales.append(
                ConexionLocal(
                    tipo=l.get("tipo", "local"),
                    nombre=l["nombre"],
                    ruta_base=l["ruta_base"],
                    allowlist=tuple(
                        CampoAllowlist(
                            esquema=a.get("esquema", ""),
                            tabla=a["tabla"],
                            campos=tuple(a.get("campos", [])),
                        )
                        for a in l.get("allowlist", [])
                    ),
                )
            )

        sftp = []
        for s in datos.get("sftp", []):
            sftp.append(
                ConexionSftp(
                    tipo=s.get("tipo", "sftp"),
                    nombre=s["nombre"],
                    host=s["host"],
                    puerto=s.get("puerto", 22),
                    secreto_nombre=s["secreto_nombre"],
                    ruta_base=s.get("ruta_base", "/"),
                    allowlist=tuple(
                        CampoAllowlist(
                            esquema=a.get("esquema", ""),
                            tabla=a["tabla"],
                            campos=tuple(a.get("campos", [])),
                        )
                        for a in s.get("allowlist", [])
                    ),
                )
            )

        return CatalogoConexiones(
            version=datos.get("version", 1),
            descripcion=datos.get("descripcion", ""),
            jdbc=tuple(jdbc),
            locales=tuple(locales),
            sftp=tuple(sftp),
        )


def cargar_catalogo(ruta: str | Path) -> CatalogoConexiones:
    return CargadorConexiones(ruta).cargar()


class ResolvedorConexiones:
    def __init__(self, catalogo: CatalogoConexiones) -> None:
        self._catalogo = catalogo
        self._cache: dict[str, Any] = {}

    def resolver_jdbc(self, nombre: str) -> dict[str, Any] | None:
        if nombre in self._cache:
            return self._cache[nombre]

        conn = self._catalogo.buscar_jdbc(nombre)
        if not conn:
            return None

        return {
            "tipo": "jdbc",
            "nombre": conn.nombre,
            "url": conn.url,
            "driver": conn.driver,
            "secreto_nombre": conn.secreto_nombre,
            "propiedades": conn.propiedades,
        }

    def resolver_local(self, nombre: str) -> dict[str, Any] | None:
        if nombre in self._cache:
            return self._cache[nombre]

        conn = self._catalogo.buscar_local(nombre)
        if not conn:
            return None

        return {
            "tipo": "local",
            "nombre": conn.nombre,
            "ruta_base": conn.ruta_base,
        }

    def resolver_sftp(self, nombre: str) -> dict[str, Any] | None:
        if nombre in self._cache:
            return self._cache[nombre]

        conn = self._catalogo.buscar_sftp(nombre)
        if not conn:
            return None

        return {
            "tipo": "sftp",
            "nombre": conn.nombre,
            "host": conn.host,
            "puerto": conn.puerto,
            "secreto_nombre": conn.secreto_nombre,
            "ruta_base": conn.ruta_base,
        }


def crear_resolvedor(catalogo: CatalogoConexiones) -> ResolvedorConexiones:
    return ResolvedorConexiones(catalogo)
