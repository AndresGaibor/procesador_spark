"""Resolución y modelo de configuración para el parámetro ``--base-destino``.

Permite desacoplar los metadatos de conexión a base de datos destino (host, puerto,
database, schema, usuario) de los secretos sensibles almacenados en
``MOTOR_SECRETOS_JSON`` o en el catálogo de conexiones.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from motor_spark.conexiones.modelos import CatalogoConexiones
from motor_spark.conexiones.secretos import AdministradorSecretos


@dataclass(frozen=True, slots=True)
class ConfiguracionBaseDestino:
    url: str
    driver: str
    usuario: str
    password: str
    esquema: str | None = None
    modo: str = "overwrite"
    propiedades: dict[str, str] | None = None

    def a_opciones_jdbc(self, tabla_ref: str) -> dict[str, str]:
        """Devuelve las opciones requeridas por PySpark DataFrameWriter JDBC."""
        nombre_tabla = str(tabla_ref).strip().lower()
        nombre_tabla = nombre_tabla.replace(" ", "_")

        esquema = str(self.esquema).strip() if self.esquema else ""
        url = str(self.url).strip()
        path_bd = ""
        match_bd = re.search(
            r"jdbc:(?:hive2|impala)://[^/]+/([^;?]+)",
            url,
            flags=re.IGNORECASE,
        )
        if match_bd:
            path_bd = match_bd.group(1).strip().strip("/")

        dbtable = nombre_tabla
        # Si la URL ya apunta a una base activa (por ejemplo default en Hive), no se
        # debe volver a prefijar el esquema en el dbtable; de lo contrario Spark
        # emite CREATE TABLE default.<tabla> y Hive falla al ver `default` como palabra reservada.
        if esquema and not (path_bd and esquema.lower() == path_bd.lower()):
            dbtable = f"{esquema}.{nombre_tabla}"

        opciones = {
            "url": url,
            "dbtable": dbtable,
            "user": self.usuario,
            "password": self.password,
            "driver": self.driver,
        }
        if re.search(r"jdbc:(?:hive2|impala)", url, flags=re.IGNORECASE) or re.search(r"hive|impala", self.driver, flags=re.IGNORECASE):
            # El driver JDBC de Hive/Impala no soporta `PreparedStatement.addBatch()`.
            # Se conserva `batchsize=1` como lo pidió el usuario; la compatibilidad se
            # resuelve en el flujo de escritura con inserción por partición y con
            # `isolationLevel=NONE` para evitar una ruta transaccional no soportada.
            opciones["batchsize"] = "1"
            opciones["isolationLevel"] = "NONE"
        if self.propiedades:
            opciones.update(self.propiedades)
        return opciones


def cargar_json_base_destino(entrada: str) -> dict[str, Any]:
    """Carga y parsea la cadena JSON o lee el archivo JSON especificado."""
    texto = entrada.strip()
    if not texto:
        raise ValueError("El parámetro --base-destino no puede estar vacío")

    # Si es una cadena JSON válida (inicia con '{' o no es un archivo existente)
    if texto.startswith("{") or not Path(texto).is_file():
        try:
            datos = json.loads(texto)
        except json.JSONDecodeError as error:
            raise ValueError(
                f"El parámetro --base-destino no contiene JSON válido: {error}"
            ) from error
        if not isinstance(datos, dict):
            raise TypeError("El parámetro --base-destino debe ser un objeto JSON")
        return datos

    archivo = Path(texto)
    try:
        contenido = archivo.read_text(encoding="utf-8")
        datos = json.loads(contenido)
    except Exception as error:
        raise ValueError(
            f"Error al leer archivo de base destino ({texto}): {error}"
        ) from error

    if not isinstance(datos, dict):
        raise TypeError("El archivo de --base-destino debe contener un objeto JSON")
    return datos


def resolver_base_destino(
    entrada: str | dict[str, Any] | ConfiguracionBaseDestino,
    *,
    catalogo: CatalogoConexiones | None = None,
    secretos: AdministradorSecretos | None = None,
) -> ConfiguracionBaseDestino:
    """Construye la ``ConfiguracionBaseDestino`` desde JSON, dict o referencia a catálogo/secretos."""
    if isinstance(entrada, ConfiguracionBaseDestino):
        return entrada

    if isinstance(entrada, str):
        datos = cargar_json_base_destino(entrada)
    elif isinstance(entrada, dict):
        datos = entrada
    else:
        raise TypeError(
            "base_destino debe ser una cadena JSON, dict o ConfiguracionBaseDestino"
        )

    admin_secretos = secretos or AdministradorSecretos()

    driver = datos.get("driver") or "org.postgresql.Driver"
    esquema = datos.get("schema") or datos.get("esquema")
    tipo_raw = str(datos.get("tipo") or "postgres").lower()
    url_raw = str(datos.get("url") or datos.get("jdbc_url") or "").lower()
    modo = datos.get("modo") or datos.get("save_mode")
    if not modo:
        if "hive" in tipo_raw or "impala" in tipo_raw or "hive" in url_raw or "impala" in url_raw:
            modo = "append"
        else:
            modo = "overwrite"
    propiedades_raw = datos.get("propiedades") or datos.get("properties") or {}

    url = datos.get("url") or datos.get("jdbc_url")
    if not url and "host" in datos:
        host = datos["host"]
        puerto = datos.get("puerto") or datos.get("port") or 5432
        database = (
            datos.get("database")
            or datos.get("db")
            or datos.get("base_datos")
            or ""
        )
        tipo = str(datos.get("tipo", "postgres")).lower()
        if "postgres" in tipo:
            url = f"jdbc:postgresql://{host}:{puerto}/{database}"
        else:
            url = f"jdbc:{tipo}://{host}:{puerto}/{database}"

    secreto_nombre = datos.get("secreto_nombre") or datos.get("secreto")

    # Mapeos simplificados como {"BASE_DESTINO": "secreto"} o {"secreto_nombre": "..."}
    if not secreto_nombre:
        for clave, valor in datos.items():
            if clave.isupper() and isinstance(valor, str):
                secreto_nombre = clave
                break

    # Si aún no hay URL, buscar si hay una conexión JDBC correspondiente en el catálogo
    conexion_catalogo = None
    if catalogo and catalogo.jdbc:
        if secreto_nombre:
            for conn in catalogo.jdbc:
                if conn.secreto_nombre == secreto_nombre or conn.nombre == secreto_nombre:
                    conexion_catalogo = conn
                    break
        if not conexion_catalogo and len(catalogo.jdbc) == 1:
            conexion_catalogo = catalogo.jdbc[0]

    if not url and conexion_catalogo:
        url = conexion_catalogo.url
        if not driver or driver == "org.postgresql.Driver":
            driver = conexion_catalogo.driver
        if not secreto_nombre:
            secreto_nombre = conexion_catalogo.secreto_nombre

    if not url:
        raise ValueError(
            "No se pudo determinar la URL JDBC para --base-destino. "
            "Especifique 'host'/'database' o 'url' en el JSON de base destino."
        )

    usuario = (
        datos.get("usuario")
        or datos.get("user")
        or datos.get("username")
        or ""
    )
    password = (
        datos.get("password")
        or datos.get("clave")
        or datos.get("contrasena")
        or ""
    )

    if secreto_nombre and admin_secretos.contiene(secreto_nombre):
        credencial = admin_secretos.obtener_obligatorio(secreto_nombre)
        u_part, sep, p_part = credencial.partition(":")
        if sep:
            if not usuario:
                usuario = u_part
            password = p_part
        else:
            if not password:
                password = credencial

    propiedades = {str(k): str(v) for k, v in propiedades_raw.items()} if propiedades_raw else None

    return ConfiguracionBaseDestino(
        url=str(url),
        driver=str(driver),
        usuario=str(usuario),
        password=str(password),
        esquema=str(esquema) if esquema else None,
        modo=str(modo),
        propiedades=propiedades,
    )
