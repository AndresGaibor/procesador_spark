"""Construcción y ejecución segura de lecturas JDBC para planes Dataflow.

Solo se interpolan identificadores validados y entrecomillados. Los valores de
conexión proceden del catálogo y los secretos se incorporan directamente como
opciones de Spark; nunca se concatenan dentro del SQL generado.
"""

from __future__ import annotations

import os
import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from motor_spark.conexiones.secretos import AdministradorSecretos
from motor_spark.dataflow_script.errores import ErrorDataflow

IDENTIFICADOR_SQL_PATRON = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
PATRON_VARIABLE_URL = re.compile(
    r"\$\{(?P<nombre>[A-Za-z_][A-Za-z0-9_]*)(?::(?P<default>[^}]*))?\}"
)

# Estas opciones controlarían el origen real o expondrían credenciales si un
# catálogo manipulado pudiera sobrescribirlas después de nuestras validaciones.
PROPIEDADES_JDBC_RESERVADAS = frozenset(
    {"url", "dbtable", "query", "driver", "user", "password"}
)


class ErrorJdbc(ErrorDataflow):
    """Error estructurado de preparación o lectura JDBC."""


@dataclass(frozen=True, slots=True)
class ColumnaJdbc:
    nombre: str
    tipo: str | None = None


@dataclass(frozen=True, slots=True)
class TablaJdbc:
    esquema: str | None
    tabla: str
    columnas: tuple[ColumnaJdbc, ...] = ()


def resolver_variables_url(url: str) -> str:
    """Resuelve ``${VARIABLE:default}`` sin permitir nombres arbitrarios.

    Una variable ausente sin valor predeterminado detiene la ejecución; dejar el
    marcador literal produciría errores de conexión difíciles de diagnosticar.
    """

    def reemplazar(coincidencia: re.Match[str]) -> str:
        nombre = coincidencia.group("nombre")
        predeterminado = coincidencia.group("default")
        valor = os.environ.get(nombre, predeterminado)
        if valor is None:
            raise ValueError(
                f"Variable de entorno {nombre!r} requerida por la URL JDBC"
            )
        if any(caracter in valor for caracter in ("\x00", "\r", "\n")):
            raise ValueError(
                f"Variable de entorno {nombre!r} contiene caracteres inválidos"
            )
        return valor

    return PATRON_VARIABLE_URL.sub(reemplazar, url)


class ConstructorSubconsulta:
    """Genera SELECT limitados a identificadores previamente validados."""

    _PALABRAS_RESERVADAS_SQL = frozenset(
        {
            "SELECT",
            "FROM",
            "WHERE",
            "JOIN",
            "LEFT",
            "RIGHT",
            "INNER",
            "OUTER",
            "ON",
            "AND",
            "OR",
            "NOT",
            "AS",
            "IN",
            "LIKE",
            "IS",
            "NULL",
            "GROUP",
            "BY",
            "HAVING",
            "ORDER",
            "UNION",
            "INSERT",
            "UPDATE",
            "DELETE",
            "CREATE",
            "DROP",
            "ALTER",
            "TABLE",
        }
    )

    def __init__(self, url: str, propiedades: dict[str, str]) -> None:
        self._url = url
        self._propiedades = dict(propiedades)
        self._errores: list[ErrorJdbc] = []

    @property
    def errores(self) -> tuple[ErrorJdbc, ...]:
        return tuple(self._errores)

    def _reportar_error(self, mensaje: str, codigo: str) -> None:
        self._errores.append(
            ErrorJdbc(
                mensaje=mensaje,
                ubicacion=None,
                codigo=codigo,
            )
        )

    def validar_identificador(self, identificador: str) -> bool:
        """Acepta solo el subconjunto portable usado por los Dataflows."""
        if not identificador:
            self._reportar_error(
                "Identificador vacio",
                "JDBC_INVALID_ID_EMPTY",
            )
            return False
        if len(identificador) > 128:
            self._reportar_error(
                f"Identificador demasiado largo: {identificador}",
                "JDBC_INVALID_ID_TOO_LONG",
            )
            return False
        if not IDENTIFICADOR_SQL_PATRON.fullmatch(identificador):
            self._reportar_error(
                f"Identificador invalido: {identificador}",
                "JDBC_INVALID_ID_FORMAT",
            )
            return False
        if identificador.upper() in self._PALABRAS_RESERVADAS_SQL:
            self._reportar_error(
                f"Identificador es palabra reservada: {identificador}",
                "JDBC_INVALID_ID_RESERVED",
            )
            return False
        return True

    @staticmethod
    def _quote_identificador(identificador: str) -> str:
        # El identificador ya pasó una allowlist de caracteres. Las comillas
        # dobles conservan mayúsculas y evitan colisiones con nombres especiales.
        return f'"{identificador}"'

    def construir_select(
        self,
        esquema: str | None,
        tabla: str,
        columnas: Sequence[str],
    ) -> str | None:
        if not columnas:
            self._reportar_error(
                "Debe especificar al menos una columna",
                "JDBC_COLUMNAS_REQUERIDAS",
            )
            return None
        if not self.validar_identificador(tabla):
            return None
        if esquema is not None and not self.validar_identificador(esquema):
            return None

        tabla_quoted = self._quote_identificador(tabla)
        referencia = (
            f"{self._quote_identificador(esquema)}.{tabla_quoted}"
            if esquema
            else tabla_quoted
        )

        columnas_quoted: list[str] = []
        for columna in columnas:
            if not self.validar_identificador(columna):
                return None
            columnas_quoted.append(self._quote_identificador(columna))

        return f"SELECT {', '.join(columnas_quoted)} FROM {referencia}"

    def construir_reader_jdbc(
        self,
        tabla: str,
        columnas: Sequence[str],
        esquema: str | None = None,
    ) -> dict[str, Any] | None:
        # ``esquema`` queda al final para conservar la API pública original.
        subconsulta = self.construir_select(esquema, tabla, columnas)
        if subconsulta is None:
            return None

        # Spark exige un alias para subconsultas JDBC en varios motores, entre
        # ellos PostgreSQL. El alias es constante y no recibe entrada del usuario.
        return {
            "properties": {
                "dbtable": f"({subconsulta}) AS qlik_dataflow_source",
            }
        }


def construir_select(
    esquema: str | None,
    tabla: str,
    columnas: Sequence[str],
    url: str,
    propiedades: dict[str, str],
) -> str | None:
    """Atajo funcional conservado para compatibilidad con llamadas existentes."""
    return ConstructorSubconsulta(url, propiedades).construir_select(
        esquema,
        tabla,
        columnas,
    )


def construir_reader_jdbc(
    tabla: str,
    columnas: Sequence[str],
    url: str,
    propiedades: dict[str, str],
    esquema: str | None = None,
) -> dict[str, Any] | None:
    """Construye las opciones de reader incluyendo el esquema cuando existe."""
    return ConstructorSubconsulta(url, propiedades).construir_reader_jdbc(
        tabla,
        columnas,
        esquema=esquema,
    )


def _seleccionar_allowlist(
    conexion,
    esquema: str | None,
    tabla: str,
):
    """Selecciona una única entrada de allowlist o falla por ambigüedad."""
    candidatas = [
        item
        for item in conexion.allowlist
        if item.tabla == tabla and (esquema is None or item.esquema == esquema)
    ]
    if not candidatas:
        referencia = f"{esquema}.{tabla}" if esquema else tabla
        raise ValueError(
            f"Tabla {referencia!r} no esta en allowlist de {conexion.nombre!r}"
        )
    if len(candidatas) > 1:
        raise ValueError(f"Tabla {tabla!r} es ambigua; debe indicarse el esquema JDBC")
    return candidatas[0]


def leer_jdbc(
    spark: Any,
    nombre_conexion: str,
    tabla: str,
    columnas: Sequence[str],
    catalogo: Any,
    secretos: AdministradorSecretos | None = None,
    esquema: str | None = None,
) -> Any:
    """Lee una tabla JDBC exacta después de validar catálogo, esquema y campos."""
    from motor_spark.conexiones.modelos import CatalogoConexiones

    if not isinstance(catalogo, CatalogoConexiones):
        # ValueError forma parte del contrato histórico de esta función.
        raise ValueError("catalogo debe ser CatalogoConexiones")  # noqa: TRY004

    conexion = catalogo.buscar_jdbc(nombre_conexion)
    if conexion is None:
        raise ValueError(f"Conexion JDBC {nombre_conexion!r} no encontrada en catalogo")

    entrada_allowlist = _seleccionar_allowlist(
        conexion,
        esquema,
        tabla,
    )
    if entrada_allowlist.campos:
        permitidas = set(entrada_allowlist.campos)
        no_permitidas = [columna for columna in columnas if columna not in permitidas]
        if no_permitidas:
            raise ValueError(
                f"Columna {no_permitidas[0]!r} no esta en allowlist para "
                f"{entrada_allowlist.esquema!r}.{tabla!r}"
            )

    propiedades_catalogo = {
        str(clave): str(valor) for clave, valor in conexion.propiedades.items()
    }
    reservadas = sorted(
        clave
        for clave in propiedades_catalogo
        if clave.lower() in PROPIEDADES_JDBC_RESERVADAS
    )
    if reservadas:
        raise ValueError(
            f"El catalogo contiene propiedad JDBC reservada: {reservadas[0]}"
        )

    administrador = secretos or AdministradorSecretos()
    credencial = administrador.obtener_obligatorio(conexion.secreto_nombre)
    usuario, separador, password = credencial.partition(":")
    if not separador or not usuario or not password:
        raise ValueError(
            f"Secreto {conexion.secreto_nombre!r} debe usar formato USUARIO:CLAVE"
        )

    esquema_real = entrada_allowlist.esquema or esquema
    constructor = ConstructorSubconsulta(
        conexion.url,
        propiedades_catalogo,
    )
    parametros = constructor.construir_reader_jdbc(
        tabla,
        columnas,
        esquema=esquema_real,
    )
    if parametros is None:
        detalle = "; ".join(error.mensaje for error in constructor.errores)
        raise ValueError(
            f"No se pudo construir la lectura JDBC de {tabla!r}: {detalle}"
        )

    url_resuelta = resolver_variables_url(conexion.url)
    opciones = {
        "url": url_resuelta,
        "dbtable": parametros["properties"]["dbtable"],
        "driver": conexion.driver,
        **propiedades_catalogo,
        "user": usuario,
        "password": password,
    }

    reader = spark.read.format("jdbc")
    for clave, valor in opciones.items():
        reader = reader.option(clave, valor)
    return reader.load()
