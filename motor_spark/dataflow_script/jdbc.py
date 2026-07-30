from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Sequence

from motor_spark.conexiones.secretos import AdministradorSecretos
from motor_spark.dataflow_script.errores import ErrorDataflow

if TYPE_CHECKING:
    pass


IDENTIFICADOR_SQL_PATRON: re.Pattern[str] = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

PATRON_SCHEMA_TABLE: re.Pattern[str] = re.compile(
    r"^(?P<schema>[a-zA-Z_][a-zA-Z0-9_]*)\.(?P<table>[a-zA-Z_][a-zA-Z0-9_]*)$"
)


class ErrorJdbc(ErrorDataflow):
    pass


@dataclass(frozen=True)
class ColumnaJdbc:
    nombre: str
    tipo: str | None = None


@dataclass(frozen=True)
class TablaJdbc:
    esquema: str | None
    tabla: str
    columnas: tuple[ColumnaJdbc, ...] = ()


class ConstructorSubconsulta:
    _PALABRAS_RESERVADAS_SQL: frozenset[str] = frozenset({
        "SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER",
        "ON", "AND", "OR", "NOT", "AS", "IN", "LIKE", "IS", "NULL",
        "GROUP", "BY", "HAVING", "ORDER", "ASC", "DESC", "LIMIT", "OFFSET",
        "UNION", "INTERSECT", "EXCEPT", "INSERT", "UPDATE", "DELETE",
        "CREATE", "DROP", "ALTER", "TABLE", "INDEX", "VIEW", "DATABASE",
        "SCHEMA", "INTO", "VALUES", "SET", "DISTINCT", "ALL",
        "CASE", "WHEN", "THEN", "ELSE", "END", "OVER", "PARTITION",
        "WINDOW", "WITH", "RECURSIVE", "EXISTS", "BETWEEN", "CROSS",
    })

    _PALABRAS_NO_PERMITIDAS_VALOR: frozenset[str] = frozenset({
        "SELECT", "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER",
        "EXEC", "EXECUTE", "UNION", "TRUNCATE", "MERGE", "--", "/*", "*/",
        ";", "xp_", "sp_", "0x",
    })

    def __init__(self, url: str, propiedades: dict[str, str]) -> None:
        self._url = url
        self._propiedades = propiedades
        self._errores: list[ErrorJdbc] = []

    @property
    def errores(self) -> tuple[ErrorJdbc, ...]:
        return tuple(self._errores)

    def validar_identificador(self, identificador: str) -> bool:
        if not identificador:
            self._reportar_error(
                f"Identificador vacio",
                "JDBC_INVALID_ID_EMPTY",
            )
            return False

        if len(identificador) > 128:
            self._reportar_error(
                f"Identificador demasiado largo: {identificador}",
                "JDBC_INVALID_ID_TOO_LONG",
            )
            return False

        if not IDENTIFICADOR_SQL_PATRON.match(identificador):
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

    def _reportar_error(
        self, mensaje: str, codigo: str, ubicacion: Any | None = None
    ) -> None:
        self._errores.append(
            ErrorJdbc(
                mensaje=mensaje,
                ubicacion=ubicacion,
                codigo=codigo,
            )
        )

    def _quote_identificador(self, identificador: str) -> str:
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

        esquema_quoted = (
            self._quote_identificador(esquema) if esquema else None
        )
        tabla_quoted = self._quote_identificador(tabla)

        if esquema_quoted:
            referencia_tabla = f"{esquema_quoted}.{tabla_quoted}"
        else:
            referencia_tabla = tabla_quoted

        columnas_validas: list[str] = []
        for col in columnas:
            if self.validar_identificador(col):
                columnas_validas.append(self._quote_identificador(col))
            else:
                return None

        select_clause = ", ".join(columnas_validas)
        subconsulta = f"SELECT {select_clause} FROM {referencia_tabla}"

        return subconsulta

    def construir_reader_jdbc(
        self,
        tabla: str,
        columnas: Sequence[str],
    ) -> dict[str, Any] | None:
        subconsulta = self.construir_select(None, tabla, columnas)
        if subconsulta is None:
            return None

        propiedades: dict[str, Any] = {
            "dbtable": f"({subconsulta})",
        }

        return {
            "properties": propiedades,
        }


def construir_select(
    esquema: str | None,
    tabla: str,
    columnas: Sequence[str],
    url: str,
    propiedades: dict[str, str],
) -> str | None:
    constructor = ConstructorSubconsulta(url, propiedades)
    return constructor.construir_select(esquema, tabla, columnas)


def construir_reader_jdbc(
    tabla: str,
    columnas: Sequence[str],
    url: str,
    propiedades: dict[str, str],
) -> dict[str, Any] | None:
    constructor = ConstructorSubconsulta(url, propiedades)
    return constructor.construir_reader_jdbc(tabla, columnas)


def leer_jdbc(
    spark: Any,
    nombre_conexion: str,
    tabla: str,
    columnas: Sequence[str],
    catalogo: Any,
    secretos: AdministradorSecretos | None = None,
) -> Any:
    from motor_spark.conexiones.modelos import ConexionJdbc, CatalogoConexiones

    if not isinstance(catalogo, CatalogoConexiones):
        raise ValueError("catalogo debe ser CatalogoConexiones")

    conn_jdbc = catalogo.buscar_jdbc(nombre_conexion)
    if conn_jdbc is None:
        raise ValueError(f"Conexion JDBC '{nombre_conexion}' no encontrada en catalogo")

    esquema_allowlist: str | None = None
    for item in conn_jdbc.allowlist:
        if item.tabla == tabla:
            esquema_allowlist = item.esquema
            campos_allowlist = item.campos
            if campos_allowlist and campos_allowlist != ():
                columnas_permitidas = set(campos_allowlist)
                for col in columnas:
                    if col not in columnas_permitidas:
                        raise ValueError(
                            f"Columna '{col}' no esta en allowlist para '{esquema_allowlist}.{tabla}'"
                        )
            break
    else:
        raise ValueError(
            f"Tabla '{tabla}' no esta en allowlist de '{nombre_conexion}'"
        )

    secretos = secretos or AdministradorSecretos()
    secreto_valor = secretos.obtener(conn_jdbc.secreto_nombre)
    if secreto_valor is None:
        raise ValueError(
            f"Secreto '{conn_jdbc.secreto_nombre}' no encontrado en entorno"
        )

    propiedades_finales = dict(conn_jdbc.propiedades)
    propiedades_finales["user"] = secreto_valor.split(":")[0] if ":" in secreto_valor else secreto_valor
    if ":" in secreto_valor:
        propiedades_finales["password"] = secreto_valor.split(":", 1)[1]

    if esquema_allowlist:
        propiedades_finales["schema"] = esquema_allowlist

    constructor = ConstructorSubconsulta(conn_jdbc.url, propiedades_finales)
    reader_params = constructor.construir_reader_jdbc(tabla, columnas)
    if reader_params is None:
        raise ValueError(f"Error construyendo reader para '{tabla}'")

    reader = (
        spark.read
        .format("jdbc")
        .option("url", conn_jdbc.url)
        .option("dbtable", reader_params["properties"]["dbtable"])
    )

    for key, value in propiedades_finales.items():
        reader = reader.option(key, value)

    return reader.load()
