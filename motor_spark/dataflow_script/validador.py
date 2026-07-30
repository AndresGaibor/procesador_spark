"""Validación semántica del subconjunto Qlik aceptado por el compilador.

El validador recorre etiquetas en orden y distingue tablas lógicas creadas en el
script de tablas externas leídas por SELECT JDBC. También valida una cadena de
preceding LOAD como una unidad: los LOAD superiores no declaran fuente propia
porque reciben la salida de la sentencia situada debajo.
"""

from __future__ import annotations

from collections.abc import Iterable

from motor_spark.dataflow_script.ast import (
    Etiqueta,
    Expresion,
    ProgramaDataflowScript,
    ProjectionItem,
    SentenciaConcatenate,
    SentenciaDropTable,
    SentenciaLibConnectTo,
    SentenciaLoad,
    SentenciaResident,
    SentenciaSelect,
    SentenciaSet,
    SentenciaStore,
    TipoExpresion,
)
from motor_spark.dataflow_script.errores import ErrorDataflow


class ValidadorSemantico:
    """Acumula todos los errores detectables sin ejecutar Spark ni abrir red."""

    FUNCIONES_WHITELIST: frozenset[str] = frozenset(
        {
            "CONCAT",
            "LEFT",
            "RIGHT",
            "TRIM",
            "UPPER",
            "LOWER",
            "LEN",
            "LENGTH",
            "SUBSTRING",
            "MID",
            "DATE",
            "YEAR",
            "MONTH",
            "DAY",
            "IF",
            "MATCH",
            "IFNULL",
            "ISNULL",
            "NUM",
            "TEXT",
            "DUAL",
            "COUNT",
            "SUM",
            "AVG",
            "MIN",
            "MAX",
            "FALSE",
            "COALESCE",
            "INDEXREGEX",
        }
    )

    def __init__(self, programa: ProgramaDataflowScript) -> None:
        self._programa = programa
        self._errores: list[ErrorDataflow] = []
        self._tablas_definidas: set[str] = set()
        self._tablas_dropped: set[str] = set()
        self._conexiones: set[str] = set()

    def _agregar_error(self, mensaje: str, codigo: str) -> None:
        self._errores.append(
            ErrorDataflow(
                mensaje=f"Semantico [{codigo}]: {mensaje}",
                ubicacion=None,
                codigo=codigo,
            )
        )

    def _validar_expresion_funciones(self, expresion: Expresion) -> None:
        if (
            expresion.tipo == TipoExpresion.FUNCION
            and expresion.valor.upper() not in self.FUNCIONES_WHITELIST
        ):
            self._agregar_error(
                f"Funcion no soportada: {expresion.valor}",
                "DFS_UNSUPPORTED_FUNCTION",
            )
        for hijo in expresion.hijos:
            self._validar_expresion_funciones(hijo)

    def _validar_proyecciones(
        self,
        proyecciones: Iterable[ProjectionItem],
    ) -> None:
        """Valida funciones y aliases dentro de una sola capa LOAD/SELECT."""
        aliases: set[str] = set()
        for proyeccion in proyecciones:
            self._validar_expresion_funciones(proyeccion.expresion)
            if not proyeccion.alias:
                continue
            if proyeccion.alias in aliases:
                self._agregar_error(
                    f"Alias duplicado: '{proyeccion.alias}' ya fue usado",
                    "DFS_SEMANTIC_DUPLICATE_ALIAS",
                )
            aliases.add(proyeccion.alias)

    def _tabla_disponible(self, tabla: str) -> bool:
        return tabla in self._tablas_definidas and tabla not in self._tablas_dropped

    def _validar_uso_tabla(self, tabla: str, contexto: str) -> None:
        if tabla in self._tablas_dropped:
            self._agregar_error(
                f"Tabla '{tabla}' fue eliminada y no puede usarse en {contexto}",
                "DFS_SEMANTIC_TABLE_DROPPED",
            )
        elif tabla not in self._tablas_definidas:
            self._agregar_error(
                f"Tabla '{tabla}' no existe o no fue definida",
                "DFS_SEMANTIC_TABLE_NOT_FOUND",
            )

    def _validar_select(self, sentencia: SentenciaSelect) -> None:
        referencia_local = sentencia.tabla or sentencia.esquema or ""
        if not referencia_local:
            self._agregar_error(
                "SELECT sin tabla origen",
                "DFS_SEMANTIC_SELECT_NO_TABLE",
            )
        elif sentencia.esquema and sentencia.tabla:
            # Una referencia esquema.tabla pertenece al origen JDBC; no tiene
            # que haber sido creada previamente como tabla lógica del script.
            if not self._conexiones:
                self._agregar_error(
                    "SELECT JDBC sin LIB CONNECT TO",
                    "DFS_SEMANTIC_SELECT_NO_CONNECTION",
                )
        else:
            self._validar_uso_tabla(referencia_local, "SELECT")

        self._validar_proyecciones(sentencia.proyecciones)
        for condicion in sentencia.condiciones_where:
            self._validar_expresion_funciones(condicion)
        for grupo in sentencia.group_by:
            self._validar_expresion_funciones(grupo)

        if sentencia.join_externo:
            join = sentencia.join_externo
            self._validar_uso_tabla(join.derecha, "JOIN")
            if not join.es_natural and (not join.izquierda or not join.derecha):
                self._agregar_error(
                    "Condicion de JOIN incompleta",
                    "DFS_SEMANTIC_JOIN_INCOMPLETE",
                )
            if join.izquierda == join.derecha and not join.es_natural:
                self._agregar_error(
                    "Posible producto cartesiano: misma tabla en ambos lados",
                    "DFS_SEMANTIC_CARTESIAN_PRODUCT",
                )

    def _validar_load(
        self,
        sentencia: SentenciaLoad,
        *,
        fuente_en_cadena: bool,
    ) -> None:
        if not sentencia.ruta and not sentencia.es_resident and not fuente_en_cadena:
            self._agregar_error(
                "LOAD sin ruta ni RESIDENT",
                "DFS_SEMANTIC_LOAD_NO_SOURCE",
            )

        if sentencia.es_resident and sentencia.etiqueta_resident:
            self._validar_uso_tabla(
                sentencia.etiqueta_resident,
                "LOAD RESIDENT",
            )

        if sentencia.concatenate_objetivo:
            self._validar_uso_tabla(
                sentencia.concatenate_objetivo,
                "CONCATENATE",
            )
        if sentencia.join_objetivo:
            self._validar_uso_tabla(
                sentencia.join_objetivo,
                "LEFT JOIN",
            )

        self._validar_proyecciones(sentencia.proyecciones)
        for condicion in sentencia.condiciones_where:
            self._validar_expresion_funciones(condicion)
        for grupo in sentencia.group_by:
            self._validar_expresion_funciones(grupo)

    def _validar_resident(self, sentencia: SentenciaResident) -> None:
        if not self._tabla_disponible(sentencia.etiqueta_origen):
            self._agregar_error(
                f"Tabla RESIDENT '{sentencia.etiqueta_origen}' no existe o "
                "no fue definida aun",
                "DFS_SEMANTIC_RESIDENT_FUTURE",
            )
        if sentencia.expresion:
            self._validar_expresion_funciones(sentencia.expresion)

    def _validar_drop_table(self, sentencia: SentenciaDropTable) -> None:
        if not self._tabla_disponible(sentencia.tabla):
            self._agregar_error(
                f"Tabla '{sentencia.tabla}' no existe para eliminar",
                "DFS_SEMANTIC_DROP_NONEXISTENT",
            )
        self._tablas_dropped.add(sentencia.tabla)

    def _validar_store(self, sentencia: SentenciaStore) -> None:
        self._validar_uso_tabla(sentencia.tabla, "STORE")
        if not sentencia.ruta_destino:
            self._agregar_error(
                "STORE sin ruta de destino",
                "DFS_SEMANTIC_STORE_NO_DESTINATION",
            )

    def _validar_concatenate(
        self,
        sentencia: SentenciaConcatenate,
    ) -> None:
        self._validar_uso_tabla(
            sentencia.etiqueta_objetivo,
            "CONCATENATE",
        )
        if sentencia.etiqueta_origen:
            self._validar_uso_tabla(
                sentencia.etiqueta_origen,
                "CONCATENATE",
            )

    @staticmethod
    def _tiene_fuente_productora(etiqueta: Etiqueta) -> bool:
        """Indica si una cadena contiene SELECT, FROM o RESIDENT fuente."""
        return any(
            isinstance(sentencia, SentenciaSelect)
            or (
                isinstance(sentencia, SentenciaLoad)
                and (sentencia.ruta or sentencia.es_resident)
            )
            or isinstance(sentencia, SentenciaResident)
            for sentencia in etiqueta.sentencias
        )

    def _validar_etiqueta(self, etiqueta: Etiqueta) -> None:
        fuente_en_cadena = self._tiene_fuente_productora(etiqueta)

        for sentencia in etiqueta.sentencias:
            produce_tabla = isinstance(
                sentencia,
                (SentenciaSelect, SentenciaLoad, SentenciaResident),
            )
            if isinstance(sentencia, SentenciaSelect):
                self._validar_select(sentencia)
            elif isinstance(sentencia, SentenciaLoad):
                self._validar_load(
                    sentencia,
                    fuente_en_cadena=fuente_en_cadena,
                )
            elif isinstance(sentencia, SentenciaResident):
                self._validar_resident(sentencia)
            elif isinstance(sentencia, SentenciaDropTable):
                self._validar_drop_table(sentencia)
            elif isinstance(sentencia, SentenciaStore):
                self._validar_store(sentencia)
            elif isinstance(sentencia, SentenciaConcatenate):
                self._validar_concatenate(sentencia)

            # STORE y DROP que aparecen después de LOAD dentro de la misma
            # etiqueta deben poder referenciar la tabla recién creada.
            if produce_tabla and etiqueta.nombre != "_anonima":
                self._tablas_definidas.add(etiqueta.nombre)
                self._tablas_dropped.discard(etiqueta.nombre)

    def validar(self) -> list[ErrorDataflow]:
        """Valida globales primero y luego las etiquetas en orden de script."""
        for sentencia in self._programa.sentencias_globales:
            if isinstance(sentencia, SentenciaLibConnectTo):
                if not sentencia.conexion:
                    self._agregar_error(
                        "LIB CONNECT TO sin nombre de conexion",
                        "DFS_SEMANTIC_CONNECT_NO_NAME",
                    )
                else:
                    self._conexiones.add(sentencia.conexion)
            elif isinstance(sentencia, SentenciaSet) and not sentencia.variable:
                self._agregar_error(
                    "SET sin nombre de variable",
                    "DFS_SEMANTIC_SET_NO_NAME",
                )

        for etiqueta in self._programa.etiquetas:
            self._validar_etiqueta(etiqueta)

        return self._errores


def validar_semantico(
    programa: ProgramaDataflowScript,
) -> list[ErrorDataflow]:
    """API funcional del validador semántico."""
    return ValidadorSemantico(programa).validar()
