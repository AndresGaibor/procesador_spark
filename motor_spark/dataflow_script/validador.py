from __future__ import annotations

from motor_spark.dataflow_script.ast import (
    CondicionJoin,
    Etiqueta,
    Expresion,
    ProgramaDataflowScript,
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
    FUNCIONES_WHITELIST: frozenset[str] = frozenset({
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
        "IFNULL",
        "ISNULL",
        "NUM",
        "TEXT",
        "DUAL",
    })

    def __init__(self, programa: ProgramaDataflowScript) -> None:
        self._programa = programa
        self._errores: list[ErrorDataflow] = []
        self._tablas_definidas: set[str] = set()
        self._tablas_dropped: set[str] = set()
        self._conexiones: set[str] = set()
        self._aliases: dict[str, str] = {}
        self._etiquetas_definidas: set[str] = set()

    def _crear_ubicacion(self, token: object) -> None:
        pass

    def _agregar_error(self, mensaje: str, codigo: str, ubicacion=None) -> None:
        self._errores.append(
            ErrorDataflow(
                mensaje=f"Semantico [{codigo}]: {mensaje}",
                ubicacion=ubicacion,
                codigo=codigo,
            )
        )

    def _validar_expresion_funciones(self, expr: Expresion) -> None:
        if expr.tipo == TipoExpresion.FUNCION:
            if expr.valor.upper() not in self.FUNCIONES_WHITELIST:
                self._agregar_error(
                    f"Funcion no soportada: {expr.valor}",
                    "DFS_UNSUPPORTED_FUNCTION"
                )
        for hijo in expr.hijos:
            self._validar_expresion_funciones(hijo)

    def _validar_alias_duplicado(self, alias: str, ubicacion=None) -> None:
        if alias in self._aliases:
            self._agregar_error(
                f"Alias duplicado: '{alias}' ya fue usado",
                "DFS_SEMANTIC_DUPLICATE_ALIAS",
                ubicacion
            )
        self._aliases[alias] = alias

    def _validar_tabla_existe(self, tabla: str) -> bool:
        if tabla not in self._tablas_definidas or tabla in self._tablas_dropped:
            return False
        return True

    def _validar_tabla_no_dropped(self, tabla: str, ubicacion=None) -> None:
        if tabla in self._tablas_dropped:
            self._agregar_error(
                f"Tabla '{tabla}' fue eliminada (DROP TABLE) y no puede ser usada",
                "DFS_SEMANTIC_TABLE_DROPPED",
                ubicacion
            )

    def _validar_select(self, sentencia: SentenciaSelect) -> None:
        if not sentencia.esquema and not sentencia.tabla:
            self._agregar_error(
                "SELECT sin tabla origen (esquema o nombre requerido)",
                "DFS_SEMANTIC_SELECT_NO_TABLE"
            )

        if sentencia.tabla:
            if sentencia.tabla in self._tablas_dropped:
                self._validar_tabla_no_dropped(sentencia.tabla)
            elif sentencia.tabla not in self._tablas_definidas:
                self._agregar_error(
                    f"Tabla '{sentencia.tabla}' no existe o no fue definida",
                    "DFS_SEMANTIC_TABLE_NOT_FOUND"
                )

        for proy in sentencia.proyecciones:
            self._validar_expresion_funciones(proy.expresion)
            if proy.alias:
                self._validar_alias_duplicado(proy.alias)

        for cond in sentencia.condiciones_where:
            self._validar_expresion_funciones(cond)

        if sentencia.group_by:
            for g in sentencia.group_by:
                self._validar_expresion_funciones(g)

        if sentencia.join_externo:
            join = sentencia.join_externo
            if not self._validar_tabla_existe(join.derecha):
                if join.derecha not in self._tablas_definidas:
                    self._agregar_error(
                        f"Tabla en JOIN '{join.derecha}' no existe",
                        "DFS_SEMANTIC_TABLE_NOT_FOUND"
                    )
                elif join.derecha in self._tablas_dropped:
                    self._validar_tabla_no_dropped(join.derecha)

            if join.es_natural:
                if not join.derecha:
                    self._agregar_error(
                        "Natural JOIN requiere tabla derecha",
                        "DFS_SEMANTIC_NATURAL_JOIN_REQUIRES_TABLE"
                    )
            else:
                if not join.izquierda or not join.derecha:
                    self._agregar_error(
                        "Condicion de JOIN incompleta",
                        "DFS_SEMANTIC_JOIN_INCOMPLETE"
                    )

            if join.izquierda == join.derecha and not join.es_natural:
                self._agregar_error(
                    "Posible producto cartesiano: misma tabla en ambos lados del JOIN",
                    "DFS_SEMANTIC_CARTESIAN_PRODUCT"
                )

        if not sentencia.esquema and not self._conexiones:
            pass

    def _validar_load(self, sentencia: SentenciaLoad) -> None:
        if not sentencia.ruta and not sentencia.es_resident:
            self._agregar_error(
                "LOAD sin ruta ni RESIDENT",
                "DFS_SEMANTIC_LOAD_NO_SOURCE"
            )

        if sentencia.es_resident and sentencia.etiqueta_resident:
            if sentencia.etiqueta_resident not in self._etiquetas_definidas:
                self._agregar_error(
                    f"Tabla RESIDENT '{sentencia.etiqueta_resident}' no existe o no fue definida",
                    "DFS_SEMANTIC_RESIDENT_NOT_FOUND"
                )
            if sentencia.etiqueta_resident in self._tablas_dropped:
                self._validar_tabla_no_dropped(sentencia.etiqueta_resident)

    def _validar_resident(self, sentencia: SentenciaResident) -> None:
        if sentencia.etiqueta_origen not in self._etiquetas_definidas:
            self._agregar_error(
                f"Tabla RESIDENT '{sentencia.etiqueta_origen}' no existe o no fue definida aun",
                "DFS_SEMANTIC_RESIDENT_FUTURE"
            )
        if sentencia.etiqueta_origen in self._tablas_dropped:
            self._validar_tabla_no_dropped(sentencia.etiqueta_origen)

    def _validar_drop_table(self, sentencia: SentenciaDropTable) -> None:
        if sentencia.tabla not in self._tablas_definidas:
            self._agregar_error(
                f"Tabla '{sentencia.tabla}' no existe para eliminar",
                "DFS_SEMANTIC_DROP_NONEXISTENT"
            )
        self._tablas_dropped.add(sentencia.tabla)

    def _validar_store(self, sentencia: SentenciaStore) -> None:
        pass

    def _validar_concatenate(self, sentencia: SentenciaConcatenate) -> None:
        if sentencia.etiqueta_objetivo not in self._tablas_definidas:
            self._agregar_error(
                f"Tabla objetivo CONCATENATE '{sentencia.etiqueta_objetivo}' no existe",
                "DFS_SEMANTIC_CONCAT_TARGET_NOT_FOUND"
            )
        if sentencia.etiqueta_origen not in self._tablas_definidas:
            self._agregar_error(
                f"Tabla origen CONCATENATE '{sentencia.etiqueta_origen}' no existe",
                "DFS_SEMANTIC_CONCAT_SOURCE_NOT_FOUND"
            )

    def _validar_etiqueta(self, etiqueta: Etiqueta) -> None:
        self._etiquetas_definidas.add(etiqueta.nombre)
        self._aliases.clear()

        for sentencia in etiqueta.sentencias:
            if isinstance(sentencia, SentenciaSelect):
                self._validar_select(sentencia)
                self._tablas_definidas.add(etiqueta.nombre)
            elif isinstance(sentencia, SentenciaLoad):
                self._validar_load(sentencia)
                self._tablas_definidas.add(etiqueta.nombre)
            elif isinstance(sentencia, SentenciaResident):
                self._validar_resident(sentencia)
            elif isinstance(sentencia, SentenciaDropTable):
                self._validar_drop_table(sentencia)
            elif isinstance(sentencia, SentenciaStore):
                self._validar_store(sentencia)
            elif isinstance(sentencia, SentenciaConcatenate):
                self._validar_concatenate(sentencia)

    def validar(self) -> list[ErrorDataflow]:
        for global_sent in self._programa.sentencias_globales:
            if isinstance(global_sent, SentenciaLibConnectTo):
                if not global_sent.conexion:
                    self._agregar_error(
                        "LIB CONNECT TO sin nombre de conexion",
                        "DFS_SEMANTIC_CONNECT_NO_NAME"
                    )
                else:
                    self._conexiones.add(global_sent.conexion)

            elif isinstance(global_sent, SentenciaSet):
                pass

        for etiqueta in self._programa.etiquetas:
            self._validar_etiqueta(etiqueta)

        return self._errores


def validar_semantico(programa: ProgramaDataflowScript) -> list[ErrorDataflow]:
    return ValidadorSemantico(programa).validar()
