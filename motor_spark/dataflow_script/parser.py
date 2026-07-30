from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace

from motor_spark.dataflow_script.ast import (
    CondicionJoin,
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
from motor_spark.dataflow_script.errores import (
    ErrorDataflow,
    SourceLocation,
    SourceSpan,
)
from motor_spark.dataflow_script.lexer import Token

TipoSentenciaEtiqueta = (
    SentenciaSelect
    | SentenciaLoad
    | SentenciaResident
    | SentenciaDropTable
    | SentenciaStore
    | SentenciaConcatenate
)


class ParserError(Exception):
    def __init__(
        self,
        mensaje: str,
        ubicacion: SourceLocation | None = None,
        codigo: str | None = None,
    ) -> None:
        self.mensaje = mensaje
        self.ubicacion = ubicacion
        self.codigo = codigo
        super().__init__(mensaje)


class Parser:
    RESERVADAS_SIN_SOPORTE: frozenset[str] = frozenset(
        {
            "UNION",
            "INTERSECT",
            "EXCEPT",
            "ORDER",
            "HAVING",
            "LIMIT",
            "OFFSET",
            "UPDATE",
            "DELETE",
            "INSERT",
            "CROSS",
            "RIGHT",
            "FULL",
            "INTO",
            "CASE",
            "WHEN",
            "THEN",
            "ELSE",
            "END",
        }
    )

    FUNCIONES_SOPORTADAS: frozenset[str] = frozenset(
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

    def __init__(self, tokens: list[Token]) -> None:
        self._tokens: list[Token] = tokens
        self._pos: int = 0
        self._errores: list[ErrorDataflow] = []
        # Dentro de un SELECT SQL, PostgreSQL usa comillas dobles para
        # identificadores. Fuera de ese contexto, STRING conserva su semántica
        # literal de Qlik.
        self._en_select_sql: bool = False

    def _actual(self) -> Token:
        if self._pos < len(self._tokens):
            return self._tokens[self._pos]
        return self._tokens[-1]

    def _avanzar(self) -> Token:
        token = self._actual()
        self._pos += 1
        return token

    def _es_tipo(self, tipo: str) -> bool:
        return self._actual().tipo == tipo

    def _es_valor(self, valor: str) -> bool:
        return self._actual().valor == valor.upper()

    def _es_valor_lower(self, valor: str) -> bool:
        return self._actual().valor == valor.lower()

    def _esperar(
        self, tipo: str, mensaje: str, codigo: str = "DFS_SYNTAX_EXPECTED"
    ) -> Token:
        if not self._es_tipo(tipo):
            self._errores.append(
                ErrorDataflow(
                    mensaje=f"Parser [{codigo}]: {mensaje}",
                    ubicacion=self._crear_ubicacion_actual(),
                    codigo=codigo,
                )
            )
        return self._avanzar()

    def _crear_ubicacion_actual(self) -> SourceLocation:
        token = self._actual()
        return SourceLocation(
            inicio=SourceSpan(
                linea=token.linea, columna=token.columna, offset=token.offset
            ),
            fin=SourceSpan(
                linea=token.linea,
                columna=token.columna + len(token.valor),
                offset=token.offset + len(token.valor),
            ),
        )

    def _reportar_error(self, mensaje: str, codigo: str) -> None:
        self._errores.append(
            ErrorDataflow(
                mensaje=f"Parser [{codigo}]: {mensaje}",
                ubicacion=self._crear_ubicacion_actual(),
                codigo=codigo,
            )
        )

    def _parse_expresion(self) -> Expresion:
        return self._parse_expresion_or()

    def _parse_expresion_or(self) -> Expresion:
        izquierda = self._parse_expresion_and()
        while self._es_valor("OR"):
            self._avanzar()
            derecha = self._parse_expresion_and()
            izquierda = Expresion(
                tipo=TipoExpresion.OPERACION_BINARIA,
                valor="OR",
                hijos=(izquierda, derecha),
            )
        return izquierda

    def _parse_expresion_and(self) -> Expresion:
        izquierda = self._parse_expresion_not()
        while self._es_valor("AND"):
            self._avanzar()
            derecha = self._parse_expresion_not()
            izquierda = Expresion(
                tipo=TipoExpresion.OPERACION_BINARIA,
                valor="AND",
                hijos=(izquierda, derecha),
            )
        return izquierda

    def _parse_expresion_not(self) -> Expresion:
        if self._es_valor("NOT"):
            self._avanzar()
            operando = self._parse_expresion_not()
            return Expresion(
                tipo=TipoExpresion.OPERACION_BINARIA,
                valor="NOT",
                hijos=(operando,),
            )
        return self._parse_expresion_comparacion()

    def _parse_expresion_comparacion(self) -> Expresion:
        izquierda = self._parse_expresion_aditiva()
        while self._es_tipo("SIMBOLO") and self._actual().valor in (
            "=",
            "<",
            ">",
            "<=",
            ">=",
            "<>",
        ):
            op = self._avanzar()
            derecha = self._parse_expresion_aditiva()
            izquierda = Expresion(
                tipo=TipoExpresion.OPERACION_BINARIA,
                valor=op.valor,
                hijos=(izquierda, derecha),
            )
        return izquierda

    def _parse_expresion_aditiva(self) -> Expresion:
        izquierda = self._parse_expresion_multiplicativa()
        while self._es_tipo("SIMBOLO") and self._actual().valor in ("+", "-"):
            op = self._avanzar()
            derecha = self._parse_expresion_multiplicativa()
            izquierda = Expresion(
                tipo=TipoExpresion.OPERACION_BINARIA,
                valor=op.valor,
                hijos=(izquierda, derecha),
            )
        return izquierda

    def _parse_expresion_multiplicativa(self) -> Expresion:
        izquierda = self._parse_expresion_unaria()
        while self._es_tipo("SIMBOLO") and self._actual().valor in ("*", "/"):
            op = self._avanzar()
            derecha = self._parse_expresion_unaria()
            izquierda = Expresion(
                tipo=TipoExpresion.OPERACION_BINARIA,
                valor=op.valor,
                hijos=(izquierda, derecha),
            )
        return izquierda

    def _parse_expresion_unaria(self) -> Expresion:
        if self._es_tipo("SIMBOLO") and self._actual().valor == "(":
            return self._parse_expresion_grupo()
        if self._es_valor("NOT"):
            self._avanzar()
            operando = self._parse_expresion_unaria()
            return Expresion(
                tipo=TipoExpresion.OPERACION_BINARIA,
                valor="NOT",
                hijos=(operando,),
            )
        if self._es_tipo("SIMBOLO") and self._actual().valor == "-":
            self._avanzar()
            operando = self._parse_expresion_unaria()
            # El operando se analiza una sola vez. La versión anterior llamaba
            # nuevamente al parser y consumía el token siguiente por error.
            return Expresion(
                tipo=TipoExpresion.OPERACION_BINARIA,
                valor="NEGATE",
                hijos=(operando,),
            )
        return self._parse_expresion_primaria()

    def _parse_expresion_grupo(self) -> Expresion:
        self._avanzar()
        expr = self._parse_expresion_or()
        self._esperar("SIMBOLO", "Se esperaba ')'", "DFS_SYNTAX_MISSING_RPAREN")
        return expr

    def _parse_expresion_primaria(self) -> Expresion:
        if self._es_valor("DISTINCT"):
            self._avanzar()
            return Expresion(
                tipo=TipoExpresion.COLUMNA,
                valor="DISTINCT",
            )

        if self._es_tipo("STRING"):
            valor = self._avanzar().valor
            if self._en_select_sql and valor.startswith('"') and valor.endswith('"'):
                return Expresion(
                    tipo=TipoExpresion.COLUMNA,
                    valor=valor[1:-1],
                )
            return Expresion(tipo=TipoExpresion.LITERAL_STRING, valor=valor)

        if self._es_tipo("NUMERO"):
            valor = self._avanzar().valor
            return Expresion(tipo=TipoExpresion.LITERAL_NUMERO, valor=valor)

        if self._es_tipo("BRACKET_ID"):
            valor = self._avanzar().valor
            return Expresion(tipo=TipoExpresion.COLUMNA, valor=valor)

        if self._es_valor("CONCAT"):
            return self._parse_funcion_concat()

        if self._es_tipo("IDENTIFICADOR"):
            return self._parse_columna_o_funcion()

        if (
            self._es_tipo("PALABRA_RESERVADA")
            and self._tokens[self._pos + 1].valor == "("
        ):
            return self._parse_columna_o_funcion()

        return Expresion(tipo=TipoExpresion.LITERAL_STRING, valor="''")

    def _parse_columna_o_funcion(self) -> Expresion:
        nombre = self._avanzar().valor

        if nombre.upper() in self.RESERVADAS_SIN_SOPORTE:
            self._reportar_error(
                f"Constructo no soportado: {nombre}", "DFS_UNSUPPORTED_CONSTRUCT"
            )

        if nombre.upper() == "WINDOW":
            return self._parse_funcion_window()

        if nombre.upper() == "WRANK":
            return self._parse_funcion_wrank()

        if self._es_tipo("SIMBOLO") and self._actual().valor == "(":
            self._avanzar()
            argumentos: tuple[Expresion, ...] = ()
            if not (self._es_tipo("SIMBOLO") and self._actual().valor == ")"):
                argumentos = (self._parse_expresion(),)
                # COUNT(DISTINCT campo) no separa DISTINCT y campo con coma.
                # Conservamos ambos como hijos para que el compilador aplique
                # countDistinct sin perder la intención del script.
                if (
                    nombre.upper() == "COUNT"
                    and argumentos[0].valor.upper() == "DISTINCT"
                    and not (self._es_tipo("SIMBOLO") and self._actual().valor == ")")
                ):
                    argumentos += (self._parse_expresion(),)
                while self._es_tipo("SIMBOLO") and self._actual().valor == ",":
                    self._avanzar()
                    argumentos += (self._parse_expresion(),)
            self._esperar("SIMBOLO", "Se esperaba ')'", "DFS_SYNTAX_MISSING_RPAREN")

            if nombre.upper() not in self.FUNCIONES_SOPORTADAS:
                self._reportar_error(
                    f"Funcion no soportada: {nombre}", "DFS_UNSUPPORTED_FUNCTION"
                )

            return Expresion(tipo=TipoExpresion.FUNCION, valor=nombre, hijos=argumentos)

        return Expresion(tipo=TipoExpresion.COLUMNA, valor=nombre)

    def _parse_funcion_window(self) -> Expresion:
        self._esperar("SIMBOLO", "Se esperaba '('", "DFS_SYNTAX_MISSING_LPAREN")
        argumentos: tuple[Expresion, ...] = ()
        if not (self._es_tipo("SIMBOLO") and self._actual().valor == ")"):
            argumentos = (self._parse_expresion(),)
            while self._es_tipo("SIMBOLO") and self._actual().valor == ",":
                self._avanzar()
                argumentos += (self._parse_expresion(),)
        self._esperar("SIMBOLO", "Se esperaba ')'", "DFS_SYNTAX_MISSING_RPAREN")
        return Expresion(tipo=TipoExpresion.WINDOW, valor="Window", hijos=argumentos)

    def _parse_funcion_wrank(self) -> Expresion:
        self._esperar("SIMBOLO", "Se esperaba '('", "DFS_SYNTAX_MISSING_LPAREN")
        argumentos: tuple[Expresion, ...] = ()
        if not (self._es_tipo("SIMBOLO") and self._actual().valor == ")"):
            argumentos = (self._parse_expresion(),)
            while self._es_tipo("SIMBOLO") and self._actual().valor == ",":
                self._avanzar()
                argumentos += (self._parse_expresion(),)
        self._esperar("SIMBOLO", "Se esperaba ')'", "DFS_SYNTAX_MISSING_RPAREN")
        return Expresion(
            tipo=TipoExpresion.WINDOW_RANK, valor="WRank", hijos=argumentos
        )

    def _parse_funcion_concat(self) -> Expresion:
        self._avanzar()
        self._esperar("SIMBOLO", "Se esperaba '('", "DFS_SYNTAX_MISSING_LPAREN")
        argumentos: tuple[Expresion, ...] = ()
        if not (self._es_tipo("SIMBOLO") and self._actual().valor == ")"):
            argumentos = (self._parse_expresion(),)
            while self._es_tipo("SIMBOLO") and self._actual().valor == ",":
                self._avanzar()
                argumentos += (self._parse_expresion(),)
        self._esperar("SIMBOLO", "Se esperaba ')'", "DFS_SYNTAX_MISSING_RPAREN")
        return Expresion(
            tipo=TipoExpresion.CONCATENACION, valor="CONCAT", hijos=argumentos
        )

    def _parse_proyeccion_select(self) -> Sequence[ProjectionItem]:
        proyecciones: list[ProjectionItem] = []

        while True:
            expr = self._parse_expresion()
            alias: str | None = None

            if self._es_valor("AS"):
                self._avanzar()
                if self._es_tipo("IDENTIFICADOR") or self._es_tipo("BRACKET_ID"):
                    alias = self._avanzar().valor

            proyecciones.append(ProjectionItem(expresion=expr, alias=alias))

            if not (self._es_tipo("SIMBOLO") and self._actual().valor == ","):
                break
            self._avanzar()

        return proyecciones

    def _parse_nombre_tabla(self) -> tuple[str | None, str]:
        esquema: str | None = None
        tabla: str = ""

        if self._es_tipo("BRACKET_ID"):
            tabla = self._avanzar().valor
            return esquema, tabla

        if self._es_tipo("IDENTIFICADOR"):
            esquema = self._avanzar().valor

            if self._es_tipo("SIMBOLO") and self._actual().valor == ".":
                self._avanzar()
                if self._es_tipo("IDENTIFICADOR") or self._es_tipo("BRACKET_ID"):
                    tabla = self._avanzar().valor
                else:
                    self._errores.append(
                        ErrorDataflow(
                            mensaje="Parser [DFS_SYNTAX_TABLE_EXPECTED]: Nombre de tabla esperado despues de '.'",
                            ubicacion=self._crear_ubicacion_actual(),
                            codigo="DFS_SYNTAX_TABLE_EXPECTED",
                        )
                    )
            else:
                tabla = esquema
                esquema = None

        return esquema, tabla

    def _parse_condiciones_join(self) -> CondicionJoin | None:
        if not self._es_valor("LEFT"):
            return None

        self._avanzar()

        if not self._es_valor("JOIN"):
            self._errores.append(
                ErrorDataflow(
                    mensaje="Parser [DFS_SYNTAX_JOIN_MISSING]: Se esperaba JOIN despues de LEFT",
                    ubicacion=self._crear_ubicacion_actual(),
                    codigo="DFS_SYNTAX_JOIN_MISSING",
                )
            )
            return None

        self._avanzar()

        if self._es_tipo("SIMBOLO") and self._actual().valor == "(":
            self._avanzar()
            _, tabla_joined = self._parse_nombre_tabla()
            self._esperar("SIMBOLO", "Se esperaba ')'", "DFS_SYNTAX_MISSING_RPAREN")
        else:
            _, tabla_joined = self._parse_nombre_tabla()

        if self._es_valor("ON"):
            self._avanzar()

            izquierda_col = ""
            derecha_col = ""
            lado_derecha = False

            while not self._es_tipo("FIN") and not (
                self._es_tipo("SIMBOLO") and self._actual().valor in (";", ")")
            ):
                if self._es_tipo("SIMBOLO") and self._actual().valor == "=":
                    self._avanzar()
                    lado_derecha = True
                    continue
                token = self._avanzar()
                if token.tipo == "IDENTIFICADOR" or token.tipo == "BRACKET_ID":
                    if not lado_derecha:
                        if izquierda_col:
                            izquierda_col += "."
                        izquierda_col += token.valor
                    else:
                        if derecha_col:
                            derecha_col += "."
                        derecha_col += token.valor
                elif token.valor == ".":
                    continue
                else:
                    pass

            while not self._es_tipo("FIN") and not (
                self._es_tipo("SIMBOLO") and self._actual().valor in (";", ")")
            ):
                self._avanzar()

            return CondicionJoin(
                izquierda=izquierda_col or "",
                derecha=derecha_col or "",
                tipo="=",
                es_natural=False,
            )

        return CondicionJoin(
            izquierda="", derecha=tabla_joined, tipo="=", es_natural=True
        )

    def _parse_select(self) -> SentenciaSelect:
        self._avanzar()

        if self._es_valor("DISTINCT"):
            self._avanzar()
            self._reportar_error(
                "SELECT DISTINCT no soportado", "DFS_UNSUPPORTED_DISTINCT"
            )

        estado_select_anterior = self._en_select_sql
        self._en_select_sql = True
        proyecciones = self._parse_proyeccion_select()

        # La fuente solo puede leerse después de consumir FROM. La versión
        # anterior intentaba analizarla también antes de FROM; ese primer pase
        # no consumía nada útil y ocultaba errores por pura casualidad.
        self._esperar(
            "PALABRA_RESERVADA",
            "Se esperaba FROM",
            "DFS_SYNTAX_FROM_MISSING",
        )

        if self._es_tipo("STRING"):
            esquema_str = self._avanzar().valor.strip('"').strip("'")
            esquema = esquema_str
            tabla = ""
            if self._es_tipo("SIMBOLO") and self._actual().valor == ".":
                self._avanzar()
                if self._es_tipo("STRING"):
                    tabla = self._avanzar().valor.strip('"').strip("'")
        else:
            esquema, tabla = self._parse_nombre_tabla()

        join_externo = self._parse_condiciones_join()

        group_by: tuple[Expresion, ...] = ()
        if self._es_valor("GROUP"):
            self._avanzar()
            if self._es_valor("BY"):
                self._avanzar()
                group_by = (self._parse_expresion(),)
                while self._es_tipo("SIMBOLO") and self._actual().valor == ",":
                    self._avanzar()
                    group_by += (self._parse_expresion(),)
            else:
                self._reportar_error(
                    "Se esperaba BY despues de GROUP", "DFS_SYNTAX_BY_MISSING"
                )

        condiciones_where: tuple[Expresion, ...] = ()
        if self._es_valor("WHERE"):
            self._avanzar()
            condiciones_where = (self._parse_expresion(),)

        self._en_select_sql = estado_select_anterior
        return SentenciaSelect(
            proyecciones=proyecciones,
            tabla=tabla,
            esquema=esquema,
            join_externo=join_externo,
            condiciones_where=condiciones_where,
            group_by=group_by,
        )

    def _parse_load(
        self,
        *,
        noconcatenate_prefijo: bool = False,
    ) -> SentenciaLoad:
        """Analiza LOAD sin perder expresiones calculadas ni cláusulas finales."""
        self._avanzar()

        noconcatenate = noconcatenate_prefijo
        distinct = False
        if self._es_valor("NOCONCATENATE"):
            noconcatenate = True
            self._avanzar()
        if self._es_valor("DISTINCT"):
            self._avanzar()
            distinct = True

        # LOAD y SELECT comparten la misma forma de lista de proyección. En LOAD
        # las comillas dobles no se fuerzan a identificador SQL porque seguimos
        # dentro del lenguaje Qlik.
        proyecciones = tuple(self._parse_proyeccion_select())
        campos = tuple(
            item.expresion.valor
            for item in proyecciones
            if item.expresion.tipo == TipoExpresion.COLUMNA
            and item.alias in (None, item.expresion.valor)
        )

        ruta = ""
        es_resident = False
        etiqueta_resident: str | None = None
        if self._es_valor("FROM"):
            self._avanzar()
            if self._es_tipo("STRING"):
                ruta = self._avanzar().valor.strip('"').strip("'")
            elif (
                self._es_tipo("IDENTIFICADOR")
                or self._es_tipo("LIB_URI")
                or self._es_tipo("BRACKET_ID")
            ):
                ruta = self._avanzar().valor
            else:
                self._reportar_error(
                    "Se esperaba ruta después de FROM",
                    "DFS_SYNTAX_LOAD_PATH_EXPECTED",
                )
        elif self._es_valor("RESIDENT"):
            self._avanzar()
            es_resident = True
            if self._es_tipo("IDENTIFICADOR") or self._es_tipo("BRACKET_ID"):
                etiqueta_resident = self._avanzar().valor
            else:
                self._reportar_error(
                    "Se esperaba etiqueta RESIDENT",
                    "DFS_SYNTAX_RESIDENT_EXPECTED",
                )

        condiciones_where: tuple[Expresion, ...] = ()
        if self._es_valor("WHERE"):
            self._avanzar()
            condiciones_where = (self._parse_expresion(),)

        group_by: tuple[Expresion, ...] = ()
        if self._es_valor("GROUP"):
            self._avanzar()
            if not self._es_valor("BY"):
                self._reportar_error(
                    "Se esperaba BY después de GROUP",
                    "DFS_SYNTAX_BY_MISSING",
                )
            else:
                self._avanzar()
                group_by = (self._parse_expresion(),)
                while self._es_tipo("SIMBOLO") and self._actual().valor == ",":
                    self._avanzar()
                    group_by += (self._parse_expresion(),)

        return SentenciaLoad(
            ruta=ruta,
            expresion=None,
            campos=campos,
            distinct=distinct,
            es_resident=es_resident,
            etiqueta_resident=etiqueta_resident,
            noconcatenate=noconcatenate,
            proyecciones=proyecciones,
            condiciones_where=condiciones_where,
            group_by=group_by,
        )

    def _parse_resident(self) -> SentenciaResident:
        self._avanzar()

        if self._es_tipo("IDENTIFICADOR") or self._es_tipo("BRACKET_ID"):
            etiqueta = self._avanzar().valor
        else:
            self._reportar_error("Se esperaba etiqueta", "DFS_SYNTAX_LABEL_EXPECTED")
            etiqueta = ""

        expresion: Expresion | None = None
        if self._es_tipo("SIMBOLO") and self._actual().valor == ",":
            self._avanzar()
            expresion = self._parse_expresion()

        return SentenciaResident(etiqueta_origen=etiqueta, expresion=expresion)

    def _parse_drop_table(self) -> SentenciaDropTable:
        self._avanzar()
        self._esperar(
            "PALABRA_RESERVADA", "Se esperaba TABLE", "DFS_SYNTAX_TABLE_EXPECTED"
        )

        _, tabla = self._parse_nombre_tabla()

        return SentenciaDropTable(esquema="", tabla=tabla)

    def _parse_store(self) -> SentenciaStore:
        self._avanzar()

        _, tabla = self._parse_nombre_tabla()

        self._esperar(
            "PALABRA_RESERVADA", "Se esperaba INTO", "DFS_SYNTAX_INTO_MISSING"
        )

        if self._es_tipo("STRING"):
            lib_path = self._avanzar().valor.strip('"').strip("'")
        elif self._es_tipo("BRACKET_ID") or self._es_tipo("LIB_URI"):
            lib_path = self._avanzar().valor
        else:
            lib_path = ""

        formato: str | None = None
        if self._es_tipo("SIMBOLO") and self._actual().valor == "(":
            self._avanzar()
            # Qlik genera ``(txt)`` sin comillas. También aceptamos STRING para
            # conservar compatibilidad con scripts escritos manualmente.
            if self._es_tipo("STRING"):
                formato = self._avanzar().valor.strip('"').strip("'")
            elif self._es_tipo("IDENTIFICADOR") or self._es_tipo("BRACKET_ID"):
                formato = self._avanzar().valor
            if not (self._es_tipo("SIMBOLO") and self._actual().valor == ")"):
                self._reportar_error("Se esperaba ')'", "DFS_SYNTAX_MISSING_RPAREN")
            else:
                self._avanzar()

        return SentenciaStore(
            esquema="", tabla=tabla, ruta_destino=lib_path, formato=formato
        )

    def _parse_concatenate(self) -> SentenciaConcatenate:
        self._avanzar()

        objetivo = ""
        if self._es_tipo("SIMBOLO") and self._actual().valor == "(":
            self._avanzar()
            if self._es_tipo("IDENTIFICADOR") or self._es_tipo("BRACKET_ID"):
                objetivo = self._avanzar().valor
            self._esperar("SIMBOLO", "Se esperaba ')'", "DFS_SYNTAX_MISSING_RPAREN")
        elif self._es_tipo("IDENTIFICADOR"):
            objetivo = self._avanzar().valor

        return SentenciaConcatenate(
            etiqueta_objetivo=objetivo, etiqueta_origen="", noconcatenate=False
        )

    def _parse_objetivo_prefijo(self) -> str:
        """Lee el nombre entre paréntesis usado por JOIN/CONCATENATE."""
        objetivo = ""
        if self._es_tipo("SIMBOLO") and self._actual().valor == "(":
            self._avanzar()
            if self._es_tipo("IDENTIFICADOR") or self._es_tipo("BRACKET_ID"):
                objetivo = self._avanzar().valor
            else:
                self._reportar_error(
                    "Se esperaba tabla objetivo del prefijo",
                    "DFS_SYNTAX_PREFIX_TARGET_EXPECTED",
                )
            if not (self._es_tipo("SIMBOLO") and self._actual().valor == ")"):
                self._reportar_error(
                    "Se esperaba ')'",
                    "DFS_SYNTAX_MISSING_RPAREN",
                )
            else:
                self._avanzar()
        elif self._es_tipo("IDENTIFICADOR") or self._es_tipo("BRACKET_ID"):
            objetivo = self._avanzar().valor
        else:
            self._reportar_error(
                "Se esperaba tabla objetivo del prefijo",
                "DFS_SYNTAX_PREFIX_TARGET_EXPECTED",
            )
        return objetivo

    def _parse_carga_prefijada(self, tipo: str) -> Etiqueta:
        """Agrupa prefijo + LOAD + SELECT para conservar su dependencia.

        El exportador Qlik escribe estas cadenas fuera de una etiqueta. Crear
        una etiqueta sintética permite que el compilador aplique primero la
        fuente SELECT, después los LOAD superiores y al final JOIN/CONCATENATE.
        """
        posicion_inicial = self._pos
        if tipo == "LEFT":
            self._avanzar()
            if not self._es_valor("JOIN"):
                self._reportar_error(
                    "Se esperaba JOIN después de LEFT",
                    "DFS_SYNTAX_JOIN_MISSING",
                )
            else:
                self._avanzar()
        else:
            self._avanzar()

        objetivo = self._parse_objetivo_prefijo()
        if self._es_tipo("SIMBOLO") and self._actual().valor == ";":
            self._avanzar()

        noconcatenate = False
        if self._es_valor("NOCONCATENATE"):
            noconcatenate = True
            self._avanzar()
        if not self._es_valor("LOAD"):
            if tipo == "CONCATENATE":
                # Compatibilidad con la forma histórica aislada. El validador
                # decidirá después si el objetivo y el origen son válidos.
                return Etiqueta(
                    nombre=f"_prefijo_concatenate_{posicion_inicial}",
                    sentencias=(
                        SentenciaConcatenate(
                            etiqueta_objetivo=objetivo,
                            etiqueta_origen="",
                        ),
                    ),
                )
            self._reportar_error(
                f"{tipo} debe preceder a LOAD",
                "DFS_SYNTAX_PREFIX_LOAD_EXPECTED",
            )
            raise ParserError(
                f"{tipo} sin LOAD",
                self._crear_ubicacion_actual(),
                "DFS_SYNTAX_PREFIX_LOAD_EXPECTED",
            )

        carga = self._parse_load(noconcatenate_prefijo=noconcatenate)
        if tipo == "LEFT":
            carga = replace(
                carga,
                join_objetivo=objetivo,
                join_tipo="LEFT",
            )
        else:
            carga = replace(carga, concatenate_objetivo=objetivo)

        sentencias: list[TipoSentenciaEtiqueta] = [carga]
        if self._es_tipo("SIMBOLO") and self._actual().valor == ";":
            self._avanzar()
        if self._es_valor("SELECT"):
            sentencias.append(self._parse_select())
        nombre = f"_prefijo_{tipo.lower()}_{posicion_inicial}"
        return Etiqueta(nombre=nombre, sentencias=tuple(sentencias))

    def _parse_set(self) -> SentenciaSet:
        self._avanzar()

        token_var = self._esperar(
            "IDENTIFICADOR", "Se esperaba nombre de variable", "DFS_SYNTAX_VAR_EXPECTED"
        )
        variable = token_var.valor

        self._esperar("SIMBOLO", "Se esperaba '='", "DFS_SYNTAX_EQ_MISSING")

        # Los SET generados por Qlik mezclan strings y números sin comillas.
        # Ambos se preservan como texto porque configuran semántica del script,
        # no cálculos de negocio dentro de Spark.
        if self._es_tipo("STRING"):
            valor = self._avanzar().valor.strip('"').strip("'")
        elif self._es_tipo("NUMERO") or self._es_tipo("IDENTIFICADOR"):
            valor = self._avanzar().valor
        else:
            self._reportar_error(
                "Se esperaba valor",
                "DFS_SYNTAX_VALUE_EXPECTED",
            )
            valor = ""

        return SentenciaSet(variable=variable, valor=valor)

    def _parse_lib_connect_to(self) -> SentenciaLibConnectTo:
        self._avanzar()

        self._esperar(
            "PALABRA_RESERVADA", "Se esperaba CONNECT", "DFS_SYNTAX_CONNECT_MISSING"
        )

        self._esperar("PALABRA_RESERVADA", "Se esperaba TO", "DFS_SYNTAX_TO_MISSING")

        if self._es_tipo("STRING"):
            conexion = self._avanzar().valor.strip('"').strip("'")
        elif self._es_tipo("BRACKET_ID") or self._es_tipo("IDENTIFICADOR"):
            conexion = self._avanzar().valor
        else:
            self._reportar_error(
                "Se esperaba nombre de conexion", "DFS_SYNTAX_CONN_EXPECTED"
            )
            conexion = ""

        return SentenciaLibConnectTo(nombre_lib="", conexion=conexion)

    def _parse_sentencia(
        self,
    ) -> (
        SentenciaSelect
        | SentenciaLoad
        | SentenciaResident
        | SentenciaDropTable
        | SentenciaStore
        | SentenciaConcatenate
        | SentenciaSet
        | SentenciaLibConnectTo
    ):
        token = self._actual()

        if self._es_valor("SELECT"):
            return self._parse_select()
        elif self._es_valor("LOAD"):
            return self._parse_load()
        elif self._es_valor("RESIDENT"):
            return self._parse_resident()
        elif self._es_valor("DROP"):
            return self._parse_drop_table()
        elif self._es_valor("STORE"):
            return self._parse_store()
        elif self._es_valor("NOCONCATENATE"):
            self._avanzar()
            if not self._es_valor("LOAD"):
                self._reportar_error(
                    "NOCONCATENATE debe preceder a LOAD",
                    "DFS_SYNTAX_NOCONCATENATE_LOAD_EXPECTED",
                )
                raise ParserError(
                    "NOCONCATENATE sin LOAD",
                    self._crear_ubicacion_actual(),
                    "DFS_SYNTAX_NOCONCATENATE_LOAD_EXPECTED",
                )
            return self._parse_load(noconcatenate_prefijo=True)
        elif self._es_valor("CONCATENATE"):
            return self._parse_concatenate()
        elif self._es_valor("SET"):
            return self._parse_set()
        elif self._es_valor("LIB"):
            return self._parse_lib_connect_to()
        else:
            self._reportar_error(
                f"Sentencia desconocida: {token.valor}", "DFS_SYNTAX_UNKNOWN_SENTENCE"
            )
            self._avanzar()
            raise ParserError(
                f"Sentencia desconocida: {token.valor}",
                self._crear_ubicacion_actual(),
                "DFS_SYNTAX_UNKNOWN_SENTENCE",
            )

    def _parse_etiqueta(self) -> Etiqueta:
        # El generador de Dataflow encierra incluso etiquetas simples entre
        # corchetes; ambos tipos representan el mismo nombre lógico.
        if self._es_tipo("IDENTIFICADOR") or self._es_tipo("BRACKET_ID"):
            nombre = self._avanzar().valor
        else:
            self._reportar_error(
                "Se esperaba nombre de etiqueta",
                "DFS_SYNTAX_LABEL_NAME_EXPECTED",
            )
            nombre = ""

        self._esperar("SIMBOLO", "Se esperaba ':'", "DFS_SYNTAX_COLON_EXPECTED")

        sentencias: list[TipoSentenciaEtiqueta] = []

        while not self._es_tipo("FIN") and not (
            (self._es_tipo("IDENTIFICADOR") or self._es_tipo("BRACKET_ID"))
            and self._tokens[self._pos + 1].valor == ":"
            if self._pos + 1 < len(self._tokens)
            else False
        ):
            if self._es_valor("SET") or self._es_valor("LIB"):
                self._reportar_error(
                    "SET y LIB CONNECT TO no son validos dentro de etiquetas",
                    "DFS_UNSUPPORTED_GLOBAL_IN_LABEL",
                )
                break
            if self._es_valor("LEFT") or self._es_valor("CONCATENATE"):
                # El prefijo gobierna la carga siguiente y debe regresar al
                # parser global, que la agrupa en una etiqueta sintética.
                break

            try:
                sentencia = self._parse_sentencia_para_etiqueta()
                sentencias.append(sentencia)
            except ParserError:
                break

            if self._es_tipo("SIMBOLO") and self._actual().valor == ";":
                self._avanzar()

            if (
                self._es_valor("SELECT")
                or self._es_valor("LOAD")
                or self._es_valor("RESIDENT")
                or self._es_valor("DROP")
                or self._es_valor("STORE")
                or self._es_valor("NOCONCATENATE")
            ):
                continue
            break

        return Etiqueta(nombre=nombre, sentencias=tuple(sentencias))

    def _parse_sentencia_para_etiqueta(
        self,
    ) -> (
        SentenciaSelect
        | SentenciaLoad
        | SentenciaResident
        | SentenciaDropTable
        | SentenciaStore
        | SentenciaConcatenate
    ):
        if self._es_valor("SELECT"):
            return self._parse_select()
        elif self._es_valor("LOAD"):
            return self._parse_load()
        elif self._es_valor("NOCONCATENATE"):
            self._avanzar()
            if not self._es_valor("LOAD"):
                raise ParserError(
                    "NOCONCATENATE sin LOAD",
                    self._crear_ubicacion_actual(),
                    "DFS_SYNTAX_NOCONCATENATE_LOAD_EXPECTED",
                )
            return self._parse_load(noconcatenate_prefijo=True)
        elif self._es_valor("RESIDENT"):
            return self._parse_resident()
        elif self._es_valor("DROP"):
            return self._parse_drop_table()
        elif self._es_valor("STORE"):
            return self._parse_store()
        elif self._es_valor("CONCATENATE"):
            return self._parse_concatenate()
        else:
            raise ParserError(
                f"Sentencia desconocida: {self._actual().valor}",
                self._crear_ubicacion_actual(),
                "DFS_SYNTAX_UNKNOWN_SENTENCE",
            )

    def parsear(self) -> tuple[ProgramaDataflowScript, list[ErrorDataflow]]:
        sentencias_globales: list[SentenciaSet | SentenciaLibConnectTo] = []
        etiquetas: list[Etiqueta] = []

        while not self._es_tipo("FIN"):
            if self._es_valor("SET"):
                try:
                    sentencias_globales.append(self._parse_set())
                except ParserError:
                    break
                if self._es_tipo("SIMBOLO") and self._actual().valor == ";":
                    self._avanzar()
            elif self._es_valor("LIB"):
                try:
                    sentencias_globales.append(self._parse_lib_connect_to())
                except ParserError:
                    break
                if self._es_tipo("SIMBOLO") and self._actual().valor == ";":
                    self._avanzar()
            elif self._es_valor("LEFT"):
                try:
                    etiquetas.append(self._parse_carga_prefijada("LEFT"))
                except ParserError:
                    self._avanzar()
                    continue
                if self._es_tipo("SIMBOLO") and self._actual().valor == ";":
                    self._avanzar()
            elif self._es_valor("CONCATENATE"):
                try:
                    etiquetas.append(self._parse_carga_prefijada("CONCATENATE"))
                except ParserError:
                    self._avanzar()
                    continue
                if self._es_tipo("SIMBOLO") and self._actual().valor == ";":
                    self._avanzar()
            elif self._es_tipo("IDENTIFICADOR") or self._es_tipo("BRACKET_ID"):
                # Una etiqueta puede ser ``Ventas:`` o ``[Ventas]:``. El lexer
                # conserva la diferencia de sintaxis, pero el AST usa el nombre
                # lógico sin corchetes.
                if (
                    self._pos + 1 < len(self._tokens)
                    and self._tokens[self._pos + 1].valor == ":"
                ):
                    etiquetas.append(self._parse_etiqueta())
                else:
                    self._errores.append(
                        ErrorDataflow(
                            mensaje=f"Parser [DFS_SYNTAX_UNEXPECTED_TOKEN]: Token inesperado fuera de etiqueta: {self._actual().valor}",
                            ubicacion=self._crear_ubicacion_actual(),
                            codigo="DFS_SYNTAX_UNEXPECTED_TOKEN",
                        )
                    )
                    self._avanzar()
            elif (
                self._es_valor("SELECT")
                or self._es_valor("LOAD")
                or self._es_valor("RESIDENT")
                or self._es_valor("DROP")
                or self._es_valor("STORE")
                or self._es_valor("NOCONCATENATE")
            ):
                try:
                    sentencia = self._parse_sentencia_para_etiqueta()
                    etiquetas.append(
                        Etiqueta(nombre="_anonima", sentencias=(sentencia,))
                    )
                except ParserError:
                    self._avanzar()
                    continue
                if self._es_tipo("SIMBOLO") and self._actual().valor == ";":
                    self._avanzar()
            elif self._es_tipo("SIMBOLO") and self._actual().valor == ";":
                self._avanzar()
            else:
                if not self._es_tipo("FIN"):
                    self._errores.append(
                        ErrorDataflow(
                            mensaje=f"Parser [DFS_SYNTAX_UNEXPECTED_TOKEN]: Token inesperado: {self._actual().valor}",
                            ubicacion=self._crear_ubicacion_actual(),
                            codigo="DFS_SYNTAX_UNEXPECTED_TOKEN",
                        )
                    )
                    self._avanzar()

        programa = ProgramaDataflowScript(
            sentencias_globales=tuple(sentencias_globales),
            etiquetas=tuple(etiquetas),
        )
        return programa, self._errores


def parsear(tokens: list[Token]) -> tuple[ProgramaDataflowScript, list[ErrorDataflow]]:
    return Parser(tokens).parsear()
