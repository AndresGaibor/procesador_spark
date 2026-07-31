from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from motor_spark.dataflow_script.ast import Expresion, TipoExpresion
from motor_spark.dataflow_script.errores import (
    SourceLocation,
)

if TYPE_CHECKING:
    from pyspark.sql import Column
    from pyspark.sql.functions import Expr as FExpr

try:
    from pyspark.sql import Window as pyspark_Window
    from pyspark.sql import functions as pyspark_f

    HAS_PYSPARK = True
except ImportError:
    pyspark_f = None  # type: ignore
    pyspark_Window = None  # type: ignore
    HAS_PYSPARK = False


FUNCIONES_WHITELIST: frozenset[str] = frozenset(
    {
        "TRIM",
        "IF",
        "MATCH",
        "COALESCE",
        "ISNULL",
        "INDEXREGEX",
        "NUM",
        "MONTH",
        "YEAR",
        "SUM",
        "AVG",
        "COUNT",
        "FALSE",
        "WINDOW",
        "WRANK",
    }
)

PATRON_REGEX: re.Pattern[str] = re.compile(r"^(?:\^?)(.+)(?:\$?)$", re.IGNORECASE)


class ErrorCompilacionExpresion(Exception):
    def __init__(
        self,
        mensaje: str,
        ubicacion: SourceLocation | None = None,
        ayuda: str | None = None,
        codigo: str | None = None,
    ) -> None:
        self.mensaje = mensaje
        self.ubicacion = ubicacion
        self.ayuda = ayuda
        self.codigo = codigo
        super().__init__(mensaje)

    def formato_estable(self) -> str:
        partes = [self.mensaje]
        if self.codigo:
            partes.insert(0, f"[{self.codigo}]")
        if self.ubicacion:
            partes.append(f" ubicacion={self.ubicacion}")
        if self.ayuda:
            partes.append(f" ayuda={self.ayuda}")
        return "".join(partes)


class CompiladorExpresion:
    def __init__(self) -> None:
        self._F: FExpr | None = None

    def _importar_F(self) -> FExpr:
        if self._F is None:
            from pyspark.sql.functions import expr as _expr

            self._F = _expr
        return self._F

    def _column(self, nombre: str) -> Column:
        pyspark = __import__("pyspark")
        return pyspark.sql.functions.col(nombre)

    def _lit(self, valor: Any) -> Column:
        pyspark = __import__("pyspark")
        return pyspark.sql.functions.lit(valor)

    def compilar(self, expresion: Expresion) -> Column:
        """Compila una expresión de valor a ``Column`` de Spark."""
        return self._compilar_expresion(expresion)

    def compilar_predicado(self, expresion: Expresion) -> Column:
        """Convierte la verdad numérica de Qlik al booleano estricto de Spark.

        Qlik representa True como -1 y False como 0; funciones como Match o
        IndexRegEx también se usan directamente en WHERE. Spark no permite una
        columna numérica como filtro, pero su cast boolean conserva 0=False y
        cualquier valor no cero=True.
        """
        return self._compilar_expresion(expresion).cast("boolean")

    def _compilar_expresion(self, expr: Expresion) -> Column:
        if expr.tipo == TipoExpresion.COLUMNA:
            return self._compilar_columna(expr)
        elif expr.tipo == TipoExpresion.LITERAL_NUMERO:
            return self._compilar_numero(expr)
        elif expr.tipo == TipoExpresion.LITERAL_STRING:
            return self._compilar_string(expr)
        elif expr.tipo == TipoExpresion.FUNCION:
            return self._compilar_funcion(expr)
        elif expr.tipo == TipoExpresion.OPERACION_BINARIA:
            return self._compilar_operacion_binaria(expr)
        elif expr.tipo == TipoExpresion.CONCATENACION:
            return self._compilar_concatenacion(expr)
        elif expr.tipo == TipoExpresion.ALIAS:
            return self._compilar_expresion(expr.hijos[0] if expr.hijos else expr)
        elif expr.tipo == TipoExpresion.WINDOW:
            return self._compilar_window(expr)
        elif expr.tipo == TipoExpresion.WINDOW_RANK:
            return self._compilar_wrank(expr)
        else:
            raise ErrorCompilacionExpresion(
                mensaje=f"Tipo de expresion no soportado: {expr.tipo}",
                codigo="EXPR_UNSUPPORTED_TYPE",
            )

    def _compilar_columna(self, expr: Expresion) -> Column:
        return self._column(expr.valor)

    def _compilar_numero(self, expr: Expresion) -> Column:
        valor = float(expr.valor)
        return self._lit(valor)

    def _compilar_string(self, expr: Expresion) -> Column:
        valor = expr.valor
        if (valor.startswith('"') and valor.endswith('"')) or (
            valor.startswith("'") and valor.endswith("'")
        ):
            valor = valor[1:-1]
        return self._lit(valor)

    def _compilar_funcion(self, expr: Expresion) -> Column:
        nombre = expr.valor.upper()

        if nombre == "TRIM":
            return self._funcion_trim(expr)
        elif nombre == "IF":
            return self._funcion_if(expr)
        elif nombre == "MATCH":
            return self._funcion_match(expr)
        elif nombre == "COALESCE":
            return self._funcion_coalesce(expr)
        elif nombre == "ISNULL":
            return self._funcion_isnull(expr)
        elif nombre == "INDEXREGEX":
            return self._funcion_indexregex(expr)
        elif nombre == "NUM":
            return self._funcion_num(expr)
        elif nombre == "MONTH":
            return self._funcion_month(expr)
        elif nombre == "YEAR":
            return self._funcion_year(expr)
        elif nombre == "SUM":
            return self._funcion_agregacion(expr, "sum")
        elif nombre == "AVG":
            return self._funcion_agregacion(expr, "avg")
        elif nombre == "COUNT":
            return self._funcion_count(expr)
        elif nombre == "FALSE":
            return self._lit(False)
        else:
            raise ErrorCompilacionExpresion(
                mensaje=f"Funcion desconocida o no permitida: {nombre}",
                codigo="EXPR_UNKNOWN_FUNCTION",
                ayuda=f"Funciones permitidas: {', '.join(sorted(FUNCIONES_WHITELIST))}",
            )

    def _funcion_trim(self, expr: Expresion) -> Column:
        pyspark = __import__("pyspark")
        if not expr.hijos:
            return pyspark.sql.functions.trim(self._lit(""))
        return pyspark.sql.functions.trim(self._compilar_expresion(expr.hijos[0]))

    def _funcion_if(self, expr: Expresion) -> Column:
        pyspark = __import__("pyspark")
        if len(expr.hijos) < 3:
            raise ErrorCompilacionExpresion(
                mensaje="IF requiere 3 argumentos: IF(condicion, valor_true, valor_false)",
                codigo="EXPR_IF_ARITY",
            )
        condicion = self.compilar_predicado(expr.hijos[0])
        valor_true = self._compilar_expresion(expr.hijos[1])
        valor_false = self._compilar_expresion(expr.hijos[2])
        return pyspark.sql.functions.when(condicion, valor_true).otherwise(valor_false)

    def _funcion_match(self, expr: Expresion) -> Column:
        pyspark = __import__("pyspark")
        if len(expr.hijos) < 2:
            raise ErrorCompilacionExpresion(
                mensaje="MATCH requiere al menos 2 argumentos: MATCH(valor, patron1, patron2, ...)",
                codigo="EXPR_MATCH_ARITY",
            )
        valor = self._compilar_expresion(expr.hijos[0])
        patrones = [self._compilar_expresion(h) for h in expr.hijos[1:]]
        resultado = pyspark.sql.functions.lit(0)
        # Cada when nuevo envuelve al anterior. Recorrer en sentido inverso
        # deja la primera coincidencia como rama exterior, igual que Qlik.
        for indice, patron in reversed(list(enumerate(patrones, start=1))):
            resultado = pyspark.sql.functions.when(
                valor.eqNullSafe(patron),
                pyspark.sql.functions.lit(indice),
            ).otherwise(resultado)
        return resultado

    def _funcion_coalesce(self, expr: Expresion) -> Column:
        pyspark = __import__("pyspark")
        if not expr.hijos:
            raise ErrorCompilacionExpresion(
                mensaje="COALESCE requiere al menos 1 argumento",
                codigo="EXPR_COALESCE_ARITY",
            )
        columnas = [self._compilar_expresion(h) for h in expr.hijos]
        return pyspark.sql.functions.coalesce(*columnas)

    def _funcion_isnull(self, expr: Expresion) -> Column:
        pyspark = __import__("pyspark")
        if not expr.hijos:
            raise ErrorCompilacionExpresion(
                mensaje="ISNULL requiere 1 argumento",
                codigo="EXPR_ISNULL_ARITY",
            )
        operando = self._compilar_expresion(expr.hijos[0])
        return pyspark.sql.functions.when(
            operando.isNull(), pyspark.sql.functions.lit(-1)
        ).otherwise(pyspark.sql.functions.lit(0))

    def _funcion_indexregex(self, expr: Expresion) -> Column:
        pyspark = __import__("pyspark")
        if len(expr.hijos) not in {2, 3}:
            raise ErrorCompilacionExpresion(
                mensaje=(
                    "INDEXREGEX requiere texto, patrón y opcionalmente "
                    "número de ocurrencia"
                ),
                codigo="EXPR_INDEXREGEX_ARITY",
            )
        cadena = self._compilar_expresion(expr.hijos[0])
        patron = expr.hijos[1].valor.strip("'\"")
        ocurrencia = 1
        if len(expr.hijos) == 3:
            ocurrencia = self._entero_literal(
                expr.hijos[2],
                "ocurrencia de INDEXREGEX",
            )
            if ocurrencia < 1:
                raise ErrorCompilacionExpresion(
                    mensaje="La ocurrencia de INDEXREGEX debe ser mayor que cero",
                    codigo="EXPR_INDEXREGEX_OCCURRENCE",
                )

        patron_compilado = re.compile(patron)

        def obtener_posicion(valor: Any) -> int | None:
            if valor is None:
                return None
            for indice, coincidencia in enumerate(
                patron_compilado.finditer(str(valor)),
                start=1,
            ):
                if indice == ocurrencia:
                    return coincidencia.start() + 1
            return 0

        # PySpark 3.4 no expone regexp_instr en functions. La UDF conserva su
        # posición 1-based, ocurrencia y propagación de NULL sin depender de
        # una versión posterior de la API Python.
        return pyspark.sql.functions.udf(obtener_posicion, "integer")(cadena)

    def _funcion_num(self, expr: Expresion) -> Column:
        pyspark = __import__("pyspark")
        if not expr.hijos:
            return pyspark.sql.functions.lit(0).cast("double")
        operando = self._compilar_expresion(expr.hijos[0])
        return pyspark.sql.functions.when(
            operando.isNull(), pyspark.sql.functions.lit(0)
        ).otherwise(
            pyspark.sql.functions.coalesce(
                pyspark.sql.functions.regexp_replace(operando, r"[^\d.,-]", "").cast(
                    "double"
                ),
                pyspark.sql.functions.lit(0),
            )
        )

    def _funcion_month(self, expr: Expresion) -> Column:
        pyspark = __import__("pyspark")
        if not expr.hijos:
            raise ErrorCompilacionExpresion(
                mensaje="MONTH requiere 1 argumento",
                codigo="EXPR_MONTH_ARITY",
            )
        operando = self._compilar_expresion(expr.hijos[0])
        return pyspark.sql.functions.month(operando)

    def _funcion_year(self, expr: Expresion) -> Column:
        pyspark = __import__("pyspark")
        if not expr.hijos:
            raise ErrorCompilacionExpresion(
                mensaje="YEAR requiere 1 argumento",
                codigo="EXPR_YEAR_ARITY",
            )
        operando = self._compilar_expresion(expr.hijos[0])
        return pyspark.sql.functions.year(operando)

    def _funcion_agregacion(self, expr: Expresion, nombre: str) -> Column:
        pyspark = __import__("pyspark")
        if not expr.hijos:
            raise ErrorCompilacionExpresion(
                mensaje=f"{nombre.upper()} requiere 1 argumento",
                codigo=f"EXPR_{nombre.upper()}_ARITY",
            )
        operando = self._compilar_expresion(expr.hijos[0])
        if nombre == "sum":
            return pyspark.sql.functions.sum(operando)
        elif nombre == "avg":
            return pyspark.sql.functions.avg(operando)
        return operando

    def _funcion_count(self, expr: Expresion) -> Column:
        pyspark = __import__("pyspark")
        es_distinct = False
        args = expr.hijos

        if args and args[0].valor.upper() == "DISTINCT":
            es_distinct = True
            args = args[1:]

        if not args:
            raise ErrorCompilacionExpresion(
                mensaje="COUNT requiere 1 argumento",
                codigo="EXPR_COUNT_ARITY",
            )

        columna = self._compilar_expresion(args[0])
        if es_distinct:
            return pyspark.sql.functions.countDistinct(columna)
        return pyspark.sql.functions.count(columna)

    @staticmethod
    def _entero_literal(expr: Expresion, nombre: str) -> int:
        """Extrae un entero de configuración sin ejecutar una columna Spark."""
        if expr.tipo != TipoExpresion.LITERAL_NUMERO:
            raise ErrorCompilacionExpresion(
                mensaje=f"{nombre} debe ser un literal numérico",
                codigo="EXPR_INTEGER_LITERAL_REQUIRED",
            )
        valor_float = float(expr.valor)
        if not valor_float.is_integer():
            raise ErrorCompilacionExpresion(
                mensaje=f"{nombre} debe ser entero",
                codigo="EXPR_INTEGER_REQUIRED",
            )
        return int(valor_float)

    def _compilar_window(self, expr: Expresion) -> Column:
        if not expr.hijos:
            raise ErrorCompilacionExpresion(
                mensaje="WINDOW requiere al menos una expresión principal",
                codigo="EXPR_WINDOW_ARITY",
            )
        principal = expr.hijos[0]
        if principal.tipo != TipoExpresion.WINDOW_RANK:
            raise ErrorCompilacionExpresion(
                mensaje="Solo WRank está soportado actualmente dentro de Window",
                codigo="EXPR_WINDOW_MAIN_UNSUPPORTED",
            )

        mode = (
            self._entero_literal(principal.hijos[0], "mode de WRank")
            if principal.hijos
            else 0
        )
        fmt = (
            self._entero_literal(principal.hijos[1], "fmt de WRank")
            if len(principal.hijos) > 1
            else 0
        )
        if mode not in {1, 4}:
            raise ErrorCompilacionExpresion(
                mensaje=f"WRank mode={mode} todavía no tiene equivalencia exacta",
                codigo="EXPR_WRANK_MODE_UNSUPPORTED",
            )
        if fmt != 1:
            raise ErrorCompilacionExpresion(
                mensaje=f"WRank fmt={fmt} requiere valores duales no soportados",
                codigo="EXPR_WRANK_FMT_UNSUPPORTED",
            )

        direccion_indice: int | None = None
        direccion = "ASC"
        for indice, hijo in enumerate(expr.hijos[1:], start=1):
            if hijo.tipo != TipoExpresion.LITERAL_STRING:
                continue
            candidato = hijo.valor.strip("'\"").upper()
            if candidato in {"ASC", "DESC"}:
                direccion_indice = indice
                direccion = candidato
                break
        if direccion_indice is None:
            raise ErrorCompilacionExpresion(
                mensaje="Window requiere 'ASC' o 'DESC' antes del campo de orden",
                codigo="EXPR_WINDOW_SORT_REQUIRED",
            )

        particiones_ast = expr.hijos[1:direccion_indice]
        orden_ast = expr.hijos[direccion_indice + 1 :]
        if not orden_ast:
            raise ErrorCompilacionExpresion(
                mensaje="Window requiere al menos una expresión de orden",
                codigo="EXPR_WINDOW_ORDER_REQUIRED",
            )

        from pyspark.sql import Window
        from pyspark.sql import functions as F

        particiones = [self._compilar_expresion(hijo) for hijo in particiones_ast]
        orden = []
        for hijo in orden_ast:
            columna = self._compilar_expresion(hijo)
            orden.append(columna.desc() if direccion == "DESC" else columna.asc())
        ventana = Window.partitionBy(*particiones).orderBy(*orden)
        if mode == 1:
            return F.rank().over(ventana)
        return F.row_number().over(ventana)

    def _compilar_wrank(self, expr: Expresion) -> Column:
        raise ErrorCompilacionExpresion(
            mensaje="WRank solo puede usarse como expresión principal de Window",
            codigo="EXPR_WRANK_OUTSIDE_WINDOW",
        )

    def _compilar_operacion_binaria(self, expr: Expresion) -> Column:
        operador = expr.valor.upper()
        __import__("pyspark")

        if operador in {"NOT", "NEGATE"}:
            if len(expr.hijos) != 1:
                raise ErrorCompilacionExpresion(
                    mensaje=f"{operador} requiere exactamente un operando",
                    codigo="EXPR_UNARY_ARITY",
                )
            if operador == "NOT":
                return ~self.compilar_predicado(expr.hijos[0])
            operando = self._compilar_expresion(expr.hijos[0])
            return -operando

        if len(expr.hijos) != 2:
            raise ErrorCompilacionExpresion(
                mensaje=f"{operador} requiere exactamente dos operandos",
                codigo="EXPR_BINARY_ARITY",
            )

        if operador == "AND":
            izquierda = self.compilar_predicado(expr.hijos[0])
            derecha = self.compilar_predicado(expr.hijos[1])
            return izquierda & derecha

        elif operador == "OR":
            izquierda = self.compilar_predicado(expr.hijos[0])
            derecha = self.compilar_predicado(expr.hijos[1])
            return izquierda | derecha

        elif operador in ("=", "<>", "<", ">", "<=", ">="):
            izquierda = self._compilar_expresion(expr.hijos[0])
            derecha = self._compilar_expresion(expr.hijos[1])

            if operador == "=":
                return izquierda.eqNullSafe(derecha)
            elif operador == "<>":
                return ~izquierda.eqNullSafe(derecha)
            elif operador == "<":
                return izquierda < derecha
            elif operador == ">":
                return izquierda > derecha
            elif operador == "<=":
                return izquierda <= derecha
            elif operador == ">=":
                return izquierda >= derecha

        elif operador in ("+", "-", "*", "/"):
            izquierda = self._compilar_expresion(expr.hijos[0])
            derecha = self._compilar_expresion(expr.hijos[1])

            if operador == "+":
                return izquierda + derecha
            elif operador == "-":
                return izquierda - derecha
            elif operador == "*":
                return izquierda * derecha
            elif operador == "/":
                return izquierda / derecha

        raise ErrorCompilacionExpresion(
            mensaje=f"Operador no soportado: {operador}",
            codigo="EXPR_UNSUPPORTED_OPERATOR",
        )

    def _compilar_concatenacion(self, expr: Expresion) -> Column:
        pyspark = __import__("pyspark")
        if not expr.hijos:
            return self._lit("")
        resultado = self._compilar_expresion(expr.hijos[0])
        for hijo in expr.hijos[1:]:
            resultado = pyspark.sql.functions.concat(
                resultado, self._compilar_expresion(hijo)
            )
        return resultado


def compilar_expresion(expresion: Expresion) -> Column:
    return CompiladorExpresion().compilar(expresion)
