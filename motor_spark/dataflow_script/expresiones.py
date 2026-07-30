from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from motor_spark.dataflow_script.ast import Expresion, TipoExpresion
from motor_spark.dataflow_script.errores import ErrorDataflow, SourceLocation, SourceSpan

if TYPE_CHECKING:
    from pyspark.sql import Column
    from pyspark.sql.functions import Expr as FExpr

try:
    from pyspark.sql import functions as pyspark_f
    from pyspark.sql import Window as pyspark_Window
    HAS_PYSPARK = True
except ImportError:
    pyspark_f = None  # type: ignore
    pyspark_Window = None  # type: ignore
    HAS_PYSPARK = False


FUNCIONES_WHITELIST: frozenset[str] = frozenset({
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
})

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
        return self._compilar_expresion(expresion)

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
        condicion = self._compilar_expresion(expr.hijos[0])
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
        for i, patron in enumerate(patrones, start=1):
            resultado = pyspark.sql.functions.when(
                valor.eqNullSafe(patron), pyspark.sql.functions.lit(i)
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
        return pyspark.sql.functions.when(operando.isNull(), pyspark.sql.functions.lit(-1)).otherwise(
            pyspark.sql.functions.lit(0)
        )

    def _funcion_indexregex(self, expr: Expresion) -> Column:
        pyspark = __import__("pyspark")
        if len(expr.hijos) < 2:
            raise ErrorCompilacionExpresion(
                mensaje="INDEXREGEX requiere 2 argumentos: INDEXREGEX(cadena, patron)",
                codigo="EXPR_INDEXREGEX_ARITY",
            )
        cadena = self._compilar_expresion(expr.hijos[0])
        patron = expr.hijos[1].valor
        patron_limpio = patron.strip("'\"")

        def buscar_indice(c: Column) -> Column:
            coincidencia = pyspark.sql.functions.regexp_extract(c, patron_limpio, 0)
            indice_valido = pyspark.sql.functions.when(
                coincidencia =="", pyspark.sql.functions.lit(0)
            ).otherwise(
                pyspark.sql.functions.lit(1)
            )
            return indice_valido

        return buscar_indice(cadena)

    def _funcion_num(self, expr: Expresion) -> Column:
        pyspark = __import__("pyspark")
        if not expr.hijos:
            return pyspark.sql.functions.lit(0).cast("double")
        operando = self._compilar_expresion(expr.hijos[0])
        return pyspark.sql.functions.when(
            operando.isNull(), pyspark.sql.functions.lit(0)
        ).otherwise(
            pyspark.sql.functions.coalesce(
                pyspark.sql.functions.regexp_replace(operando, r"[^\d.,-]", "").cast("double"),
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

    def _compilar_window(self, expr: Expresion) -> Column:
        pyspark = __import__("pyspark")
        if not expr.hijos:
            raise ErrorCompilacionExpresion(
                mensaje="WINDOW requiere al menos 1 argumento",
                codigo="EXPR_WINDOW_ARITY",
            )

        if HAS_PYSPARK and pyspark_Window is not None:
            from pyspark.sql import Window as pyspark_Window
            from pyspark.sql import functions as F

            primer_hijo = expr.hijos[0]
            if primer_hijo.tipo == TipoExpresion.WINDOW_RANK:
                partition_cols = []
                order_cols = []
                sort_direction = "asc"
                partition_start = 1
                if len(expr.hijos) > 1:
                    for i, hijo in enumerate(expr.hijos[1:]):
                        if i == 0 and hijo.valor.upper() in ("ASC", "DESC"):
                            sort_direction = hijo.valor.upper()
                            continue
                        col = self._compilar_expresion(hijo)
                        if i == partition_start:
                            partition_cols.append(col)
                        else:
                            order_cols.append(col)

                ventana = pyspark_Window.partitionBy(*partition_cols).orderBy(*order_cols)
                return F.row_number().over(ventana)

        return self._lit(0)

    def _compilar_wrank(self, expr: Expresion) -> Column:
        pyspark = __import__("pyspark")
        return self._lit(0)

    def _compilar_operacion_binaria(self, expr: Expresion) -> Column:
        operador = expr.valor.upper()
        pyspark = __import__("pyspark")

        if operador == "AND":
            izquierda = self._compilar_expresion(expr.hijos[0])
            derecha = self._compilar_expresion(expr.hijos[1])
            return izquierda & derecha

        elif operador == "OR":
            izquierda = self._compilar_expresion(expr.hijos[0])
            derecha = self._compilar_expresion(expr.hijos[1])
            return izquierda | derecha

        elif operador == "NOT":
            operando = self._compilar_expresion(expr.hijos[0])
            return ~operando

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
            resultado = pyspark.sql.functions.concat(resultado, self._compilar_expresion(hijo))
        return resultado


def compilar_expresion(expresion: Expresion) -> Column:
    return CompiladorExpresion().compilar(expresion)
