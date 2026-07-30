from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Sequence

from motor_spark.dataflow_script.ast import (
    Etiqueta,
    Expresion,
    ProjectionItem,
    SentenciaConcatenate,
    SentenciaLoad,
    SentenciaResident,
    SentenciaSelect,
    TipoExpresion,
)
from motor_spark.dataflow_script.errores import ErrorDataflow, SourceLocation, SourceSpan
from motor_spark.dataflow_script.expresiones import CompiladorExpresion, ErrorCompilacionExpresion

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql import Window

try:
    from pyspark.sql import functions as pyspark_f
    from pyspark.sql import Window as pyspark_Window
    HAS_PYSPARK = True
except ImportError:
    HAS_PYSPARK = False
    pyspark_f = None  # type: ignore
    pyspark_Window = None  # type: ignore


@dataclass(frozen=True)
class RegistroDataframe:
    etiqueta: str
    dataframe: DataFrame


@dataclass(frozen=True)
class ResultadoOperacion:
    etiqueta: str
    dataframe: DataFrame


class ErrorEjecucionDataflow(ErrorDataflow):
    pass


class ContextoEjecucionDataflow:
    def __init__(self, spark: Any | None = None) -> None:
        self._spark = spark
        self._registros: dict[str, RegistroDataframe] = {}
        self._compilador = CompiladorExpresion()
        self._errores: list[ErrorDataflow] = []

    @property
    def errores(self) -> tuple[ErrorDataflow, ...]:
        return tuple(self._errores)

    def tiene_errores(self) -> bool:
        return len(self._errores) > 0

    def registrar_dataframe(
        self, etiqueta: str, dataframe: DataFrame, reemplazo: bool = False
    ) -> None:
        if etiqueta in self._registros and not reemplazo:
            raise ValueError(
                f"Dataframe '{etiqueta}' ya registrado. Usa reemplazo=True para sobreescribir."
            )
        registro = RegistroDataframe(etiqueta=etiqueta, dataframe=dataframe)
        self._registros[etiqueta] = registro

    def obtener_dataframe(self, etiqueta: str) -> DataFrame | None:
        registro = self._registros.get(etiqueta)
        return registro.dataframe if registro else None

    def tiene_dataframe(self, etiqueta: str) -> bool:
        return etiqueta in self._registros

    def ejecutar_etiqueta(self, etiqueta: Etiqueta) -> None:
        for sentencia in etiqueta.sentencias:
            if isinstance(sentencia, SentenciaSelect):
                self._ejecutar_select(sentencia, etiqueta.nombre)
            elif isinstance(sentencia, SentenciaLoad):
                self._ejecutar_load(sentencia, etiqueta.nombre)
            elif isinstance(sentencia, SentenciaResident):
                self._ejecutar_resident(sentencia, etiqueta.nombre)
            elif isinstance(sentencia, SentenciaConcatenate):
                self._ejecutar_concatenate(sentencia)
            else:
                self._agregar_error(
                    f"Tipo de sentencia no soportado en ejecucion: {type(sentencia).__name__}",
                    "EXEC_UNSUPPORTED_SENTENCE",
                )

    def _ejecutar_select(
        self, sentencia: SentenciaSelect, nombre_etiqueta: str
    ) -> None:
        if self._spark is None:
            self._agregar_error("Spark no esta inicializado", "EXEC_SPARK_NOT_INIT")
            return

        df_origen = self._obtener_dataframe_origen(sentencia)
        if df_origen is None:
            self._agregar_error(
                f"Tabla no encontrada: {sentencia.esquema or ''}.{sentencia.tabla}",
                "EXEC_TABLE_NOT_FOUND",
            )
            return

        df = df_origen

        if sentencia.join_externo:
            df = self._ejecutar_join(df, sentencia)

        if sentencia.condiciones_where:
            df = self._ejecutar_filtro_qlik(df, sentencia.condiciones_where)

        if sentencia.group_by:
            df = self._ejecutar_agregacion(df, sentencia)

        df = self._ejecutar_proyeccion(df, sentencia.proyecciones)

        self.registrar_dataframe(nombre_etiqueta, df)

    def _obtener_dataframe_origen(self, sentencia: SentenciaSelect) -> DataFrame | None:
        nombre_tabla = sentencia.tabla or sentencia.esquema
        if not nombre_tabla:
            return None

        if self.tiene_dataframe(nombre_tabla):
            return self.obtener_dataframe(nombre_tabla)

        if self._spark is not None:
            try:
                return self._spark.table(nombre_tabla)
            except Exception:
                return None
        return None

    def _ejecutar_join(
        self, df_izquierdo: DataFrame, sentencia: SentenciaSelect
    ) -> DataFrame:
        if self._spark is None or sentencia.join_externo is None:
            return df_izquierdo

        join_cond = sentencia.join_externo
        df_derecho = self.obtener_dataframe(join_cond.derecha)

        if df_derecho is None:
            self._agregar_error(
                f"Tabla join no encontrada: {join_cond.derecha}",
                "EXEC_JOIN_TABLE_NOT_FOUND",
            )
            return df_izquierdo

        if not HAS_PYSPARK:
            self._agregar_error(
                "pyspark no disponible para ejecutar join",
                "EXEC_SPARK_NOT_AVAILABLE",
            )
            return df_izquierdo

        cols_izq = set(df_izquierdo.columns)
        cols_der = set(df_derecho.columns)

        if join_cond.es_natural:
            campos_comunes = cols_izq & cols_der
            if not campos_comunes:
                self._agregar_error(
                    f"No se encontraron campos comunes para natural JOIN entre '{sentencia.tabla}' y '{join_cond.derecha}'",
                    "EXEC_NATURAL_JOIN_NO_COMMON_COLUMNS",
                )
                return df_izquierdo
            try:
                condiciones = [pyspark_f.col(c) == pyspark_f.col(c) for c in campos_comunes]
                from functools import reduce
                from pyspark.sql.functions import and_
                condicion = reduce(and_, condiciones)
                df_resultado = df_izquierdo.join(df_derecho, condicion, how="left")
                return df_resultado
            except Exception as e:
                self._agregar_error(
                    f"Error en natural join: {str(e)}",
                    "EXEC_NATURAL_JOIN_ERROR",
                )
                return df_izquierdo

        col_izq = join_cond.izquierda
        col_der = join_cond.derecha

        if col_izq not in cols_izq:
            self._agregar_error(
                f"Columna join izquierda '{col_izq}' no existe en tabla izquierda",
                "EXEC_JOIN_KEY_NOT_FOUND",
            )
            return df_izquierdo

        if col_der not in cols_der:
            self._agregar_error(
                f"Columna join derecha '{col_der}' no existe en tabla derecha",
                "EXEC_JOIN_KEY_NOT_FOUND",
            )
            return df_izquierdo

        try:
            condicion = pyspark_f.col(col_izq) == pyspark_f.col(col_der)
            df_resultado = df_izquierdo.join(df_derecho, condicion, how="left")
            return df_resultado
        except Exception as e:
            self._agregar_error(
                f"Error en join: {str(e)}",
                "EXEC_JOIN_ERROR",
            )
            return df_izquierdo

    def _ejecutar_filtro_qlik(
        self, df: DataFrame, condiciones: tuple[Expresion, ...]
    ) -> DataFrame:
        if not HAS_PYSPARK:
            return df

        filtro_compuesto: Column | None = None

        for condicion in condiciones:
            try:
                columna_filtro = self._compilador.compilar(condicion)
                if filtro_compuesto is None:
                    filtro_compuesto = columna_filtro
                else:
                    filtro_compuesto = pyspark_f.and_(
                        filtro_compuesto, columna_filtro
                    )
            except ErrorCompilacionExpresion as e:
                self._agregar_error(
                    f"Error compilando filtro: {e.mensaje}",
                    e.codigo or "EXEC_FILTER_ERROR",
                )

        if filtro_compuesto is not None:
            return df.filter(filtro_compuesto)
        return df

    def _ejecutar_agregacion(
        self, df: DataFrame, sentencia: SentenciaSelect
    ) -> DataFrame:
        if not sentencia.group_by:
            return df

        expresiones_grupo = []
        for expr_grupo in sentencia.group_by:
            try:
                col_expr = self._compilador.compilar(expr_grupo)
                expresiones_grupo.append(col_expr)
            except ErrorCompilacionExpresion as e:
                self._agregar_error(
                    f"Error compilando group by: {e.mensaje}",
                    e.codigo or "EXEC_GROUPBY_ERROR",
                )

        expresiones_select = list(expresiones_grupo)

        for item in sentencia.proyecciones:
            try:
                col_expr = self._compilador.compilar(item.expresion)
                nombre_col = item.alias or self._nombre_columna_expr(item.expresion)
                expresiones_select.append(col_expr.alias(nombre_col))
            except ErrorCompilacionExpresion as e:
                self._agregar_error(
                    f"Error compilando agregacion: {e.mensaje}",
                    e.codigo or "EXEC_AGG_ERROR",
                )

        if not expresiones_select:
            return df.groupBy(*expresiones_grupo).count()

        return df.groupBy(*expresiones_grupo).agg(*expresiones_select[len(expresiones_grupo):])

    def _ejecutar_proyeccion(
        self, df: DataFrame, proyecciones: Sequence[ProjectionItem]
    ) -> DataFrame:
        expresiones_select: list[Column] = []

        for item in proyecciones:
            try:
                col_expr = self._compilador.compilar(item.expresion)
                if item.alias:
                    expresiones_select.append(col_expr.alias(item.alias))
                elif item.expresion.tipo == TipoExpresion.COLUMNA:
                    expresiones_select.append(col_expr)
                else:
                    nombre_col = self._nombre_columna_expr(item.expresion)
                    expresiones_select.append(col_expr.alias(nombre_col))
            except ErrorCompilacionExpresion as e:
                self._agregar_error(
                    f"Error compilando proyeccion: {e.mensaje}",
                    e.codigo or "EXEC_PROJECTION_ERROR",
                )

        if not expresiones_select:
            return df

        df_resultado = df.select(*expresiones_select)

        return df_resultado

    def _nombre_columna_expr(self, expr: Expresion) -> str:
        if expr.tipo == TipoExpresion.COLUMNA:
            return expr.valor
        elif expr.tipo == TipoExpresion.FUNCION:
            args_str = "_".join(self._nombre_columna_expr(h) for h in expr.hijos)
            return f"{expr.valor}_{args_str}"
        elif expr.tipo == TipoExpresion.OPERACION_BINARIA:
            return f"{expr.valor}_{self._nombre_columna_expr(expr.hijos[0])}_{self._nombre_columna_expr(expr.hijos[1])}"
        elif expr.tipo == TipoExpresion.LITERAL_NUMERO:
            return f"num_{expr.valor}"
        elif expr.tipo == TipoExpresion.LITERAL_STRING:
            return f"str_{expr.valor}"
        return "expr"

    def _ejecutar_load(
        self, sentencia: SentenciaLoad, nombre_etiqueta: str
    ) -> None:
        if self._spark is None:
            self._agregar_error("Spark no esta inicializado", "EXEC_SPARK_NOT_INIT")
            return

        if sentencia.es_resident:
            df = self.obtener_dataframe(sentencia.etiqueta_resident or "")
            if df is None:
                self._agregar_error(
                    f"Tabla resident no encontrada: {sentencia.etiqueta_resident}",
                    "EXEC_RESIDENT_NOT_FOUND",
                )
                return
        elif sentencia.ruta:
            try:
                df = self._spark.read.load(sentencia.ruta)
            except Exception as e:
                self._agregar_error(
                    f"Error cargando tabla: {str(e)}",
                    "EXEC_LOAD_ERROR",
                )
                return
        else:
            self._agregar_error(
                "LOAD sin ruta ni RESIDENT",
                "EXEC_LOAD_MISSING",
            )
            return

        if sentencia.campos:
            if not HAS_PYSPARK:
                self._agregar_error(
                    "pyspark no disponible para seleccionar campos",
                    "EXEC_SPARK_NOT_AVAILABLE",
                )
                return
            df = df.select(*[pyspark_f.col(c) for c in sentencia.campos])

        if sentencia.distinct:
            df = df.distinct()

        self.registrar_dataframe(nombre_etiqueta, df)

    def _ejecutar_resident(
        self, sentencia: SentenciaResident, nombre_etiqueta: str
    ) -> None:
        df_origen = self.obtener_dataframe(sentencia.etiqueta_origen)
        if df_origen is None:
            self._agregar_error(
                f"Tabla resident no encontrada: {sentencia.etiqueta_origen}",
                "EXEC_RESIDENT_NOT_FOUND",
            )
            return

        df = df_origen

        if sentencia.expresion:
            try:
                filtro = self._compilador.compilar(sentencia.expresion)
                if HAS_PYSPARK:
                    df = df.filter(filtro)
            except ErrorCompilacionExpresion as e:
                self._agregar_error(
                    f"Error en filtro RESIDENT: {e.mensaje}",
                    e.codigo or "EXEC_RESIDENT_FILTER_ERROR",
                )

        self.registrar_dataframe(nombre_etiqueta, df)

    def _ejecutar_concatenate(self, sentencia: SentenciaConcatenate) -> None:
        df_objetivo = self.obtener_dataframe(sentencia.etiqueta_objetivo)
        df_origen = self.obtener_dataframe(sentencia.etiqueta_origen)

        if df_objetivo is None:
            self._agregar_error(
                f"Tabla objetivo no encontrada para concatenate: {sentencia.etiqueta_objetivo}",
                "EXEC_CONCAT_TARGET_NOT_FOUND",
            )
            return

        if df_origen is None:
            self._agregar_error(
                f"Tabla origen no encontrada para concatenate: {sentencia.etiqueta_origen}",
                "EXEC_CONCAT_SOURCE_NOT_FOUND",
            )
            return

        columnas_objetivo = set(df_objetivo.columns)
        columnas_origen = set(df_origen.columns)

        if columnas_objetivo != columnas_origen:
            columnas_faltantes_origen = columnas_objetivo - columnas_origen
            columnas_faltantes_objetivo = columnas_origen - columnas_objetivo

            if columnas_faltantes_origen:
                for col_name in columnas_faltantes_origen:
                    df_origen = df_origen.withColumn(
                        col_name, pyspark_f.lit(None)
                    )

            if columnas_faltantes_objetivo:
                for col_name in columnas_faltantes_objetivo:
                    df_objetivo = df_objetivo.withColumn(
                        col_name, pyspark_f.lit(None)
                    )

        df_resultado = df_objetivo.union(df_origen.select(df_objetivo.columns))
        self.registrar_dataframe(sentencia.etiqueta_objetivo, df_resultado)

    def _agregar_error(
        self, mensaje: str, codigo: str, ubicacion: SourceLocation | None = None
    ) -> None:
        self._errores.append(
            ErrorDataflow(
                mensaje=mensaje,
                ubicacion=ubicacion,
                codigo=codigo,
            )
        )

    def w_rank(self, partition_by: list[str], order_by: list[str]) -> Column:
        if not HAS_PYSPARK:
            raise RuntimeError("pyspark no disponible")

        particion = [pyspark_f.col(c) for c in partition_by] if partition_by else None
        orden = [pyspark_f.col(c).asc() for c in order_by] if order_by else [pyspark_f.col("1").asc()]

        ventana = pyspark_Window.partitionBy(*particion).orderBy(*orden) if particion else pyspark_Window.orderBy(*orden)

        return pyspark_f.rank().over(ventana)

    def wrank(self, partition_by: list[str], order_by: list[str]) -> Column:
        return self.w_rank(partition_by, order_by)
