from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from motor_spark.aplicacion.ejecutor_dataflow import (
    _compilar_plan,
    _construir_resultado_error_dataflow,
    _lexear,
    _normalizar_script,
    _parsear,
    _validar,
)
from motor_spark.configuracion.argumentos import ArgumentosDataflowScript
from motor_spark.dataflow_script import normalizador, tokenizar, parsear, validar_semantico
from motor_spark.dataflow_script.expresiones import CompiladorExpresion, compilar_expresion
from motor_spark.dataflow_script.ast import Expresion, TipoExpresion
from motor_spark.plan.compilador import compilar
from motor_spark.plan.modelos import PlanDataflow

pytestmark = pytest.mark.spark


def _skip_if_no_spark():
    if shutil.which("java") is None:
        pytest.skip("Java no instalado")
    try:
        import pyspark
    except ImportError:
        pytest.skip("PySpark no instalado")


class TestParsearValidarCompilar:
    def test_parsear_script_valido(self, script_path):
        _skip_if_no_spark()
        script = script_path("script_valido_load_csv.qvs")
        contenido = script.read_text(encoding="utf-8")
        contenido_norm, _ = normalizador.normalizar(contenido)
        tokens, _ = tokenizar(contenido_norm)
        programa, errores = parsear(tokens)
        assert len(errores) == 0
        tokens_vacio, _ = tokenizar("")
        programa_vacio, _ = parsear(tokens_vacio)
        assert isinstance(programa, type(programa_vacio))

    def test_parsear_script_invalido_reporta_errores(self, script_path):
        _skip_if_no_spark()
        script = script_path("script_invalido_select_sin_from.qvs")
        contenido = script.read_text(encoding="utf-8")
        contenido_norm, _ = normalizador.normalizar(contenido)
        tokens, _ = tokenizar(contenido_norm)
        _, errores = parsear(tokens)
        assert len(errores) > 0

    def test_validar_semantico_script_valido(self, script_path):
        _skip_if_no_spark()
        script = script_path("script_valido_load_csv.qvs")
        contenido = script.read_text(encoding="utf-8")
        contenido_norm, _ = normalizador.normalizar(contenido)
        tokens, _ = tokenizar(contenido_norm)
        programa, _ = parsear(tokens)
        errores_sem = validar_semantico(programa)
        assert len(errores_sem) == 0

    def test_validar_semantico_drop_inexistente(self, script_path):
        _skip_if_no_spark()
        script = script_path("script_invalido_drop_inexistente.qvs")
        contenido = script.read_text(encoding="utf-8")
        contenido_norm, _ = normalizador.normalizar(contenido)
        tokens, _ = tokenizar(contenido_norm)
        programa, _ = parsear(tokens)
        errores_sem = validar_semantico(programa)
        assert len(errores_sem) > 0

    def test_compilar_plan_desde_script_valido(self, script_path):
        _skip_if_no_spark()
        script = script_path("script_valido_load_csv.qvs")
        contenido = script.read_text(encoding="utf-8")
        contenido_norm, _ = normalizador.normalizar(contenido)
        tokens, _ = tokenizar(contenido_norm)
        programa, _ = parsear(tokens)
        plan = compilar(programa)
        assert isinstance(plan, PlanDataflow)
        assert len(plan.operaciones) > 0


class TestSoloCompilar:
    def test_solo_compilar_genera_plan_sin_ejecutar(self, script_path, tmp_path):
        _skip_if_no_spark()
        script = script_path("script_valido_load_csv.qvs")
        resultado = tmp_path / "resultado.json"
        plan_salida = tmp_path / "plan.json"

        args = ArgumentosDataflowScript(
            dataflow_script=str(script),
            conexiones=str(Path(__file__).parent.parent / "recursos" / "dataflow" / "conexiones" / "catalogo_vacio.json"),
            ejecucion_id="test-solo-compilar",
            resultado=str(resultado),
            solo_compilar=True,
            plan_salida=str(plan_salida),
        )

        from motor_spark.aplicacion.ejecutor_dataflow import ejecutar_dataflow
        codigo = ejecutar_dataflow(args)

        assert codigo == 0
        assert plan_salida.exists()
        datos_plan = json.loads(plan_salida.read_text(encoding="utf-8"))
        assert "operaciones" in datos_plan

    def test_solo_compilar_sin_plan_salida_falla(self, script_path, tmp_path):
        _skip_if_no_spark()
        script = script_path("script_valido_load_csv.qvs")
        resultado = tmp_path / "resultado.json"

        args = ArgumentosDataflowScript(
            dataflow_script=str(script),
            conexiones=str(Path(__file__).parent.parent / "recursos" / "dataflow" / "conexiones" / "catalogo_vacio.json"),
            ejecucion_id="test-solo-compilar-error",
            resultado=str(resultado),
            solo_compilar=True,
            plan_salida=None,
        )

        from motor_spark.aplicacion.ejecutor_dataflow import ejecutar_dataflow
        codigo = ejecutar_dataflow(args)

        assert codigo == 1


class TestExpresiones:
    def test_compilar_expresion_columna(self, spark_local):
        _skip_if_no_spark()
        expr = Expresion(tipo=TipoExpresion.COLUMNA, valor="nombre")
        resultado = compilar_expresion(expr)
        assert resultado is not None

    def test_compilar_expresion_literal_string(self, spark_local):
        _skip_if_no_spark()
        expr = Expresion(tipo=TipoExpresion.LITERAL_STRING, valor="'hola'")
        resultado = compilar_expresion(expr)
        assert resultado is not None

    def test_compilar_expresion_literal_numero(self, spark_local):
        _skip_if_no_spark()
        expr = Expresion(tipo=TipoExpresion.LITERAL_NUMERO, valor="42.5")
        resultado = compilar_expresion(expr)
        assert resultado is not None

    def test_compilar_expresion_funcion_if(self, spark_local):
        _skip_if_no_spark()
        cond = Expresion(tipo=TipoExpresion.COLUMNA, valor="x")
        val_true = Expresion(tipo=TipoExpresion.LITERAL_NUMERO, valor="1")
        val_false = Expresion(tipo=TipoExpresion.LITERAL_NUMERO, valor="0")
        expr = Expresion(
            tipo=TipoExpresion.FUNCION,
            valor="IF",
            hijos=(cond, val_true, val_false),
        )
        resultado = compilar_expresion(expr)
        assert resultado is not None

    def test_compilar_expresion_operacion_binaria(self, spark_local):
        _skip_if_no_spark()
        izq = Expresion(tipo=TipoExpresion.COLUMNA, valor="a")
        der = Expresion(tipo=TipoExpresion.COLUMNA, valor="b")
        expr = Expresion(
            tipo=TipoExpresion.OPERACION_BINARIA,
            valor="+",
            hijos=(izq, der),
        )
        resultado = compilar_expresion(expr)
        assert resultado is not None

    def test_compilar_expresion_logica_and_or(self, spark_local):
        _skip_if_no_spark()
        izq = Expresion(tipo=TipoExpresion.COLUMNA, valor="x")
        der = Expresion(tipo=TipoExpresion.COLUMNA, valor="y")
        and_expr = Expresion(
            tipo=TipoExpresion.OPERACION_BINARIA,
            valor="AND",
            hijos=(izq, der),
        )
        resultado = compilar_expresion(and_expr)
        assert resultado is not None


class TestConcatenacion:
    def test_concatenate_dos_tablas_iguales(self, spark_local, script_path):
        _skip_if_no_spark()
        parte1 = spark_local.createDataFrame(
            [(1, "Alpha"), (2, "Beta")],
            ["id", "nombre"],
        )
        parte2 = spark_local.createDataFrame(
            [(3, "Gamma"), (4, "Delta")],
            ["id", "nombre"],
        )
        parte1.createOrReplaceTempView("parte1")
        parte2.createOrReplaceTempView("parte2")

        from motor_spark.dataflow_script.ejecucion import ContextoEjecucionDataflow

        contexto = ContextoEjecucionDataflow(spark=spark_local)
        contexto.registrar_dataframe("tabla1", parte1)
        contexto.registrar_dataframe("tabla2", parte2)

        resultado = parte1.union(parte2.select(parte1.columns))
        contexto.registrar_dataframe("tabla1", resultado, reemplazo=True)

        df_resultado = contexto.obtener_dataframe("tabla1")
        assert df_resultado is not None
        assert df_resultado.count() == 4


class TestJoinsSeguros:
    def test_join_left_con_condiciones_correctas(self, spark_local):
        _skip_if_no_spark()
        df_izq = spark_local.createDataFrame(
            [(1, "Ana"), (2, "Carlos")],
            ["id", "nombre"],
        )
        df_der = spark_local.createDataFrame(
            [(1, 150.0), (2, 90.0)],
            ["id", "monto"],
        )

        from motor_spark.dataflow_script.ejecucion import ContextoEjecucionDataflow
        from pyspark.sql import functions as F

        contexto = ContextoEjecucionDataflow(spark=spark_local)
        contexto.registrar_dataframe("tabla_izq", df_izq)
        contexto.registrar_dataframe("tabla_der", df_der)

        df_join = df_izq.join(df_der, "id", how="left")

        assert df_join.count() == 2
        assert "nombre" in df_join.columns
        assert "monto" in df_join.columns

    def test_join_con_columnas_inexistentes_reporta_error(self, spark_local):
        _skip_if_no_spark()
        df_izq = spark_local.createDataFrame(
            [(1, "Ana")],
            ["id", "nombre"],
        )
        df_der = spark_local.createDataFrame(
            [(1, 150.0)],
            ["cliente_id", "monto"],
        )

        from motor_spark.dataflow_script.ejecucion import ContextoEjecucionDataflow

        contexto = ContextoEjecucionDataflow(spark=spark_local)
        contexto.registrar_dataframe("tabla_izq", df_izq)
        contexto.registrar_dataframe("tabla_der", df_der)

        assert contexto.tiene_errores() is False


class TestFiltros:
    def test_filtro_where_simple(self, spark_local):
        _skip_if_no_spark()
        df = spark_local.createDataFrame(
            [(1, "Ana", 150.0), (2, "Carlos", 90.0), (3, "Maria", 200.0)],
            ["id", "nombre", "monto"],
        )

        from motor_spark.dataflow_script.ejecucion import ContextoEjecucionDataflow
        from motor_spark.dataflow_script.expresiones import CompiladorExpresion
        from motor_spark.dataflow_script.ast import Expresion, TipoExpresion

        contexto = ContextoEjecucionDataflow(spark=spark_local)
        contexto.registrar_dataframe("ventas", df)

        expr = Expresion(
            tipo=TipoExpresion.OPERACION_BINARIA,
            valor=">",
            hijos=(
                Expresion(tipo=TipoExpresion.COLUMNA, valor="monto"),
                Expresion(tipo=TipoExpresion.LITERAL_NUMERO, valor="100"),
            ),
        )
        compilador = CompiladorExpresion()
        filtro_col = compilador.compilar(expr)

        df_filtrado = df.filter(filtro_col)
        assert df_filtrado.count() == 2

    def test_filtro_compuesto_and(self, spark_local):
        _skip_if_no_spark()
        df = spark_local.createDataFrame(
            [(1, "Ana", 150.0, "Norte"), (2, "Carlos", 90.0, "Sur"), (3, "Maria", 200.0, "Norte")],
            ["id", "nombre", "monto", "region"],
        )

        from motor_spark.dataflow_script.ejecucion import ContextoEjecucionDataflow
        from motor_spark.dataflow_script.expresiones import CompiladorExpresion
        from motor_spark.dataflow_script.ast import Expresion, TipoExpresion

        contexto = ContextoEjecucionDataflow(spark=spark_local)
        contexto.registrar_dataframe("ventas", df)

        monto_cond = Expresion(
            tipo=TipoExpresion.OPERACION_BINARIA,
            valor=">",
            hijos=(
                Expresion(tipo=TipoExpresion.COLUMNA, valor="monto"),
                Expresion(tipo=TipoExpresion.LITERAL_NUMERO, valor="100"),
            ),
        )
        region_cond = Expresion(
            tipo=TipoExpresion.OPERACION_BINARIA,
            valor="=",
            hijos=(
                Expresion(tipo=TipoExpresion.COLUMNA, valor="region"),
                Expresion(tipo=TipoExpresion.LITERAL_STRING, valor="'Norte'"),
            ),
        )
        and_compuesto = Expresion(
            tipo=TipoExpresion.OPERACION_BINARIA,
            valor="AND",
            hijos=(monto_cond, region_cond),
        )

        compilador = CompiladorExpresion()
        filtro_col = compilador.compilar(and_compuesto)

        df_filtrado = df.filter(filtro_col)
        assert df_filtrado.count() == 2


class TestAgregacion:
    def test_agregacion_sum_count(self, spark_local):
        _skip_if_no_spark()
        df = spark_local.createDataFrame(
            [("Norte", 100.0), ("Norte", 200.0), ("Sur", 150.0)],
            ["region", "monto"],
        )

        from pyspark.sql import functions as F

        df_agg = df.groupBy("region").agg(
            F.sum("monto").alias("total"),
            F.count("*").alias("cantidad"),
        )
        resultados = {row.region: (row.total, row.cantidad) for row in df_agg.collect()}

        assert resultados["Norte"][0] == 300.0
        assert resultados["Norte"][1] == 2
        assert resultados["Sur"][0] == 150.0
        assert resultados["Sur"][1] == 1

    def test_agregacion_avg_min_max(self, spark_local):
        _skip_if_no_spark()
        df = spark_local.createDataFrame(
            [("Norte", 100.0), ("Norte", 200.0), ("Sur", 150.0)],
            ["region", "monto"],
        )

        from pyspark.sql import functions as F

        df_agg = df.groupBy("region").agg(
            F.avg("monto").alias("promedio"),
            F.min("monto").alias("minimo"),
            F.max("monto").alias("maximo"),
        )
        resultados = {row.region: (row.promedio, row.minimo, row.maximo) for row in df_agg.collect()}

        assert resultados["Norte"][0] == 150.0
        assert resultados["Norte"][1] == 100.0
        assert resultados["Norte"][2] == 200.0


class TestWRank:
    def test_wrank_partition_order(self, spark_local):
        _skip_if_no_spark()
        df = spark_local.createDataFrame(
            [
                ("A", 1, 100.0),
                ("A", 2, 200.0),
                ("A", 1, 150.0),
                ("B", 1, 80.0),
            ],
            ["region", "prioridad", "monto"],
        )

        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        ventana = Window.partitionBy("region").orderBy(F.col("monto").desc())
        df_con_rank = df.withColumn("rank", F.rank().over(ventana))

        assert "rank" in df_con_rank.columns
        resultados = {
            (row.region, row.prioridad): row.rank
            for row in df_con_rank.orderBy("region", "monto").collect()
        }
        assert resultados[("A", 2)] == 1
        assert resultados[("A", 1)] == 2


class TestCsvLocal:
    def test_leer_csv_local_con_spark(self, spark_local, datos_path):
        _skip_if_no_spark()
        csv_path = datos_path / "ventas.csv"
        df = spark_local.read.csv(str(csv_path), header=True, inferSchema=True)

        assert df.count() == 5
        assert "cliente" in df.columns
        assert "monto" in df.columns

    def test_escribir_csv_local(self, spark_local, tmp_path):
        _skip_if_no_spark()
        df = spark_local.createDataFrame(
            [(1, "Ana", 150.0), (2, "Carlos", 90.0)],
            ["id", "nombre", "monto"],
        )
        salida = tmp_path / "salida_csv"
        salida.mkdir()

        df.write.csv(str(salida), header=True, mode="overwrite")

        archivos = list(salida.glob("*.csv"))
        assert len(archivos) >= 1 or list(salida.glob("part-*.csv"))
