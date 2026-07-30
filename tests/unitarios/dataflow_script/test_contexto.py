from __future__ import annotations

import pytest

from motor_spark.dataflow_script.ast import (
    Etiqueta,
    Expresion,
    ProjectionItem,
    SentenciaLoad,
    SentenciaSelect,
    TipoExpresion,
)
from motor_spark.dataflow_script.ejecucion import (
    ContextoEjecucionDataflow,
    ErrorEjecucionDataflow,
    RegistroDataframe,
)


class TestContextoEjecucionDataflow:
    def setup_method(self) -> None:
        self._contexto = ContextoEjecucionDataflow()

    def test_contexto_inicial_sin_errores(self) -> None:
        assert not self._contexto.tiene_errores()
        assert len(self._contexto.errores) == 0

    def test_registrar_dataframe(self) -> None:
        class FakeDataFrame:
            pass

        df = FakeDataFrame()
        self._contexto.registrar_dataframe("test", df)

        assert self._contexto.tiene_dataframe("test")
        assert self._contexto.obtener_dataframe("test") is df

    def test_obtener_dataframe_no_existe(self) -> None:
        assert self._contexto.obtener_dataframe("inexistente") is None

    def test_registro_inmutable(self) -> None:
        class FakeDataFrame:
            pass

        df1 = FakeDataFrame()
        self._contexto.registrar_dataframe("test", df1)
        df2 = FakeDataFrame()
        self._contexto.registrar_dataframe("test2", df2)

        assert self._contexto.obtener_dataframe("test") is df1
        assert self._contexto.obtener_dataframe("test2") is df2
        assert len(self._contexto._registros) == 2

    def test_error_ejecucion_dataflow_formato_estable(self) -> None:
        error = ErrorEjecucionDataflow(
            mensaje="Error de prueba",
            ubicacion=None,
            codigo="TEST_ERROR",
        )
        formato = error.formato_estable()
        assert "TEST_ERROR" in formato
        assert "Error de prueba" in formato

    def test_registro_dataframe_tiene_etiqueta(self) -> None:
        class FakeDataFrame:
            pass

        df = FakeDataFrame()
        self._contexto.registrar_dataframe("mi_etiqueta", df)

        registro = RegistroDataframe(
            etiqueta="mi_etiqueta",
            dataframe=df,
        )
        assert registro.etiqueta == "mi_etiqueta"
        assert registro.dataframe is df


class TestSentenciasSelectSinSpark:
    def test_ejecutar_select_sin_spark_reporta_error(self) -> None:
        contexto = ContextoEjecucionDataflow(spark=None)

        sentencia = SentenciaSelect(
            proyecciones=(
                ProjectionItem(
                    expresion=Expresion(
                        tipo=TipoExpresion.COLUMNA,
                        valor="id",
                    ),
                    alias="id",
                ),
            ),
            tabla="usuarios",
            esquema=None,
        )

        etiqueta = Etiqueta(
            nombre="test_select",
            sentencias=(sentencia,),
        )

        contexto.ejecutar_etiqueta(etiqueta)

        assert contexto.tiene_errores()
        assert any(
            e.codigo == "EXEC_SPARK_NOT_INIT" for e in contexto.errores
        )


class TestSentenciasLoadSinSpark:
    def test_ejecutar_load_ruta_sin_spark_reporta_error(self) -> None:
        contexto = ContextoEjecucionDataflow(spark=None)

        sentencia = SentenciaLoad(
            ruta="/path/to/data",
            expresion=None,
            campos=(),
            distinct=False,
            es_resident=False,
        )

        etiqueta = Etiqueta(
            nombre="test_load",
            sentencias=(sentencia,),
        )

        contexto.ejecutar_etiqueta(etiqueta)

        assert contexto.tiene_errores()
        assert any(
            e.codigo == "EXEC_SPARK_NOT_INIT" for e in contexto.errores
        )

    def test_ejecutar_load_resident_sin_tabla_reporta_error(self) -> None:
        class FakeSpark:
            pass

        contexto = ContextoEjecucionDataflow(spark=FakeSpark())

        sentencia = SentenciaLoad(
            ruta="",
            expresion=None,
            campos=(),
            distinct=False,
            es_resident=True,
            etiqueta_resident="tabla_inexistente",
        )

        etiqueta = Etiqueta(
            nombre="test_load_resident",
            sentencias=(sentencia,),
        )

        contexto.ejecutar_etiqueta(etiqueta)

        assert contexto.tiene_errores()
        assert any(
            e.codigo == "EXEC_RESIDENT_NOT_FOUND" for e in contexto.errores
        )


class TestConcatenateSinSpark:
    def test_concatenate_objetivo_no_existe(self) -> None:
        from motor_spark.dataflow_script.ast import SentenciaConcatenate

        contexto = ContextoEjecucionDataflow(spark=None)

        class FakeDataFrame:
            columns = ["a", "b"]

        df_origen = FakeDataFrame()
        contexto.registrar_dataframe("origen", df_origen)

        sentencia = SentenciaConcatenate(
            etiqueta_objetivo="objetivo_inexistente",
            etiqueta_origen="origen",
        )

        contexto._ejecutar_concatenate(sentencia)

        assert contexto.tiene_errores()
        assert any(
            e.codigo == "EXEC_CONCAT_TARGET_NOT_FOUND" for e in contexto.errores
        )

    def test_concatenate_origen_no_existe(self) -> None:
        from motor_spark.dataflow_script.ast import SentenciaConcatenate

        contexto = ContextoEjecucionDataflow(spark=None)

        class FakeDataFrame:
            columns = ["a", "b"]

        df_objetivo = FakeDataFrame()
        contexto.registrar_dataframe("objetivo", df_objetivo)

        sentencia = SentenciaConcatenate(
            etiqueta_objetivo="objetivo",
            etiqueta_origen="origen_inexistente",
        )

        contexto._ejecutar_concatenate(sentencia)

        assert contexto.tiene_errores()
        assert any(
            e.codigo == "EXEC_CONCAT_SOURCE_NOT_FOUND" for e in contexto.errores
        )


class TestWRank:
    def test_w_rank_devuelve_columna(self) -> None:
        pytest.importorskip("pyspark")
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.master("local[1]").appName(
            "test-wrank"
        ).getOrCreate()
        try:
            contexto = ContextoEjecucionDataflow(spark=spark)
            resultado = contexto.w_rank(
                partition_by=["depto"],
                order_by=["fecha"],
            )
            assert resultado is not None
        finally:
            spark.stop()
