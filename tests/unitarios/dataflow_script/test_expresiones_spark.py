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
from motor_spark.dataflow_script.expresiones import (
    CompiladorExpresion,
    ErrorCompilacionExpresion,
)
from motor_spark.dataflow_script.jdbc import (
    ConstructorSubconsulta,
    construir_reader_jdbc,
    construir_select,
)
from motor_spark.conexiones.modelos import ConexionJdbc


pytestmark = pytest.mark.spark


class TestCompiladorExpresionSpark:
    def test_compilar_columna(self, spark_local: pytest.FixtureRequest) -> None:
        from pyspark.sql import SparkSession

        spark = spark_local
        df = spark.createDataFrame([{"id": 1, "nombre": "test"}])
        compilador = CompiladorExpresion()

        expresion = Expresion(tipo=TipoExpresion.COLUMNA, valor="id")
        columna = compilador.compilar(expresion)

        resultado = df.select(columna).collect()
        assert resultado[0]["id"] == 1

    def test_compilar_literal_numero(self, spark_local: pytest.FixtureRequest) -> None:
        spark = spark_local
        df = spark.createDataFrame([{"x": 1}])
        compilador = CompiladorExpresion()

        expresion = Expresion(tipo=TipoExpresion.LITERAL_NUMERO, valor="42.5")
        columna = compilador.compilar(expresion)

        resultado = df.select(columna.alias("valor")).collect()
        assert resultado[0]["valor"] == 42.5

    def test_compilar_literal_string(self, spark_local: pytest.FixtureRequest) -> None:
        spark = spark_local
        df = spark.createDataFrame([{"x": 1}])
        compilador = CompiladorExpresion()

        expresion = Expresion(tipo=TipoExpresion.LITERAL_STRING, valor="'hola'")
        columna = compilador.compilar(expresion)

        resultado = df.select(columna.alias("valor")).collect()
        assert resultado[0]["valor"] == "hola"

    def test_compilar_operacion_aritmetica(self, spark_local: pytest.FixtureRequest) -> None:
        spark = spark_local
        df = spark.createDataFrame([{"a": 10, "b": 3}])
        compilador = CompiladorExpresion()

        expresion = Expresion(
            tipo=TipoExpresion.OPERACION_BINARIA,
            valor="+",
            hijos=(
                Expresion(tipo=TipoExpresion.COLUMNA, valor="a"),
                Expresion(tipo=TipoExpresion.COLUMNA, valor="b"),
            ),
        )
        columna = compilador.compilar(expresion)

        resultado = df.select(columna.alias("suma")).collect()
        assert resultado[0]["suma"] == 13

    def test_compilar_comparacion(self, spark_local: pytest.FixtureRequest) -> None:
        spark = spark_local
        df = spark.createDataFrame([{"a": 10, "b": 3}, {"a": 2, "b": 2}])
        compilador = CompiladorExpresion()

        expresion = Expresion(
            tipo=TipoExpresion.OPERACION_BINARIA,
            valor=">",
            hijos=(
                Expresion(tipo=TipoExpresion.COLUMNA, valor="a"),
                Expresion(tipo=TipoExpresion.COLUMNA, valor="b"),
            ),
        )
        columna = compilador.compilar(expresion)

        resultado = df.filter(columna).select("a", "b").collect()
        assert len(resultado) == 1
        assert resultado[0]["a"] == 10

    def test_compilar_and(self, spark_local: pytest.FixtureRequest) -> None:
        spark = spark_local
        df = spark.createDataFrame([{"a": 5, "b": 3}, {"a": 5, "b": 5}, {"a": 3, "b": 3}])
        compilador = CompiladorExpresion()

        expresion = Expresion(
            tipo=TipoExpresion.OPERACION_BINARIA,
            valor="AND",
            hijos=(
                Expresion(
                    tipo=TipoExpresion.OPERACION_BINARIA,
                    valor=">",
                    hijos=(
                        Expresion(tipo=TipoExpresion.COLUMNA, valor="a"),
                        Expresion(tipo=TipoExpresion.LITERAL_NUMERO, valor="3"),
                    ),
                ),
                Expresion(
                    tipo=TipoExpresion.OPERACION_BINARIA,
                    valor="<",
                    hijos=(
                        Expresion(tipo=TipoExpresion.COLUMNA, valor="b"),
                        Expresion(tipo=TipoExpresion.LITERAL_NUMERO, valor="5"),
                    ),
                ),
            ),
        )
        columna = compilador.compilar(expresion)

        resultado = df.filter(columna).collect()
        assert len(resultado) == 1
        assert resultado[0]["a"] == 5

    def test_compilar_not(self, spark_local: pytest.FixtureRequest) -> None:
        spark = spark_local
        df = spark.createDataFrame([{"a": True}, {"a": False}])
        compilador = CompiladorExpresion()

        expresion = Expresion(
            tipo=TipoExpresion.OPERACION_BINARIA,
            valor="NOT",
            hijos=(
                Expresion(tipo=TipoExpresion.COLUMNA, valor="a"),
            ),
        )
        columna = compilador.compilar(expresion)

        resultado = df.filter(columna).collect()
        assert len(resultado) == 1
        assert resultado[0]["a"] is False

    def test_compilar_trim(self, spark_local: pytest.FixtureRequest) -> None:
        spark = spark_local
        df = spark.createDataFrame([{"texto": "  hola  "}])
        compilador = CompiladorExpresion()

        expresion = Expresion(
            tipo=TipoExpresion.FUNCION,
            valor="TRIM",
            hijos=(
                Expresion(tipo=TipoExpresion.COLUMNA, valor="texto"),
            ),
        )
        columna = compilador.compilar(expresion)

        resultado = df.select(columna.alias("recortado")).collect()
        assert resultado[0]["recortado"] == "hola"

    def test_compilar_if(self, spark_local: pytest.FixtureRequest) -> None:
        spark = spark_local
        df = spark.createDataFrame([{"a": 5, "b": 10}, {"a": 3, "b": 2}])
        compilador = CompiladorExpresion()

        expresion = Expresion(
            tipo=TipoExpresion.FUNCION,
            valor="IF",
            hijos=(
                Expresion(
                    tipo=TipoExpresion.OPERACION_BINARIA,
                    valor=">",
                    hijos=(
                        Expresion(tipo=TipoExpresion.COLUMNA, valor="a"),
                        Expresion(tipo=TipoExpresion.COLUMNA, valor="b"),
                    ),
                ),
                Expresion(tipo=TipoExpresion.LITERAL_STRING, valor="'mayor'"),
                Expresion(tipo=TipoExpresion.LITERAL_STRING, valor="'menor'"),
            ),
        )
        columna = compilador.compilar(expresion)

        resultado = df.select(columna.alias("comparacion")).collect()
        assert resultado[0]["comparacion"] == "menor"
        assert resultado[1]["comparacion"] == "mayor"

    def test_compilar_coalesce(self, spark_local: pytest.FixtureRequest) -> None:
        spark = spark_local
        df = spark.createDataFrame([
            {"a": "otro", "b": "valor"},
            {"a": None, "b": "valor"},
        ])
        compilador = CompiladorExpresion()

        expresion = Expresion(
            tipo=TipoExpresion.FUNCION,
            valor="COALESCE",
            hijos=(
                Expresion(tipo=TipoExpresion.COLUMNA, valor="a"),
                Expresion(tipo=TipoExpresion.COLUMNA, valor="b"),
            ),
        )
        columna = compilador.compilar(expresion)

        resultado = df.select(columna.alias("coalescido")).collect()
        assert resultado[1]["coalescido"] == "valor"

    def test_compilar_isnull(self, spark_local: pytest.FixtureRequest) -> None:
        spark = spark_local
        df = spark.createDataFrame([{"a": None}, {"a": "valor"}])
        compilador = CompiladorExpresion()

        expresion = Expresion(
            tipo=TipoExpresion.FUNCION,
            valor="ISNULL",
            hijos=(
                Expresion(tipo=TipoExpresion.COLUMNA, valor="a"),
            ),
        )
        columna = compilador.compilar(expresion)

        resultado = df.select(columna.alias("es_nulo")).collect()
        assert resultado[0]["es_nulo"] == -1
        assert resultado[1]["es_nulo"] == 0

    def test_compilar_sum(self, spark_local: pytest.FixtureRequest) -> None:
        spark = spark_local
        df = spark.createDataFrame([{"a": 10}, {"a": 20}, {"a": 30}])
        compilador = CompiladorExpresion()

        expresion = Expresion(
            tipo=TipoExpresion.FUNCION,
            valor="SUM",
            hijos=(
                Expresion(tipo=TipoExpresion.COLUMNA, valor="a"),
            ),
        )
        columna = compilador.compilar(expresion)

        resultado = df.select(columna.alias("total")).collect()
        assert resultado[0]["total"] == 60

    def test_compilar_avg(self, spark_local: pytest.FixtureRequest) -> None:
        spark = spark_local
        df = spark.createDataFrame([{"a": 10}, {"a": 20}])
        compilador = CompiladorExpresion()

        expresion = Expresion(
            tipo=TipoExpresion.FUNCION,
            valor="AVG",
            hijos=(
                Expresion(tipo=TipoExpresion.COLUMNA, valor="a"),
            ),
        )
        columna = compilador.compilar(expresion)

        resultado = df.select(columna.alias("promedio")).collect()
        assert resultado[0]["promedio"] == 15.0

    def test_compilar_count_distinct(self, spark_local: pytest.FixtureRequest) -> None:
        spark = spark_local
        df = spark.createDataFrame([{"a": 1}, {"a": 1}, {"a": 2}])
        compilador = CompiladorExpresion()

        expresion = Expresion(
            tipo=TipoExpresion.FUNCION,
            valor="COUNT",
            hijos=(
                Expresion(tipo=TipoExpresion.COLUMNA, valor="DISTINCT"),
                Expresion(tipo=TipoExpresion.COLUMNA, valor="a"),
            ),
        )
        columna = compilador.compilar(expresion)

        resultado = df.select(columna.alias("conteo")).collect()
        assert resultado[0]["conteo"] == 2

    def test_compilar_funcion_desconocida_lanza_error(self, spark_local: pytest.FixtureRequest) -> None:
        spark = spark_local
        df = spark.createDataFrame([{"a": 1}])
        compilador = CompiladorExpresion()

        expresion = Expresion(
            tipo=TipoExpresion.FUNCION,
            valor="FUNCION_DESCONOCIDA",
            hijos=(),
        )

        with pytest.raises(ErrorCompilacionExpresion) as exc_info:
            compilador.compilar(expresion)

        assert "FUNCION_DESCONOCIDA" in str(exc_info.value)

    def test_compilar_num(self, spark_local: pytest.FixtureRequest) -> None:
        spark = spark_local
        df = spark.createDataFrame([{"a": "123.45"}])
        compilador = CompiladorExpresion()

        expresion = Expresion(
            tipo=TipoExpresion.FUNCION,
            valor="NUM",
            hijos=(
                Expresion(tipo=TipoExpresion.COLUMNA, valor="a"),
            ),
        )
        columna = compilador.compilar(expresion)

        resultado = df.select(columna.alias("numero")).collect()
        assert resultado[0]["numero"] == 123.45

    def test_compilar_month(self, spark_local: pytest.FixtureRequest) -> None:
        spark = spark_local
        df = spark.createDataFrame([{"fecha": "2024-07-15"}])
        compilador = CompiladorExpresion()

        expresion = Expresion(
            tipo=TipoExpresion.FUNCION,
            valor="MONTH",
            hijos=(
                Expresion(tipo=TipoExpresion.COLUMNA, valor="fecha"),
            ),
        )
        columna = compilador.compilar(expresion)

        resultado = df.select(columna.alias("mes")).collect()
        assert resultado[0]["mes"] == 7

    def test_compilar_year(self, spark_local: pytest.FixtureRequest) -> None:
        spark = spark_local
        df = spark.createDataFrame([{"fecha": "2024-07-15"}])
        compilador = CompiladorExpresion()

        expresion = Expresion(
            tipo=TipoExpresion.FUNCION,
            valor="YEAR",
            hijos=(
                Expresion(tipo=TipoExpresion.COLUMNA, valor="fecha"),
            ),
        )
        columna = compilador.compilar(expresion)

        resultado = df.select(columna.alias("anio")).collect()
        assert resultado[0]["anio"] == 2024


class TestJdbcSpark:
    def test_construir_select_con_columnas(self, spark_local: pytest.FixtureRequest) -> None:
        resultado = construir_select(
            esquema="public",
            tabla="usuarios",
            columnas=["id", "nombre"],
            url="jdbc:postgresql://localhost:5432/test",
            propiedades={"schema": "public"},
        )

        assert resultado is not None
        assert "SELECT" in resultado
        assert '"public"."usuarios"' in resultado
        assert "SELECT *" not in resultado

    def test_construir_reader_jdbc(self, spark_local: pytest.FixtureRequest) -> None:
        resultado = construir_reader_jdbc(
            tabla="usuarios",
            columnas=["id"],
            url="jdbc:postgresql://localhost:5432/test",
            propiedades={"schema": "public"},
        )

        assert "properties" in resultado
        assert "dbtable" in resultado["properties"]
        assert "SELECT" in resultado["properties"]["dbtable"]
        assert "SELECT *" not in resultado["properties"]["dbtable"]
