"""Equivalencias Spark de funciones Qlik con semántica no evidente."""

import pytest

from motor_spark.dataflow_script.ast import Expresion, TipoExpresion
from motor_spark.dataflow_script.expresiones import (
    CompiladorExpresion,
    ErrorCompilacionExpresion,
)

pytestmark = pytest.mark.spark


def _columna(nombre: str) -> Expresion:
    return Expresion(tipo=TipoExpresion.COLUMNA, valor=nombre)


def _numero(valor: int) -> Expresion:
    return Expresion(tipo=TipoExpresion.LITERAL_NUMERO, valor=str(valor))


def _texto(valor: str) -> Expresion:
    # El AST conserva las comillas originales del script Qlik.
    return Expresion(
        tipo=TipoExpresion.LITERAL_STRING,
        valor=f"'{valor}'",
    )


def test_match_devuelve_primera_posicion_con_patrones_repetidos(spark_local):
    dataframe = spark_local.createDataFrame([("A",), ("B",), ("X",)], ["valor"])
    expresion = Expresion(
        tipo=TipoExpresion.FUNCION,
        valor="MATCH",
        hijos=(_columna("valor"), _texto("A"), _texto("B"), _texto("A")),
    )

    resultados = [
        fila.posicion
        for fila in dataframe.select(
            CompiladorExpresion().compilar(expresion).alias("posicion")
        ).collect()
    ]

    assert resultados == [1, 2, 0]


def test_indexregex_devuelve_posicion_inicial_uno_based(spark_local):
    dataframe = spark_local.createDataFrame(
        [("abc-123",), ("sin numero",)],
        ["texto"],
    )
    expresion = Expresion(
        tipo=TipoExpresion.FUNCION,
        valor="INDEXREGEX",
        hijos=(_columna("texto"), _texto(r"\d+")),
    )

    resultados = [
        fila.posicion
        for fila in dataframe.select(
            CompiladorExpresion().compilar(expresion).alias("posicion")
        ).collect()
    ]

    assert resultados == [5, 0]


def test_window_wrank_modo_uno_con_empates(spark_local):
    dataframe = spark_local.createDataFrame(
        [
            ("Norte", 200.0),
            ("Norte", 200.0),
            ("Norte", 100.0),
            ("Sur", 50.0),
        ],
        ["region", "total"],
    )
    wrank = Expresion(
        tipo=TipoExpresion.WINDOW_RANK,
        valor="WRank",
        hijos=(_numero(1), _numero(1)),
    )
    ventana = Expresion(
        tipo=TipoExpresion.WINDOW,
        valor="Window",
        hijos=(
            wrank,
            _columna("region"),
            _texto("DESC"),
            _columna("total"),
        ),
    )

    filas = (
        dataframe.select(
            "region",
            "total",
            CompiladorExpresion().compilar(ventana).alias("ranking"),
        )
        .orderBy("region", dataframe.total.desc())
        .collect()
    )

    assert [(fila.region, fila.total, fila.ranking) for fila in filas] == [
        ("Norte", 200.0, 1),
        ("Norte", 200.0, 1),
        ("Norte", 100.0, 3),
        ("Sur", 50.0, 1),
    ]


def test_wrank_fuera_de_window_se_rechaza(spark_local):
    expresion = Expresion(
        tipo=TipoExpresion.WINDOW_RANK,
        valor="WRank",
        hijos=(_numero(1), _numero(1)),
    )

    with pytest.raises(ErrorCompilacionExpresion, match="solo puede usarse"):
        CompiladorExpresion().compilar(expresion)


def test_window_wrank_modo_no_implementado_se_rechaza(spark_local):
    expresion = Expresion(
        tipo=TipoExpresion.WINDOW,
        valor="Window",
        hijos=(
            Expresion(
                tipo=TipoExpresion.WINDOW_RANK,
                valor="WRank",
                hijos=(_numero(2), _numero(1)),
            ),
            _texto("DESC"),
            _columna("total"),
        ),
    )

    with pytest.raises(ErrorCompilacionExpresion, match="mode=2"):
        CompiladorExpresion().compilar(expresion)


def test_if_acepta_match_numerico_como_condicion(spark_local):
    dataframe = spark_local.createDataFrame([("OK",), ("ERROR",)], ["estado"])
    match = Expresion(
        tipo=TipoExpresion.FUNCION,
        valor="MATCH",
        hijos=(_columna("estado"), _texto("OK")),
    )
    expresion = Expresion(
        tipo=TipoExpresion.FUNCION,
        valor="IF",
        hijos=(match, _texto("valido"), _texto("invalido")),
    )

    valores = [
        fila.valor
        for fila in dataframe.select(
            CompiladorExpresion().compilar(expresion).alias("valor")
        ).collect()
    ]

    assert valores == ["valido", "invalido"]


def test_predicado_convierte_indexregex_a_booleano(spark_local):
    dataframe = spark_local.createDataFrame(
        [("COMPLETADA",), ("PENDIENTE",)],
        ["estado"],
    )
    expresion = Expresion(
        tipo=TipoExpresion.FUNCION,
        valor="INDEXREGEX",
        hijos=(_columna("estado"), _texto(r"^(COMPLETADA|DEVUELTA)$")),
    )

    resultado = dataframe.filter(
        CompiladorExpresion().compilar_predicado(expresion)
    ).collect()

    assert [fila.estado for fila in resultado] == ["COMPLETADA"]


def test_not_isnull_se_compila_como_negacion_booleana(spark_local):
    dataframe = spark_local.createDataFrame(
        [(1, "valor"), (2, None)],
        ["id", "dato"],
    )
    is_null = Expresion(
        tipo=TipoExpresion.FUNCION,
        valor="ISNULL",
        hijos=(_columna("dato"),),
    )
    expresion = Expresion(
        tipo=TipoExpresion.OPERACION_BINARIA,
        valor="NOT",
        hijos=(is_null,),
    )

    resultado = dataframe.filter(
        CompiladorExpresion().compilar_predicado(expresion)
    ).collect()

    assert [fila.id for fila in resultado] == [1]
