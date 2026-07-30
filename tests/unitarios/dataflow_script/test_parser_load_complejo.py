"""Regresiones del LOAD complejo generado por Qlik Dataflow."""

from motor_spark.dataflow_script.ast import SentenciaLoad, TipoExpresion
from motor_spark.dataflow_script.lexer import tokenizar
from motor_spark.dataflow_script.parser import parsear


def _parsear_load(script: str) -> SentenciaLoad:
    tokens, errores_lexicos = tokenizar(script)
    programa, errores_parser = parsear(tokens)
    assert errores_lexicos == []
    assert errores_parser == []
    sentencia = programa.etiquetas[0].sentencias[0]
    assert isinstance(sentencia, SentenciaLoad)
    return sentencia


def test_load_preserva_funciones_aliases_y_aritmetica():
    sentencia = _parsear_load(
        """
        [Curada]:
        LOAD
            Trim([metodo_pago]) AS [metodo_pago],
            [cantidad] * [precio_unitario] AS [venta_bruta],
            IF(Match([estado], 'COMPLETADA'), 'SI', 'NO') AS [es_valida]
        RESIDENT [Ventas];
        """
    )

    assert sentencia.es_resident is True
    assert sentencia.etiqueta_resident == "Ventas"
    assert [item.alias for item in sentencia.proyecciones] == [
        "metodo_pago",
        "venta_bruta",
        "es_valida",
    ]
    assert sentencia.proyecciones[0].expresion.tipo == TipoExpresion.FUNCION
    assert sentencia.proyecciones[1].expresion.tipo == TipoExpresion.OPERACION_BINARIA


def test_load_resident_preserva_where_y_group_by():
    sentencia = _parsear_load(
        """
        [Resumen]:
        LOAD
            [sucursal_id],
            Sum([total_venta]) AS [venta_mensual],
            Count(DISTINCT [venta_id]) AS [numero_ventas]
        RESIDENT [Ranking]
        WHERE [total_venta] > 0
        GROUP BY [sucursal_id];
        """
    )

    assert len(sentencia.condiciones_where) == 1
    assert sentencia.condiciones_where[0].tipo == TipoExpresion.OPERACION_BINARIA
    assert len(sentencia.group_by) == 1
    assert sentencia.group_by[0].valor == "sucursal_id"
    count = sentencia.proyecciones[2].expresion
    assert count.valor.upper() == "COUNT"
    assert [hijo.valor for hijo in count.hijos] == ["DISTINCT", "venta_id"]


def test_load_preserva_window_wrank():
    sentencia = _parsear_load(
        """
        [Ranking]:
        LOAD
            [venta_id],
            Window(
                WRank(1, 1),
                [sucursal_id],
                [anio_venta_year],
                [mes_venta_month],
                'DESC',
                [total_venta]
            ) AS [ranking_venta_mensual]
        RESIDENT [Ventas];
        """
    )

    ventana = sentencia.proyecciones[1].expresion
    assert ventana.tipo == TipoExpresion.WINDOW
    assert ventana.hijos[0].tipo == TipoExpresion.WINDOW_RANK
    assert sentencia.proyecciones[1].alias == "ranking_venta_mensual"
