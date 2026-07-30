"""Contratos de prefijos Qlik aplicados a la carga siguiente."""

from pathlib import Path

from motor_spark.dataflow_script.ast import SentenciaLoad, SentenciaSelect
from motor_spark.dataflow_script.lexer import tokenizar
from motor_spark.dataflow_script.normalizador import normalizar
from motor_spark.dataflow_script.parser import parsear
from motor_spark.dataflow_script.validador import validar_semantico


def _parsear(script: str):
    tokens, errores_lexicos = tokenizar(script)
    programa, errores_parser = parsear(tokens)
    assert errores_lexicos == []
    assert errores_parser == []
    return programa


def test_set_acepta_valor_numerico_sin_comillas():
    programa = _parsear("SET FirstWeekDay=6; SET ReferenceDay=0;")

    assert [sentencia.valor for sentencia in programa.sentencias_globales] == [
        "6",
        "0",
    ]


def test_concatenate_agrupa_load_y_select_bajo_un_prefijo():
    programa = _parsear(
        """
        CONCATENATE([Ventas])
        LOAD [id], [monto];
        SELECT "id", "monto" FROM "demo"."ventas_2026";
        """
    )

    assert len(programa.etiquetas) == 1
    carga, fuente = programa.etiquetas[0].sentencias
    assert isinstance(carga, SentenciaLoad)
    assert isinstance(fuente, SentenciaSelect)
    assert carga.concatenate_objetivo == "Ventas"
    assert carga.join_objetivo is None


def test_left_join_agrupa_load_y_select_y_conserva_objetivo():
    programa = _parsear(
        """
        LEFT JOIN([Ventas])
        LOAD [cliente_id], [nombres] AS [cliente_nombres];
        SELECT "cliente_id", "nombres" FROM "demo"."clientes";
        """
    )

    assert len(programa.etiquetas) == 1
    carga, fuente = programa.etiquetas[0].sentencias
    assert isinstance(carga, SentenciaLoad)
    assert isinstance(fuente, SentenciaSelect)
    assert carga.join_objetivo == "Ventas"
    assert carga.join_tipo == "LEFT"
    assert carga.concatenate_objetivo is None


def test_script_bancolombia_completo_llega_al_ast():
    script = (
        Path(__file__).parents[2]
        / "recursos"
        / "dataflow"
        / "scripts"
        / "bancolombia_ventas_completo.qvs"
    )
    contenido = script.read_text(encoding="utf-8")
    contenido, errores_normalizacion = normalizar(contenido)

    tokens, errores_lexicos = tokenizar(contenido)
    programa, errores_parser = parsear(tokens)

    assert errores_normalizacion == []
    assert errores_lexicos == []
    assert errores_parser == []
    assert validar_semantico(programa) == []
    assert len(programa.etiquetas) >= 10


def test_compilador_materializa_concatenate():
    from motor_spark.plan.compilador import compilar
    from motor_spark.plan.modelos import Concatenar, EliminarTabla

    programa = _parsear(
        """
        [Ventas]: LOAD [id] FROM [lib://Archivos/ventas.csv];
        CONCATENATE([Ventas]) LOAD [id];
        SELECT "id" FROM "demo"."ventas_2026";
        """
    )
    plan = compilar(programa)
    concatenaciones = [
        operacion for operacion in plan.operaciones if isinstance(operacion, Concatenar)
    ]

    assert len(concatenaciones) == 1
    assert concatenaciones[0].tabla_objetivo == "Ventas"
    assert any(
        isinstance(operacion, EliminarTabla)
        and operacion.nombre.startswith("_prefijo_concatenate_")
        for operacion in plan.operaciones
    )


def test_compilador_materializa_left_join():
    from motor_spark.plan.compilador import compilar
    from motor_spark.plan.modelos import EliminarTabla, Unir

    programa = _parsear(
        """
        [Ventas]: LOAD [cliente_id] FROM [lib://Archivos/ventas.csv];
        LEFT JOIN([Ventas]) LOAD [cliente_id], [nombres];
        SELECT "cliente_id", "nombres" FROM "demo"."clientes";
        """
    )
    plan = compilar(programa)
    joins = [operacion for operacion in plan.operaciones if isinstance(operacion, Unir)]

    assert len(joins) == 1
    assert joins[0].tabla_izquierda == "Ventas"
    assert joins[0].condicion_on == "NATURAL"
    assert any(
        isinstance(operacion, EliminarTabla)
        and operacion.nombre.startswith("_prefijo_left_")
        for operacion in plan.operaciones
    )
