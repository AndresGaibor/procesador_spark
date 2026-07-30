import pytest

from motor_spark.configuracion.argumentos import (
    ArgumentosDataflowScript,
    ArgumentosEjecucion,
    analizar_argumentos,
)


def test_dataflow_script_requiere_conexiones():
    with pytest.raises(SystemExit):
        analizar_argumentos([
            "--dataflow-script", "/tmp/script.df",
            "--ejecucion-id", "e-1",
        ])


def test_dataflow_script_requiere_ejecucion_id():
    with pytest.raises(SystemExit):
        analizar_argumentos([
            "--dataflow-script", "/tmp/script.df",
            "--conexiones", "/tmp/conexiones.json",
        ])


def test_dataflow_script_argumentos_validos():
    resultado = analizar_argumentos([
        "--dataflow-script", "/tmp/script.df",
        "--conexiones", "/tmp/conexiones.json",
        "--ejecucion-id", "e-1",
        "--resultado", "/tmp/result.json",
        "--solo-compilar",
        "--plan-salida", "/tmp/plan.json",
    ])

    assert isinstance(resultado, ArgumentosDataflowScript)
    assert resultado.dataflow_script == "/tmp/script.df"
    assert resultado.conexiones == "/tmp/conexiones.json"
    assert resultado.ejecucion_id == "e-1"
    assert resultado.resultado == "/tmp/result.json"
    assert resultado.solo_compilar is True
    assert resultado.plan_salida == "/tmp/plan.json"


def test_dataflow_script_atajo_dataflowscript():
    resultado = analizar_argumentos([
        "-dataflowscript", "/tmp/script.df",
        "--conexiones", "/tmp/conexiones.json",
        "--ejecucion-id", "e-1",
    ])

    assert isinstance(resultado, ArgumentosDataflowScript)
    assert resultado.dataflow_script == "/tmp/script.df"


def test_receta_funciona_con_entrada_salida():
    resultado = analizar_argumentos([
        "--receta", "/tmp/receta.json",
        "--entrada", "/tmp/in.csv",
        "--salida", "/tmp/out",
        "--ejecucion-id", "e-1",
    ])

    assert isinstance(resultado, ArgumentosEjecucion)
    assert resultado.receta == "/tmp/receta.json"
    assert resultado.entrada == "/tmp/in.csv"
    assert resultado.salida == "/tmp/out"


def test_dataflow_script_rechaza_entrada():
    with pytest.raises(SystemExit):
        analizar_argumentos([
            "--dataflow-script", "/tmp/script.df",
            "--conexiones", "/tmp/conexiones.json",
            "--ejecucion-id", "e-1",
            "--entrada", "/tmp/in.csv",
        ])


def test_dataflow_script_rechaza_salida():
    with pytest.raises(SystemExit):
        analizar_argumentos([
            "--dataflow-script", "/tmp/script.df",
            "--conexiones", "/tmp/conexiones.json",
            "--ejecucion-id", "e-1",
            "--salida", "/tmp/out",
        ])


def test_dataflow_script_rechaza_esquema():
    with pytest.raises(SystemExit):
        analizar_argumentos([
            "--dataflow-script", "/tmp/script.df",
            "--conexiones", "/tmp/conexiones.json",
            "--ejecucion-id", "e-1",
            "--esquema", "id:entero",
        ])


def test_solo_uno_de_receta_o_dataflow_script():
    with pytest.raises(SystemExit):
        analizar_argumentos([
            "--receta", "/tmp/receta.json",
            "--dataflow-script", "/tmp/script.df",
            "--conexiones", "/tmp/conexiones.json",
            "--ejecucion-id", "e-1",
        ])
