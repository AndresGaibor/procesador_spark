import pytest

from motor_spark.configuracion.argumentos import analizar_argumentos, crear_argumentos


def test_parser_conserva_argumentos_obligatorios():
    parser = crear_argumentos()
    with pytest.raises(SystemExit) as error:
        parser.parse_args([])
    assert error.value.code == 2


def test_parser_acepta_contrato_actual():
    argumentos = analizar_argumentos([
        "--receta", "{}",
        "--entrada", "/tmp/in.csv",
        "--salida", "/tmp/out",
        "--esquema", "id:entero",
        "--resultado", "/tmp/result.json",
        "--ejecucion-id", "e-1",
    ])
    assert argumentos.receta == "{}"
    assert argumentos.entrada == "/tmp/in.csv"
    assert argumentos.salida == "/tmp/out"
    assert argumentos.esquema == "id:entero"
    assert argumentos.resultado == "/tmp/result.json"
    assert argumentos.ejecucion_id == "e-1"
