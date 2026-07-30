import pytest

from motor_spark.configuracion.argumentos import (
    ArgumentosDataflowScript,
    ArgumentosEjecucion,
    analizar_argumentos,
    crear_argumentos,
)


def test_parser_conserva_argumentos_obligatorios():
    parser = crear_argumentos()
    with pytest.raises(SystemExit) as error:
        parser.parse_args([])
    assert error.value.code == 2


def test_parser_acepta_contrato_actual():
    argumentos = analizar_argumentos(
        [
            "--receta",
            "{}",
            "--entrada",
            "/tmp/in.csv",
            "--salida",
            "/tmp/out",
            "--esquema",
            "id:entero",
            "--resultado",
            "/tmp/result.json",
            "--ejecucion-id",
            "e-1",
        ]
    )
    assert isinstance(argumentos, ArgumentosEjecucion)
    assert argumentos.receta == "{}"
    assert argumentos.entrada == "/tmp/in.csv"
    assert argumentos.salida == "/tmp/out"
    assert argumentos.esquema == "id:entero"
    assert argumentos.resultado == "/tmp/result.json"
    assert argumentos.ejecucion_id == "e-1"


def test_secreto_se_conserva_solo_en_argumentos_dataflow_script():
    dataflow = analizar_argumentos(
        [
            "--dataflow-script",
            "/tmp/script.df",
            "--conexiones",
            "/tmp/conexiones.json",
            "--ejecucion-id",
            "e-1",
            "--secreto",
            "DB_PASSWORD=valor-privado",
        ]
    )

    assert isinstance(dataflow, ArgumentosDataflowScript)
    assert dataflow.secretos == (("DB_PASSWORD", "valor-privado"),)

    with pytest.raises(SystemExit):
        analizar_argumentos(
            [
                "--receta",
                "{}",
                "--entrada",
                "/tmp/in.csv",
                "--salida",
                "/tmp/out",
                "--ejecucion-id",
                "e-1",
                "--secreto",
                "DB_PASSWORD=valor-privado",
            ]
        )


@pytest.mark.parametrize(
    "secretos, mensaje",
    [
        (["DB_PASSWORD"], "formato"),
        (["DB_PASSWORD=uno", "DB_PASSWORD=dos"], "repetir"),
    ],
)
def test_secreto_invalido_o_duplicado_no_expone_valor(secretos, mensaje, capsys):
    argumentos = [
        "--dataflow-script",
        "/tmp/script.df",
        "--conexiones",
        "/tmp/conexiones.json",
        "--ejecucion-id",
        "e-1",
    ]
    for secreto in secretos:
        argumentos.extend(["--secreto", secreto])

    with pytest.raises(SystemExit):
        analizar_argumentos(argumentos)

    stderr = capsys.readouterr().err
    assert mensaje in stderr
    assert "uno" not in stderr
    assert "dos" not in stderr
