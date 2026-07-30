import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from motor_spark.aplicacion import ejecutor_dataflow
from motor_spark.aplicacion import ejecutor_motor
from motor_spark.configuracion.argumentos import ArgumentosDataflowScript


class SparkFalso:
    def __init__(self):
        self.detenciones = 0

    def stop(self):
        self.detenciones += 1


class DataFrameFalso:
    columns = ["id", "nombre"]
    schema = MagicMock()
    schema.simpleString.return_value = "struct<id:int,nombre:string>"
    schema.jsonValue.return_value = {"type": "struct", "fields": []}


def argumentos_dataflow(tmp_path, script="SELECT id, nombre FROM schema.tabla;", solo_compilar=False, plan_salida=None):
    resultado = tmp_path / "resultado.json"
    plan_path = tmp_path / "plan.json" if plan_salida else plan_salida
    return ArgumentosDataflowScript(
        dataflow_script=str(tmp_path / "script.df"),
        conexiones=str(tmp_path / "conexiones.json"),
        ejecucion_id="e-dataflow-1",
        resultado=str(resultado) if resultado else None,
        solo_compilar=solo_compilar,
        plan_salida=str(plan_path) if plan_path else None,
    )


def test_script_invalido_no_crea_spark(monkeypatch, tmp_path, capsys):
    (tmp_path / "script.df").write_text("SELECT FROM", encoding="utf-8")
    (tmp_path / "conexiones.json").write_text("{}", encoding="utf-8")

    creado = False

    def crear_sesion_mock(*args, **kwargs):
        nonlocal creado
        creado = True
        raise AssertionError("No debia crear Spark")

    monkeypatch.setattr(ejecutor_dataflow, "_crear_sesion_dataflow", crear_sesion_mock)
    monkeypatch.setattr(ejecutor_dataflow, "cargar_catalogo", lambda x: MagicMock())

    args = argumentos_dataflow(tmp_path)
    codigo = ejecutor_dataflow.ejecutar_dataflow(args)

    assert codigo == 1
    assert creado is False
    resultado = json.loads((tmp_path / "resultado.json").read_text())
    assert resultado["estado"] == "ERROR"
    assert "errores" in resultado


def test_solo_compilar_no_crea_spark_ni_conexiones(monkeypatch, tmp_path):
    (tmp_path / "script.df").write_text("LOAD 'ruta.csv';", encoding="utf-8")
    (tmp_path / "conexiones.json").write_text("{}", encoding="utf-8")
    (tmp_path / "plan.json").write_text("", encoding="utf-8")

    spark_creado = False
    jdbc_abierto = False

    def crear_sesion_mock(*args, **kwargs):
        nonlocal spark_creado
        spark_creado = True
        return SparkFalso()

    def leer_jdbc_mock(*args, **kwargs):
        nonlocal jdbc_abierto
        jdbc_abierto = True
        raise AssertionError("No debia abrir JDBC")

    monkeypatch.setattr(ejecutor_dataflow, "_crear_sesion_dataflow", crear_sesion_mock)
    monkeypatch.setattr(ejecutor_dataflow, "leer_jdbc", leer_jdbc_mock)

    args = argumentos_dataflow(tmp_path, solo_compilar=True, plan_salida=str(tmp_path / "plan.json"))
    codigo = ejecutor_dataflow.ejecutar_dataflow(args)

    assert codigo == 0
    assert spark_creado is False
    assert jdbc_abierto is False
    resultado = json.loads((tmp_path / "resultado.json").read_text())
    assert resultado["estado"] == "COMPILADO"
    assert "hash" in resultado
    assert "operaciones" in resultado
    plan_contenido = (tmp_path / "plan.json").read_text(encoding="utf-8")
    assert "operaciones" in plan_contenido


def test_solo_compilar_sin_plan_salida_falla(monkeypatch, tmp_path):
    (tmp_path / "script.df").write_text("LOAD 'ruta.csv';", encoding="utf-8")
    (tmp_path / "conexiones.json").write_text("{}", encoding="utf-8")

    monkeypatch.setattr(ejecutor_dataflow, "cargar_catalogo", lambda x: MagicMock())

    args = argumentos_dataflow(tmp_path, solo_compilar=True, plan_salida=None)
    codigo = ejecutor_dataflow.ejecutar_dataflow(args)

    assert codigo == 1
    resultado = json.loads((tmp_path / "resultado.json").read_text())
    assert resultado["estado"] == "ERROR"
    errores = resultado.get("errores", [])
    assert any("--solo-compilar requiere --plan-salida" in e.get("mensaje", "") for e in errores)


def test_resultado_no_filtra_secretos(monkeypatch, tmp_path):
    (tmp_path / "script.df").write_text("SELECT id, password FROM schema.usuarios;", encoding="utf-8")
    conexiones_content = {
        "jdbc": [{
            "nombre": "schema",
            "url": "jdbc:postgresql://localhost/db",
            "driver": "org.postgresql.Driver",
            "secreto_nombre": "DB_PASSWORD",
            "allowlist": [{"esquema": "schema", "tabla": "usuarios", "campos": ["id", "password"]}]
        }]
    }
    (tmp_path / "conexiones.json").write_text(json.dumps(conexiones_content), encoding="utf-8")

    spark_mock = SparkFalso()

    monkeypatch.setattr(ejecutor_dataflow, "_crear_sesion_dataflow", lambda *a: spark_mock)
    monkeypatch.setattr(
        ejecutor_dataflow,
        "cargar_catalogo",
        lambda x: MagicMock(**{
            "buscar_jdbc.return_value": MagicMock(
                url="jdbc:postgresql://localhost/db",
                secreto_nombre="DB_PASSWORD",
                propiedades={},
                allowlist=[MagicMock(esquema="schema", tabla="usuarios", campos=())]
            )
        })
    )

    args = argumentos_dataflow(tmp_path)
    codigo = ejecutor_dataflow.ejecutar_dataflow(args)

    resultado = json.loads((tmp_path / "resultado.json").read_text())
    if resultado.get("estado") == "ERROR":
        for err in resultado.get("errores", []):
            assert "secret" not in str(err).lower()
            assert "password" not in str(err).lower()
            assert "DB_PASSWORD" not in str(err)


def test_receta_sigue_despachando_al_flujo_original(monkeypatch, tmp_path):
    from motor_spark.configuracion.modelos.receta import RecetaConfig

    receta = RecetaConfig.model_validate({
        "nombre": "TestReceta",
        "entrada": {"modo_esquema": "inferir"},
        "salida": {},
    })

    datos = DataFrameFalso()
    spark = SparkFalso()

    monkeypatch.setattr(ejecutor_motor, "cargar_receta", lambda valor: receta)
    monkeypatch.setattr(
        ejecutor_motor,
        "resolver_esquema_entrada",
        lambda espec, config: ("inferir", None),
    )
    monkeypatch.setattr(ejecutor_motor, "crear_sesion_spark", lambda *a: spark)
    monkeypatch.setattr(ejecutor_motor, "leer_datos", lambda **kw: datos)
    monkeypatch.setattr(ejecutor_motor, "aplicar_pasos", lambda df, pasos: df)
    monkeypatch.setattr(
        ejecutor_motor,
        "escribir_datos",
        lambda **kw: {"archivo_success": True},
    )

    from motor_spark.configuracion.argumentos import ArgumentosEjecucion
    from motor_spark.aplicacion.ejecutor_motor import ejecutar_motor

    args_receta = ArgumentosEjecucion(
        receta="receta.json",
        entrada="entrada.csv",
        salida="salida",
        esquema="",
        resultado=str(tmp_path / "resultado.json"),
        ejecucion_id="e-receta-1",
    )

    codigo = ejecutar_motor(args_receta)

    assert codigo == 0
    resultado = json.loads((tmp_path / "resultado.json").read_text())
    assert resultado["estado"] == "COMPLETADO"
    assert resultado["receta"] == "TestReceta"


def test_operacion_no_ejecutable_aborta_antes_de_publicar(monkeypatch, tmp_path):
    (tmp_path / "script.df").write_text("STORE tabla INTO 'lib://destino/salida.csv';", encoding="utf-8")
    (tmp_path / "conexiones.json").write_text("{}", encoding="utf-8")

    spark_creado = False

    def crear_sesion_mock(*args, **kwargs):
        nonlocal spark_creado
        spark_creado = True
        return SparkFalso()

    monkeypatch.setattr(ejecutor_dataflow, "_crear_sesion_dataflow", crear_sesion_mock)
    monkeypatch.setattr(ejecutor_dataflow, "cargar_catalogo", lambda x: MagicMock())

    args = argumentos_dataflow(tmp_path)
    codigo = ejecutor_dataflow.ejecutar_dataflow(args)

    assert codigo == 1
    assert spark_creado is False
    resultado = json.loads((tmp_path / "resultado.json").read_text())
    assert resultado["estado"] == "ERROR"
    errores = resultado.get("errores", [])
    assert any("publicar" in e.get("mensaje", "").lower() or "PUBLICAR" in e.get("codigo", "") for e in errores)


def test_stop_llamado_exactamente_una_vez(monkeypatch, tmp_path):
    (tmp_path / "script.df").write_text("SELECT id, nombre FROM esquema.tabla WHERE id > 0;", encoding="utf-8")
    conexiones_content = {
        "jdbc": [{
            "nombre": "esquema",
            "url": "jdbc:postgresql://localhost/db",
            "driver": "org.postgresql.Driver",
            "secreto_nombre": "DB_PASSWORD",
            "allowlist": [{"esquema": "esquema", "tabla": "tabla", "campos": ["id", "nombre"]}]
        }]
    }
    (tmp_path / "conexiones.json").write_text(json.dumps(conexiones_content), encoding="utf-8")

    spark = SparkFalso()

    def crear_sesion_mock(*args, **kwargs):
        return spark

    def leer_jdbc_mock(spark, nombre_conexion, tabla, columnas, catalogo):
        return DataFrameFalso()

    def validar_mock(*args):
        return []

    monkeypatch.setattr(ejecutor_dataflow, "_crear_sesion_dataflow", crear_sesion_mock)
    monkeypatch.setattr(ejecutor_dataflow, "leer_jdbc", leer_jdbc_mock)
    monkeypatch.setattr(ejecutor_dataflow, "_validar", validar_mock)
    monkeypatch.setattr(
        ejecutor_dataflow,
        "cargar_catalogo",
        lambda x: MagicMock(**{
            "buscar_jdbc.return_value": MagicMock(
                url="jdbc:postgresql://localhost/db",
                secreto_nombre="DB_PASSWORD",
                propiedades={},
                allowlist=[MagicMock(esquema="esquema", tabla="tabla", campos=("id", "nombre"))]
            )
        })
    )

    args = argumentos_dataflow(tmp_path)
    codigo = ejecutor_dataflow.ejecutar_dataflow(args)

    assert spark.detenciones == 1
