import json

from motor_spark.aplicacion import ejecutor_motor
from motor_spark.configuracion.argumentos import ArgumentosEjecucion
from motor_spark.configuracion.modelos.receta import RecetaConfig


class EsquemaFalso:
    def jsonValue(self):
        return {"type": "struct", "fields": []}

    def simpleString(self):
        return "struct<>"


class DataFrameFalso:
    columns = ["id"]
    schema = EsquemaFalso()


class SparkFalso:
    def __init__(self):
        self.detenciones = 0

    def stop(self):
        self.detenciones += 1


def argumentos(tmp_path, receta):
    return ArgumentosEjecucion(
        receta=receta,
        entrada="entrada.csv",
        salida="salida",
        esquema="",
        resultado=str(tmp_path / "resultado.json"),
        ejecucion_id="e-1",
    )


def test_receta_invalida_no_crea_spark(monkeypatch, tmp_path, capsys):
    creado = False

    def crear_sesion(*args, **kwargs):
        nonlocal creado
        creado = True
        raise AssertionError("No debía crear Spark")

    monkeypatch.setattr(ejecutor_motor, "crear_sesion_spark", crear_sesion)
    codigo = ejecutor_motor.ejecutar_motor(argumentos(tmp_path, '{"entrada": {}}'))

    assert codigo == 1
    assert creado is False
    resultado = json.loads((tmp_path / "resultado.json").read_text())
    assert resultado["estado"] == "ERROR"
    assert resultado["tipo_error"] == "ErrorReceta"
    assert "RESULTADO_MOTOR=" in capsys.readouterr().err


def test_flujo_completo_devuelve_cero_y_detiene_spark(monkeypatch, tmp_path, capsys):
    spark = SparkFalso()
    receta = RecetaConfig.model_validate(
        {
            "nombre": "Ventas",
            "entrada": {"modo_esquema": "inferir"},
            "salida": {},
        }
    )
    datos = DataFrameFalso()

    monkeypatch.setattr(ejecutor_motor, "cargar_receta", lambda valor: receta)
    monkeypatch.setattr(
        ejecutor_motor,
        "resolver_esquema_entrada",
        lambda especificacion, configuracion: ("inferir", None),
    )
    monkeypatch.setattr(ejecutor_motor, "crear_sesion_spark", lambda *args: spark)
    monkeypatch.setattr(ejecutor_motor, "leer_datos", lambda **kwargs: datos)
    monkeypatch.setattr(ejecutor_motor, "aplicar_pasos", lambda df, pasos: df)
    monkeypatch.setattr(
        ejecutor_motor,
        "escribir_datos",
        lambda **kwargs: {
            "archivo_success": True,
            "cantidad_archivos_parquet": 1,
            "bytes_parquet": 10,
            "esquema_almacenamiento": "file",
        },
    )

    codigo = ejecutor_motor.ejecutar_motor(argumentos(tmp_path, "receta.json"))

    assert codigo == 0
    assert spark.detenciones == 1
    resultado = json.loads((tmp_path / "resultado.json").read_text())
    assert resultado["estado"] == "COMPLETADO"
    assert resultado["receta"] == "Ventas"
    salida = capsys.readouterr().out
    assert "EJECUCION_INICIO=e-1" in salida
    assert "MODO_ESQUEMA_ENTRADA=inferir" in salida
    assert "RESULTADO_MOTOR=" in salida
