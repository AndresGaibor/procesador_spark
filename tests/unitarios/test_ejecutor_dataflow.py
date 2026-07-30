import json
from unittest.mock import MagicMock

from motor_spark.aplicacion import ejecutor_dataflow, ejecutor_motor
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


def argumentos_dataflow(
    tmp_path,
    script="SELECT id, nombre FROM schema.tabla;",
    solo_compilar=False,
    plan_salida=None,
):
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


def test_plan_sin_operaciones_falla_antes_de_crear_spark(monkeypatch, tmp_path):
    # Un script compuesto solo por SET es sintácticamente válido, pero no
    # representa trabajo ejecutable. Aceptarlo como COMPLETADO ocultaría
    # scripts truncados o dañados durante el transporte desde Talend/Fish.
    (tmp_path / "script.df").write_text(
        "SET ThousandSep=',';",
        encoding="utf-8",
    )
    (tmp_path / "conexiones.json").write_text("{}", encoding="utf-8")

    spark_creado = False

    def crear_sesion_mock(*args, **kwargs):
        nonlocal spark_creado
        spark_creado = True
        raise AssertionError("Un plan vacío no debe crear Spark")

    monkeypatch.setattr(
        ejecutor_dataflow,
        "_crear_sesion_dataflow",
        crear_sesion_mock,
    )

    codigo = ejecutor_dataflow.ejecutar_dataflow(argumentos_dataflow(tmp_path))

    assert codigo == 1
    assert spark_creado is False
    resultado = json.loads((tmp_path / "resultado.json").read_text())
    assert resultado["estado"] == "ERROR"
    assert any(
        error.get("codigo") == "DFS_EMPTY_PLAN"
        for error in resultado.get("errores", [])
    )


def test_solo_compilar_no_crea_spark_ni_conexiones(monkeypatch, tmp_path):
    (tmp_path / "script.df").write_text("LOAD id FROM 'ruta.csv';", encoding="utf-8")
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
    # La compilación pura no instancia el ejecutor del plan, por lo que no
    # existe ninguna ruta capaz de abrir JDBC en esta prueba.

    args = argumentos_dataflow(
        tmp_path, solo_compilar=True, plan_salida=str(tmp_path / "plan.json")
    )
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
    (tmp_path / "script.df").write_text("LOAD id FROM 'ruta.csv';", encoding="utf-8")
    (tmp_path / "conexiones.json").write_text("{}", encoding="utf-8")

    monkeypatch.setattr(ejecutor_dataflow, "cargar_catalogo", lambda x: MagicMock())

    args = argumentos_dataflow(tmp_path, solo_compilar=True, plan_salida=None)
    codigo = ejecutor_dataflow.ejecutar_dataflow(args)

    assert codigo == 1
    resultado = json.loads((tmp_path / "resultado.json").read_text())
    assert resultado["estado"] == "ERROR"
    errores = resultado.get("errores", [])
    assert any(
        "--solo-compilar requiere --plan-salida" in e.get("mensaje", "")
        for e in errores
    )


def test_resultado_no_filtra_secretos(monkeypatch, tmp_path):
    (tmp_path / "script.df").write_text(
        "SELECT id, password FROM schema.usuarios;", encoding="utf-8"
    )
    conexiones_content = {
        "jdbc": [
            {
                "nombre": "schema",
                "url": "jdbc:postgresql://localhost/db",
                "driver": "org.postgresql.Driver",
                "secreto_nombre": "DB_PASSWORD",
                "allowlist": [
                    {
                        "esquema": "schema",
                        "tabla": "usuarios",
                        "campos": ["id", "password"],
                    }
                ],
            }
        ]
    }
    (tmp_path / "conexiones.json").write_text(
        json.dumps(conexiones_content), encoding="utf-8"
    )

    spark_mock = SparkFalso()

    monkeypatch.setattr(
        ejecutor_dataflow, "_crear_sesion_dataflow", lambda *a: spark_mock
    )
    monkeypatch.setattr(
        ejecutor_dataflow,
        "cargar_catalogo",
        lambda x: MagicMock(
            **{
                "buscar_jdbc.return_value": MagicMock(
                    url="jdbc:postgresql://localhost/db",
                    secreto_nombre="DB_PASSWORD",
                    propiedades={},
                    allowlist=[
                        MagicMock(esquema="schema", tabla="usuarios", campos=())
                    ],
                )
            }
        ),
    )

    args = argumentos_dataflow(tmp_path)
    ejecutor_dataflow.ejecutar_dataflow(args)

    resultado = json.loads((tmp_path / "resultado.json").read_text())
    if resultado.get("estado") == "ERROR":
        for err in resultado.get("errores", []):
            assert "secret" not in str(err).lower()
            assert "password" not in str(err).lower()
            assert "DB_PASSWORD" not in str(err)


def test_receta_sigue_despachando_al_flujo_original(monkeypatch, tmp_path):
    from motor_spark.configuracion.modelos.receta import RecetaConfig

    receta = RecetaConfig.model_validate(
        {
            "nombre": "TestReceta",
            "entrada": {"modo_esquema": "inferir"},
            "salida": {},
        }
    )

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

    from motor_spark.aplicacion.ejecutor_motor import ejecutar_motor
    from motor_spark.configuracion.argumentos import ArgumentosEjecucion

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
    (tmp_path / "script.df").write_text(
        "STORE tabla INTO 'lib://destino/salida.csv';", encoding="utf-8"
    )
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
    assert any("Tabla 'tabla' no existe" in e.get("mensaje", "") for e in errores)


def test_stop_llamado_exactamente_una_vez(monkeypatch, tmp_path):
    (tmp_path / "script.df").write_text(
        "LIB CONNECT TO [esquema]; SELECT id, nombre FROM esquema.tabla WHERE id > 0;",
        encoding="utf-8",
    )
    conexiones_content = {
        "jdbc": [
            {
                "nombre": "esquema",
                "url": "jdbc:postgresql://localhost/db",
                "driver": "org.postgresql.Driver",
                "secreto_nombre": "DB_PASSWORD",
                "allowlist": [
                    {"esquema": "esquema", "tabla": "tabla", "campos": ["id", "nombre"]}
                ],
            }
        ]
    }
    (tmp_path / "conexiones.json").write_text(
        json.dumps(conexiones_content), encoding="utf-8"
    )

    spark = SparkFalso()

    def crear_sesion_mock(*args, **kwargs):
        return spark

    def leer_jdbc_mock(spark, nombre_conexion, tabla, columnas, catalogo):
        return DataFrameFalso()

    def validar_mock(*args):
        return []

    monkeypatch.setattr(ejecutor_dataflow, "_crear_sesion_dataflow", crear_sesion_mock)
    monkeypatch.setattr(ejecutor_dataflow, "_validar", validar_mock)
    monkeypatch.setattr(
        ejecutor_dataflow.EjecutorPlanDataflow,
        "ejecutar",
        lambda self, plan: {
            "operaciones_ejecutadas": len(plan.operaciones),
            "tablas_disponibles": (),
            "publicaciones": (),
        },
    )
    monkeypatch.setattr(
        ejecutor_dataflow,
        "cargar_catalogo",
        lambda x: MagicMock(
            **{
                "buscar_jdbc.return_value": MagicMock(
                    url="jdbc:postgresql://localhost/db",
                    secreto_nombre="DB_PASSWORD",
                    propiedades={},
                    allowlist=[
                        MagicMock(
                            esquema="esquema", tabla="tabla", campos=("id", "nombre")
                        )
                    ],
                )
            }
        ),
    )

    args = argumentos_dataflow(tmp_path)
    ejecutor_dataflow.ejecutar_dataflow(args)

    assert spark.detenciones == 1


def test_contenido_directo_compila_sin_tratarlo_como_ruta(monkeypatch, tmp_path):
    script = "[Ventas]: LOAD [id] FROM 'ventas.csv';"
    conexiones = tmp_path / "conexiones.json"
    conexiones.write_text("{}", encoding="utf-8")
    plan = tmp_path / "plan.json"
    resultado = tmp_path / "resultado.json"

    args = ArgumentosDataflowScript(
        dataflow_script=None,
        dataflow_script_contenido=script,
        conexiones=str(conexiones),
        ejecucion_id="contenido-1",
        resultado=str(resultado),
        solo_compilar=True,
        plan_salida=str(plan),
    )

    codigo = ejecutor_dataflow.ejecutar_dataflow(args)

    assert codigo == 0
    payload = json.loads(resultado.read_text(encoding="utf-8"))
    assert payload["origen_script"] == "parametro"
    assert payload["referencia_script"] is None
    assert payload["hash_script"]
    assert script not in json.dumps(payload, ensure_ascii=False)


def test_contenido_directo_rechaza_exceso_de_tamano(tmp_path, monkeypatch):
    conexiones = tmp_path / "conexiones.json"
    conexiones.write_text("{}", encoding="utf-8")
    resultado = tmp_path / "resultado.json"
    monkeypatch.setattr(ejecutor_dataflow, "LIMITE_TAMANIO_ARCHIVO", 8)

    args = ArgumentosDataflowScript(
        dataflow_script=None,
        dataflow_script_contenido="LOAD campo FROM 'archivo.csv';",
        conexiones=str(conexiones),
        ejecucion_id="contenido-grande",
        resultado=str(resultado),
    )

    codigo = ejecutor_dataflow.ejecutar_dataflow(args)

    assert codigo == 1
    payload = json.loads(resultado.read_text(encoding="utf-8"))
    assert payload["errores"][0]["codigo"] == "DFS_FILE_TOO_LARGE"
    assert "LOAD campo" not in json.dumps(payload, ensure_ascii=False)


def test_llamada_programatica_rechaza_ruta_y_contenido_simultaneos(tmp_path):
    resultado = tmp_path / "resultado.json"
    args = ArgumentosDataflowScript(
        dataflow_script=str(tmp_path / "script.qvs"),
        dataflow_script_contenido="LOAD id FROM 'ventas.csv';",
        conexiones=str(tmp_path / "conexiones.json"),
        ejecucion_id="conflicto-1",
        resultado=str(resultado),
    )

    codigo = ejecutor_dataflow.ejecutar_dataflow(args)

    assert codigo == 1
    payload = json.loads(resultado.read_text(encoding="utf-8"))
    assert payload["errores"][0]["codigo"] == "DFS_SCRIPT_SOURCE_CONFLICT"
    assert "LOAD id" not in json.dumps(payload, ensure_ascii=False)


def test_error_de_parser_no_expone_contenido_directo(tmp_path):
    script = "SELECT FROM secreto_super_confidencial"
    resultado = tmp_path / "resultado.json"
    args = ArgumentosDataflowScript(
        dataflow_script_contenido=script,
        conexiones=str(tmp_path / "conexiones.json"),
        ejecucion_id="error-contenido-1",
        resultado=str(resultado),
    )

    codigo = ejecutor_dataflow.ejecutar_dataflow(args)

    assert codigo == 1
    payload_texto = resultado.read_text(encoding="utf-8")
    payload = json.loads(payload_texto)
    assert payload["origen_script"] == "parametro"
    assert payload["referencia_script"] is None
    assert payload["hash_script"]
    # El error puede mencionar el identificador puntual que falló, pero nunca
    # debe repetir el texto completo enviado por el parámetro.
    assert script not in payload_texto


def test_cargar_catalogo_argumentos_usa_json_inline(monkeypatch):
    contenido = '{"version":1,"jdbc":[],"locales":[],"sftp":[]}'
    esperado = MagicMock()
    llamadas = []

    monkeypatch.setattr(
        ejecutor_dataflow,
        "cargar_catalogo_contenido",
        lambda valor: llamadas.append(("contenido", valor)) or esperado,
    )
    monkeypatch.setattr(
        ejecutor_dataflow,
        "cargar_catalogo",
        lambda valor: llamadas.append(("archivo", valor)) or MagicMock(),
    )
    argumentos = ArgumentosDataflowScript(
        conexiones=None,
        conexiones_contenido=contenido,
        ejecucion_id="inline-conexiones-1",
        dataflow_script_contenido="LOAD id FROM 'ventas.csv';",
    )

    catalogo = ejecutor_dataflow._cargar_catalogo_argumentos(argumentos)

    assert catalogo is esperado
    assert llamadas == [("contenido", contenido)]


def test_cargar_catalogo_argumentos_rechaza_dos_origenes():
    argumentos = ArgumentosDataflowScript(
        conexiones="/tmp/conexiones.json",
        conexiones_contenido="{}",
        ejecucion_id="conflicto-conexiones-1",
        dataflow_script_contenido="LOAD id FROM 'ventas.csv';",
    )

    try:
        ejecutor_dataflow._cargar_catalogo_argumentos(argumentos)
    except ValueError as error:
        assert "simultáneamente" in str(error)
    else:
        raise AssertionError("Se esperaba rechazo de dos orígenes de conexiones")


def test_ejecucion_inline_sin_archivos_json(monkeypatch, capsys):
    script = "[Ventas]: LOAD id FROM 'lib://Landing/ventas.csv';"
    conexiones = json.dumps(
        {
            "version": 1,
            "jdbc": [],
            "locales": [
                {
                    "nombre": "Landing",
                    "ruta_base": "/srv/landing",
                    "allowlist": [{"esquema": "", "tabla": "ventas.csv", "campos": []}],
                }
            ],
            "sftp": [],
        }
    )
    spark = SparkFalso()

    class EjecutorFalso:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def ejecutar(self, plan):
            return {
                "operaciones_ejecutadas": len(plan.operaciones),
                "tablas_disponibles": (),
                "publicaciones": (),
            }

    monkeypatch.setattr(ejecutor_dataflow, "_crear_sesion_dataflow", lambda *a: spark)
    monkeypatch.setattr(ejecutor_dataflow, "EjecutorPlanDataflow", EjecutorFalso)

    argumentos = ArgumentosDataflowScript(
        conexiones=None,
        conexiones_contenido=conexiones,
        ejecucion_id="todo-inline-1",
        dataflow_script_contenido=script,
        resultado=None,
    )
    codigo = ejecutor_dataflow.ejecutar_dataflow(argumentos)

    salida = capsys.readouterr().out
    assert codigo == 0
    assert "RESULTADO_MOTOR=" in salida
    assert '"estado":"COMPLETADO"' in salida
    assert conexiones not in salida
    assert script not in salida
    assert spark.detenciones == 1


def test_cargar_catalogo_argumentos_rechaza_ausencia_programatica():
    argumentos = ArgumentosDataflowScript(
        conexiones=None,
        conexiones_contenido=None,
        ejecucion_id="sin-conexiones-1",
        dataflow_script_contenido="LOAD id FROM 'ventas.csv';",
    )

    try:
        ejecutor_dataflow._cargar_catalogo_argumentos(argumentos)
    except ValueError as error:
        assert "No se recibió" in str(error)
    else:
        raise AssertionError("Se esperaba rechazo por catálogo ausente")


def test_catalogo_inline_invalido_no_crea_spark_ni_expone_contenido(
    monkeypatch, capsys
):
    catalogo_invalido = '{"jdbc":[MARCADOR_SUPER_SECRETO]}'
    spark_creado = False

    def crear_sesion(*args):
        nonlocal spark_creado
        spark_creado = True
        return SparkFalso()

    monkeypatch.setattr(ejecutor_dataflow, "_crear_sesion_dataflow", crear_sesion)
    argumentos = ArgumentosDataflowScript(
        conexiones=None,
        conexiones_contenido=catalogo_invalido,
        ejecucion_id="catalogo-invalido-1",
        dataflow_script_contenido="[Ventas]: LOAD id FROM 'ventas.csv';",
        resultado=None,
    )

    codigo = ejecutor_dataflow.ejecutar_dataflow(argumentos)

    salida = capsys.readouterr()
    combinado = salida.out + salida.err
    assert codigo == 1
    assert spark_creado is False
    assert catalogo_invalido not in combinado
    assert "MARCADOR_SUPER_SECRETO" not in combinado
    assert "RESULTADO_MOTOR=" in combinado
