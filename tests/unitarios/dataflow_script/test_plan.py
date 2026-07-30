import json
import tempfile
from pathlib import Path

import pytest

from motor_spark.conexiones.cargador import cargar_catalogo
from motor_spark.conexiones.modelos import (
    CampoAllowlist,
    CatalogoConexiones,
    ConexionJdbc,
)
from motor_spark.conexiones.sanitizacion import SanitizadorInput
from motor_spark.conexiones.secretos import AdministradorSecretos, ValidadorSecretos
from motor_spark.plan.compilador import compilar
from motor_spark.plan.explicador import explicar_breve, explicar_plan
from motor_spark.plan.modelos import (
    CargarCsv,
    CargarLocal,
    Filtrar,
    LeerJdbc,
    PlanDataflow,
    Proyectar,
    Publicar,
    TipoOperacion,
    generar_id_estable,
)
from motor_spark.plan.serializador import (
    deserializar_plan,
    serializar_plan,
)


def test_generar_id_estable():
    id1 = generar_id_estable("tabla1", "leer", 0)
    id2 = generar_id_estable("tabla1", "leer", 0)
    id3 = generar_id_estable("tabla1", "leer", 1)
    assert id1 == id2
    assert id1 != id3
    assert len(id1) == 16


def test_plan_frozen():
    plan = PlanDataflow(version=1, operaciones=())
    with pytest.raises(Exception):
        plan.version = 2


def test_operaciones_frozen():
    op = LeerJdbc(
        id="test123",
        nombre_tabla="mitabla",
        conexion_nombre="conn1",
        esquema="dbo",
        tabla="mitabla",
    )
    with pytest.raises(Exception):
        op.conexion_nombre = "otro"


def test_plan_hash_determista():
    ops = (
        LeerJdbc(
            id="op1",
            nombre_tabla="mitabla",
            conexion_nombre="conn1",
            esquema="dbo",
            tabla="mitabla",
        ),
    )
    plan1 = PlanDataflow(version=1, operaciones=ops)
    plan2 = PlanDataflow(version=1, operaciones=ops)
    assert plan1.hash_determista() == plan2.hash_determista()


def test_plan_id_por_posicion():
    ops = (
        LeerJdbc(
            id="op1", nombre_tabla="t1", conexion_nombre="c1", esquema="e1", tabla="t1"
        ),
        Proyectar(id="op2", tabla_origen="t1", campos=("a", "b")),
    )
    plan = PlanDataflow(operaciones=ops)
    assert plan.id_por_posicion(0) == "op1"
    assert plan.id_por_posicion(1) == "op2"
    assert plan.id_por_posicion(5) is None


def test_serializador_redondo():
    ops = (
        LeerJdbc(
            id="op1", nombre_tabla="t1", conexion_nombre="c1", esquema="e1", tabla="t1"
        ),
    )
    plan = PlanDataflow(version=1, operaciones=ops)
    json_str = serializar_plan(plan)
    plan2 = deserializar_plan(json_str)
    assert plan.hash_determista() == plan2.hash_determista()


def test_explicar_breve():
    ops = (
        LeerJdbc(
            id="op1", nombre_tabla="t1", conexion_nombre="c1", esquema="e1", tabla="t1"
        ),
        Proyectar(id="op2", tabla_origen="t1", campos=("a", "b")),
        Filtrar(id="op3", tabla_origen="t1", condicion="a > 1"),
    )
    plan = PlanDataflow(operaciones=ops)
    resultado = explicar_breve(plan)
    assert "leer_jdbc" in resultado
    assert "proyectar" in resultado
    assert "filtrar" in resultado


def test_explicar_plan():
    ops = (
        LeerJdbc(
            id="op1", nombre_tabla="t1", conexion_nombre="c1", esquema="e1", tabla="t1"
        ),
    )
    plan = PlanDataflow(operaciones=ops)
    resultado = explicar_plan(plan)
    assert "Plan Dataflow" in resultado
    assert "Hash:" in resultado


def test_catalogo_conexiones_buscar():
    cat = CatalogoConexiones(
        jdbc=(
            ConexionJdbc(
                nombre="jdbc1",
                url="jdbc:sqlserver://localhost",
                driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",
                secreto_nombre="SECRET_JDBC",
            ),
        )
    )
    assert cat.buscar_jdbc("jdbc1") is not None
    assert cat.buscar_jdbc("jdbc2") is None


def test_catalogo_allowlist():
    cat = CatalogoConexiones(
        jdbc=(
            ConexionJdbc(
                nombre="jdbc1",
                url="jdbc:sqlserver://localhost",
                driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",
                secreto_nombre="SECRET_JDBC",
                allowlist=(
                    CampoAllowlist(esquema="dbo", tabla="mitabla", campos=("a", "b")),
                ),
            ),
        )
    )
    assert cat.esta_en_allowlist("jdbc1", "dbo", "mitabla") is True
    assert cat.esta_en_allowlist("jdbc1", "dbo", "otra") is False


def test_sanitizador_identificador_valido():
    assert SanitizadorInput.sanitizar_identificador("mi_var") == "mi_var"
    assert SanitizadorInput.sanitizar_identificador("MiVar123") == "MiVar123"


def test_sanitizador_identificador_invalido():
    with pytest.raises(ValueError):
        SanitizadorInput.sanitizar_identificador("123var")


def test_sanitizador_seguro_sql():
    assert SanitizadorInput.es_seguro_para_sql("valor normal") is True
    assert SanitizadorInput.es_seguro_para_sql("'; DROP TABLE --") is False
    assert SanitizadorInput.es_seguro_para_sql("1 OR 1=1") is False


def test_admin_secretos():
    admin = AdministradorSecretos()
    assert admin.obtener("NO_EXISTE") is None


def test_admin_secretos_prefiere_valor_inyectado(monkeypatch):
    monkeypatch.setenv("DB_PASSWORD", "valor-del-entorno")
    admin = AdministradorSecretos({"DB_PASSWORD": "valor-inyectado"})

    assert admin.obtener("DB_PASSWORD") == "valor-inyectado"


def test_validador_secretos():
    admin = AdministradorSecretos()
    val = ValidadorSecretos(admin)
    datos = {"usuario": "test", "password": "secreto123"}
    encontrados = val.validar_no_exponer(datos)
    assert "password" in encontrados


def test_cargar_catalogo_json():
    cat_dict = {
        "version": 1,
        "descripcion": "Test",
        "jdbc": [
            {
                "nombre": "conn1",
                "url": "jdbc:postgresql://localhost/test",
                "driver": "org.postgresql.Driver",
                "secreto_nombre": "DB_PASSWORD",
                "allowlist": [],
                "propiedades": {},
            }
        ],
        "locales": [],
        "sftp": [],
    }
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(cat_dict, f)
        f.flush()
        cat = cargar_catalogo(f.name)
        Path(f.name).unlink()

    assert cat.version == 1
    assert len(cat.jdbc) == 1
    assert cat.jdbc[0].nombre == "conn1"


def test_serializador_preserva_campos_y_subtipo_de_operacion():
    """Un plan persistido debe poder ejecutarse sin volver a leer el script Qlik."""
    operacion = LeerJdbc(
        id="leer-clientes",
        nombre_tabla="Clientes",
        conexion_nombre="Postgres",
        esquema="demo",
        tabla="clientes",
        campos=("cliente_id", "nombres"),
    )
    plan = PlanDataflow(operaciones=(operacion,), tabla_resultado="Clientes")

    serializado = serializar_plan(plan)
    restaurado = deserializar_plan(serializado)

    assert '"conexion_nombre": "Postgres"' in serializado
    assert isinstance(restaurado.operaciones[0], LeerJdbc)
    assert restaurado.operaciones[0] == operacion


def test_compilador_conserva_nombres_logicos_y_ultima_tabla():
    """DROP y STORE hacen referencia a etiquetas Qlik, no a IDs internos."""
    script = """
    [Ventas]: LOAD [venta_id] FROM [lib://SFTP//datos/ventas.csv];
    [Sucursales]: LOAD [sucursal_id] FROM [lib://SFTP//datos/sucursales.csv];
    DROP TABLE [Ventas];
    STORE [Sucursales] INTO [lib://SFTP//salida/sucursales.csv] (txt);
    """
    from motor_spark.dataflow_script.lexer import tokenizar
    from motor_spark.dataflow_script.parser import parsear

    tokens, errores_lexicos = tokenizar(script)
    programa, errores_parser = parsear(tokens)
    plan = compilar(programa)

    assert errores_lexicos == []
    assert errores_parser == []
    assert isinstance(plan.operaciones[0], CargarCsv)
    assert plan.operaciones[0].nombre_tabla == "Ventas"
    assert isinstance(plan.operaciones[1], Proyectar)
    assert plan.operaciones[1].tabla_origen == "Ventas"
    assert isinstance(plan.operaciones[-1], Publicar)
    assert plan.operaciones[-1].tabla_origen == "Sucursales"
    assert plan.tabla_resultado == "Sucursales"


def test_compila_select_sobre_tabla_logica_ya_cargada():
    """Un SELECT de una etiqueta viva no debe abrir una conexión JDBC."""
    script = """
    [Datos]: LOAD [id], [nombre] FROM [lib://Archivos/entrada.csv];
    SELECT [id], [nombre] FROM "Datos";
    """
    from motor_spark.dataflow_script.lexer import tokenizar
    from motor_spark.dataflow_script.parser import parsear

    tokens, errores_lexicos = tokenizar(script)
    programa, errores_parser = parsear(tokens)
    plan = compilar(programa)

    assert errores_lexicos == []
    assert errores_parser == []
    assert plan.metadata["errores"] == ()
    assert sum(isinstance(op, CargarCsv) for op in plan.operaciones) == 1
    assert not any(isinstance(op, LeerJdbc) for op in plan.operaciones)
    assert plan.tabla_resultado == "Datos"


def test_compila_preceding_load_simple_en_orden_de_ejecucion():
    """El SELECT inferior se ejecuta antes del LOAD escrito encima."""
    script = """
    LIB CONNECT TO [Postgres];
    [Clientes]:
    LOAD [cliente_id], [nombres];
    SELECT "cliente_id", "nombres" FROM "demo"."clientes";
    """
    from motor_spark.dataflow_script.lexer import tokenizar
    from motor_spark.dataflow_script.parser import parsear

    tokens, errores_lexicos = tokenizar(script)
    programa, errores_parser = parsear(tokens)
    plan = compilar(programa)

    assert errores_lexicos == []
    assert errores_parser == []
    assert plan.metadata["errores"] == ()
    assert isinstance(plan.operaciones[0], LeerJdbc)
    assert plan.operaciones[0].nombre_tabla == "Clientes"
    assert isinstance(plan.operaciones[-1], Proyectar)
    assert plan.operaciones[-1].tabla_origen == "Clientes"


def test_roundtrip_no_confunde_cargar_local_con_cargar_csv():
    """Operaciones con campos similares deben conservar su clase concreta."""

    plan = PlanDataflow(
        operaciones=(
            CargarCsv(
                id="csv",
                nombre_tabla="Entrada",
                ruta="lib://Archivos/entrada.csv",
            ),
            CargarLocal(
                id="resident",
                nombre_tabla="Curada",
                ruta="Entrada",
            ),
        )
    )

    restaurado = deserializar_plan(serializar_plan(plan))

    assert isinstance(restaurado.operaciones[0], CargarCsv)
    assert isinstance(restaurado.operaciones[1], CargarLocal)
    assert restaurado.operaciones[1].tipo == TipoOperacion.CARGAR_LOCAL
