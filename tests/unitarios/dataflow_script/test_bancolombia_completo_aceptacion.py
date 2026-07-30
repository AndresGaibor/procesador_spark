"""Gates de aceptación del Dataflow Bancolombia completo de 954 líneas.

Este corpus representa el objetivo funcional real del compilador. Cambiar el
fixture o producir una matriz distinta exige una revisión consciente: una
prueba pequeña no puede detectar que desapareció una fuente, un JOIN o STORE.
"""

from __future__ import annotations

import hashlib
from collections import Counter
from pathlib import Path

from motor_spark.dataflow_script.lexer import tokenizar
from motor_spark.dataflow_script.normalizador import normalizar
from motor_spark.dataflow_script.parser import parsear
from motor_spark.dataflow_script.validador import validar_semantico
from motor_spark.plan.compilador import compilar
from motor_spark.plan.modelos import LeerJdbc, Publicar
from motor_spark.plan.serializador import deserializar_plan, serializar_plan

SCRIPT = (
    Path(__file__).parents[2]
    / "recursos"
    / "dataflow"
    / "scripts"
    / "bancolombia_ventas_completo.qvs"
)
SHA256_ESPERADO = "d72e2c1a946e2a3e7746afdf497991dc4ff2ace01bc4e66e09d7d57eb55485d6"


def _compilar_fixture():
    """Ejecuta las etapas puras y exige que ninguna reporte errores."""
    contenido = SCRIPT.read_text(encoding="utf-8")
    normalizado, errores_normalizacion = normalizar(contenido)
    tokens, errores_lexicos = tokenizar(normalizado)
    programa, errores_parser = parsear(tokens)
    errores_semanticos = validar_semantico(programa)

    assert errores_normalizacion == []
    assert errores_lexicos == []
    assert errores_parser == []
    assert errores_semanticos == []

    plan = compilar(programa)
    assert plan.metadata["errores"] == ()
    return plan


def test_fixture_no_fue_modificado_accidentalmente():
    contenido = SCRIPT.read_bytes()

    assert len(contenido.splitlines()) == 954
    assert hashlib.sha256(contenido).hexdigest() == SHA256_ESPERADO


def test_plan_contiene_la_matriz_operativa_completa():
    plan = _compilar_fixture()
    conteo = Counter(operacion.tipo.value for operacion in plan.operaciones)

    assert len(plan.operaciones) == 72
    assert conteo == {
        "proyectar": 29,
        "eliminar_tabla": 14,
        "cargar_local": 9,
        "leer_jdbc": 7,
        "unir": 5,
        "publicar": 4,
        "filtrar": 2,
        "concatenar": 1,
        "agregar": 1,
    }
    assert plan.tabla_resultado == "Ranking"


def test_plan_incluye_fuentes_y_destinos_esperados():
    plan = _compilar_fixture()
    fuentes = {
        operacion.tabla
        for operacion in plan.operaciones
        if isinstance(operacion, LeerJdbc)
    }
    destinos = {
        operacion.destino
        for operacion in plan.operaciones
        if isinstance(operacion, Publicar)
    }

    assert fuentes == {
        "ventas_2025",
        "ventas_2026",
        "clientes",
        "productos",
        "sucursales",
        "vendedores",
        "devoluciones",
    }
    assert destinos == {
        "lib://Bancolombia prueba:SFTP//upload/ventas_rechazadas.csv",
        "lib://Bancolombia prueba:SFTP//upload/ventas_curadas.csv",
        "lib://Bancolombia prueba:SFTP//upload/muestra_calidad.csv",
        "lib://Bancolombia prueba:SFTP//upload/resumen_mensual.csv",
    }


def test_compilacion_y_roundtrip_son_deterministas():
    plan_primero = _compilar_fixture()
    plan_segundo = _compilar_fixture()
    serializado = serializar_plan(plan_primero)
    restaurado = deserializar_plan(serializado)

    assert plan_primero.hash_determista() == plan_segundo.hash_determista()
    assert plan_primero.hash_determista() == (
        "c428e97e456a96dd6c20953b5cc0e00d756a6468ccd5ad20dc42ebd79427edf3"
    )
    assert restaurado == plan_primero
    assert serializar_plan(restaurado) == serializado
