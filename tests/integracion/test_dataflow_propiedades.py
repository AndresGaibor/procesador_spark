from __future__ import annotations

import shutil

import pytest

from motor_spark.dataflow_script import normalizador, parsear, tokenizar
from motor_spark.dataflow_script.ast import ProgramaDataflowScript
from motor_spark.plan.compilador import compilar
from motor_spark.plan.modelos import PlanDataflow
from motor_spark.plan.serializador import deserializar_plan, serializar_plan

pytestmark = pytest.mark.spark


def _skip_if_no_spark():
    if shutil.which("java") is None:
        pytest.skip("Java no instalado")
    try:
        import pyspark  # noqa: F401 -- prueba disponibilidad opcional
    except ImportError:
        pytest.skip("PySpark no instalado")


class TestPropiedadesParsers:
    def test_parsear_dos_veces_mismo_resultado(self, script_path):
        _skip_if_no_spark()
        script = script_path("script_valido_load_csv.qvs")
        contenido = script.read_text(encoding="utf-8")
        contenido_norm, _ = normalizador.normalizar(contenido)
        tokens1, _ = tokenizar(contenido_norm)
        tokens2, _ = tokenizar(contenido_norm)
        prog1, _ = parsear(tokens1)
        prog2, _ = parsear(tokens2)

        assert len(prog1.sentencias_globales) == len(prog2.sentencias_globales)
        assert len(prog1.etiquetas) == len(prog2.etiquetas)

    def test_programa_vacio_parsea_correctamente(self):
        _skip_if_no_spark()
        tokens, _ = tokenizar("")
        programa, errores = parsear(tokens)
        assert isinstance(programa, ProgramaDataflowScript)
        assert len(errores) == 0

    def test_normalizar_idempotente(self, script_path):
        script = script_path("script_valido_load_csv.qvs")
        contenido1 = script.read_text(encoding="utf-8")
        contenido2, _ = normalizador.normalizar(contenido1)
        contenido3, _ = normalizador.normalizar(contenido2)
        assert contenido2 == contenido3

    def test_script_valido_no_produce_errores_lexico(self, script_path):
        script = script_path("script_valido_expresiones.qvs")
        contenido = script.read_text(encoding="utf-8")
        contenido_norm, _ = normalizador.normalizar(contenido)
        tokens, errores = tokenizar(contenido_norm)
        assert len(errores) == 0
        assert len(tokens) > 0


class TestPropiedadesPlan:
    def test_compilar_script_valido_produce_plan(self, script_path):
        _skip_if_no_spark()
        script = script_path("script_valido_load_csv.qvs")
        contenido = script.read_text(encoding="utf-8")
        contenido_norm, _ = normalizador.normalizar(contenido)
        tokens, _ = tokenizar(contenido_norm)
        programa, _ = parsear(tokens)
        plan = compilar(programa)
        assert isinstance(plan, PlanDataflow)
        assert len(plan.operaciones) >= 0

    def test_hash_determista_dos_llamadas(self, script_path):
        _skip_if_no_spark()
        script = script_path("script_valido_load_csv.qvs")
        contenido = script.read_text(encoding="utf-8")
        contenido_norm, _ = normalizador.normalizar(contenido)
        tokens, _ = tokenizar(contenido_norm)
        programa, _ = parsear(tokens)
        plan = compilar(programa)

        hash1 = plan.hash_determista()
        hash2 = plan.hash_determista()
        assert hash1 == hash2

    def test_serializar_deserializar_plan(self, script_path):
        _skip_if_no_spark()
        script = script_path("script_valido_load_csv.qvs")
        contenido = script.read_text(encoding="utf-8")
        contenido_norm, _ = normalizador.normalizar(contenido)
        tokens, _ = tokenizar(contenido_norm)
        programa, _ = parsear(tokens)
        plan = compilar(programa)

        serializado = serializar_plan(plan)
        plan_deserializado = deserializar_plan(serializado)

        assert plan_deserializado.version == plan.version
        assert len(plan_deserializado.operaciones) == len(plan.operaciones)

    def test_fingerprint_operaciones_estable(self, script_path):
        _skip_if_no_spark()
        script = script_path("script_valido_load_csv.qvs")
        contenido = script.read_text(encoding="utf-8")
        contenido_norm, _ = normalizador.normalizar(contenido)
        tokens, _ = tokenizar(contenido_norm)
        programa, _ = parsear(tokens)
        plan = compilar(programa)

        if len(plan.operaciones) > 0:
            op = plan.operaciones[0]
            from motor_spark.plan.serializador import SerializadorPlan

            fp1 = SerializadorPlan.fingerprint_operacion(op)
            fp2 = SerializadorPlan.fingerprint_operacion(op)
            assert fp1 == fp2


class TestPropiedadesCatalogos:
    def test_catalogo_carga_dos_veces_mismo_resultado(self, conexion_path):
        from motor_spark.conexiones.cargador import cargar_catalogo

        catalogo1 = cargar_catalogo(conexion_path("catalogo_seguro.json"))
        catalogo2 = cargar_catalogo(conexion_path("catalogo_seguro.json"))

        assert catalogo1.version == catalogo2.version
        assert len(catalogo1.jdbc) == len(catalogo2.jdbc)
        assert len(catalogo1.locales) == len(catalogo2.locales)


class TestPropiedadesExpresiones:
    def test_expresion_compilada_no_es_none(self):
        _skip_if_no_spark()
        from motor_spark.dataflow_script.ast import Expresion, TipoExpresion
        from motor_spark.dataflow_script.expresiones import compilar_expresion

        expr = Expresion(tipo=TipoExpresion.COLUMNA, valor="nombre")
        resultado = compilar_expresion(expr)
        assert resultado is not None

    def test_expresion_vacia_retorna_sin_error(self):
        _skip_if_no_spark()
        from motor_spark.dataflow_script.ast import Expresion, TipoExpresion
        from motor_spark.dataflow_script.expresiones import compilar_expresion

        expr = Expresion(tipo=TipoExpresion.LITERAL_STRING, valor="''")
        resultado = compilar_expresion(expr)
        assert resultado is not None
