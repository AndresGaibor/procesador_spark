import pytest

from motor_spark.dataflow_script.ast import (
    SentenciaConcatenate,
    SentenciaDropTable,
    SentenciaLibConnectTo,
    SentenciaLoad,
    SentenciaResident,
    SentenciaSet,
    SentenciaStore,
)
from motor_spark.dataflow_script.lexer import tokenizar
from motor_spark.dataflow_script.parser import parsear


class TestParserBancolombia:
    def test_parsear_set(self):
        tokens, _ = tokenizar('SET variable = "valor";')
        programa, errores = parsear(tokens)
        assert len(errores) == 0
        assert isinstance(programa.sentencias_globales[0], SentenciaSet)

    def test_parsear_lib_connect_to_con_bracket(self):
        tokens, _ = tokenizar("LIB CONNECT TO [Mi Conexion];")
        programa, errores = parsear(tokens)
        assert len(errores) == 0
        assert isinstance(programa.sentencias_globales[0], SentenciaLibConnectTo)
        assert programa.sentencias_globales[0].conexion == "Mi Conexion"

    def test_parsear_lib_connect_to_con_espacios(self):
        tokens, _ = tokenizar("LIB CONNECT TO [Bancolombia prueba:Postgres_BanColombia_Prueba];")
        programa, errores = parsear(tokens)
        assert len(errores) == 0
        assert isinstance(programa.sentencias_globales[0], SentenciaLibConnectTo)
        assert "Bancolombia" in programa.sentencias_globales[0].conexion

    def test_parsear_drop_table_bracket(self):
        tokens, _ = tokenizar("DROP TABLE [mi_tabla];")
        programa, errores = parsear(tokens)
        assert len(errores) == 0
        etiqueta = programa.etiquetas[0]
        sentencia = etiqueta.sentencias[0]
        assert isinstance(sentencia, SentenciaDropTable)
        assert sentencia.tabla == "mi_tabla"


class TestLexerBancolombia:
    def test_tokenizar_bracket_id_con_espacios(self):
        tokens, errores = tokenizar("[Mi Tabla con Espacios]")
        assert len(errores) == 0
        assert tokens[0].tipo == "BRACKET_ID"
        assert tokens[0].valor == "Mi Tabla con Espacios"

    def test_tokenizar_lib_uri_con_espacios_en_nombre(self):
        tokens, errores = tokenizar("[lib://Bancolombia prueba:SFTP//upload/archivo.csv]")
        assert len(errores) == 0
        lib_tokens = [t for t in tokens if t.tipo == "LIB_URI"]
        assert len(lib_tokens) == 1
        assert "Bancolombia" in lib_tokens[0].valor

    def test_tokenizar_lib_uri_sin_corchetes(self):
        tokens, errores = tokenizar("lib://Bancolombia prueba:SFTP//upload/archivo.csv")
        assert len(errores) == 0
        lib_tokens = [t for t in tokens if t.tipo == "LIB_URI"]
        assert len(lib_tokens) == 1
        assert "Bancolombia" in lib_tokens[0].valor


class TestExpresionesBancolombia:
    def test_parsear_coalesce(self):
        tokens, _ = tokenizar("SELECT Coalesce(a, 0) FROM t;")
        programa, errores = parsear(tokens)
        assert len(errores) == 0

    def test_parsear_isnull(self):
        tokens, _ = tokenizar("SELECT IsNull(a) FROM t;")
        programa, errores = parsear(tokens)
        assert len(errores) == 0

    def test_parsear_indexregex(self):
        tokens, _ = tokenizar("SELECT IndexRegEx(a, '^[0-9]+$') FROM t;")
        programa, errores = parsear(tokens)
        assert len(errores) == 0

    def test_parsear_num(self):
        tokens, _ = tokenizar("SELECT Num(a) FROM t;")
        programa, errores = parsear(tokens)
        assert len(errores) == 0

    def test_parsear_month(self):
        tokens, _ = tokenizar("SELECT Month(fecha) FROM t;")
        programa, errores = parsear(tokens)
        assert len(errores) == 0

    def test_parsear_year(self):
        tokens, _ = tokenizar("SELECT Year(fecha) FROM t;")
        programa, errores = parsear(tokens)
        assert len(errores) == 0


class TestRechazosExplícitos:
    """Construcciones Qlik rechazadas explicitamente por seguridad o diseño."""

    def test_rechaza_eval(self):
        tokens, _ = tokenizar("SELECT eval('1+1') FROM t;")
        _, errores = parsear(tokens)
        assert len(errores) > 0

    def test_rechaza_sql_libre_union(self):
        tokens, _ = tokenizar("SELECT * FROM (SELECT a FROM t1 UNION SELECT b FROM t2);")
        _, errores = parsear(tokens)
        assert len(errores) > 0

    def test_rechaza_subquery(self):
        tokens, _ = tokenizar("SELECT * FROM (SELECT a FROM t1);")
        _, errores = parsear(tokens)
        assert len(errores) > 0

    def test_rechaza_order(self):
        tokens, _ = tokenizar("SELECT a FROM t ORDER BY a;")
        _, errores = parsear(tokens)
        assert len(errores) > 0

    def test_rechaza_union(self):
        tokens, _ = tokenizar("SELECT a FROM t1 UNION SELECT b FROM t2;")
        _, errores = parsear(tokens)
        assert len(errores) > 0

    def test_rechaza_right_join(self):
        tokens, _ = tokenizar("SELECT * FROM t1 RIGHT JOIN t2 ON t1.a = t2.a;")
        _, errores = parsear(tokens)
        assert len(errores) > 0

    def test_rechaza_having(self):
        tokens, _ = tokenizar("SELECT a, Count(*) FROM t GROUP BY a HAVING Count(*) > 1;")
        _, errores = parsear(tokens)
        assert len(errores) > 0

    def test_rechaza_case(self):
        tokens, _ = tokenizar("SELECT CASE WHEN a = 1 THEN 'uno' ELSE 'otro' END FROM t;")
        _, errores = parsear(tokens)
        assert len(errores) > 0


class TestLimitacionesDocumentadas:
    """
    Limitaciones conocidas del parser Qlik actual.

    Estas construcciones son complejidad de Qlik que el parser actual no soporta
    plenamente y son aceptadas solo parcialmente.
    """

    def test_count_distinct_parcialmente_soportado(self):
        tokens, _ = tokenizar("SELECT Count(DISTINCT a) FROM t;")
        _, errores = parsear(tokens)
        assert len(errores) > 0, "COUNT(DISTINCT) es parcialmente soportado"

    def test_window_wrank_parcialmente_soportado(self):
        tokens, _ = tokenizar("SELECT Window(WRank(1,1), [col1]) AS r FROM t;")
        _, errores = parsear(tokens)
        assert len(errores) == 0, "Window(WRank) es aceptado por el parser"

    def test_noconcatenate_prefix_soportado(self):
        tokens, _ = tokenizar("NOCONCATENATE LOAD a FROM 'f1.csv';")
        _, errores = parsear(tokens)
        assert len(errores) == 0, "NOCONCATENATE es aceptado por el parser"

    def test_group_by_en_resident_no_soportado(self):
        tokens, _ = tokenizar("LOAD a, Count(b) AS cnt RESIDENT t GROUP BY a;")
        _, errores = parsear(tokens)
        assert len(errores) > 0, "GROUP BY en RESIDENT requiere parser especializado"
