from motor_spark.dataflow_script.ast import (
    SentenciaDropTable,
    SentenciaLibConnectTo,
    SentenciaLoad,
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
        tokens, _ = tokenizar(
            "LIB CONNECT TO [Bancolombia prueba:Postgres_BanColombia_Prueba];"
        )
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
        tokens, errores = tokenizar(
            "[lib://Bancolombia prueba:SFTP//upload/archivo.csv]"
        )
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
        _programa, errores = parsear(tokens)
        assert len(errores) == 0

    def test_parsear_isnull(self):
        tokens, _ = tokenizar("SELECT IsNull(a) FROM t;")
        _programa, errores = parsear(tokens)
        assert len(errores) == 0

    def test_parsear_indexregex(self):
        tokens, _ = tokenizar("SELECT IndexRegEx(a, '^[0-9]+$') FROM t;")
        _programa, errores = parsear(tokens)
        assert len(errores) == 0

    def test_parsear_num(self):
        tokens, _ = tokenizar("SELECT Num(a) FROM t;")
        _programa, errores = parsear(tokens)
        assert len(errores) == 0

    def test_parsear_month(self):
        tokens, _ = tokenizar("SELECT Month(fecha) FROM t;")
        _programa, errores = parsear(tokens)
        assert len(errores) == 0

    def test_parsear_year(self):
        tokens, _ = tokenizar("SELECT Year(fecha) FROM t;")
        _programa, errores = parsear(tokens)
        assert len(errores) == 0


class TestRechazosExplícitos:
    """Construcciones Qlik rechazadas explicitamente por seguridad o diseño."""

    def test_rechaza_eval(self):
        tokens, _ = tokenizar("SELECT eval('1+1') FROM t;")
        _, errores = parsear(tokens)
        assert len(errores) > 0

    def test_rechaza_sql_libre_union(self):
        tokens, _ = tokenizar(
            "SELECT * FROM (SELECT a FROM t1 UNION SELECT b FROM t2);"
        )
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
        tokens, _ = tokenizar(
            "SELECT a, Count(*) FROM t GROUP BY a HAVING Count(*) > 1;"
        )
        _, errores = parsear(tokens)
        assert len(errores) > 0

    def test_rechaza_case(self):
        tokens, _ = tokenizar(
            "SELECT CASE WHEN a = 1 THEN 'uno' ELSE 'otro' END FROM t;"
        )
        _, errores = parsear(tokens)
        assert len(errores) > 0


class TestLimitacionesDocumentadas:
    """
    Limitaciones conocidas del parser Qlik actual.

    Estas construcciones son complejidad de Qlik que el parser actual no soporta
    plenamente y son aceptadas solo parcialmente.
    """

    def test_count_distinct_soportado(self):
        tokens, errores_lexicos = tokenizar("SELECT Count(DISTINCT a) AS total FROM t;")
        programa, errores = parsear(tokens)
        assert errores_lexicos == []
        assert errores == []
        assert programa.etiquetas[0].sentencias[0].proyecciones[0].alias == "total"

    def test_window_wrank_parcialmente_soportado(self):
        tokens, _ = tokenizar("SELECT Window(WRank(1,1), [col1]) AS r FROM t;")
        _, errores = parsear(tokens)
        assert len(errores) == 0, "Window(WRank) es aceptado por el parser"

    def test_noconcatenate_prefix_soportado(self):
        tokens, _ = tokenizar("NOCONCATENATE LOAD a FROM 'f1.csv';")
        _, errores = parsear(tokens)
        assert len(errores) == 0, "NOCONCATENATE es aceptado por el parser"

    def test_group_by_en_resident_soportado(self):
        tokens, errores_lexicos = tokenizar(
            "LOAD a, Count(b) AS cnt RESIDENT t GROUP BY a;"
        )
        programa, errores = parsear(tokens)
        assert errores_lexicos == []
        assert errores == []
        carga = programa.etiquetas[0].sentencias[0]
        assert carga.es_resident is True
        assert len(carga.group_by) == 1


class TestRegresionesScriptBancolombia:
    """Casos mínimos extraídos de fallos observados en el fixture completo."""

    def test_etiqueta_qlik_entre_corchetes(self):
        tokens, errores_lexicos = tokenizar(
            "[Ventas]: LOAD [venta_id] FROM 'ventas.csv';"
        )
        programa, errores_parser = parsear(tokens)

        assert errores_lexicos == []
        assert errores_parser == []
        assert programa.etiquetas[0].nombre == "Ventas"

    def test_load_acepta_uri_lib_entre_corchetes(self):
        script = (
            "[Ventas]: LOAD [venta_id] FROM [lib://Conexion SFTP//datos/ventas.csv];"
        )
        tokens, errores_lexicos = tokenizar(script)
        programa, errores_parser = parsear(tokens)

        assert errores_lexicos == []
        assert errores_parser == []
        sentencia = programa.etiquetas[0].sentencias[0]
        assert isinstance(sentencia, SentenciaLoad)
        assert sentencia.ruta == "lib://Conexion SFTP//datos/ventas.csv"

    def test_store_acepta_formato_txt_sin_comillas(self):
        script = "STORE [Sucursales] INTO [lib://Conexion SFTP//upload/s.csv] (txt);"
        tokens, errores_lexicos = tokenizar(script)
        programa, errores_parser = parsear(tokens)

        assert errores_lexicos == []
        assert errores_parser == []
        sentencia = programa.etiquetas[0].sentencias[0]
        assert isinstance(sentencia, SentenciaStore)
        assert sentencia.formato == "txt"


def test_select_sql_interpreta_comillas_dobles_como_identificadores():
    """PostgreSQL usa comillas dobles para columnas y tablas, no strings."""
    tokens, errores_lexicos = tokenizar(
        'SELECT "cliente_id", "nombres" FROM "demo"."clientes";'
    )
    programa, errores_parser = parsear(tokens)

    assert errores_lexicos == []
    assert errores_parser == []
    sentencia = programa.etiquetas[0].sentencias[0]
    assert all(item.expresion.tipo.name == "COLUMNA" for item in sentencia.proyecciones)
    assert [item.expresion.valor for item in sentencia.proyecciones] == [
        "cliente_id",
        "nombres",
    ]
    assert sentencia.esquema == "demo"
    assert sentencia.tabla == "clientes"
