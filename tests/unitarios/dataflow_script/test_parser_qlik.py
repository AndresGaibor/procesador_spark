from motor_spark.dataflow_script.ast import (
    SentenciaConcatenate,
    SentenciaDropTable,
    SentenciaLoad,
    SentenciaResident,
    SentenciaSelect,
    SentenciaStore,
)
from motor_spark.dataflow_script.lexer import tokenizar
from motor_spark.dataflow_script.parser import parsear


def test_parsear_load_distinct():
    tokens, _ = tokenizar("LOAD DISTINCT col1, col2 FROM '/ruta/archivo.csv';")
    programa, errores = parsear(tokens)
    assert len(errores) == 0
    etiqueta = programa.etiquetas[0]
    sentencia = etiqueta.sentencias[0]
    assert isinstance(sentencia, SentenciaLoad)
    assert sentencia.distinct is True
    assert sentencia.campos == ("col1", "col2")


def test_parsear_load_con_campos():
    tokens, _ = tokenizar("LOAD col1, col2, col3 FROM '/ruta/archivo.csv';")
    programa, errores = parsear(tokens)
    assert len(errores) == 0
    etiqueta = programa.etiquetas[0]
    sentencia = etiqueta.sentencias[0]
    assert isinstance(sentencia, SentenciaLoad)
    assert sentencia.campos == ("col1", "col2", "col3")


def test_parsear_load_resident():
    tokens, _ = tokenizar("LOAD col1, col2 RESIDENT miTabla;")
    programa, errores = parsear(tokens)
    assert len(errores) == 0
    etiqueta = programa.etiquetas[0]
    sentencia = etiqueta.sentencias[0]
    assert isinstance(sentencia, SentenciaLoad)
    assert sentencia.es_resident is True
    assert sentencia.etiqueta_resident == "miTabla"


def test_parsear_resident():
    tokens, _ = tokenizar("RESIDENT miTabla;")
    programa, errores = parsear(tokens)
    assert len(errores) == 0
    etiqueta = programa.etiquetas[0]
    sentencia = etiqueta.sentencias[0]
    assert isinstance(sentencia, SentenciaResident)
    assert sentencia.etiqueta_origen == "miTabla"


def test_parsear_drop_table():
    tokens, _ = tokenizar("DROP TABLE [mi_tabla];")
    programa, errores = parsear(tokens)
    assert len(errores) == 0
    etiqueta = programa.etiquetas[0]
    sentencia = etiqueta.sentencias[0]
    assert isinstance(sentencia, SentenciaDropTable)
    assert sentencia.tabla == "mi_tabla"


def test_parsear_store():
    tokens, _ = tokenizar("STORE tabla INTO 'lib://mi_ruta/datos.txt' ('txt');")
    programa, errores = parsear(tokens)
    assert len(errores) == 0
    etiqueta = programa.etiquetas[0]
    sentencia = etiqueta.sentencias[0]
    assert isinstance(sentencia, SentenciaStore)
    assert sentencia.tabla == "tabla"


def test_parsear_concatenate():
    tokens, _ = tokenizar("CONCATENATE tabla1;")
    programa, errores = parsear(tokens)
    assert len(errores) == 0
    etiqueta = programa.etiquetas[0]
    sentencia = etiqueta.sentencias[0]
    assert isinstance(sentencia, SentenciaConcatenate)
    assert sentencia.etiqueta_objetivo == "tabla1"


def test_parsear_select_group_by():
    tokens, _ = tokenizar("SELECT col1, COUNT(*) FROM tabla GROUP BY col1;")
    programa, errores = parsear(tokens)
    assert len(errores) == 0
    etiqueta = programa.etiquetas[0]
    sentencia = etiqueta.sentencias[0]
    assert isinstance(sentencia, SentenciaSelect)
    assert len(sentencia.group_by) == 1


def test_parsear_select_bracket_tabla():
    tokens, _ = tokenizar("SELECT col1 FROM [Mi Tabla];")
    programa, errores = parsear(tokens)
    assert len(errores) == 0
    etiqueta = programa.etiquetas[0]
    sentencia = etiqueta.sentencias[0]
    assert isinstance(sentencia, SentenciaSelect)
    assert sentencia.tabla == "Mi Tabla"


def test_parsear_label():
    tokens, _ = tokenizar("miLabel: SELECT * FROM tabla;")
    programa, errores = parsear(tokens)
    assert len(errores) == 0
    assert programa.etiquetas[0].nombre == "miLabel"


def test_parsear_preceding_load():
    tokens, _ = tokenizar("LOAD a FROM '/f1.csv'; LOAD b, a RESIDENT temp;")
    programa, errores = parsear(tokens)
    assert len(errores) == 0
    assert len(programa.etiquetas) == 2
    assert all(e.nombre == "_anonima" for e in programa.etiquetas)


def test_parsear_funciones_permitidas():
    casos = [
        "SELECT CONCAT(a,b) FROM t;",
        "SELECT LEFT(c,1) FROM t;",
        "SELECT UPPER(d) FROM t;",
        "SELECT LOWER(e) FROM t;",
        "SELECT TRIM(f) FROM t;",
    ]
    for caso in casos:
        tokens, _ = tokenizar(caso)
        _, errores = parsear(tokens)
        assert len(errores) == 0, f"Error en {caso}: {errores}"


def test_parsear_rechaza_funcion_no_soportada():
    tokens, _ = tokenizar("SELECT FUNCIONDESCONOCIDA(a) FROM t;")
    _, errores = parsear(tokens)
    assert len(errores) > 0
    assert any(e.codigo == "DFS_UNSUPPORTED_FUNCTION" for e in errores if e.codigo)


def test_parsear_rechaza_union():
    tokens, _ = tokenizar("SELECT a FROM t1 UNION SELECT a FROM t2;")
    _, errores = parsear(tokens)
    assert len(errores) > 0


def test_parsear_rechaza_subquery():
    tokens, _ = tokenizar("SELECT * FROM (SELECT a FROM t1);")
    _, errores = parsear(tokens)
    assert len(errores) > 0
