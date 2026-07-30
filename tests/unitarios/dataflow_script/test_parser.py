from motor_spark.dataflow_script.ast import (
    ProgramaDataflowScript,
    SentenciaLibConnectTo,
    SentenciaSelect,
    SentenciaSet,
)
from motor_spark.dataflow_script.lexer import tokenizar
from motor_spark.dataflow_script.parser import parsear


def test_parsear_set_simple():
    tokens, _ = tokenizar('SET variable = "valor";')
    programa, errores = parsear(tokens)
    assert len(errores) == 0
    assert len(programa.sentencias_globales) == 1
    assert isinstance(programa.sentencias_globales[0], SentenciaSet)


def test_parsear_lib_connect_to():
    tokens, _ = tokenizar('LIB CONNECT TO "mi_conexion";')
    programa, errores = parsear(tokens)
    assert len(errores) == 0
    assert isinstance(programa.sentencias_globales[0], SentenciaLibConnectTo)


def test_parsear_select_simple():
    tokens, _ = tokenizar("SELECT * FROM esquema.tabla;")
    programa, errores = parsear(tokens)
    assert len(errores) == 0
    assert len(programa.etiquetas) == 1


def test_parsear_etiqueta():
    tokens, _ = tokenizar("miEtiqueta: SELECT * FROM esquema.tabla;")
    programa, errores = parsear(tokens)
    assert len(errores) == 0
    assert len(programa.etiquetas) == 1
    assert programa.etiquetas[0].nombre == "miEtiqueta"


def test_parsear_load():
    tokens, _ = tokenizar('LOAD "/ruta/archivo.csv";')
    programa, errores = parsear(tokens)
    assert len(errores) == 0
    assert len(programa.etiquetas) == 1


def test_parsear_proyeccion_columnas():
    tokens, _ = tokenizar("SELECT col1, col2 FROM esquema.tabla;")
    programa, errores = parsear(tokens)
    assert len(errores) == 0
    etiqueta = programa.etiquetas[0]
    sentencia = etiqueta.sentencias[0]
    assert isinstance(sentencia, SentenciaSelect)
    assert len(sentencia.proyecciones) == 2


def test_parsear_proyeccion_con_alias():
    tokens, _ = tokenizar("SELECT col1 AS alias1 FROM esquema.tabla;")
    programa, errores = parsear(tokens)
    assert len(errores) == 0
    etiqueta = programa.etiquetas[0]
    sentencia = etiqueta.sentencias[0]
    assert isinstance(sentencia, SentenciaSelect)
    assert sentencia.proyecciones[0].alias == "alias1"


def test_parsear_select_con_where():
    tokens, _ = tokenizar("SELECT * FROM esquema.tabla WHERE col1 = 42;")
    programa, errores = parsear(tokens)
    assert len(errores) == 0
    etiqueta = programa.etiquetas[0]
    sentencia = etiqueta.sentencias[0]
    assert isinstance(sentencia, SentenciaSelect)
    assert len(sentencia.condiciones_where) == 1


def test_parsear_select_con_left_join():
    tokens, _ = tokenizar(
        "SELECT * FROM esquema.tabla1 LEFT JOIN esquema.tabla2 ON tabla1.id = tabla2.id;"
    )
    programa, errores = parsear(tokens)
    assert len(errores) == 0
    etiqueta = programa.etiquetas[0]
    sentencia = etiqueta.sentencias[0]
    assert isinstance(sentencia, SentenciaSelect)
    assert sentencia.join_externo is not None


def test_parsear_expresion_aditiva():
    tokens, _ = tokenizar("SELECT col1 + col2 FROM esquema.tabla;")
    _programa, errores = parsear(tokens)
    assert len(errores) == 0


def test_parsear_expresion_logica():
    tokens, _ = tokenizar("SELECT * FROM esquema.tabla WHERE col1 = 1 AND col2 = 2;")
    _programa, errores = parsear(tokens)
    assert len(errores) == 0


def test_parsear_funcion_concat():
    tokens, _ = tokenizar("SELECT CONCAT(col1, col2) FROM esquema.tabla;")
    _programa, errores = parsear(tokens)
    assert len(errores) == 0


def test_parsear_multiple_sentencias_etiqueta():
    tokens, _ = tokenizar(
        "etiq: SELECT 1 FROM esquema.tabla; SELECT 2 FROM esquema.tabla2;"
    )
    programa, errores = parsear(tokens)
    assert len(errores) == 0
    assert len(programa.etiquetas) == 1
    assert len(programa.etiquetas[0].sentencias) == 2


def test_parsear_varias_etiquetas():
    tokens, _ = tokenizar(
        "etiq1: SELECT 1 FROM esquema.tabla1; etiq2: SELECT 2 FROM esquema.tabla2;"
    )
    programa, errores = parsear(tokens)
    assert len(errores) == 0
    assert len(programa.etiquetas) == 2


def test_parsear_sentencias_globales_y_etiquetas():
    tokens, _ = tokenizar('SET var = "valor"; etiq: SELECT * FROM tabla;')
    programa, errores = parsear(tokens)
    assert len(errores) == 0
    assert len(programa.sentencias_globales) == 1
    assert len(programa.etiquetas) == 1


def test_programa_dataflow_script_vacio():
    tokens, _ = tokenizar("")
    programa, _errores = parsear(tokens)
    assert isinstance(programa, ProgramaDataflowScript)
    assert len(programa.sentencias_globales) == 0
    assert len(programa.etiquetas) == 0
