from motor_spark.dataflow_script.lexer import tokenizar


def test_tokenizar_select_simple():
    tokens, errores = tokenizar("SELECT * FROM esquema.tabla;")
    assert len(errores) == 0
    assert len(tokens) >= 5
    assert tokens[0].tipo == "PALABRA_RESERVADA"
    assert tokens[0].valor == "SELECT"


def test_tokenizar_identificador():
    tokens, errores = tokenizar("miVariable")
    assert len(errores) == 0
    assert tokens[0].tipo == "IDENTIFICADOR"
    assert tokens[0].valor == "miVariable"


def test_tokenizar_numero():
    tokens, errores = tokenizar("42")
    assert len(errores) == 0
    assert tokens[0].tipo == "NUMERO"
    assert tokens[0].valor == "42"


def test_tokenizar_numero_decimal():
    tokens, errores = tokenizar("3.14")
    assert len(errores) == 0
    assert tokens[0].tipo == "NUMERO"
    assert tokens[0].valor == "3.14"


def test_tokenizar_string_doble():
    tokens, errores = tokenizar('"hola mundo"')
    assert len(errores) == 0
    assert tokens[0].tipo == "STRING"
    assert tokens[0].valor == '"hola mundo"'


def test_tokenizar_string_simple():
    tokens, errores = tokenizar("'hola mundo'")
    assert len(errores) == 0
    assert tokens[0].tipo == "STRING"
    assert tokens[0].valor == "'hola mundo'"


def test_tokenizar_simbolos():
    tokens, errores = tokenizar("( ) , ; = < >")
    assert len(errores) == 0
    valores = [t.valor for t in tokens[:-1]]
    assert valores == ["(", ")", ",", ";", "=", "<", ">"]


def test_tokenizar_operadores_logicos():
    tokens, errores = tokenizar("AND OR NOT")
    assert len(errores) == 0
    valores = [t.valor for t in tokens[:-1]]
    assert valores == ["AND", "OR", "NOT"]


def test_tokenizar_palabras_reservadas():
    tokens, errores = tokenizar("SELECT FROM WHERE LEFT JOIN")
    assert len(errores) == 0
    valores = [t.valor for t in tokens[:-1]]
    assert valores == ["SELECT", "FROM", "WHERE", "LEFT", "JOIN"]


def test_tokenizar_expresion_completa():
    contenido = "SELECT col1, col2 AS alias FROM esquema.tabla WHERE col1 = 42;"
    tokens, errores = tokenizar(contenido)
    assert len(errores) == 0
    assert tokens[-1].tipo == "FIN"


def test_tokenizar_fin_archivo():
    tokens, errores = tokenizar("SELECT 1")
    assert len(errores) == 0
    assert tokens[-1].tipo == "FIN"
    assert tokens[-1].valor == ""


def test_tokenizar_error_caracter_desconocido():
    _tokens, errores = tokenizar("SELECT @ FROM tabla")
    assert len(errores) > 0


def test_tokenizar_linea_vacia():
    tokens, errores = tokenizar("")
    assert len(errores) == 0
    assert len(tokens) == 1
    assert tokens[0].tipo == "FIN"


def test_tokenizar_espacios():
    tokens, errores = tokenizar("   SELECT   *   FROM   tabla   ")
    assert len(errores) == 0
    assert tokens[0].valor == "SELECT"


def test_tokenizar_multilinea():
    contenido = "SELECT *\nFROM tabla\nWHERE x = 1;"
    _tokens, errores = tokenizar(contenido)
    assert len(errores) == 0
