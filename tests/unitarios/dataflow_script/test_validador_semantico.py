from motor_spark.dataflow_script.lexer import tokenizar
from motor_spark.dataflow_script.parser import parsear
from motor_spark.dataflow_script.validador import validar_semantico


def test_validar_tabla_inexistente():
    tokens, _ = tokenizar("SELECT * FROM tabla_inexistente;")
    programa, errores_parser = parsear(tokens)
    assert len(errores_parser) == 0
    errores = validar_semantico(programa)
    assert len(errores) > 0
    assert any("TABLE_NOT_FOUND" in e.codigo for e in errores if e.codigo)


def test_validar_tabla_dropeada():
    tokens, _ = tokenizar(
        "t1: LOAD a FROM '/f.csv'; t2: SELECT * FROM t1; DROP TABLE t1; SELECT * FROM t1;"
    )
    programa, errores_parser = parsear(tokens)
    assert len(errores_parser) == 0
    errores = validar_semantico(programa)
    assert len(errores) > 0
    assert any("TABLE_DROPPED" in e.codigo for e in errores if e.codigo)


def test_validar_resident_futuro():
    tokens, _ = tokenizar("RESIDENT temp; LOAD a FROM '/f.csv';")
    programa, errores_parser = parsear(tokens)
    assert len(errores_parser) == 0
    errores = validar_semantico(programa)
    assert len(errores) > 0
    assert any("RESIDENT_FUTURE" in e.codigo for e in errores if e.codigo)


def test_validar_alias_duplicado():
    tokens, _ = tokenizar("SELECT a AS x, b AS x FROM t;")
    programa, errores_parser = parsear(tokens)
    assert len(errores_parser) == 0
    errores = validar_semantico(programa)
    assert len(errores) > 0
    assert any("DUPLICATE_ALIAS" in e.codigo for e in errores if e.codigo)


def test_validar_funcion_no_whitelist():
    tokens, _ = tokenizar("SELECT FUNCIONDESCONOCIDA(a) FROM t;")
    _programa, errores_parser = parsear(tokens)
    assert len(errores_parser) > 0


def test_validar_producto_cartesiano():
    tokens, _ = tokenizar(
        "t1: LOAD a FROM '/f.csv'; t2: SELECT * FROM t1 LEFT JOIN t1 ON id = id;"
    )
    programa, errores_parser = parsear(tokens)
    assert len(errores_parser) == 0
    errores = validar_semantico(programa)
    assert len(errores) > 0
    assert any("CARTESIAN" in e.codigo for e in errores if e.codigo)


def test_validar_concat_tabla_inexistente():
    tokens, _ = tokenizar("CONCATENATE tabla_inexistente;")
    programa, errores_parser = parsear(tokens)
    assert len(errores_parser) == 0
    errores = validar_semantico(programa)
    assert len(errores) > 0


def test_validar_drop_tabla_inexistente():
    tokens, _ = tokenizar("DROP TABLE tabla_inexistente;")
    programa, errores_parser = parsear(tokens)
    assert len(errores_parser) == 0
    errores = validar_semantico(programa)
    assert len(errores) > 0
    assert any("DROP_NONEXISTENT" in e.codigo for e in errores if e.codigo)


def test_validar_script_valido():
    tokens, _ = tokenizar("LIB CONNECT TO 'conn1'; t1: LOAD a, b FROM '/f.csv';")
    programa, errores_parser = parsear(tokens)
    assert len(errores_parser) == 0
    errores = validar_semantico(programa)
    assert len(errores) == 0


def test_validar_conexion_sin_nombre():
    tokens, _ = tokenizar("LIB CONNECT TO '';")
    programa, errores_parser = parsear(tokens)
    assert len(errores_parser) == 0
    errores = validar_semantico(programa)
    assert len(errores) > 0
