import pytest

from motor_spark.dataflow_script.lexer import Lexer, LexerError, Token, tokenizar


def test_tokenizar_bracket_id():
    tokens, errores = tokenizar("[mi_tabla]")
    assert len(errores) == 0
    assert tokens[0].tipo == "BRACKET_ID"
    assert tokens[0].valor == "mi_tabla"


def test_tokenizar_bracket_id_con_espacios():
    tokens, errores = tokenizar("[My Table]")
    assert len(errores) == 0
    assert tokens[0].tipo == "BRACKET_ID"
    assert tokens[0].valor == "My Table"


def test_tokenizar_bracket_id_con_numero():
    tokens, errores = tokenizar("[tabla123]")
    assert len(errores) == 0
    assert tokens[0].tipo == "BRACKET_ID"
    assert tokens[0].valor == "tabla123"


def test_tokenizar_bracket_id_error_no_cierra():
    tokens, errores = tokenizar("[tabla sin cerrar\n")
    assert len(errores) > 0
    assert any("BRACKET_UNCLOSED" in e.codigo for e in errores if e.codigo)


def test_tokenizar_keyword_case_insensitive():
    tokens, errores = tokenizar("select * from tabla")
    assert len(errores) == 0
    assert tokens[0].tipo == "PALABRA_RESERVADA"
    assert tokens[0].valor == "SELECT"


def test_tokenizar_keyword_mixed_case():
    tokens, errores = tokenizar("SeLeCt * FrOm tabla")
    assert len(errores) == 0
    assert tokens[0].valor == "SELECT"
    assert tokens[2].valor == "FROM"


def test_tokenizar_identificador_case_sensitive():
    tokens, errores = tokenizar("miVar MiVar MIVAR")
    assert len(errores) == 0
    valores = [t.valor for t in tokens if t.tipo == "IDENTIFICADOR"]
    assert valores == ["miVar", "MiVar", "MIVAR"]


def test_tokenizar_decimal():
    tokens, errores = tokenizar("3.14159")
    assert len(errores) == 0
    assert tokens[0].tipo == "NUMERO"
    assert tokens[0].valor == "3.14159"


def test_tokenizar_group_by():
    tokens, errores = tokenizar("SELECT col1, COUNT(*) FROM tabla GROUP BY col1")
    assert len(errores) == 0
    valores = [t.valor for t in tokens[:-1]]
    assert "GROUP" in valores
    assert "BY" in valores


def test_tokenizar_distinct():
    tokens, errores = tokenizar("SELECT DISTINCT col1 FROM tabla")
    assert len(errores) == 0
    valores = [t.valor for t in tokens[:-1]]
    assert "DISTINCT" in valores


def test_tokenizar_concatenate():
    tokens, errores = tokenizar("CONCATENATE tabla1")
    assert len(errores) == 0
    assert any(t.valor == "CONCATENATE" for t in tokens)


def test_tokenizar_noconcatenate():
    tokens, errores = tokenizar("NOCONCATENATE")
    assert len(errores) == 0
    assert any(t.valor == "NOCONCATENATE" for t in tokens)


def test_tokenizar_drop_table():
    tokens, errores = tokenizar("DROP TABLE [mi_tabla]")
    assert len(errores) == 0
    assert any(t.valor == "DROP" for t in tokens)
    assert any(t.valor == "TABLE" for t in tokens)


def test_tokenizar_store_into():
    tokens, errores = tokenizar("STORE tabla INTO [lib://ruta](txt)")
    assert len(errores) == 0
    assert any(t.valor == "STORE" for t in tokens)
    assert any(t.valor == "INTO" for t in tokens)


def test_tokenizar_funciones_permitidas():
    funciones = ["CONCAT(a,b)", "LEFT(c,1)", "UPPER(d)", "LOWER(e)", "TRIM(f)", "IFNULL(g,0)"]
    for func in funciones:
        tokens, errores = tokenizar(f"SELECT {func} FROM t")
        assert len(errores) == 0, f"Error tokenizing {func}: {errores}"
