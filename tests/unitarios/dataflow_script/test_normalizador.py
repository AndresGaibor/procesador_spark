from motor_spark.dataflow_script.normalizador import Normalizador, normalizar


def test_quitar_bom():
    contenido = "\ufeffSELECT * FROM esquema.tabla;"
    normalizado, errores = normalizar(contenido)
    assert not normalizado.startswith("\ufeff")
    assert len(errores) == 0


def test_normalizar_crlf_a_lf():
    contenido = "SELECT * FROM\r\ntabla;\r\n"
    normalizado, errores = normalizar(contenido)
    assert "\r\n" not in normalizado
    assert "\r" not in normalizado
    assert len(errores) == 0


def test_quitar_comentario_linea():
    contenido = "SELECT * FROM tabla; // esto es un comentario"
    normalizado, errores = normalizar(contenido)
    assert "//" not in normalizado
    assert len(errores) == 0


def test_quitar_comentario_bloque():
    contenido = "SELECT * FROM tabla; /* comentario\nen varias\nlineas */"
    normalizado, errores = normalizar(contenido)
    assert "/*" not in normalizado
    assert "*/" not in normalizado
    assert len(errores) == 0


def test_comentario_no_quitar_dentro_string():
    contenido = 'SELECT "//" FROM tabla;'
    normalizado, _errores = normalizar(contenido)
    assert "//" in normalizado


def test_comentario_no_quitar_dentro_parentesis():
    contenido = "SELECT func(/* comentario */) FROM tabla;"
    normalizado, _errores = normalizar(contenido)
    assert "/*" in normalizado or "comentario" in normalizado


def test_procesar_integra_todo():
    contenido = "\ufeffSELECT * FROM\r\ntabla; // comentario"
    normalizado, errores = normalizar(contenido)
    assert not normalizado.startswith("\ufeff")
    assert "\r\n" not in normalizado
    assert "//" not in normalizado
    assert len(errores) == 0


def test_normalizador_clase():
    n = Normalizador("SELECT * FROM tabla;")
    resultado, errores = n.procesar()
    assert resultado == "SELECT * FROM tabla;"
    assert len(errores) == 0


def test_no_confunde_doble_slash_de_lib_uri_con_comentario():
    """El ``//`` de una URI Qlik es parte del dato, no inicia comentario."""
    contenido = "FROM [lib://Conexion SFTP//datos/ventas.csv];"

    normalizado, errores = normalizar(contenido)

    assert normalizado == contenido
    assert errores == []


def test_quita_comentario_sql_de_doble_guion_sin_mover_lineas():
    """Los fixtures y algunos exportadores incluyen comentarios ``--``."""
    contenido = "-- comentario inicial\nSELECT campo FROM tabla;"

    normalizado, errores = normalizar(contenido)

    assert "comentario inicial" not in normalizado
    assert normalizado.count("\n") == contenido.count("\n")
    assert "SELECT campo FROM tabla;" in normalizado
    assert errores == []
