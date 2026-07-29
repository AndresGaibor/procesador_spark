from motor_spark.compartido.eventos_consola import emitir


def test_emitir_escribe_stdout(capsys):
    emitir("EJECUCION_INICIO=e-1")
    captura = capsys.readouterr()
    assert captura.out == "EJECUCION_INICIO=e-1\n"
    assert captura.err == ""


def test_emitir_error_escribe_stderr(capsys):
    emitir("RESULTADO_MOTOR={}", error=True)
    captura = capsys.readouterr()
    assert captura.err == "RESULTADO_MOTOR={}\n"
    assert captura.out == ""
