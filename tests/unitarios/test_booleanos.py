import pytest

from motor_spark.compartido.booleanos import convertir_booleano
from motor_spark.dominio.errores import ErrorReceta


@pytest.mark.parametrize("valor", [True, "1", "true", "si", "sí", "yes"])
def test_convertir_booleano_verdadero(valor):
    assert convertir_booleano(valor, False) is True


@pytest.mark.parametrize("valor", [False, "0", "false", "no"])
def test_convertir_booleano_falso(valor):
    assert convertir_booleano(valor, True) is False


def test_convertir_booleano_usa_predeterminado_para_none():
    assert convertir_booleano(None, True) is True


def test_convertir_booleano_rechaza_valor_desconocido():
    with pytest.raises(ErrorReceta, match="Valor booleano inválido"):
        convertir_booleano("quizas", False)
