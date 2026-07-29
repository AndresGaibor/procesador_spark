import pytest

from motor_spark.dominio.columnas import normalizar_nombre_columna
from motor_spark.dominio.errores import ErrorReceta


def test_normalizar_nombre_columna_elimina_tildes_y_simbolos():
    assert normalizar_nombre_columna("  Método de Pago  ") == "metodo_de_pago"


def test_normalizar_nombre_columna_prefija_numeros():
    assert normalizar_nombre_columna("2026 Total") == "col_2026_total"


def test_normalizar_nombre_columna_rechaza_nombre_vacio():
    with pytest.raises(ErrorReceta, match="columna sin nombre"):
        normalizar_nombre_columna("  ")
