import pytest

from motor_spark.configuracion.cargador_receta import cargar_receta
from motor_spark.dominio.errores import ErrorReceta


def test_cargar_receta_desde_json_directo():
    receta = cargar_receta('{"entrada": {}, "salida": {}, "pasos": []}')
    assert receta.entrada.formato == "csv"


def test_cargar_receta_desde_archivo(tmp_path):
    archivo = tmp_path / "receta.json"
    archivo.write_text(
        '{"entrada": {}, "salida": {}, "pasos": []}',
        encoding="utf-8",
    )
    assert cargar_receta(str(archivo)).salida.formato == "parquet"


def test_cargar_receta_reporta_linea_y_columna_json():
    with pytest.raises(ErrorReceta, match=r"línea=1, columna=2"):
        cargar_receta("{")


def test_cargar_receta_rechaza_raiz_no_objeto(tmp_path):
    archivo = tmp_path / "lista.json"
    archivo.write_text("[]", encoding="utf-8")
    with pytest.raises(ErrorReceta, match="La receta debe ser un objeto JSON"):
        cargar_receta(str(archivo))


def test_cargar_receta_conserva_error_paso_sin_tipo():
    with pytest.raises(ErrorReceta, match="El paso 1 no tiene tipo"):
        cargar_receta('{"salida": {}, "pasos": [{}]}')


def test_cargar_receta_conserva_error_operacion_desconocida():
    with pytest.raises(
        ErrorReceta,
        match="Operación no soportada en el paso 1: inventada",
    ):
        cargar_receta('{"salida": {}, "pasos": [{"tipo": "inventada"}]}')


def test_cargar_receta_conserva_error_entrada_no_objeto():
    with pytest.raises(
        ErrorReceta,
        match="receta.entrada debe ser un objeto",
    ):
        cargar_receta('{"entrada": [], "salida": {}}')


def test_cargar_receta_conserva_error_modo_esquema_invalido():
    with pytest.raises(ErrorReceta) as error:
        cargar_receta(
            '{"entrada": {"modo_esquema": "automatico"}, "salida": {}}'
        )
    assert str(error.value) == (
        "entrada.modo_esquema debe ser 'estricto' o 'inferir', "
        "pero se recibió: 'automatico'"
    )


def test_cargar_receta_conserva_error_tipos_forzados_no_objeto():
    with pytest.raises(
        ErrorReceta,
        match="entrada.tipos_forzados debe ser un objeto",
    ):
        cargar_receta(
            '{"entrada": {"tipos_forzados": []}, "salida": {}}'
        )
