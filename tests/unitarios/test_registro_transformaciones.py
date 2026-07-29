from motor_spark.transformaciones.registro import REGISTRO_TRANSFORMACIONES


def test_registro_contiene_operaciones_actuales():
    assert set(REGISTRO_TRANSFORMACIONES) == {
        "seleccionar_columnas",
        "eliminar_columnas",
        "renombrar_columna",
        "convertir_tipo",
        "crear_columna",
        "filtrar",
        "rellenar_nulos",
        "normalizar_texto",
        "eliminar_duplicados",
        "agrupar",
        "reparticionar",
    }
