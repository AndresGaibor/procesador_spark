from motor_spark.infraestructura.spark.lector import convertir_opciones


def test_convertir_opciones_convierte_booleanos_en_minusculas():
    assert convertir_opciones({
        "header": True,
        "inferSchema": False,
        "delimiter": ";",
        "maxColumns": 20,
    }) == {
        "header": "true",
        "inferSchema": "false",
        "delimiter": ";",
        "maxColumns": "20",
    }
