import pytest

pytest.importorskip("pyspark")

from motor_spark.configuracion.modelos.receta import RecetaConfig
from motor_spark.transformaciones.ejecutor import aplicar_pasos

pytestmark = pytest.mark.spark


def test_transformaciones_registradas_en_cadena(spark_local):
    datos = spark_local.createDataFrame(
        [
            (1, " ANA ", 10.0),
            (2, "LUIS", 20.0),
            (2, "LUIS", 20.0),
        ],
        ["id", "nombre", "total"],
    )
    receta = RecetaConfig.model_validate({
        "salida": {},
        "pasos": [
            {
                "tipo": "normalizar_texto",
                "columnas": ["nombre"],
                "operaciones": ["trim", "lower"],
            },
            {
                "tipo": "eliminar_duplicados",
                "columnas": ["id"],
            },
            {
                "tipo": "crear_columna",
                "nombre": "total_doble",
                "expresion": "total * 2",
            },
            {
                "tipo": "filtrar",
                "expresion": "total_doble >= 20",
            },
            {
                "tipo": "seleccionar_columnas",
                "columnas": ["id", "nombre", "total_doble"],
            },
        ],
    })
    filas = aplicar_pasos(datos, receta.pasos).orderBy("id").collect()
    assert [(f.id, f.nombre, f.total_doble) for f in filas] == [
        (1, "ana", 20.0),
        (2, "luis", 40.0),
    ]
