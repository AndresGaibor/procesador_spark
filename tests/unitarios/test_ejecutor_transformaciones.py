from motor_spark.configuracion.modelos.receta import RecetaConfig
from motor_spark.transformaciones.ejecutor import aplicar_pasos


class DataFrameFalso:
    def __init__(self, columnas):
        self.columns = list(columnas)

    def select(self, *columnas):
        return DataFrameFalso(columnas)


def test_aplicar_pasos_emite_eventos_y_delega(capsys):
    receta = RecetaConfig.model_validate(
        {
            "salida": {},
            "pasos": [
                {
                    "tipo": "seleccionar_columnas",
                    "columnas": ["id"],
                }
            ],
        }
    )

    resultado = aplicar_pasos(
        DataFrameFalso(["id", "nombre"]),
        receta.pasos,
    )

    assert resultado.columns == ["id"]
    assert capsys.readouterr().out.splitlines() == [
        "PASO_INICIO numero=1 tipo=seleccionar_columnas",
        "PASO_FIN numero=1 tipo=seleccionar_columnas columnas=['id']",
    ]
