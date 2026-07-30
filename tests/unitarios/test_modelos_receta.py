import pytest
from pydantic import ValidationError

from motor_spark.configuracion.modelos.receta import RecetaConfig


def test_receta_aplica_predeterminados_actuales():
    receta = RecetaConfig.model_validate(
        {
            "entrada": {},
            "salida": {},
            "pasos": [],
        }
    )
    assert receta.nombre == "Motor Spark mecánico"
    assert receta.entrada.formato == "csv"
    assert receta.entrada.modo_esquema == "estricto"
    assert receta.salida.formato == "parquet"
    assert receta.salida.modo == "error"
    assert receta.salida.compresion == "snappy"
    assert receta.incremental.activo is False
    assert receta.auditoria.contar_registros is False


def test_receta_normaliza_alias_modo_esquema():
    receta = RecetaConfig.model_validate(
        {
            "entrada": {"modo_esquema": "dinámico"},
            "salida": {},
        }
    )
    assert receta.entrada.modo_esquema == "inferir"


def test_receta_construye_paso_filtrar_tipado():
    receta = RecetaConfig.model_validate(
        {
            "entrada": {},
            "salida": {},
            "pasos": [{"tipo": "filtrar", "expresion": "total > 0"}],
        }
    )
    assert receta.pasos[0].tipo == "filtrar"
    assert receta.pasos[0].expresion == "total > 0"


def test_receta_requiere_salida_por_compatibilidad():
    with pytest.raises(ValidationError):
        RecetaConfig.model_validate({"entrada": {}})


def test_receta_conserva_nombre_nulo_explicito():
    receta = RecetaConfig.model_validate({"nombre": None, "salida": {}})
    assert receta.nombre is None


def test_salida_no_elimina_espacios_que_motor_original_conservaba():
    receta = RecetaConfig.model_validate(
        {"salida": {"formato": " PARQUET ", "modo": " ERROR "}}
    )
    assert receta.salida.formato == " parquet "
    assert receta.salida.modo == " error "
