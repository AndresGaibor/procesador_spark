from __future__ import annotations

from types import MappingProxyType

from motor_spark.transformaciones.agregaciones import aplicar_agrupacion
from motor_spark.transformaciones.columnas import (
    aplicar_creacion_columna,
    aplicar_eliminacion,
    aplicar_relleno_nulos,
    aplicar_renombrado,
    aplicar_seleccion,
)
from motor_spark.transformaciones.conversion import aplicar_conversion_tipo
from motor_spark.transformaciones.duplicados import aplicar_eliminacion_duplicados
from motor_spark.transformaciones.filtros import aplicar_filtro
from motor_spark.transformaciones.particiones import aplicar_reparticion
from motor_spark.transformaciones.texto import aplicar_normalizacion_texto

REGISTRO_TRANSFORMACIONES = MappingProxyType(
    {
        "seleccionar_columnas": aplicar_seleccion,
        "eliminar_columnas": aplicar_eliminacion,
        "renombrar_columna": aplicar_renombrado,
        "convertir_tipo": aplicar_conversion_tipo,
        "crear_columna": aplicar_creacion_columna,
        "filtrar": aplicar_filtro,
        "rellenar_nulos": aplicar_relleno_nulos,
        "normalizar_texto": aplicar_normalizacion_texto,
        "eliminar_duplicados": aplicar_eliminacion_duplicados,
        "agrupar": aplicar_agrupacion,
        "reparticionar": aplicar_reparticion,
    }
)
