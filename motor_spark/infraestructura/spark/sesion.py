from __future__ import annotations

from typing import Any

from motor_spark.configuracion.modelos.receta import SparkConfig


def crear_sesion_spark(
    nombre: str,
    ejecucion_id: str,
    configuracion: SparkConfig,
) -> Any:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName(f"{nombre} - {ejecucion_id}").getOrCreate()
    spark.sparkContext.setLogLevel(configuracion.nivel_log)
    if configuracion.shuffle_partitions:
        spark.conf.set(
            "spark.sql.shuffle.partitions",
            int(configuracion.shuffle_partitions),
        )
    return spark
