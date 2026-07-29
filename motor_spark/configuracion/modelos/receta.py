from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from motor_spark.configuracion.modelos.entrada import EntradaConfig
from motor_spark.configuracion.modelos.incremental import IncrementalConfig
from motor_spark.configuracion.modelos.pasos import PasoConfig
from motor_spark.configuracion.modelos.salida import SalidaConfig


class SparkConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    nivel_log: str = "WARN"
    shuffle_partitions: Any | None = None


class AuditoriaConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    contar_registros: Any = False


class RecetaConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    nombre: Any = "Motor Spark mecánico"
    version: Any | None = None
    spark: SparkConfig = Field(default_factory=SparkConfig)
    entrada: EntradaConfig = Field(default_factory=EntradaConfig)
    pasos: list[PasoConfig] = Field(default_factory=list)
    incremental: IncrementalConfig = Field(default_factory=IncrementalConfig)
    salida: SalidaConfig
    auditoria: AuditoriaConfig = Field(default_factory=AuditoriaConfig)

    def a_dict_compatible(self) -> dict[str, Any]:
        return self.model_dump(mode="python", exclude_none=False)
