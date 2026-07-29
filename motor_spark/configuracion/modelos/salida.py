from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class SalidaConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    formato: str = "parquet"
    modo: str = "error"
    compresion: Any = "snappy"
    numero_particiones: Any | None = None
    columnas_reparticion: list[str] = Field(default_factory=list)
    particionar_por: list[str] = Field(default_factory=list)

    @field_validator("formato", "modo", mode="before")
    @classmethod
    def normalizar_texto(cls, valor: Any) -> str:
        return str(valor).lower()
