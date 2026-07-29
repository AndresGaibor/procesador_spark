from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


_ALIASES_MODO_ESQUEMA = {
    "strict": "estricto",
    "schema": "estricto",
    "infer": "inferir",
    "inferido": "inferir",
    "dinamico": "inferir",
    "dinámico": "inferir",
}


def normalizar_modo_esquema(valor: Any) -> str:
    modo = str(valor if valor is not None else "estricto").strip().lower()
    modo = _ALIASES_MODO_ESQUEMA.get(modo, modo)
    if modo not in {"estricto", "inferir"}:
        raise ValueError(
            "entrada.modo_esquema debe ser 'estricto' o 'inferir', "
            f"pero se recibió: {modo!r}"
        )
    return modo


class EntradaConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    formato: str = "csv"
    modo_esquema: str = "estricto"
    opciones: dict[str, Any] = Field(default_factory=dict)
    inferir_tipos: Any = True
    normalizar_nombres_columnas: Any = True
    tipos_forzados: dict[str, Any] | None = Field(default_factory=dict)

    @field_validator("formato", mode="before")
    @classmethod
    def normalizar_formato(cls, valor: Any) -> str:
        return str(valor if valor is not None else "csv").strip().lower()

    @field_validator("modo_esquema", mode="before")
    @classmethod
    def validar_modo_esquema(cls, valor: Any) -> str:
        return normalizar_modo_esquema(valor)
