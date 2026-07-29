from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class IncrementalConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    activo: Any = False
    duplicados: str = "ignorar"
    claves: list[Any] = Field(default_factory=list)
