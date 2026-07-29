from __future__ import annotations

from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, ConfigDict, Field


class _PasoBase(BaseModel):
    model_config = ConfigDict(extra="allow")


class SeleccionarColumnasPaso(_PasoBase):
    tipo: Literal["seleccionar_columnas"]
    columnas: list[str]


class EliminarColumnasPaso(_PasoBase):
    tipo: Literal["eliminar_columnas"]
    columnas: list[str]


class RenombrarColumnaPaso(_PasoBase):
    tipo: Literal["renombrar_columna"]
    origen: str
    destino: str


class ConvertirTipoPaso(_PasoBase):
    tipo: Literal["convertir_tipo"]
    columna: str
    destino: str
    formato: str | None = None


class CrearColumnaPaso(_PasoBase):
    tipo: Literal["crear_columna"]
    nombre: str
    expresion: str


class FiltrarPaso(_PasoBase):
    tipo: Literal["filtrar"]
    expresion: str


class RellenarNulosPaso(_PasoBase):
    tipo: Literal["rellenar_nulos"]
    valores: dict[str, Any] = Field(default_factory=dict)


class NormalizarTextoPaso(_PasoBase):
    tipo: Literal["normalizar_texto"]
    columnas: list[str] = Field(default_factory=list)
    operaciones: list[str] = Field(default_factory=list)


class EliminarDuplicadosPaso(_PasoBase):
    tipo: Literal["eliminar_duplicados"]
    columnas: list[str] = Field(default_factory=list)


class MetricaConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    operacion: str
    alias: str
    columna: str = "*"


class AgruparPaso(_PasoBase):
    tipo: Literal["agrupar"]
    columnas: list[str] = Field(default_factory=list)
    metricas: list[MetricaConfig] = Field(default_factory=list)


class ReparticionarPaso(_PasoBase):
    tipo: Literal["reparticionar"]
    cantidad: int
    columnas: list[str] = Field(default_factory=list)


PasoConfig = Annotated[
    Union[
        SeleccionarColumnasPaso,
        EliminarColumnasPaso,
        RenombrarColumnaPaso,
        ConvertirTipoPaso,
        CrearColumnaPaso,
        FiltrarPaso,
        RellenarNulosPaso,
        NormalizarTextoPaso,
        EliminarDuplicadosPaso,
        AgruparPaso,
        ReparticionarPaso,
    ],
    Field(discriminator="tipo"),
]

TIPOS_PASO_SOPORTADOS = {
    "seleccionar_columnas",
    "eliminar_columnas",
    "renombrar_columna",
    "convertir_tipo",
    "crear_columna",
    "filtrar",
    "rellenar_nulos",
    "normalizar_texto",
    "eliminar_duplicados",
    "agrupar",
    "reparticionar",
}
