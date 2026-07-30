"""Modelos inmutables del plan intermedio Dataflow → Spark.

El plan es el contrato entre compilación y ejecución. Debe contener toda la
información necesaria para ejecutarse sin volver a interpretar el script Qlik.
Por esa razón las operaciones se modelan como una unión de subtipos concretos;
serializarlas como la clase base eliminaría silenciosamente sus parámetros.
"""

from __future__ import annotations

import hashlib
import json
from enum import Enum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class TipoOperacion(str, Enum):
    """Conjunto cerrado de instrucciones que entiende el ejecutor del plan."""

    LEER_JDBC = "leer_jdbc"
    PROYECTAR = "proyectar"
    FILTRAR = "filtrar"
    CONCATENAR = "concatenar"
    UNIR = "unir"
    AGREGAR = "agregar"
    ELIMINAR_TABLA = "eliminar_tabla"
    PUBLICAR = "publicar"
    CARGAR_CSV = "cargar_csv"
    CARGAR_LOCAL = "cargar_local"


class TipoExpresionPlan(str, Enum):
    """Tipos estables del árbol de expresión persistido en el plan."""

    COLUMNA = "columna"
    LITERAL_NUMERO = "literal_numero"
    LITERAL_STRING = "literal_string"
    FUNCION = "funcion"
    OPERACION_BINARIA = "operacion_binaria"
    CONCATENACION = "concatenacion"
    ALIAS = "alias"
    WINDOW = "window"
    WINDOW_RANK = "window_rank"


class ExpresionPlan(BaseModel):
    """Árbol recursivo independiente del texto y serializable a JSON."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    tipo: TipoExpresionPlan
    valor: str
    hijos: tuple[ExpresionPlan, ...] = Field(default_factory=tuple)


class SeleccionPlan(BaseModel):
    """Expresión de salida junto con el nombre visible de la columna."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    expresion: ExpresionPlan
    alias: str | None = None


class Operacion(BaseModel):
    """Campos comunes; no debe usarse solo para persistir una operación."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        arbitrary_types_allowed=True,
    )

    id: str = Field(..., min_length=1, description="ID estable de la operación")
    tipo: TipoOperacion


class LeerJdbc(Operacion):
    """Lee una tabla permitida y la registra con un nombre lógico Qlik."""

    tipo: Literal[TipoOperacion.LEER_JDBC] = TipoOperacion.LEER_JDBC
    nombre_tabla: str = Field(..., min_length=1)
    conexion_nombre: str = Field(..., min_length=1)
    esquema: str = Field(..., min_length=1)
    tabla: str = Field(..., min_length=1)
    campos: tuple[str, ...] = Field(default_factory=tuple)
    filtros_where: tuple[str, ...] = Field(default_factory=tuple)


class Proyectar(Operacion):
    """Selecciona columnas y opcionalmente registra el resultado con otro nombre."""

    tipo: Literal[TipoOperacion.PROYECTAR] = TipoOperacion.PROYECTAR
    tabla_origen: str = Field(..., min_length=1)
    campos: tuple[str, ...] = Field(..., min_length=1)
    alias: str | None = None
    # Cada par es ``(columna_origen, alias_salida)``. Se mantiene separado de
    # ``campos`` para no interpretar texto arbitrario mediante selectExpr.
    aliases: tuple[tuple[str, str], ...] = Field(default_factory=tuple)
    # ``selecciones`` es la representación completa. ``campos`` y ``aliases``
    # permanecen para planes v1 simples y compatibilidad de API.
    selecciones: tuple[SeleccionPlan, ...] = Field(default_factory=tuple)
    distinct: bool = False


class Filtrar(Operacion):
    """Aplica una condición tipada; ``condicion`` conserva planes v1."""

    tipo: Literal[TipoOperacion.FILTRAR] = TipoOperacion.FILTRAR
    tabla_origen: str = Field(..., min_length=1)
    condicion: str = ""
    expresion: ExpresionPlan | None = None


class Concatenar(Operacion):
    """Replica CONCATENATE por nombre de columna, incluyendo campos ausentes."""

    tipo: Literal[TipoOperacion.CONCATENAR] = TipoOperacion.CONCATENAR
    tabla_objetivo: str = Field(..., min_length=1)
    tabla_origen: str = Field(..., min_length=1)
    noconcatenate: bool = False


class Unir(Operacion):
    """Une dos tablas; NATURAL se resuelve usando todos los campos comunes."""

    tipo: Literal[TipoOperacion.UNIR] = TipoOperacion.UNIR
    tabla_izquierda: str = Field(..., min_length=1)
    tabla_derecha: str = Field(..., min_length=1)
    condicion_on: str = Field(..., min_length=1)
    tipo_join: str = Field(default="LEFT", min_length=1)


class Agregar(Operacion):
    """Agrupa filas y calcula expresiones agregadas explícitas."""

    tipo: Literal[TipoOperacion.AGREGAR] = TipoOperacion.AGREGAR
    tabla_origen: str = Field(..., min_length=1)
    grupo_por: tuple[str, ...] = Field(..., min_length=1)
    funciones: tuple[str, ...] = Field(default_factory=tuple)
    selecciones: tuple[SeleccionPlan, ...] = Field(default_factory=tuple)
    tabla_resultado: str | None = None


class EliminarTabla(Operacion):
    """Libera una tabla lógica del registro en memoria del ejecutor."""

    tipo: Literal[TipoOperacion.ELIMINAR_TABLA] = TipoOperacion.ELIMINAR_TABLA
    nombre: str = Field(..., min_length=1)


class Publicar(Operacion):
    """Publica una tabla ya calculada en un destino declarado por ``lib://``."""

    tipo: Literal[TipoOperacion.PUBLICAR] = TipoOperacion.PUBLICAR
    tabla_origen: str = Field(..., min_length=1)
    destino: str = Field(..., min_length=1)
    formato: str = Field(default="txt", min_length=1)


class CargarCsv(Operacion):
    """Carga un CSV y lo registra bajo la etiqueta lógica indicada."""

    tipo: Literal[TipoOperacion.CARGAR_CSV] = TipoOperacion.CARGAR_CSV
    nombre_tabla: str = Field(..., min_length=1)
    ruta: str = Field(..., min_length=1)
    tiene_header: bool = True
    delimitador: str = Field(default=",", min_length=1, max_length=1)


class CargarLocal(Operacion):
    """Copia una tabla RESIDENT o lee una ruta local controlada."""

    tipo: Literal[TipoOperacion.CARGAR_LOCAL] = TipoOperacion.CARGAR_LOCAL
    ruta: str = Field(..., min_length=1)
    nombre_tabla: str = Field(..., min_length=1)


# La anotación concreta es deliberada: Pydantic usa esta unión para conservar
# todos los campos al serializar y reconstruir el subtipo correcto al leer JSON.
OperacionPlan = Annotated[
    LeerJdbc
    | Proyectar
    | Filtrar
    | Concatenar
    | Unir
    | Agregar
    | EliminarTabla
    | Publicar
    | CargarCsv
    | CargarLocal,
    Field(discriminator="tipo"),
]


class PlanDataflow(BaseModel):
    """Plan versionado, inmutable y serializable de forma canónica."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        arbitrary_types_allowed=True,
    )

    version: int = Field(default=1, ge=1)
    operaciones: tuple[OperacionPlan, ...] = Field(default_factory=tuple)
    tabla_resultado: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("metadata", mode="before")
    @classmethod
    def _normalizar_metadata(cls, valor: Any) -> dict[str, Any]:
        """Normaliza tipos que JSON no puede distinguir por sí solo.

        El compilador usa una tupla para indicar una colección inmutable de
        errores. JSON la representa como lista; restaurarla aquí permite que el
        plan original y el deserializado sean estructuralmente iguales, además
        de producir el mismo hash canónico.
        """
        datos = dict(valor or {})
        if "errores" in datos:
            datos["errores"] = tuple(datos["errores"] or ())
        return datos

    def hash_determista(self) -> str:
        """Calcula SHA-256 sobre JSON canónico, independiente de espacios."""
        datos = self.model_dump(mode="json")
        contenido = json.dumps(
            datos,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        return hashlib.sha256(contenido.encode("utf-8")).hexdigest()

    def id_por_posicion(self, posicion: int) -> str | None:
        """Devuelve el ID posicional sin lanzar IndexError fuera de rango."""
        if 0 <= posicion < len(self.operaciones):
            return self.operaciones[posicion].id
        return None


def generar_id_estable(nombre_tabla: str, operacion: str, indice: int) -> str:
    """Genera IDs repetibles para facilitar auditoría y comparación de planes."""
    contenido = f"{nombre_tabla}_{operacion}_{indice}".encode()
    return hashlib.sha256(contenido).hexdigest()[:16]


ExpresionPlan.model_rebuild()
