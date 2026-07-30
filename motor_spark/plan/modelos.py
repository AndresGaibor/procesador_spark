from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Sequence

from pydantic import BaseModel, ConfigDict, Field


class TipoOperacion(Enum):
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


class Operacion(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, arbitrary_types_allowed=True)

    id: str = Field(..., description="ID unico de la operacion")
    tipo: TipoOperacion


class LeerJdbc(Operacion):
    model_config = ConfigDict(extra="forbid", frozen=True)

    tipo: TipoOperacion = TipoOperacion.LEER_JDBC
    conexion_nombre: str = Field(..., description="Nombre de la conexion JDBC")
    esquema: str = Field(..., description="Esquema o base de datos")
    tabla: str = Field(..., description="Nombre de la tabla")
    campos: tuple[str, ...] = Field(default_factory=tuple, description="Campos a leer")
    filtros_where: tuple[str, ...] = Field(default_factory=tuple, description="Condiciones WHERE")


class Proyectar(Operacion):
    model_config = ConfigDict(extra="forbid", frozen=True)

    tipo: TipoOperacion = TipoOperacion.PROYECTAR
    tabla_origen: str = Field(..., description="Tabla origen")
    campos: tuple[str, ...] = Field(..., description="Campos a proyectar")
    alias: str | None = Field(default=None, description="Alias de la tabla resultado")


class Filtrar(Operacion):
    model_config = ConfigDict(extra="forbid", frozen=True)

    tipo: TipoOperacion = TipoOperacion.FILTRAR
    tabla_origen: str = Field(..., description="Tabla a filtrar")
    condicion: str = Field(..., description="Condicion WHERE")


class Concatenar(Operacion):
    model_config = ConfigDict(extra="forbid", frozen=True)

    tipo: TipoOperacion = TipoOperacion.CONCATENAR
    tabla_objetivo: str = Field(..., description="Tabla objetivo")
    tabla_origen: str = Field(..., description="Tabla origen")
    noconcatenate: bool = Field(default=False, description="Si es NOCONCATENATE")


class Unir(Operacion):
    model_config = ConfigDict(extra="forbid", frozen=True)

    tipo: TipoOperacion = TipoOperacion.UNIR
    tabla_izquierda: str = Field(..., description="Tabla izquierda del JOIN")
    tabla_derecha: str = Field(..., description="Tabla derecha del JOIN")
    condicion_on: str = Field(..., description="Condicion ON")
    tipo_join: str = Field(default="LEFT", description="Tipo de JOIN")


class Agregar(Operacion):
    model_config = ConfigDict(extra="forbid", frozen=True)

    tipo: TipoOperacion = TipoOperacion.AGREGAR
    tabla_origen: str = Field(..., description="Tabla a agregar")
    grupo_por: tuple[str, ...] = Field(..., description="Campos GROUP BY")
    funciones: tuple[str, ...] = Field(..., description="Funciones de agregacion")


class EliminarTabla(Operacion):
    model_config = ConfigDict(extra="forbid", frozen=True)

    tipo: TipoOperacion = TipoOperacion.ELIMINAR_TABLA
    nombre: str = Field(..., description="Nombre de la tabla a eliminar")


class Publicar(Operacion):
    model_config = ConfigDict(extra="forbid", frozen=True)

    tipo: TipoOperacion = TipoOperacion.PUBLICAR
    tabla_origen: str = Field(..., description="Tabla a publicar")
    destino: str = Field(..., description="Destino (lib://path)")
    formato: str = Field(default="txt", description="Formato de salida")


class CargarCsv(Operacion):
    model_config = ConfigDict(extra="forbid", frozen=True)

    tipo: TipoOperacion = TipoOperacion.CARGAR_CSV
    ruta: str = Field(..., description="Ruta del archivo CSV")
    tiene_header: bool = Field(default=True, description="Si tiene header")
    delimitador: str = Field(default=",", description="Delimitador")


class CargarLocal(Operacion):
    model_config = ConfigDict(extra="forbid", frozen=True)

    tipo: TipoOperacion = TipoOperacion.CARGAR_LOCAL
    ruta: str = Field(..., description="Ruta del archivo")
    nombre_tabla: str = Field(..., description="Nombre para la tabla")


class PlanDataflow(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, arbitrary_types_allowed=True)

    version: int = Field(default=1, description="Version del plan")
    operaciones: tuple[Operacion, ...] = Field(default_factory=tuple, description="Operaciones del plan")
    tabla_resultado: str | None = Field(default=None, description="Nombre de la tabla resultado final")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Metadata adicional")

    def hash_determista(self) -> str:
        import json
        datos = self.model_dump(mode='json')
        contenido = json.dumps(datos, sort_keys=True, default=str)
        return hashlib.sha256(contenido.encode()).hexdigest()

    def id_por_posicion(self, posicion: int) -> str | None:
        if 0 <= posicion < len(self.operaciones):
            return self.operaciones[posicion].id
        return None


def generar_id_estable(nombre_tabla: str, operacion: str, indice: int) -> str:
    raw = f"{nombre_tabla}_{operacion}_{indice}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]
