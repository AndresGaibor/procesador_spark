from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum, auto


class TipoToken(Enum):
    IDENTIFICADOR = auto()
    SIMBOLO = auto()
    NUMERO = auto()
    STRING = auto()
    PALABRA_RESERVADA = auto()
    FIN = auto()
    DESCONOCIDO = auto()
    BRACKET_ID = auto()


class TipoSentencia(Enum):
    SET = auto()
    LIB_CONNECT_TO = auto()
    ETIQUETA = auto()
    SELECT = auto()
    LOAD = auto()
    RESIDENT = auto()
    DROP_TABLE = auto()
    STORE = auto()
    CONCATENATE = auto()
    NOCONCATENATE = auto()


class TipoExpresion(Enum):
    COLUMNA = auto()
    LITERAL_NUMERO = auto()
    LITERAL_STRING = auto()
    FUNCION = auto()
    OPERACION_BINARIA = auto()
    CONCATENACION = auto()
    ALIAS = auto()
    WINDOW = auto()
    WINDOW_RANK = auto()


@dataclass(frozen=True, slots=True)
class SourceSpan:
    linea: int
    columna: int
    offset: int


@dataclass(frozen=True, slots=True)
class Token:
    tipo: TipoToken
    valor: str
    ubicacion: SourceSpan


@dataclass(frozen=True, slots=True)
class Expresion:
    tipo: TipoExpresion
    valor: str
    hijos: tuple[Expresion, ...] = ()


@dataclass(frozen=True, slots=True)
class ProjectionItem:
    expresion: Expresion
    alias: str | None = None


@dataclass(frozen=True, slots=True)
class CondicionJoin:
    izquierda: str
    derecha: str
    tipo: str = "="
    es_natural: bool = False


@dataclass(frozen=True, slots=True)
class SentenciaSelect:
    proyecciones: Sequence[ProjectionItem]
    tabla: str
    esquema: str | None = None
    join_externo: CondicionJoin | None = None
    condiciones_where: tuple[Expresion, ...] = ()
    etiqueta_origen: str | None = None
    group_by: tuple[Expresion, ...] = ()


@dataclass(frozen=True, slots=True)
class SentenciaLoad:
    """LOAD de Qlik con sus proyecciones y cláusulas preservadas.

    ``campos`` se conserva por compatibilidad con la primera versión del AST;
    las nuevas rutas deben usar ``proyecciones`` porque también representa
    funciones, cálculos y aliases.
    """

    ruta: str
    expresion: Expresion | None = None
    campos: tuple[str, ...] = ()
    distinct: bool = False
    es_resident: bool = False
    etiqueta_resident: str | None = None
    noconcatenate: bool = False
    proyecciones: tuple[ProjectionItem, ...] = ()
    condiciones_where: tuple[Expresion, ...] = ()
    group_by: tuple[Expresion, ...] = ()
    # Los prefijos globales de Qlik modifican la carga que aparece justo
    # después. Guardarlos en la propia sentencia impide perder esa relación
    # cuando el AST reorganiza el script en etiquetas.
    concatenate_objetivo: str | None = None
    join_objetivo: str | None = None
    join_tipo: str | None = None


@dataclass(frozen=True, slots=True)
class SentenciaResident:
    etiqueta_origen: str
    expresion: Expresion | None = None


@dataclass(frozen=True, slots=True)
class SentenciaSet:
    variable: str
    valor: str


@dataclass(frozen=True, slots=True)
class SentenciaLibConnectTo:
    nombre_lib: str
    conexion: str


@dataclass(frozen=True, slots=True)
class SentenciaDropTable:
    esquema: str
    tabla: str


@dataclass(frozen=True, slots=True)
class SentenciaStore:
    esquema: str
    tabla: str
    ruta_destino: str
    formato: str | None = None


@dataclass(frozen=True, slots=True)
class SentenciaConcatenate:
    etiqueta_objetivo: str
    etiqueta_origen: str
    noconcatenate: bool = False


@dataclass(frozen=True, slots=True)
class Etiqueta:
    nombre: str
    sentencias: tuple[
        SentenciaSelect
        | SentenciaLoad
        | SentenciaResident
        | SentenciaDropTable
        | SentenciaStore
        | SentenciaConcatenate,
        ...,
    ]


@dataclass(frozen=True, slots=True)
class ProgramaDataflowScript:
    sentencias_globales: tuple[SentenciaSet | SentenciaLibConnectTo, ...]
    etiquetas: tuple[Etiqueta, ...]
