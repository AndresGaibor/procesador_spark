from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, ConfigDict, Field


class TipoConexion(Enum):
    JDBC = "jdbc"
    LOCAL = "local"
    SFTP = "sftp"


class CampoAllowlist(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    esquema: str = Field(..., description="Esquema permitido")
    tabla: str = Field(..., description="Tabla permitida")
    campos: tuple[str, ...] = Field(
        default_factory=tuple, description="Campos permitidos (vacio = todos)"
    )


class ConexionJdbc(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    tipo: TipoConexion = TipoConexion.JDBC
    nombre: str = Field(..., description="Nombre unico de la conexion")
    url: str = Field(..., description="URL JDBC (sin secretos)")
    driver: str = Field(..., description="Clase del driver JDBC")
    secreto_nombre: str = Field(
        ..., description="Nombre de la variable de entorno con credenciales"
    )
    allowlist: tuple[CampoAllowlist, ...] = Field(
        default_factory=tuple, description="Tablas y campos permitidos"
    )
    propiedades: dict[str, str] = Field(
        default_factory=dict, description="Propiedades adicionales"
    )


class ConexionLocal(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    tipo: TipoConexion = TipoConexion.LOCAL
    nombre: str = Field(..., description="Nombre unico de la conexion")
    ruta_base: str = Field(..., description="Ruta base para archivos locales")
    allowlist: tuple[CampoAllowlist, ...] = Field(
        default_factory=tuple, description="Rutas permitidas"
    )


class ConexionSftp(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    tipo: TipoConexion = TipoConexion.SFTP
    nombre: str = Field(..., description="Nombre unico de la conexion")
    host: str = Field(..., description="Host SFTP")
    puerto: int = Field(default=22, description="Puerto SFTP")
    secreto_nombre: str = Field(
        ..., description="Nombre de la variable de entorno con credenciales"
    )
    ruta_base: str = Field(default="/", description="Ruta base remota")
    allowlist: tuple[CampoAllowlist, ...] = Field(
        default_factory=tuple, description="Rutas permitidas"
    )


class CatalogoConexiones(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    version: int = Field(default=1, description="Version del catalogo")
    descripcion: str = Field(default="", description="Descripcion del catalogo")
    jdbc: tuple[ConexionJdbc, ...] = Field(
        default_factory=tuple, description="Conexiones JDBC"
    )
    locales: tuple[ConexionLocal, ...] = Field(
        default_factory=tuple, description="Conexiones locales"
    )
    sftp: tuple[ConexionSftp, ...] = Field(
        default_factory=tuple, description="Conexiones SFTP"
    )

    def buscar_jdbc(self, nombre: str) -> ConexionJdbc | None:
        for conn in self.jdbc:
            if conn.nombre == nombre:
                return conn
        return None

    def buscar_local(self, nombre: str) -> ConexionLocal | None:
        for conn in self.locales:
            if conn.nombre == nombre:
                return conn
        return None

    def buscar_sftp(self, nombre: str) -> ConexionSftp | None:
        for conn in self.sftp:
            if conn.nombre == nombre:
                return conn
        return None

    def buscar_por_nombre(
        self, nombre: str
    ) -> ConexionJdbc | ConexionLocal | ConexionSftp | None:
        return (
            self.buscar_jdbc(nombre)
            or self.buscar_local(nombre)
            or self.buscar_sftp(nombre)
        )

    def esta_en_allowlist(self, nombre_conn: str, esquema: str, tabla: str) -> bool:
        conn = self.buscar_por_nombre(nombre_conn)
        if not conn:
            return False

        for item in conn.allowlist:
            if item.esquema == esquema and item.tabla == tabla:
                return True
        return False
