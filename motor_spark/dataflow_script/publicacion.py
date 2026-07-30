from __future__ import annotations

import base64
import binascii
import csv
import hashlib
import io
import os
import re
import shutil
from collections.abc import Iterable
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any
from uuid import uuid4

from typing_extensions import Self

try:
    import paramiko

    HAS_PARAMIKO = True
except ImportError:
    HAS_PARAMIKO = False


LIMITE_CLAVE_PRIVADA_BYTES = 1024 * 1024


def decodificar_clave_privada_base64(valor: str) -> str:
    """Decodifica una clave privada transportada como Base64 de una sola línea.

    El Base64 evita que el CLI y Fish alteren saltos de línea. El resultado se
    conserva solo en memoria y nunca se escribe a un archivo temporal.
    """
    if not isinstance(valor, str) or not valor:
        raise ValueError("La clave privada Base64 está vacía")
    try:
        contenido_bytes = base64.b64decode(valor, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("La clave privada no contiene Base64 válido") from exc
    if not contenido_bytes or len(contenido_bytes) > LIMITE_CLAVE_PRIVADA_BYTES:
        raise ValueError("Tamaño de clave privada Base64 inválido")
    try:
        contenido = contenido_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("La clave privada Base64 no contiene texto UTF-8") from exc

    lineas = contenido.strip().splitlines()
    if len(lineas) < 3:
        raise ValueError("Contenido de clave privada incompleto")
    encabezado = lineas[0].strip()
    pie = lineas[-1].strip()
    if not (
        encabezado.startswith("-----BEGIN ") and encabezado.endswith("PRIVATE KEY-----")
    ):
        raise ValueError("Encabezado de clave privada no reconocido")
    etiqueta = encabezado.removeprefix("-----BEGIN ").removesuffix("-----")
    if pie != f"-----END {etiqueta}-----":
        raise ValueError("Cierre de clave privada no coincide con el encabezado")
    return contenido


class EstadoPublicacion(Enum):
    PENDIENTE = "pendiente"
    STAGED = "staged"
    PUBLICADO = "publicado"
    FALLIDO = "fallido"


@dataclass(frozen=True)
class UriParseResult:
    conexion: str
    ruta: str


class UriLib:
    PATRON = re.compile(r"^lib://([^/]+)/(.+)$")

    @classmethod
    def parsear(cls, uri: str) -> UriParseResult:
        if not isinstance(uri, str):
            raise ValueError(f"URI debe ser string, recibido {type(uri).__name__}")  # noqa: TRY004
        match = cls.PATRON.match(uri)
        if not match:
            raise ValueError(f"URI invalida: {uri}")
        conexion_raw, ruta_raw = match.groups()
        conexion = cls._validar_conexion(conexion_raw)
        ruta = cls._validar_ruta(ruta_raw)
        return UriParseResult(conexion=conexion, ruta=ruta)

    @classmethod
    def _validar_conexion(cls, valor: str) -> str:
        if not valor:
            raise ValueError("Nombre de conexion vacio")
        if len(valor) > 255:
            raise ValueError("Nombre de conexion demasiado largo")
        for char in valor:
            if ord(char) < 32 or char in ("/", "\\", "@", "?", "#", "[", "]"):
                raise ValueError(f"Caracter invalido en conexion: {char!r}")
        return valor

    @classmethod
    def _validar_ruta(cls, valor: str) -> str:
        if not valor:
            raise ValueError("Ruta vacia")
        if valor.startswith("/"):
            raise ValueError("Ruta no puede ser absoluta")
        if ".." in valor.split("/"):
            raise ValueError("Ruta no puede contener traversal")
        if "\\" in valor:
            raise ValueError("Ruta no puede contener backslash")
        if "\x00" in valor:
            raise ValueError("Ruta no puede contener NUL")
        if "%" in valor or "+" in valor:
            raise ValueError("URL encoding no permitido")
        componentes = valor.split("/")
        for comp in componentes:
            if not comp or comp in (".", "CON", "PRN", "AUX", "NUL", "COM1", "LPT1"):
                raise ValueError(f"Componente invalido en ruta: {comp!r}")
            if comp.endswith("."):
                raise ValueError(f"Componente no puede terminar en punto: {comp!r}")
        if not valor.endswith((".csv", ".txt")):
            raise ValueError("Destino debe ser .csv o .txt")
        return valor

    @classmethod
    def resolver_local(cls, uri: UriParseResult, directorio_base: Path) -> Path:
        normalized = uri.ruta.replace("\\", "/")
        base_resolved = directorio_base.resolve()
        dest_resolved = (base_resolved / normalized).resolve()
        if not dest_resolved.is_relative_to(base_resolved):
            raise ValueError("Traversal detectado fuera del directorio base")
        dest_parts = dest_resolved.parts
        base_parts = base_resolved.parts
        if len(dest_parts) > len(base_parts) and dest_parts[len(base_parts)] == "..":
            raise ValueError("Traversal detectado fuera del directorio base")
        return dest_resolved


class StagingManager:
    _PATRON_COMPONENTE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")

    def __init__(self, directorio_base: Path):
        self._directorio_base = directorio_base
        self._salidas_creadas: dict[tuple[str, str], Path] = {}

    @classmethod
    def _validar_componente(cls, valor: str, etiqueta: str) -> str:
        """Acepta un único componente portable, nunca una ruta compuesta."""
        if not isinstance(valor, str) or not cls._PATRON_COMPONENTE.fullmatch(valor):
            raise ValueError(f"{etiqueta} inválido: debe ser un identificador atómico")
        if valor in {".", ".."}:
            raise ValueError(f"{etiqueta} inválido: debe ser un identificador atómico")
        return valor

    def crear_staging(self, id_ejecucion: str) -> Path:
        id_ejecucion = self._validar_componente(
            id_ejecucion, "identificador de ejecución"
        )
        staging_root = self._directorio_base / ".staging" / id_ejecucion
        staging_root.mkdir(parents=True, exist_ok=True)
        os.chmod(staging_root, 0o700)
        return staging_root

    def crear_staging_salida(self, id_ejecucion: str, nombre_salida: str) -> Path:
        id_ejecucion = self._validar_componente(
            id_ejecucion, "identificador de ejecución"
        )
        nombre_salida = self._validar_componente(nombre_salida, "nombre de salida")
        clave = (id_ejecucion, nombre_salida)
        if clave in self._salidas_creadas:
            raise ValueError(
                f"Salida '{nombre_salida}' ya fue creada para ejecucion '{id_ejecucion}'"
            )
        staging = self.crear_staging(id_ejecucion)
        salida_staging = staging / nombre_salida
        if salida_staging.exists():
            raise ValueError(f"Directorio de salida ya existe: {salida_staging}")
        salida_staging.mkdir(parents=False, exist_ok=False)
        os.chmod(salida_staging, 0o700)
        self._salidas_creadas[clave] = salida_staging
        return salida_staging

    def limpiar_staging(self, id_ejecucion: str) -> None:
        id_ejecucion = self._validar_componente(
            id_ejecucion, "identificador de ejecución"
        )
        staging_path = self._directorio_base / ".staging" / id_ejecucion
        if staging_path.exists():
            shutil.rmtree(staging_path)

    def verificar_permisos(self, path: Path) -> bool:
        try:
            perms = os.stat(path).st_mode & 0o777
            return perms == 0o700
        except OSError:
            return False


class CsvWriter:
    def __init__(self, path: Path, encabezado: Iterable[str]):
        self._path = path
        self._encabezado = list(encabezado)
        self._archivo: io.TextIOWrapper | None = None
        self._writer: csv.writer | None = None
        self._cerrado = False

    def _abrir(self) -> None:
        if self._archivo is None:
            # El descriptor se conserva entre llamadas y se cierra en ``cerrar``.
            self._archivo = open(  # noqa: SIM115
                self._path, "w", newline="", encoding="utf-8-sig"
            )
            writer = csv.writer(
                self._archivo,
                delimiter=",",
                quoting=csv.QUOTE_MINIMAL,
                lineterminator="\r\n",
            )
            writer.writerow(self._encabezado)
            self._writer = writer

    def escribir_fila(self, fila: Iterable[str]) -> None:
        if self._cerrado:
            raise RuntimeError("CsvWriter ya fue cerrado")
        self._abrir()
        self._writer.writerow([str(v) for v in fila])

    def escribir_filas(self, filas: Iterable[Iterable[str]]) -> None:
        for fila in filas:
            self.escribir_fila(fila)

    def cerrar(self) -> None:
        if self._cerrado:
            return
        self._cerrado = True
        if self._archivo is not None:
            with suppress(OSError, ValueError):
                self._archivo.flush()
                self._archivo.close()
            self._archivo = None
            self._writer = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.cerrar()

    @classmethod
    def escribir_desde_iterable(
        cls,
        path: Path,
        encabezado: Iterable[str],
        filas: Iterable[Iterable[str]],
    ) -> None:
        writer = cls(path, encabezado)
        try:
            writer.escribir_filas(filas)
        finally:
            writer.cerrar()


def calcular_sha256(path: Path, bloque_tamano: int = 65536) -> str:
    sha = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            bloque = f.read(bloque_tamano)
            if not bloque:
                break
            sha.update(bloque)
    return sha.hexdigest()


@dataclass
class ManifiestoPublicacion:
    archivo: str
    bytes: int
    sha256: str
    filas: int | None = None
    estado: EstadoPublicacion = EstadoPublicacion.PENDIENTE
    timestamp: str = field(
        default_factory=lambda: (
            datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        )
    )

    def a_dict(self) -> dict[str, Any]:
        return {
            "archivo": self.archivo,
            "bytes": self.bytes,
            "sha256": self.sha256,
            "filas": self.filas,
            "estado": self.estado.value,
            "timestamp": self.timestamp,
        }


class PublicacionLocal:
    def __init__(self, directorio_base: Path):
        self._directorio_base = directorio_base

    def publicar(self, archivo_staged: Path, destino: Path) -> ManifiestoPublicacion:
        sha_destino_existente = calcular_sha256(destino) if destino.exists() else None

        parcial = destino.parent / (destino.name + ".partial")
        shutil.copy2(archivo_staged, parcial)

        sha_nuevo = calcular_sha256(parcial)

        if sha_destino_existente and sha_destino_existente != sha_nuevo:
            os.remove(parcial)
            raise ValueError(
                f"Hash diff: existente={sha_destino_existente}, nuevo={sha_nuevo}"
            )

        os.replace(parcial, destino)

        return ManifiestoPublicacion(
            archivo=destino.name,
            bytes=destino.stat().st_size,
            sha256=sha_nuevo,
            estado=EstadoPublicacion.PUBLICADO,
        )


class PublicacionSftp:
    def __init__(
        self,
        host: str,
        puerto: int,
        usuario: str,
        clave_privada: Path | None = None,
        clave_privada_contenido: str | None = None,
        password: str | None = None,
        passphrase: str | None = None,
        timeout: float = 30.0,
    ):
        if not HAS_PARAMIKO:
            raise ImportError("paramiko no esta instalado")
        self._host = host
        self._puerto = puerto
        self._usuario = usuario
        modos = (bool(password), bool(clave_privada), bool(clave_privada_contenido))
        if sum(modos) != 1:
            raise ValueError(
                "PublicacionSftp requiere exactamente password, clave_privada "
                "o clave_privada_contenido"
            )
        self._clave_privada = clave_privada
        self._clave_privada_contenido = clave_privada_contenido
        self._password = password
        self._passphrase = passphrase
        self._timeout = timeout
        self._cliente: paramiko.SSHClient | None = None
        self._sftp: paramiko.SFTPClient | None = None

    @staticmethod
    def _cargar_clave_desde_contenido(
        contenido: str,
        passphrase: str | None,
    ):
        """Detecta Ed25519/ECDSA/RSA sin crear archivos temporales."""
        candidatos = [
            paramiko.Ed25519Key,
            paramiko.ECDSAKey,
            paramiko.RSAKey,
        ]
        dss = getattr(paramiko, "DSSKey", None)
        if dss is not None:
            candidatos.append(dss)

        errores: list[Exception] = []
        for clase in candidatos:
            try:
                return clase.from_private_key(
                    io.StringIO(contenido),
                    password=passphrase,
                )
            except paramiko.PasswordRequiredException as exc:
                if passphrase is None:
                    raise ValueError(
                        "La clave privada está cifrada y requiere passphrase"
                    ) from exc
                errores.append(exc)
            except (paramiko.SSHException, ValueError) as exc:
                errores.append(exc)

        raise ValueError(
            "Formato de clave privada SFTP no soportado o passphrase incorrecta"
        ) from (errores[-1] if errores else None)

    def _crear_cliente(self) -> paramiko.SSHClient:
        """Crea un cliente que rechaza hosts no registrados.

        ``load_system_host_keys`` carga ``~/.ssh/known_hosts`` y las fuentes
        configuradas por Paramiko. ``RejectPolicy`` evita aceptar una clave
        distinta o desconocida de forma silenciosa.
        """
        cliente = paramiko.SSHClient()  # type: ignore
        cliente.load_system_host_keys()
        cliente.set_missing_host_key_policy(paramiko.RejectPolicy())
        return cliente

    def conectar(self) -> None:
        self._cliente = self._crear_cliente()
        try:
            if self._clave_privada_contenido:
                clave_memoria = self._cargar_clave_desde_contenido(
                    self._clave_privada_contenido,
                    self._passphrase,
                )
                self._cliente.connect(
                    self._host,
                    port=self._puerto,
                    username=self._usuario,
                    pkey=clave_memoria,
                    timeout=self._timeout,
                    look_for_keys=False,
                    allow_agent=False,
                )
            elif self._clave_privada:
                # ``key_filename`` delega a Paramiko la detección del formato
                # OpenSSH/RSA/ECDSA/Ed25519. Cargar RSAKey manualmente rompería
                # claves modernas como la utilizada por este SFTP.
                clave = self._clave_privada.expanduser()
                if not clave.is_file():
                    raise FileNotFoundError(
                        f"Clave privada SFTP no encontrada: {clave}"
                    )
                if clave.stat().st_mode & 0o077:
                    raise PermissionError(
                        "La clave privada SFTP debe tener permisos 600 o más restrictivos"
                    )
                self._cliente.connect(
                    self._host,
                    port=self._puerto,
                    username=self._usuario,
                    key_filename=str(clave),
                    passphrase=self._passphrase,
                    timeout=self._timeout,
                    look_for_keys=False,
                    allow_agent=False,
                )
            else:
                self._cliente.connect(
                    self._host,
                    port=self._puerto,
                    username=self._usuario,
                    password=self._password,
                    timeout=self._timeout,
                    look_for_keys=False,
                    allow_agent=False,
                )
            self._sftp = self._cliente.open_sftp()
        except Exception:
            self._cliente.close()
            raise

    def cerrar(self) -> None:
        if self._sftp:
            # El cierre es best-effort y no debe ocultar el error principal.
            with suppress(Exception):
                self._sftp.close()
            self._sftp = None
        if self._cliente:
            with suppress(Exception):
                self._cliente.close()
            self._cliente = None

    @staticmethod
    def _es_posix_rename_no_soportado(excepcion: Exception) -> bool:
        """Distingue ausencia de la extensión de un fallo real de permisos.

        Paramiko expone ``posix_rename`` aunque el servidor no anuncie la
        extensión OpenSSH. En ese caso el servidor suele responder
        ``Operation unsupported``. Otros errores, por ejemplo permisos o disco
        lleno, deben propagarse y no activar un fallback que podría ocultarlos.
        """
        if isinstance(excepcion, AttributeError):
            return True
        mensaje = str(excepcion).casefold()
        return "unsupported" in mensaje or "not supported" in mensaje

    @staticmethod
    def _ruta_remota_existe(sftp: Any, ruta: str) -> bool:
        """Consulta existencia sin convertir otros errores en "no existe"."""
        try:
            sftp.stat(ruta)
            return True
        except FileNotFoundError:
            return False
        except OSError as excepcion:
            mensaje = str(excepcion).casefold()
            if getattr(excepcion, "errno", None) == 2 or "no such file" in mensaje:
                return False
            raise

    @classmethod
    def _promover_parcial(
        cls,
        sftp: Any,
        parcial_remoto: str,
        destino_remoto: str,
    ) -> None:
        """Promueve el parcial reemplazando de forma segura el destino previo.

        OpenSSH ofrece ``posix-rename@openssh.com``, equivalente remoto de
        ``os.replace``: el destino anterior se reemplaza atómicamente. Si un
        servidor no soporta esa extensión, se usa un protocolo con backup y
        rollback para no perder el archivo anterior ante un fallo intermedio.
        """
        try:
            sftp.posix_rename(parcial_remoto, destino_remoto)
            return
        except Exception as excepcion:
            if not cls._es_posix_rename_no_soportado(excepcion):
                raise

        if not cls._ruta_remota_existe(sftp, destino_remoto):
            sftp.rename(parcial_remoto, destino_remoto)
            return

        backup_remoto = f"{destino_remoto}.backup-{uuid4().hex}"
        sftp.rename(destino_remoto, backup_remoto)
        try:
            sftp.rename(parcial_remoto, destino_remoto)
        except Exception:
            try:
                sftp.rename(backup_remoto, destino_remoto)
            except Exception as rollback_error:
                raise RuntimeError(
                    "Falló la promoción SFTP y también el rollback del archivo anterior"
                ) from rollback_error
            raise
        else:
            # El destino nuevo ya está publicado. Un fallo al borrar el backup
            # no invalida el archivo final, por lo que la limpieza es best-effort.
            with suppress(Exception):
                sftp.remove(backup_remoto)

    def publicar(
        self,
        archivo_local: Path,
        ruta_remota: Path,
    ) -> ManifiestoPublicacion:
        if not self._sftp:
            raise RuntimeError("No conectado. Llama a conectar() primero.")

        sftp = self._sftp
        sha_local = calcular_sha256(archivo_local)
        bytes_local = archivo_local.stat().st_size
        nombre_archivo = archivo_local.name

        parcial_remoto = str(ruta_remota) + ".partial"

        try:
            sftp.put(
                str(archivo_local),
                parcial_remoto,
                confirm=True,
            )
            self._promover_parcial(sftp, parcial_remoto, str(ruta_remota))
        except Exception as e:
            # El rollback remoto puede fallar si el servidor nunca creó el parcial.
            with suppress(Exception):
                sftp.remove(parcial_remoto)
            self.cerrar()
            raise RuntimeError(f"Error en publicacion SFTP: {e}") from e

        return ManifiestoPublicacion(
            archivo=nombre_archivo,
            bytes=bytes_local,
            sha256=sha_local,
            estado=EstadoPublicacion.PUBLICADO,
        )

    def __enter__(self) -> Self:
        self.conectar()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.cerrar()


try:
    from pyspark.sql import DataFrame, SparkSession

    HAS_PYSPARK = True
except ImportError:
    HAS_PYSPARK = False
    DataFrame = None  # type: ignore
    SparkSession = None  # type: ignore


class AdaptadorSparkCsv:
    def __init__(self, spark=None):
        if not HAS_PYSPARK:
            raise ImportError("pyspark no esta instalado")
        self._spark = spark or SparkSession.getActiveSession()  # type: ignore

    def escribir_csv_streaming(
        self,
        df,
        staging_path: Path,
        encabezado: tuple[str, ...],
    ) -> Path:
        if not self._spark:
            raise RuntimeError("No hay Spark activo")

        salida_staging = staging_path / "output"
        salida_staging.mkdir(parents=True, exist_ok=True)

        writer = (
            df.writeStream.format("csv")
            .option("header", "true")
            .option("delimiter", ",")
            .option("encoding", "UTF-8")
            .option("path", str(salida_staging))
            .option("checkpointLocation", str(staging_path / "_checkpoint"))
            .trigger(once=True)
            .outputMode("append")
        )
        query = writer.start()
        query.awaitTermination(timeout=300)
        query.stop()

        return self._promover_part_file(salida_staging)

    def _promover_part_file(self, staging_path: Path) -> Path:
        part_files = sorted(staging_path.glob("part-*.csv"))
        if not part_files:
            raise RuntimeError(f"No se encontro part-file en {staging_path}")
        part_file = part_files[0]
        destino = staging_path.parent / part_file.name
        shutil.move(str(part_file), str(destino))
        return destino
