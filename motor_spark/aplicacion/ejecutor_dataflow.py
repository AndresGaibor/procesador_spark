"""Orquestación del modo ``--dataflow-script``.

La capa de aplicación coordina etapas puras (normalizar, tokenizar, parsear,
validar y compilar) antes de crear Spark. Solo un plan completamente válido
puede abrir JDBC, leer archivos o publicar resultados. La ejecución concreta se
delega a ``EjecutorPlanDataflow`` para evitar dos implementaciones divergentes.
"""

from __future__ import annotations

import hashlib
import json
import traceback
from pathlib import Path
from typing import Any

from motor_spark.compartido.eventos_consola import emitir
from motor_spark.conexiones.cargador import (
    cargar_catalogo,
    cargar_catalogo_contenido,
)
from motor_spark.conexiones.modelos import CatalogoConexiones
from motor_spark.conexiones.secretos import AdministradorSecretos
from motor_spark.configuracion.argumentos import ArgumentosDataflowScript
from motor_spark.dataflow_script import (
    ErrorDataflow,
    lexer,
    normalizador,
    parser,
    validador,
)
from motor_spark.dataflow_script.limites import LIMITE_TAMANIO_ARCHIVO
from motor_spark.infraestructura.resultados.escritor_json import guardar_resultado
from motor_spark.plan import PlanDataflow, compilar, serializar_plan
from motor_spark.plan.ejecutor import EjecutorPlanDataflow, ErrorEjecucionPlan


class ErrorDataflowInesperado(RuntimeError):
    """Marca un fallo fuera de los errores esperados del compilador."""


def _error(
    mensaje: str,
    codigo: str,
    *,
    ayuda: str | None = None,
) -> ErrorDataflow:
    """Crea errores de etapa con el mismo contrato JSON del resto del motor."""
    return ErrorDataflow(
        mensaje=mensaje,
        ubicacion=None,
        codigo=codigo,
        ayuda=ayuda,
    )


def _leer_script(
    argumentos: ArgumentosDataflowScript,
) -> tuple[str, list[ErrorDataflow]]:
    """Resuelve el único origen permitido y aplica el mismo límite en bytes.

    El límite se calcula sobre UTF-8, no sobre caracteres, porque ese es el volumen
    real que terminaría en memoria, logs o transporte entre Talend y el proceso.
    """
    if argumentos.dataflow_script and argumentos.dataflow_script_contenido is not None:
        return "", [
            _error(
                "No se puede enviar ruta y contenido de script simultáneamente",
                "DFS_SCRIPT_SOURCE_CONFLICT",
            )
        ]

    if argumentos.dataflow_script_contenido is not None:
        contenido = argumentos.dataflow_script_contenido
        contenido_bytes = contenido.encode("utf-8")
        if not contenido.strip():
            return "", [
                _error("El contenido del script está vacío", "DFS_SCRIPT_EMPTY")
            ]
        if len(contenido_bytes) > LIMITE_TAMANIO_ARCHIVO:
            return "", [
                _error(
                    f"Script excede limite de tamanio: {LIMITE_TAMANIO_ARCHIVO} bytes",
                    "DFS_FILE_TOO_LARGE",
                )
            ]
        return contenido, []

    ruta = argumentos.dataflow_script
    if not ruta:
        return "", [
            _error("No se recibió un origen de script", "DFS_SCRIPT_SOURCE_MISSING")
        ]

    archivo = Path(ruta)
    try:
        if not archivo.is_file():
            return "", [
                _error(
                    f"Archivo no encontrado: {ruta}",
                    "DFS_FILE_NOT_FOUND",
                )
            ]
        contenido_bytes = archivo.read_bytes()
    except OSError as excepcion:
        return "", [
            _error(
                f"Error leyendo script: {excepcion}",
                "DFS_READ_ERROR",
            )
        ]

    if len(contenido_bytes) > LIMITE_TAMANIO_ARCHIVO:
        return "", [
            _error(
                f"Archivo excede limite de tamanio: {LIMITE_TAMANIO_ARCHIVO} bytes",
                "DFS_FILE_TOO_LARGE",
            )
        ]
    try:
        contenido = contenido_bytes.decode("utf-8")
    except UnicodeDecodeError as excepcion:
        return "", [
            _error(
                f"Archivo no es UTF-8 valido: {excepcion}",
                "DFS_ENCODING_INVALID",
            )
        ]
    if not contenido.strip():
        return "", [_error("El contenido del script está vacío", "DFS_SCRIPT_EMPTY")]
    return contenido, []


def _metadatos_script(
    argumentos: ArgumentosDataflowScript,
    contenido: str | None = None,
) -> dict[str, str | None]:
    """Devuelve identidad auditable sin incluir el script ni consultas sensibles."""
    return {
        "origen_script": argumentos.origen_script,
        "referencia_script": (
            argumentos.dataflow_script
            if argumentos.origen_script == "archivo"
            else None
        ),
        "hash_script": (
            hashlib.sha256(contenido.encode("utf-8")).hexdigest()
            if contenido is not None
            else None
        ),
    }


def _cargar_catalogo_argumentos(
    argumentos: ArgumentosDataflowScript,
) -> CatalogoConexiones:
    """Resuelve exactamente un origen del catálogo sin persistir JSON inline.

    La comprobación también protege llamadas programáticas que construyan el
    dataclass directamente y, por tanto, no atraviesen la exclusión de argparse.
    """
    if argumentos.conexiones and argumentos.conexiones_contenido is not None:
        raise ValueError(
            "No se puede enviar ruta y contenido de conexiones simultáneamente"
        )
    if argumentos.conexiones_contenido is not None:
        return cargar_catalogo_contenido(argumentos.conexiones_contenido)
    if argumentos.conexiones:
        return cargar_catalogo(argumentos.conexiones)
    raise ValueError("No se recibió --conexiones ni --conexiones-contenido")


def _normalizar_script(contenido: str) -> tuple[str, list[ErrorDataflow]]:
    """Normaliza conservando errores estructurados incluso ante un bug interno."""
    try:
        return normalizador.normalizar(contenido)
    except (TypeError, ValueError, RuntimeError) as excepcion:
        return "", [
            _error(
                f"Error en normalizacion: {excepcion}",
                "DFS_NORMALIZE_ERROR",
            )
        ]


def _lexear(contenido: str) -> tuple[list[Any], list[ErrorDataflow]]:
    """Convierte texto normalizado en tokens con posiciones de origen."""
    try:
        return lexer.tokenizar(contenido)
    except (TypeError, ValueError, RuntimeError) as excepcion:
        return [], [
            _error(
                f"Error en lexing: {excepcion}",
                "DFS_LEX_ERROR",
            )
        ]


def _parsear(tokens: list[Any]) -> tuple[Any, list[ErrorDataflow]]:
    """Construye el AST sin ejecutar ninguna expresión del script."""
    try:
        return parser.parsear(tokens)
    except (TypeError, ValueError, RuntimeError) as excepcion:
        return None, [
            _error(
                f"Error en parsing: {excepcion}",
                "DFS_PARSE_ERROR",
            )
        ]


def _validar(programa: Any) -> list[ErrorDataflow]:
    """Ejecuta validaciones semánticas antes de compilar el plan."""
    try:
        return validador.validar_semantico(programa)
    except (TypeError, ValueError, RuntimeError) as excepcion:
        return [
            _error(
                f"Error en validacion: {excepcion}",
                "DFS_VALIDATE_ERROR",
            )
        ]


def _compilar_plan(
    programa: Any,
) -> tuple[PlanDataflow | None, list[ErrorDataflow]]:
    """Compila y convierte incompatibilidades fail-closed en errores públicos."""
    try:
        plan = compilar(programa)
    except (TypeError, ValueError, RuntimeError) as excepcion:
        return None, [
            _error(
                f"Error compilando plan: {excepcion}",
                "DFS_COMPILE_ERROR",
            )
        ]

    incompatibilidades = tuple(plan.metadata.get("errores", ()))
    if incompatibilidades:
        return None, [
            _error(
                str(mensaje),
                "DFS_COMPILE_UNSUPPORTED",
                ayuda=(
                    "La construcción debe implementarse explícitamente antes "
                    "de ejecutar el Dataflow en Spark"
                ),
            )
            for mensaje in incompatibilidades
        ]
    return plan, []


def _serializar_hash(plan: PlanDataflow) -> str:
    """Mantiene un alias estable para consumidores de la versión inicial."""
    return plan.hash_determista()


def _construir_resultado_error_dataflow(
    argumentos: ArgumentosDataflowScript,
    errores: list[ErrorDataflow],
) -> dict[str, Any]:
    """Construye un resultado serializable sin incluir catálogos ni secretos."""
    return {
        "estado": "ERROR",
        "ejecucion_id": argumentos.ejecucion_id,
        **_metadatos_script(
            argumentos,
            argumentos.dataflow_script_contenido,
        ),
        "errores": [
            {
                "mensaje": error.mensaje,
                "codigo": error.codigo,
                "ubicacion": (str(error.ubicacion) if error.ubicacion else None),
                "ayuda": error.ayuda,
            }
            for error in errores
        ],
    }


def _construir_resultado_compilado(
    argumentos: ArgumentosDataflowScript,
    plan: PlanDataflow,
    hash_plan: str,
) -> dict[str, Any]:
    """Expone el plan completo para auditoría, no una versión truncada."""
    return {
        "estado": "COMPILADO",
        "ejecucion_id": argumentos.ejecucion_id,
        **_metadatos_script(
            argumentos,
            argumentos.dataflow_script_contenido,
        ),
        "hash": hash_plan,
        "operaciones": [
            operacion.model_dump(mode="json") for operacion in plan.operaciones
        ],
        "tabla_resultado": plan.tabla_resultado,
    }


def _escribir_plan_salida(plan: PlanDataflow, ruta: str) -> None:
    """Persiste el plan de forma atómica para impedir JSON parcialmente escrito."""
    destino = Path(ruta)
    destino.parent.mkdir(parents=True, exist_ok=True)
    temporal = destino.with_suffix(destino.suffix + ".tmp")
    temporal.write_text(
        serializar_plan(plan),
        encoding="utf-8",
    )
    temporal.replace(destino)


def _verificar_operaciones_ejecutables(
    plan: PlanDataflow,
) -> list[ErrorDataflow]:
    """Compatibilidad: la unión tipada ya garantiza operaciones conocidas."""
    return []


def _ejecutar_operaciones(
    contexto: Any,
    plan: PlanDataflow,
    catalogo: CatalogoConexiones,
    secretos: AdministradorSecretos,
    ejecucion_id: str = "dataflow",
) -> tuple[dict[str, Any], list[ErrorDataflow]]:
    """Adaptador de compatibilidad hacia el ejecutor único del plan."""
    try:
        ejecutor = EjecutorPlanDataflow(
            spark=contexto._spark,
            catalogo=catalogo,
            secretos=secretos,
            ejecucion_id=ejecucion_id,
        )
        return ejecutor.ejecutar(plan), []
    except ErrorEjecucionPlan as excepcion:
        return {}, [
            _error(
                str(excepcion),
                "DFS_EXECUTION_ERROR",
            )
        ]


def _crear_sesion_dataflow(nombre: str, ejecucion_id: str) -> Any:
    """Crea Spark con semántica estricta y reproducible para Dataflows."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName(f"dataflow-{nombre}-{ejecucion_id}")
        .config("spark.sql.caseSensitive", "true")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def _guardar_y_emitir(
    argumentos: ArgumentosDataflowScript,
    resultado: dict[str, Any],
    *,
    error: bool = False,
) -> None:
    """Publica el mismo resultado en archivo y consola para Talend/TMC."""
    guardar_resultado(argumentos.resultado, resultado)
    emitir(
        "RESULTADO_MOTOR="
        + json.dumps(resultado, ensure_ascii=False, separators=(",", ":")),
        error=error,
    )


def ejecutar_dataflow(argumentos: ArgumentosDataflowScript) -> int:
    """Ejecuta el pipeline completo y garantiza ``SparkSession.stop()``."""
    spark: Any | None = None
    secretos = AdministradorSecretos(dict(argumentos.secretos))

    try:
        contenido, errores = _leer_script(argumentos)
        if errores:
            resultado_error = _construir_resultado_error_dataflow(argumentos, errores)
            resultado_error.update(_metadatos_script(argumentos, contenido or None))
            _guardar_y_emitir(argumentos, resultado_error, error=True)
            return 1

        normalizado, errores = _normalizar_script(contenido)
        if errores:
            _guardar_y_emitir(
                argumentos,
                _construir_resultado_error_dataflow(argumentos, errores),
                error=True,
            )
            return 1

        tokens, errores = _lexear(normalizado)
        if errores:
            _guardar_y_emitir(
                argumentos,
                _construir_resultado_error_dataflow(argumentos, errores),
                error=True,
            )
            return 1

        programa, errores = _parsear(tokens)
        if errores or programa is None:
            errores = errores or [
                _error(
                    "El parser no produjo un programa",
                    "DFS_PARSE_FAILED",
                )
            ]
            _guardar_y_emitir(
                argumentos,
                _construir_resultado_error_dataflow(argumentos, errores),
                error=True,
            )
            return 1

        errores = _validar(programa)
        if errores:
            _guardar_y_emitir(
                argumentos,
                _construir_resultado_error_dataflow(argumentos, errores),
                error=True,
            )
            return 1

        plan, errores = _compilar_plan(programa)
        if errores or plan is None:
            errores = errores or [
                _error(
                    "El compilador no produjo un plan",
                    "DFS_COMPILE_FAILED",
                )
            ]
            _guardar_y_emitir(
                argumentos,
                _construir_resultado_error_dataflow(argumentos, errores),
                error=True,
            )
            return 1

        hash_plan = _serializar_hash(plan)
        if argumentos.solo_compilar:
            if not argumentos.plan_salida:
                resultado = _construir_resultado_error_dataflow(
                    argumentos,
                    [
                        _error(
                            "--solo-compilar requiere --plan-salida",
                            "DFS_SOLO_COMPILAR_REQUIRES_PLAN",
                        )
                    ],
                )
                _guardar_y_emitir(argumentos, resultado, error=True)
                return 1

            _escribir_plan_salida(plan, argumentos.plan_salida)
            _guardar_y_emitir(
                argumentos,
                {
                    **_construir_resultado_compilado(
                        argumentos,
                        plan,
                        hash_plan,
                    ),
                    **_metadatos_script(argumentos, contenido),
                },
            )
            return 0

        # El catálogo se carga después de compilar: un script inválido nunca
        # provoca lecturas de configuración ni creación de recursos externos.
        catalogo = _cargar_catalogo_argumentos(argumentos)
        nombre_aplicacion = (
            Path(argumentos.dataflow_script).stem
            if argumentos.dataflow_script
            else "contenido-parametro"
        )
        spark = _crear_sesion_dataflow(
            nombre_aplicacion,
            argumentos.ejecucion_id,
        )
        ejecutor = EjecutorPlanDataflow(
            spark=spark,
            catalogo=catalogo,
            secretos=secretos,
            ejecucion_id=argumentos.ejecucion_id,
        )
        metricas = ejecutor.ejecutar(plan)

        resultado = {
            "estado": "COMPLETADO",
            "ejecucion_id": argumentos.ejecucion_id,
            **_metadatos_script(argumentos, contenido),
            "hash": hash_plan,
            "operaciones": len(plan.operaciones),
            **metricas,
        }
        _guardar_y_emitir(argumentos, resultado)
        return 0

    except Exception as excepcion:  # noqa: BLE001
        # Este es el límite del proceso: captura también defectos inesperados para
        # producir el contrato JSON. Los valores secretos conocidos se redactan.
        mensaje_seguro = secretos.redactar_texto(str(excepcion))
        resultado = _construir_resultado_error_dataflow(
            argumentos,
            [
                _error(
                    mensaje_seguro,
                    type(excepcion).__name__.upper(),
                )
            ],
        )
        _guardar_y_emitir(argumentos, resultado, error=True)
        traceback.print_exc()
        return 1
    finally:
        if spark is not None:
            # stop() pertenece a esta capa porque aquí se creó la sesión. El
            # ejecutor del plan recibe una sesión prestada y nunca debe cerrarla.
            spark.stop()
