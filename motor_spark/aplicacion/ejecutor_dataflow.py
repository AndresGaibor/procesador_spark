from __future__ import annotations

import json
import traceback
from pathlib import Path
from typing import Any

from motor_spark.conexiones.cargador import cargar_catalogo
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
from motor_spark.dataflow_script.errores import SourceLocation, SourceSpan
from motor_spark.dataflow_script.ejecucion import ContextoEjecucionDataflow
from motor_spark.dataflow_script.jdbc import leer_jdbc
from motor_spark.dataflow_script.limites import LIMITE_TAMANIO_ARCHIVO
from motor_spark.plan import PlanDataflow, compilar, serializar_plan
from motor_spark.plan.modelos import (
    Agregar,
    CargarCsv,
    CargarLocal,
    Concatenar,
    EliminarTabla,
    Filtrar,
    LeerJdbc,
    Operacion,
    Proyectar,
    Publicar,
    TipoOperacion,
    Unir,
)
from motor_spark.infraestructura.resultados.escritor_json import guardar_resultado


OPERACIONES_EJECUTABLES: frozenset[TipoOperacion] = frozenset({
    TipoOperacion.LEER_JDBC,
    TipoOperacion.PROYECTAR,
    TipoOperacion.FILTRAR,
    TipoOperacion.CONCATENAR,
    TipoOperacion.UNIR,
    TipoOperacion.AGREGAR,
    TipoOperacion.ELIMINAR_TABLA,
})


class ErrorDataflowInesperado(Exception):
    pass


def _leer_script(ruta: str) -> tuple[str, list[ErrorDataflow]]:
    errores: list[ErrorDataflow] = []
    try:
        ruta_path = Path(ruta)
        if not ruta_path.exists():
            errores.append(ErrorDataflow(
                mensaje=f"Archivo no encontrado: {ruta}",
                ubicacion=None,
                codigo="DFS_FILE_NOT_FOUND",
            ))
            return "", errores

        contenido_bytes = ruta_path.read_bytes()
        if len(contenido_bytes) > LIMITE_TAMANIO_ARCHIVO:
            errores.append(ErrorDataflow(
                mensaje=f"Archivo excede limite de tamanio: {LIMITE_TAMANIO_ARCHIVO} bytes",
                ubicacion=None,
                codigo="DFS_FILE_TOO_LARGE",
            ))
            return "", errores

        try:
            contenido = contenido_bytes.decode("utf-8")
        except UnicodeDecodeError as e:
            errores.append(ErrorDataflow(
                mensaje=f"Archivo no es UTF-8 valido: {str(e)}",
                ubicacion=None,
                codigo="DFS_ENCODING_INVALID",
            ))
            return "", errores

        return contenido, errores
    except Exception as ex:
        errores.append(ErrorDataflow(
            mensaje=f"Error leyendo script: {str(ex)}",
            ubicacion=None,
            codigo="DFS_READ_ERROR",
        ))
        return "", errores


def _normalizar_script(contenido: str) -> tuple[str, list[ErrorDataflow]]:
    try:
        return normalizador.normalizar(contenido)
    except Exception as e:
        return "", [ErrorDataflow(
            mensaje=f"Error en normalizacion: {str(e)}",
            ubicacion=None,
            codigo="DFS_NORMALIZE_ERROR",
        )]


def _lexear(contenido: str) -> tuple[list[Any], list[ErrorDataflow]]:
    try:
        return lexer.tokenizar(contenido)
    except Exception as e:
        return [], [ErrorDataflow(
            mensaje=f"Error en lexing: {str(e)}",
            ubicacion=None,
            codigo="DFS_LEX_ERROR",
        )]


def _parsear(tokens: list[Any]) -> tuple[Any, list[ErrorDataflow]]:
    try:
        return parser.parsear(tokens)
    except Exception as e:
        return None, [ErrorDataflow(
            mensaje=f"Error en parsing: {str(e)}",
            ubicacion=None,
            codigo="DFS_PARSE_ERROR",
        )]


def _validar(programa: Any) -> list[ErrorDataflow]:
    try:
        return validador.validar_semantico(programa)
    except Exception as e:
        return [ErrorDataflow(
            mensaje=f"Error en validacion: {str(e)}",
            ubicacion=None,
            codigo="DFS_VALIDATE_ERROR",
        )]


def _compilar_plan(programa: Any) -> tuple[PlanDataflow | None, list[ErrorDataflow]]:
    try:
        plan = compilar(programa)
        return plan, []
    except Exception as e:
        return None, [ErrorDataflow(
            mensaje=f"Error compilando plan: {str(e)}",
            ubicacion=None,
            codigo="DFS_COMPILE_ERROR",
        )]


def _serializar_hash(plan: PlanDataflow) -> str:
    return plan.hash_determista()


def _construir_resultado_error_dataflow(
    argumentos: ArgumentosDataflowScript,
    errores: list[ErrorDataflow],
) -> dict[str, Any]:
    errores_sanitizados = []
    for err in errores:
        errores_sanitizados.append({
            "mensaje": err.mensaje,
            "codigo": err.codigo,
            "ubicacion": str(err.ubicacion) if err.ubicacion else None,
            "ayuda": err.ayuda,
        })

    return {
        "estado": "ERROR",
        "ejecucion_id": argumentos.ejecucion_id,
        "dataflow_script": argumentos.dataflow_script,
        "errores": errores_sanitizados,
    }


def _construir_resultado_compilado(
    argumentos: ArgumentosDataflowScript,
    plan: PlanDataflow,
    hash_plan: str,
) -> dict[str, Any]:
    operaciones = []
    for op in plan.operaciones:
        op_dict: dict[str, Any] = {
            "id": op.id,
            "tipo": op.tipo.value,
        }
        if isinstance(op, LeerJdbc):
            op_dict["conexion_nombre"] = op.conexion_nombre
            op_dict["esquema"] = op.esquema
            op_dict["tabla"] = op.tabla
        elif isinstance(op, Proyectar):
            op_dict["tabla_origen"] = op.tabla_origen
            op_dict["campos"] = list(op.campos)
        elif isinstance(op, Filtrar):
            op_dict["tabla_origen"] = op.tabla_origen
            op_dict["condicion"] = op.condicion
        elif isinstance(op, Concatenar):
            op_dict["tabla_objetivo"] = op.tabla_objetivo
            op_dict["tabla_origen"] = op.tabla_origen
        elif isinstance(op, Unir):
            op_dict["tabla_izquierda"] = op.tabla_izquierda
            op_dict["tabla_derecha"] = op.tabla_derecha
            op_dict["condicion_on"] = op.condicion_on
        elif isinstance(op, Agregar):
            op_dict["tabla_origen"] = op.tabla_origen
            op_dict["grupo_por"] = list(op.grupo_por)
            op_dict["funciones"] = list(op.funciones)
        elif isinstance(op, EliminarTabla):
            op_dict["nombre"] = op.nombre
        elif isinstance(op, Publicar):
            op_dict["tabla_origen"] = op.tabla_origen
            op_dict["destino"] = op.destino
        elif isinstance(op, CargarCsv):
            op_dict["ruta"] = op.ruta
        elif isinstance(op, CargarLocal):
            op_dict["ruta"] = op.ruta
            op_dict["nombre_tabla"] = op.nombre_tabla
        operaciones.append(op_dict)

    return {
        "estado": "COMPILADO",
        "ejecucion_id": argumentos.ejecucion_id,
        "dataflow_script": argumentos.dataflow_script,
        "hash": hash_plan,
        "operaciones": operaciones,
        "tabla_resultado": plan.tabla_resultado,
    }


def _escribir_plan_salida(plan: PlanDataflow, ruta: str) -> None:
    import json
    plan_path = Path(ruta)
    plan_path.parent.mkdir(parents=True, exist_ok=True)
    serializado = serializar_plan(plan)
    temporal = plan_path.with_suffix(".tmp")
    temporal.write_text(serializado, encoding="utf-8")
    temporal.replace(plan_path)


def _verificar_operaciones_ejecutables(plan: PlanDataflow) -> list[ErrorDataflow]:
    errores: list[ErrorDataflow] = []
    for op in plan.operaciones:
        if op.tipo not in OPERACIONES_EJECUTABLES:
            errores.append(ErrorDataflow(
                mensaje=f"Operacion no ejecutable: {op.tipo.value} (id={op.id})",
                ubicacion=None,
                codigo="DFS_OPERATION_NOT_EXECUTABLE",
                ayuda=f"Solo operaciones en {sorted(o.value for o in OPERACIONES_EJECUTABLES)} son ejecutables",
            ))
    return errores


def _ejecutar_operaciones(
    contexto: ContextoEjecucionDataflow,
    plan: PlanDataflow,
    catalogo: CatalogoConexiones,
    secretos: AdministradorSecretos,
) -> tuple[dict[str, Any], list[ErrorDataflow]]:
    resultados: dict[str, Any] = {}
    errores: list[ErrorDataflow] = []

    for op in plan.operaciones:
        if op.tipo == TipoOperacion.LEER_JDBC:
            if not isinstance(op, LeerJdbc):
                continue
            try:
                df = leer_jdbc(
                    spark=contexto._spark,
                    nombre_conexion=op.conexion_nombre,
                    tabla=op.tabla,
                    columnas=list(op.campos) if op.campos else [],
                    catalogo=catalogo,
                    secretos=secretos,
                )
                contexto.registrar_dataframe(op.id, df)
            except Exception as e:
                errores.append(ErrorDataflow(
                    mensaje=f"Error ejecutando LEER_JDBC: {str(e)}",
                    ubicacion=None,
                    codigo="DFS_EXEC_LEER_JDBC_ERROR",
                ))

        elif op.tipo == TipoOperacion.PROYECTAR:
            if not isinstance(op, Proyectar):
                continue
            df_origen = contexto.obtener_dataframe(op.tabla_origen)
            if df_origen is None:
                errores.append(ErrorDataflow(
                    mensaje=f"Tabla no encontrada para PROYECTAR: {op.tabla_origen}",
                    ubicacion=None,
                    codigo="DFS_EXEC_PROYECTAR_TABLE_NOT_FOUND",
                ))
                continue
            try:
                from pyspark.sql import functions as F
                df_resultado = df_origen.select(*[F.col(c) for c in op.campos])
                nombre_tabla = op.alias or op.tabla_origen
                contexto.registrar_dataframe(nombre_tabla, df_resultado)
            except Exception as e:
                errores.append(ErrorDataflow(
                    mensaje=f"Error ejecutando PROYECTAR: {str(e)}",
                    ubicacion=None,
                    codigo="DFS_EXEC_PROYECTAR_ERROR",
                ))

        elif op.tipo == TipoOperacion.FILTRAR:
            if not isinstance(op, Filtrar):
                continue
            df_origen = contexto.obtener_dataframe(op.tabla_origen)
            if df_origen is None:
                errores.append(ErrorDataflow(
                    mensaje=f"Tabla no encontrada para FILTRAR: {op.tabla_origen}",
                    ubicacion=None,
                    codigo="DFS_EXEC_FILTRAR_TABLE_NOT_FOUND",
                ))
                continue
            try:
                from pyspark.sql import functions as F
                df_resultado = df_origen.filter(op.condicion)
                contexto.registrar_dataframe(op.tabla_origen, df_resultado)
            except Exception as e:
                errores.append(ErrorDataflow(
                    mensaje=f"Error ejecutando FILTRAR: {str(e)}",
                    ubicacion=None,
                    codigo="DFS_EXEC_FILTRAR_ERROR",
                ))

        elif op.tipo == TipoOperacion.AGREGAR:
            if not isinstance(op, Agregar):
                continue
            df_origen = contexto.obtener_dataframe(op.tabla_origen)
            if df_origen is None:
                errores.append(ErrorDataflow(
                    mensaje=f"Tabla no encontrada para AGREGAR: {op.tabla_origen}",
                    ubicacion=None,
                    codigo="DFS_EXEC_AGREGAR_TABLE_NOT_FOUND",
                ))
                continue
            try:
                from pyspark.sql import functions as F
                exprs_grupo = [F.col(c) for c in op.grupo_por]
                funcs = []
                for f_str in op.funciones:
                    if f_str == "COUNT":
                        funcs.append(F.count("*"))
                    elif f_str == "SUM":
                        funcs.append(F.sum("*"))
                    elif f_str == "AVG":
                        funcs.append(F.avg("*"))
                    elif f_str == "MIN":
                        funcs.append(F.min("*"))
                    elif f_str == "MAX":
                        funcs.append(F.max("*"))
                df_resultado = df_origen.groupBy(*exprs_grupo).agg(*funcs)
                contexto.registrar_dataframe(op.tabla_origen, df_resultado)
            except Exception as e:
                errores.append(ErrorDataflow(
                    mensaje=f"Error ejecutando AGREGAR: {str(e)}",
                    ubicacion=None,
                    codigo="DFS_EXEC_AGREGAR_ERROR",
                ))

        elif op.tipo == TipoOperacion.UNIR:
            if not isinstance(op, Unir):
                continue
            df_izq = contexto.obtener_dataframe(op.tabla_izquierda)
            df_der = contexto.obtener_dataframe(op.tabla_derecha)
            if df_izq is None or df_der is None:
                errores.append(ErrorDataflow(
                    mensaje=f"Tabla no encontrada para UNIR: izq={op.tabla_izquierda}, der={op.tabla_derecha}",
                    ubicacion=None,
                    codigo="DFS_EXEC_UNIR_TABLE_NOT_FOUND",
                ))
                continue
            try:
                from pyspark.sql import functions as F
                condicion = F.col(op.condicion_on.split("=")[0].strip()) == F.col(op.condicion_on.split("=")[1].strip())
                df_resultado = df_izq.join(df_der, condicion, how=op.tipo_join.lower())
                contexto.registrar_dataframe(op.tabla_izquierda, df_resultado)
            except Exception as e:
                errores.append(ErrorDataflow(
                    mensaje=f"Error ejecutando UNIR: {str(e)}",
                    ubicacion=None,
                    codigo="DFS_EXEC_UNIR_ERROR",
                ))

        elif op.tipo == TipoOperacion.CONCATENAR:
            if not isinstance(op, Concatenar):
                continue
            df_obj = contexto.obtener_dataframe(op.tabla_objetivo)
            df_org = contexto.obtener_dataframe(op.tabla_origen)
            if df_obj is None or df_org is None:
                errores.append(ErrorDataflow(
                    mensaje=f"Tabla no encontrada para CONCATENAR",
                    ubicacion=None,
                    codigo="DFS_EXEC_CONCAT_TABLE_NOT_FOUND",
                ))
                continue
            try:
                df_resultado = df_obj.union(df_org.select(df_obj.columns))
                contexto.registrar_dataframe(op.tabla_objetivo, df_resultado)
            except Exception as e:
                errores.append(ErrorDataflow(
                    mensaje=f"Error ejecutando CONCATENAR: {str(e)}",
                    ubicacion=None,
                    codigo="DFS_EXEC_CONCAT_ERROR",
                ))

        elif op.tipo == TipoOperacion.ELIMINAR_TABLA:
            if not isinstance(op, EliminarTabla):
                continue
            if contexto.tiene_dataframe(op.nombre):
                del contexto._registros[op.nombre]

    return resultados, errores


def _crear_sesion_dataflow(nombre: str, ejecucion_id: str) -> Any:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName(f"dataflow-{nombre}-{ejecucion_id}") \
        .config("spark.sql.caseSensitive", "true") \
        .getOrCreate()
    return spark


def ejecutar_dataflow(argumentos: ArgumentosDataflowScript) -> int:
    spark: Any | None = None
    errores_pipeline: list[ErrorDataflow] = []
    secretos = AdministradorSecretos(dict(argumentos.secretos))

    try:
        contenido, err_lec = _leer_script(argumentos.dataflow_script)
        errores_pipeline.extend(err_lec)
        if errores_pipeline:
            resultado = _construir_resultado_error_dataflow(argumentos, errores_pipeline)
            guardar_resultado(argumentos.resultado, resultado)
            return 1

        contenido_norm, err_norm = _normalizar_script(contenido)
        errores_pipeline.extend(err_norm)
        if errores_pipeline:
            resultado = _construir_resultado_error_dataflow(argumentos, errores_pipeline)
            guardar_resultado(argumentos.resultado, resultado)
            return 1

        tokens, err_lex = _lexear(contenido_norm)
        errores_pipeline.extend(err_lex)
        if errores_pipeline:
            resultado = _construir_resultado_error_dataflow(argumentos, errores_pipeline)
            guardar_resultado(argumentos.resultado, resultado)
            return 1

        programa, err_parse = _parsear(tokens)
        errores_pipeline.extend(err_parse)
        if errores_pipeline or programa is None:
            errores_pipeline = errores_pipeline or [ErrorDataflow(
                mensaje="Programa no pudo ser parsed",
                ubicacion=None,
                codigo="DFS_PARSE_FAILED",
            )]
            resultado = _construir_resultado_error_dataflow(argumentos, errores_pipeline)
            guardar_resultado(argumentos.resultado, resultado)
            return 1

        err_validacion = _validar(programa)
        errores_pipeline.extend(err_validacion)
        if errores_pipeline:
            resultado = _construir_resultado_error_dataflow(argumentos, errores_pipeline)
            guardar_resultado(argumentos.resultado, resultado)
            return 1

        catalogo = cargar_catalogo(argumentos.conexiones)

        plan, err_comp = _compilar_plan(programa)
        errores_pipeline.extend(err_comp)
        if errores_pipeline or plan is None:
            errores_pipeline = errores_pipeline or [ErrorDataflow(
                mensaje="Plan no pudo ser compilado",
                ubicacion=None,
                codigo="DFS_COMPILE_FAILED",
            )]
            resultado = _construir_resultado_error_dataflow(argumentos, errores_pipeline)
            guardar_resultado(argumentos.resultado, resultado)
            return 1

        hash_plan = _serializar_hash(plan)

        if argumentos.solo_compilar:
            if not argumentos.plan_salida:
                errores_pipeline.append(ErrorDataflow(
                    mensaje="--solo-compilar requiere --plan-salida",
                    ubicacion=None,
                    codigo="DFS_SOLO_COMPILAR_REQUIRES_PLAN",
                ))
                resultado = _construir_resultado_error_dataflow(argumentos, errores_pipeline)
                guardar_resultado(argumentos.resultado, resultado)
                return 1

            _escribir_plan_salida(plan, argumentos.plan_salida)
            resultado = _construir_resultado_compilado(argumentos, plan, hash_plan)
            guardar_resultado(argumentos.resultado, resultado)
            return 0

        err_ops = _verificar_operaciones_ejecutables(plan)
        if err_ops:
            errores_pipeline.extend(err_ops)
            resultado = _construir_resultado_error_dataflow(argumentos, errores_pipeline)
            guardar_resultado(argumentos.resultado, resultado)
            return 1

        spark = _crear_sesion_dataflow(
            Path(argumentos.dataflow_script).stem,
            argumentos.ejecucion_id,
        )

        contexto = ContextoEjecucionDataflow(spark=spark)

        _, err_exec = _ejecutar_operaciones(contexto, plan, catalogo, secretos)
        errores_pipeline.extend(err_exec)

        if errores_pipeline:
            resultado = _construir_resultado_error_dataflow(argumentos, errores_pipeline)
            guardar_resultado(argumentos.resultado, resultado)
            return 1

        resultado = {
            "estado": "COMPLETADO",
            "ejecucion_id": argumentos.ejecucion_id,
            "dataflow_script": argumentos.dataflow_script,
            "hash": hash_plan,
            "operaciones": len(plan.operaciones),
        }
        guardar_resultado(argumentos.resultado, resultado)
        return 0

    except Exception as excepcion:
        resultado_error = _construir_resultado_error_dataflow(
            argumentos,
            [ErrorDataflow(
                mensaje=str(excepcion),
                ubicacion=None,
                codigo=type(excepcion).__name__.upper(),
            )]
        )
        guardar_resultado(argumentos.resultado, resultado_error)
        traceback.print_exc()
        return 1

    finally:
        if spark is not None:
            spark.stop()
