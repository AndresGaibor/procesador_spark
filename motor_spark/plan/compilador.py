from __future__ import annotations

import hashlib
from typing import Any

from motor_spark.dataflow_script.ast import (
    Etiqueta,
    Expresion,
    ProgramaDataflowScript,
    SentenciaConcatenate,
    SentenciaDropTable,
    SentenciaLibConnectTo,
    SentenciaLoad,
    SentenciaResident,
    SentenciaSelect,
    SentenciaSet,
    SentenciaStore,
    TipoExpresion,
)
from motor_spark.plan.modelos import (
    Agregar,
    CargarCsv,
    CargarLocal,
    Concatenar,
    EliminarTabla,
    Filtrar,
    LeerJdbc,
    PlanDataflow,
    Proyectar,
    Publicar,
    TipoOperacion,
    Unir,
    generar_id_estable,
)


class CompiladorDataflow:
    def __init__(self, programa: ProgramaDataflowScript) -> None:
        self._programa = programa
        self._operaciones: list[Any] = []
        self._tablas_definidas: set[str] = set()
        self._tablas_dropped: set[str] = set()
        self._errores: list[str] = []
        self._indice_op: int = 0

    def _generar_id(self, nombre_tabla: str, operacion: str) -> str:
        resultado = generar_id_estable(nombre_tabla, operacion, self._indice_op)
        self._indice_op += 1
        return resultado

    def _registrar_tabla(self, nombre: str) -> None:
        self._tablas_definidas.add(nombre)

    def _tabla_fue_dropped(self, nombre: str) -> bool:
        return nombre in self._tablas_dropped

    def _compilar_expresion(self, expr: Expresion) -> str:
        if expr.tipo == TipoExpresion.COLUMNA:
            return expr.valor
        elif expr.tipo == TipoExpresion.LITERAL_STRING:
            return expr.valor
        elif expr.tipo == TipoExpresion.LITERAL_NUMERO:
            return expr.valor
        elif expr.tipo == TipoExpresion.FUNCION:
            args = ", ".join(self._compilar_expresion(h) for h in expr.hijos)
            return f"{expr.valor}({args})"
        elif expr.tipo == TipoExpresion.OPERACION_BINARIA:
            left = self._compilar_expresion(expr.hijos[0])
            right = self._compilar_expresion(expr.hijos[1])
            return f"{left} {expr.valor} {right}"
        elif expr.tipo == TipoExpresion.CONCATENACION:
            args = ", ".join(self._compilar_expresion(h) for h in expr.hijos)
            return f"CONCAT({args})"
        return ""

    def _compilar_select(self, sentencia: SentenciaSelect, nombre_tabla: str) -> None:
        if sentencia.esquema:
            self._operaciones.append(
                LeerJdbc(
                    id=self._generar_id(nombre_tabla, "leer_jdbc"),
                    conexion_nombre=sentencia.esquema,
                    esquema=sentencia.esquema,
                    tabla=sentencia.tabla,
                    campos=tuple(p.expresion.valor for p in sentencia.proyecciones if p.expresion.tipo == TipoExpresion.COLUMNA),
                    filtros_where=tuple(self._compilar_expresion(c) for c in sentencia.condiciones_where),
                )
            )
        else:
            self._operaciones.append(
                CargarLocal(
                    id=self._generar_id(nombre_tabla, "cargar_local"),
                    ruta=sentencia.tabla,
                    nombre_tabla=nombre_tabla,
                )
            )

        campos = tuple(p.alias or p.expresion.valor for p in sentencia.proyecciones if p.expresion.tipo == TipoExpresion.COLUMNA)
        if campos:
            self._operaciones.append(
                Proyectar(
                    id=self._generar_id(nombre_tabla, "proyectar"),
                    tabla_origen=nombre_tabla,
                    campos=campos,
                    alias=nombre_tabla,
                )
            )

        for cond in sentencia.condiciones_where:
            self._operaciones.append(
                Filtrar(
                    id=self._generar_id(nombre_tabla, "filtrar"),
                    tabla_origen=nombre_tabla,
                    condicion=self._compilar_expresion(cond),
                )
            )

        if sentencia.group_by:
            grupo_por = tuple(self._compilar_expresion(g) for g in sentencia.group_by)
            self._operaciones.append(
                Agregar(
                    id=self._generar_id(nombre_tabla, "agregar"),
                    tabla_origen=nombre_tabla,
                    grupo_por=grupo_por,
                    funciones=("COUNT",),
                )
            )

        if sentencia.join_externo:
            join = sentencia.join_externo
            if join.es_natural:
                condicion = "NATURAL"
            else:
                condicion = f"{join.izquierda} = {join.derecha}"
            self._operaciones.append(
                Unir(
                    id=self._generar_id(nombre_tabla, "unir"),
                    tabla_izquierda=nombre_tabla,
                    tabla_derecha=join.derecha,
                    condicion_on=condicion,
                    tipo_join="LEFT",
                )
            )

        self._registrar_tabla(nombre_tabla)

    def _compilar_load(self, sentencia: SentenciaLoad, nombre_tabla: str) -> None:
        if sentencia.es_resident:
            self._operaciones.append(
                CargarLocal(
                    id=self._generar_id(nombre_tabla, "cargar_local"),
                    ruta=sentencia.etiqueta_resident or "",
                    nombre_tabla=nombre_tabla,
                )
            )
        elif sentencia.ruta:
            self._operaciones.append(
                CargarCsv(
                    id=self._generar_id(nombre_tabla, "cargar_csv"),
                    ruta=sentencia.ruta,
                    tiene_header=True,
                )
            )

        if sentencia.campos:
            self._operaciones.append(
                Proyectar(
                    id=self._generar_id(nombre_tabla, "proyectar"),
                    tabla_origen=nombre_tabla,
                    campos=sentencia.campos,
                    alias=nombre_tabla,
                )
            )

        self._registrar_tabla(nombre_tabla)

    def _compilar_resident(self, sentencia: SentenciaResident, nombre_tabla: str) -> None:
        if self._tabla_fue_dropped(sentencia.etiqueta_origen):
            self._errores.append(f"Tabla '{sentencia.etiqueta_origen}' fue eliminada antes de ser referenciada")

        self._operaciones.append(
            CargarLocal(
                id=self._generar_id(nombre_tabla, "cargar_local"),
                ruta=sentencia.etiqueta_origen,
                nombre_tabla=nombre_tabla,
            )
        )
        self._registrar_tabla(nombre_tabla)

    def _compilar_drop_table(self, sentencia: SentenciaDropTable) -> None:
        self._operaciones.append(
            EliminarTabla(
                id=self._generar_id(sentencia.tabla, "eliminar_tabla"),
                nombre=sentencia.tabla,
            )
        )
        self._tablas_dropped.add(sentencia.tabla)

    def _compilar_store(self, sentencia: SentenciaStore) -> None:
        self._operaciones.append(
            Publicar(
                id=self._generar_id(sentencia.tabla, "publicar"),
                tabla_origen=sentencia.tabla,
                destino=sentencia.ruta_destino,
                formato=sentencia.formato or "txt",
            )
        )

    def _compilar_concatenate(self, sentencia: SentenciaConcatenate) -> None:
        self._operaciones.append(
            Concatenar(
                id=self._generar_id(sentencia.etiqueta_objetivo, "concatenar"),
                tabla_objetivo=sentencia.etiqueta_objetivo,
                tabla_origen=sentencia.etiqueta_origen,
                noconcatenate=sentencia.noconcatenate,
            )
        )

    def compilar(self) -> PlanDataflow:
        for global_sent in self._programa.sentencias_globales:
            if isinstance(global_sent, SentenciaSet):
                pass
            elif isinstance(global_sent, SentenciaLibConnectTo):
                pass

        for etiqueta in self._programa.etiquetas:
            for i, sentencia in enumerate(etiqueta.sentencias):
                nombre_tabla = f"{etiqueta.nombre}_{i}"

                if isinstance(sentencia, SentenciaSelect):
                    self._compilar_select(sentencia, nombre_tabla)
                elif isinstance(sentencia, SentenciaLoad):
                    self._compilar_load(sentencia, nombre_tabla)
                elif isinstance(sentencia, SentenciaResident):
                    self._compilar_resident(sentencia, nombre_tabla)
                elif isinstance(sentencia, SentenciaDropTable):
                    self._compilar_drop_table(sentencia)
                elif isinstance(sentencia, SentenciaStore):
                    self._compilar_store(sentencia)
                elif isinstance(sentencia, SentenciaConcatenate):
                    self._compilar_concatenate(sentencia)

        return PlanDataflow(
            version=1,
            operaciones=tuple(self._operaciones),
            tabla_resultado=self._tablas_definidas.pop() if self._tablas_definidas else None,
            metadata={"errores": self._errores},
        )


def compilar(programa: ProgramaDataflowScript) -> PlanDataflow:
    return CompiladorDataflow(programa).compilar()
