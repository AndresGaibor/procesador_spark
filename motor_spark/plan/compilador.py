"""Compilación del AST Qlik a un plan intermedio seguro y determinista.

El compilador no ejecuta Spark ni resuelve secretos. Su responsabilidad es
producir operaciones autocontenidas o registrar un error explícito. Nunca debe
"aproximar" una construcción Qlik: una traducción incompleta sería más peligrosa
que detener la ejecución.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import ClassVar

from motor_spark.dataflow_script.ast import (
    Etiqueta,
    Expresion,
    ProgramaDataflowScript,
    ProjectionItem,
    SentenciaConcatenate,
    SentenciaDropTable,
    SentenciaLibConnectTo,
    SentenciaLoad,
    SentenciaResident,
    SentenciaSelect,
    SentenciaStore,
    TipoExpresion,
)
from motor_spark.plan.modelos import (
    Agregar,
    CargarCsv,
    CargarLocal,
    Concatenar,
    EliminarTabla,
    ExpresionPlan,
    Filtrar,
    LeerJdbc,
    PlanDataflow,
    Proyectar,
    Publicar,
    SeleccionPlan,
    TipoExpresionPlan,
    Unir,
    generar_id_estable,
)


class CompiladorDataflow:
    """Construye un plan y acumula incompatibilidades semánticas encontradas."""

    def __init__(self, programa: ProgramaDataflowScript) -> None:
        self._programa = programa
        self._operaciones: list[object] = []
        self._tablas_vivas: list[str] = []
        self._errores: list[str] = []
        self._indice_op = 0
        self._conexion_jdbc = self._resolver_conexion_jdbc()

    def _resolver_conexion_jdbc(self) -> str | None:
        """Obtiene la conexión declarada sin inventar asociación por posición.

        El AST actual separa sentencias globales de etiquetas, por lo que pierde
        el punto exacto donde cambia una conexión. Varias conexiones diferentes
        serían ambiguas; se rechazan hasta que el AST preserve el orden total.
        Repetir la misma conexión, como hace el Dataflow Bancolombia, es seguro.
        """
        conexiones = [
            sentencia.conexion
            for sentencia in self._programa.sentencias_globales
            if isinstance(sentencia, SentenciaLibConnectTo)
        ]
        unicas = list(dict.fromkeys(conexiones))
        if len(unicas) > 1:
            self._errores.append(
                "El script usa varias conexiones LIB diferentes y el AST actual "
                "no conserva su posición relativa"
            )
            return None
        return unicas[0] if unicas else None

    def _generar_id(self, nombre_tabla: str, operacion: str) -> str:
        identificador = generar_id_estable(
            nombre_tabla,
            operacion,
            self._indice_op,
        )
        self._indice_op += 1
        return identificador

    def _registrar_tabla(self, nombre: str) -> None:
        """Registra una tabla viva y conserva un orden final determinista."""
        if nombre in self._tablas_vivas:
            self._tablas_vivas.remove(nombre)
        self._tablas_vivas.append(nombre)

    def _eliminar_tabla(self, nombre: str) -> None:
        if nombre in self._tablas_vivas:
            self._tablas_vivas.remove(nombre)

    def _agregar_error(self, mensaje: str) -> None:
        if mensaje not in self._errores:
            self._errores.append(mensaje)

    def _compilar_expresion(self, expresion: Expresion) -> str:
        """Serializa el subconjunto de expresiones aceptado por el plan.

        Esta cadena no se evalúa como Python. El ejecutor la entrega únicamente
        al analizador SQL de Spark después de que lexer, parser y validador hayan
        limitado identificadores, operadores y funciones permitidas.
        """
        if expresion.tipo in {
            TipoExpresion.COLUMNA,
            TipoExpresion.LITERAL_STRING,
            TipoExpresion.LITERAL_NUMERO,
        }:
            return expresion.valor

        if expresion.tipo == TipoExpresion.FUNCION:
            argumentos = ", ".join(
                self._compilar_expresion(hijo) for hijo in expresion.hijos
            )
            return f"{expresion.valor}({argumentos})"

        if expresion.tipo == TipoExpresion.OPERACION_BINARIA:
            operador = expresion.valor.upper()
            if operador in {"NOT", "NEGATE"} and len(expresion.hijos) == 1:
                operando = self._compilar_expresion(expresion.hijos[0])
                simbolo = "NOT" if operador == "NOT" else "-"
                return f"({simbolo} {operando})"
            if len(expresion.hijos) != 2:
                self._agregar_error(
                    f"Operación {expresion.valor!r} tiene aridad inválida"
                )
                return ""
            izquierda = self._compilar_expresion(expresion.hijos[0])
            derecha = self._compilar_expresion(expresion.hijos[1])
            return f"({izquierda} {expresion.valor} {derecha})"

        if expresion.tipo == TipoExpresion.CONCATENACION:
            argumentos = ", ".join(
                self._compilar_expresion(hijo) for hijo in expresion.hijos
            )
            return f"CONCAT({argumentos})"

        self._agregar_error(
            f"Tipo de expresión no representable en el plan: {expresion.tipo.name}"
        )
        return ""

    _MAPA_TIPOS_EXPRESION: ClassVar[dict] = {
        TipoExpresion.COLUMNA: TipoExpresionPlan.COLUMNA,
        TipoExpresion.LITERAL_NUMERO: TipoExpresionPlan.LITERAL_NUMERO,
        TipoExpresion.LITERAL_STRING: TipoExpresionPlan.LITERAL_STRING,
        TipoExpresion.FUNCION: TipoExpresionPlan.FUNCION,
        TipoExpresion.OPERACION_BINARIA: TipoExpresionPlan.OPERACION_BINARIA,
        TipoExpresion.CONCATENACION: TipoExpresionPlan.CONCATENACION,
        TipoExpresion.ALIAS: TipoExpresionPlan.ALIAS,
        TipoExpresion.WINDOW: TipoExpresionPlan.WINDOW,
        TipoExpresion.WINDOW_RANK: TipoExpresionPlan.WINDOW_RANK,
    }

    def _convertir_expresion(self, expresion: Expresion) -> ExpresionPlan:
        """Copia el AST a un modelo Pydantic estable y autocontenido."""
        tipo = self._MAPA_TIPOS_EXPRESION.get(expresion.tipo)
        if tipo is None:
            self._agregar_error(f"Tipo de expresión no serializable: {expresion.tipo}")
            tipo = TipoExpresionPlan.LITERAL_STRING
        return ExpresionPlan(
            tipo=tipo,
            valor=expresion.valor,
            hijos=tuple(self._convertir_expresion(hijo) for hijo in expresion.hijos),
        )

    def _convertir_selecciones(
        self,
        proyecciones: Iterable[ProjectionItem],
    ) -> tuple[SeleccionPlan, ...]:
        return tuple(
            SeleccionPlan(
                expresion=self._convertir_expresion(item.expresion),
                alias=item.alias,
            )
            for item in proyecciones
        )

    def _columnas_referenciadas(self, expresion: Expresion) -> tuple[str, ...]:
        """Obtiene columnas hoja sin incluir el marcador COUNT DISTINCT."""
        resultado: list[str] = []
        if (
            expresion.tipo == TipoExpresion.COLUMNA
            and expresion.valor.upper() != "DISTINCT"
        ):
            resultado.append(expresion.valor)
        for hijo in expresion.hijos:
            for columna in self._columnas_referenciadas(hijo):
                if columna not in resultado:
                    resultado.append(columna)
        return tuple(resultado)

    def _separar_proyecciones(
        self,
        proyecciones: Iterable[ProjectionItem],
    ) -> tuple[tuple[str, ...], tuple[tuple[str, str], ...]]:
        """Devuelve columnas fuente y aliases simples sin perder cálculos.

        ``campos`` se usa para limitar la lectura JDBC. Las expresiones completas
        se guardan por separado en ``selecciones`` dentro de la operación.
        """
        campos: list[str] = []
        aliases: list[tuple[str, str]] = []
        for item in proyecciones:
            for columna in self._columnas_referenciadas(item.expresion):
                if columna not in campos:
                    campos.append(columna)
            if (
                item.expresion.tipo == TipoExpresion.COLUMNA
                and item.alias
                and item.alias != item.expresion.valor
            ):
                aliases.append((item.expresion.valor, item.alias))
        return tuple(campos), tuple(aliases)

    def _compilar_select(
        self,
        sentencia: SentenciaSelect,
        nombre_tabla: str,
    ) -> None:
        campos_lista, aliases = self._separar_proyecciones(sentencia.proyecciones)
        campos_lectura = list(campos_lista)
        for expresion in (
            *sentencia.condiciones_where,
            *sentencia.group_by,
        ):
            for columna in self._columnas_referenciadas(expresion):
                if columna not in campos_lectura:
                    campos_lectura.append(columna)
        selecciones = self._convertir_selecciones(sentencia.proyecciones)

        referencia_local = sentencia.tabla or sentencia.esquema or ""
        usa_tabla_logica = referencia_local in self._tablas_vivas
        if usa_tabla_logica:
            if referencia_local != nombre_tabla:
                self._operaciones.append(
                    CargarLocal(
                        id=self._generar_id(nombre_tabla, "cargar_resident"),
                        ruta=referencia_local,
                        nombre_tabla=nombre_tabla,
                    )
                )
                self._registrar_tabla(nombre_tabla)
        else:
            if not self._conexion_jdbc:
                self._agregar_error(
                    f"SELECT de {sentencia.esquema or ''}.{sentencia.tabla} no "
                    "tiene una conexión LIB CONNECT TO inequívoca"
                )
                return
            if not sentencia.esquema or not sentencia.tabla:
                self._agregar_error(
                    f"SELECT de {referencia_local!r} no declara esquema y tabla JDBC"
                )
                return
            if not campos_lectura:
                self._agregar_error(
                    f"SELECT de {sentencia.tabla!r} no contiene columnas legibles"
                )
                return

            self._operaciones.append(
                LeerJdbc(
                    id=self._generar_id(nombre_tabla, "leer_jdbc"),
                    nombre_tabla=nombre_tabla,
                    conexion_nombre=self._conexion_jdbc,
                    esquema=sentencia.esquema,
                    tabla=sentencia.tabla,
                    campos=tuple(campos_lectura),
                    filtros_where=tuple(
                        self._compilar_expresion(condicion)
                        for condicion in sentencia.condiciones_where
                    ),
                )
            )
            self._registrar_tabla(nombre_tabla)

        if sentencia.join_externo:
            join = sentencia.join_externo
            condicion = (
                "NATURAL" if join.es_natural else f"{join.izquierda} = {join.derecha}"
            )
            self._operaciones.append(
                Unir(
                    id=self._generar_id(nombre_tabla, "unir"),
                    tabla_izquierda=nombre_tabla,
                    tabla_derecha=join.derecha,
                    condicion_on=condicion,
                    tipo_join="LEFT",
                )
            )

        for condicion in sentencia.condiciones_where:
            self._operaciones.append(
                Filtrar(
                    id=self._generar_id(nombre_tabla, "filtrar"),
                    tabla_origen=nombre_tabla,
                    condicion=self._compilar_expresion(condicion),
                    expresion=self._convertir_expresion(condicion),
                )
            )

        if sentencia.group_by:
            if any(
                expresion.tipo != TipoExpresion.COLUMNA
                for expresion in sentencia.group_by
            ):
                self._agregar_error(
                    f"GROUP BY complejo no soportado en {nombre_tabla!r}"
                )
            grupos = tuple(expresion.valor for expresion in sentencia.group_by)
            self._operaciones.append(
                Agregar(
                    id=self._generar_id(nombre_tabla, "agregar"),
                    tabla_origen=nombre_tabla,
                    grupo_por=grupos,
                    selecciones=selecciones,
                    tabla_resultado=nombre_tabla,
                )
            )
        elif selecciones:
            self._operaciones.append(
                Proyectar(
                    id=self._generar_id(nombre_tabla, "proyectar"),
                    tabla_origen=nombre_tabla,
                    campos=tuple(campos_lista),
                    alias=nombre_tabla,
                    aliases=aliases,
                    selecciones=selecciones,
                )
            )
        self._registrar_tabla(nombre_tabla)

    def _aplicar_transformacion_load(
        self,
        sentencia: SentenciaLoad,
        nombre_tabla: str,
    ) -> None:
        """Aplica WHERE, GROUP BY y proyecciones sobre una fuente ya registrada."""
        for condicion in sentencia.condiciones_where:
            self._operaciones.append(
                Filtrar(
                    id=self._generar_id(nombre_tabla, "filtrar"),
                    tabla_origen=nombre_tabla,
                    condicion=self._compilar_expresion(condicion),
                    expresion=self._convertir_expresion(condicion),
                )
            )

        proyecciones = sentencia.proyecciones
        campos, aliases = self._separar_proyecciones(proyecciones)
        selecciones = self._convertir_selecciones(proyecciones)
        if sentencia.group_by:
            if any(
                expresion.tipo != TipoExpresion.COLUMNA
                for expresion in sentencia.group_by
            ):
                self._agregar_error(
                    f"GROUP BY complejo no soportado en LOAD {nombre_tabla!r}"
                )
            self._operaciones.append(
                Agregar(
                    id=self._generar_id(nombre_tabla, "agregar"),
                    tabla_origen=nombre_tabla,
                    grupo_por=tuple(
                        expresion.valor for expresion in sentencia.group_by
                    ),
                    selecciones=selecciones,
                    tabla_resultado=nombre_tabla,
                )
            )
        elif selecciones:
            self._operaciones.append(
                Proyectar(
                    id=self._generar_id(nombre_tabla, "proyectar"),
                    tabla_origen=nombre_tabla,
                    campos=campos,
                    alias=nombre_tabla,
                    aliases=aliases,
                    selecciones=selecciones,
                    distinct=sentencia.distinct,
                )
            )
        elif sentencia.distinct:
            self._agregar_error(
                f"LOAD DISTINCT de {nombre_tabla!r} no declara proyecciones"
            )

    def _compilar_load(
        self,
        sentencia: SentenciaLoad,
        nombre_tabla: str,
    ) -> None:
        if sentencia.es_resident:
            origen = sentencia.etiqueta_resident or ""
            if not origen:
                self._agregar_error(
                    f"LOAD RESIDENT de {nombre_tabla!r} no indica tabla origen"
                )
                return
            self._operaciones.append(
                CargarLocal(
                    id=self._generar_id(nombre_tabla, "cargar_resident"),
                    ruta=origen,
                    nombre_tabla=nombre_tabla,
                )
            )
        elif sentencia.ruta:
            self._operaciones.append(
                CargarCsv(
                    id=self._generar_id(nombre_tabla, "cargar_csv"),
                    nombre_tabla=nombre_tabla,
                    ruta=sentencia.ruta,
                    tiene_header=True,
                )
            )
        else:
            self._agregar_error(f"LOAD de {nombre_tabla!r} no tiene FROM ni RESIDENT")
            return

        self._registrar_tabla(nombre_tabla)
        self._aplicar_transformacion_load(sentencia, nombre_tabla)
        self._registrar_tabla(nombre_tabla)

    def _compilar_resident(
        self,
        sentencia: SentenciaResident,
        nombre_tabla: str,
    ) -> None:
        origen = sentencia.etiqueta_origen
        if origen not in self._tablas_vivas:
            self._agregar_error(
                f"Tabla RESIDENT {origen!r} no existe al crear {nombre_tabla!r}"
            )
            return

        self._operaciones.append(
            CargarLocal(
                id=self._generar_id(nombre_tabla, "cargar_resident"),
                ruta=origen,
                nombre_tabla=nombre_tabla,
            )
        )
        if sentencia.expresion:
            condicion = self._compilar_expresion(sentencia.expresion)
            if condicion:
                self._operaciones.append(
                    Filtrar(
                        id=self._generar_id(nombre_tabla, "filtrar"),
                        tabla_origen=nombre_tabla,
                        condicion=condicion,
                    )
                )
        self._registrar_tabla(nombre_tabla)

    def _compilar_drop_table(self, sentencia: SentenciaDropTable) -> None:
        if sentencia.tabla not in self._tablas_vivas:
            self._agregar_error(
                f"DROP TABLE referencia una tabla no disponible: {sentencia.tabla!r}"
            )
        self._operaciones.append(
            EliminarTabla(
                id=self._generar_id(sentencia.tabla, "eliminar_tabla"),
                nombre=sentencia.tabla,
            )
        )
        self._eliminar_tabla(sentencia.tabla)

    def _compilar_store(self, sentencia: SentenciaStore) -> None:
        if sentencia.tabla not in self._tablas_vivas:
            self._agregar_error(
                f"STORE referencia una tabla no disponible: {sentencia.tabla!r}"
            )
        self._operaciones.append(
            Publicar(
                id=self._generar_id(sentencia.tabla, "publicar"),
                tabla_origen=sentencia.tabla,
                destino=sentencia.ruta_destino,
                formato=sentencia.formato or "txt",
            )
        )

    def _compilar_concatenate(
        self,
        sentencia: SentenciaConcatenate,
    ) -> None:
        if sentencia.noconcatenate:
            self._agregar_error(
                "NOCONCATENATE es un modificador de creación de tabla, no una "
                "operación de unión vertical independiente"
            )
            return
        if not sentencia.etiqueta_objetivo or not sentencia.etiqueta_origen:
            self._agregar_error(
                "CONCATENATE no contiene origen y objetivo completos en el AST"
            )
            return
        self._operaciones.append(
            Concatenar(
                id=self._generar_id(
                    sentencia.etiqueta_objetivo,
                    "concatenar",
                ),
                tabla_objetivo=sentencia.etiqueta_objetivo,
                tabla_origen=sentencia.etiqueta_origen,
            )
        )

    @staticmethod
    def _es_preceding_load_simple(etiqueta: Etiqueta) -> bool:
        """Reconoce LOAD sin origen encima de un SELECT fuente.

        Qlik escribe primero la transformación y debajo la fuente. Solo se
        acepta aquí el caso cuyos LOAD contienen campos simples, porque el AST
        actual no conserva todavía expresiones calculadas completas.
        """
        productoras = [
            sentencia
            for sentencia in etiqueta.sentencias
            if isinstance(
                sentencia,
                (SentenciaSelect, SentenciaLoad, SentenciaResident),
            )
        ]
        if len(productoras) < 2:
            return False
        fuente = productoras[-1]
        fuente_valida = isinstance(fuente, (SentenciaSelect, SentenciaResident)) or (
            isinstance(fuente, SentenciaLoad)
            and bool(fuente.ruta or fuente.es_resident)
        )
        if not fuente_valida:
            return False
        return all(
            isinstance(sentencia, SentenciaLoad)
            and not sentencia.ruta
            and not sentencia.es_resident
            for sentencia in productoras[:-1]
        )

    @staticmethod
    def _es_cadena_local_secuencial(etiqueta: Etiqueta) -> bool:
        """Reconoce una carga fuente seguida de SELECT sobre la misma etiqueta."""
        productoras = [
            sentencia
            for sentencia in etiqueta.sentencias
            if isinstance(
                sentencia,
                (SentenciaSelect, SentenciaLoad, SentenciaResident),
            )
        ]
        if len(productoras) <= 1:
            return True
        primera = productoras[0]
        if not (
            isinstance(primera, SentenciaLoad) and (primera.ruta or primera.es_resident)
        ):
            return False
        for sentencia in productoras[1:]:
            if not isinstance(sentencia, SentenciaSelect):
                return False
            referencia = sentencia.tabla or sentencia.esquema or ""
            if referencia != etiqueta.nombre:
                return False
        return True

    def _compilar_preceding_load(
        self,
        sentencia: SentenciaLoad,
        nombre_tabla: str,
    ) -> None:
        """Aplica un LOAD superior después de compilar su fuente inferior."""
        if not sentencia.proyecciones:
            self._agregar_error(
                f"Preceding LOAD de {nombre_tabla!r} no declara proyecciones"
            )
            return
        self._aplicar_transformacion_load(sentencia, nombre_tabla)
        self._registrar_tabla(nombre_tabla)

    def _aplicar_prefijo_carga(
        self,
        carga: SentenciaLoad | None,
        tabla_temporal: str,
    ) -> None:
        """Aplica JOIN/CONCATENATE después de materializar la carga temporal.

        El prefijo está escrito antes del LOAD, pero su efecto ocurre cuando la
        cadena LOAD+SELECT ya produjo un DataFrame. La tabla temporal se elimina
        del registro para no contaminar el resultado final del plan.
        """
        if carga is None:
            return
        objetivo = carga.concatenate_objetivo or carga.join_objetivo
        if not objetivo:
            return
        if objetivo not in self._tablas_vivas:
            self._agregar_error(
                f"El prefijo de carga referencia una tabla no disponible: {objetivo!r}"
            )
            return

        if carga.concatenate_objetivo:
            self._operaciones.append(
                Concatenar(
                    id=self._generar_id(objetivo, "concatenar"),
                    tabla_objetivo=objetivo,
                    tabla_origen=tabla_temporal,
                )
            )
        else:
            self._operaciones.append(
                Unir(
                    id=self._generar_id(objetivo, "unir"),
                    tabla_izquierda=objetivo,
                    tabla_derecha=tabla_temporal,
                    condicion_on="NATURAL",
                    tipo_join=carga.join_tipo or "LEFT",
                )
            )

        if tabla_temporal != objetivo:
            self._operaciones.append(
                EliminarTabla(
                    id=self._generar_id(tabla_temporal, "eliminar_tabla"),
                    nombre=tabla_temporal,
                )
            )
            self._eliminar_tabla(tabla_temporal)
        self._registrar_tabla(objetivo)

    def compilar(self) -> PlanDataflow:
        """Compila todas las etiquetas conservando nombres y orden lógico."""
        for etiqueta in self._programa.etiquetas:
            nombre_tabla = (
                f"_anonima_{self._indice_op}"
                if etiqueta.nombre == "_anonima"
                else etiqueta.nombre
            )
            carga_prefijada = next(
                (
                    sentencia
                    for sentencia in etiqueta.sentencias
                    if isinstance(sentencia, SentenciaLoad)
                    and (sentencia.concatenate_objetivo or sentencia.join_objetivo)
                ),
                None,
            )

            if self._es_preceding_load_simple(etiqueta):
                productoras = [
                    sentencia
                    for sentencia in etiqueta.sentencias
                    if isinstance(
                        sentencia,
                        (SentenciaSelect, SentenciaLoad, SentenciaResident),
                    )
                ]
                fuente = productoras[-1]
                if isinstance(fuente, SentenciaSelect):
                    self._compilar_select(fuente, nombre_tabla)
                elif isinstance(fuente, SentenciaLoad):
                    self._compilar_load(fuente, nombre_tabla)
                else:
                    self._compilar_resident(fuente, nombre_tabla)
                # Los LOAD superiores se ejecutan desde el más cercano a la
                # fuente hacia arriba, es decir, en orden textual inverso.
                for carga in reversed(productoras[:-1]):
                    self._compilar_preceding_load(carga, nombre_tabla)
                restantes = [
                    sentencia
                    for sentencia in etiqueta.sentencias
                    if sentencia not in productoras
                ]
            else:
                productoras = [
                    sentencia
                    for sentencia in etiqueta.sentencias
                    if isinstance(
                        sentencia,
                        (SentenciaSelect, SentenciaLoad, SentenciaResident),
                    )
                ]
                if len(productoras) > 1 and not self._es_cadena_local_secuencial(
                    etiqueta
                ):
                    self._agregar_error(
                        f"La etiqueta {etiqueta.nombre!r} contiene varias fuentes "
                        "cuya relación no puede determinarse de forma segura"
                    )
                restantes = list(etiqueta.sentencias)

            for sentencia in restantes:
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

            self._aplicar_prefijo_carga(carga_prefijada, nombre_tabla)

        variables = {
            sentencia.variable: sentencia.valor
            for sentencia in self._programa.sentencias_globales
            if hasattr(sentencia, "variable")
        }
        return PlanDataflow(
            version=1,
            operaciones=tuple(self._operaciones),
            tabla_resultado=(self._tablas_vivas[-1] if self._tablas_vivas else None),
            metadata={
                "errores": tuple(self._errores),
                "variables": variables,
            },
        )


def compilar(programa: ProgramaDataflowScript) -> PlanDataflow:
    """API funcional usada por la capa de aplicación y por las pruebas."""
    return CompiladorDataflow(programa).compilar()
