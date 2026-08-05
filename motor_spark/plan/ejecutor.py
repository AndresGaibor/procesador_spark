"""Ejecución fail-fast de planes Dataflow sobre PySpark.

Este módulo es la única ruta de producción para ejecutar un ``PlanDataflow``.
El parser y el compilador se ejecutan antes; aquí no se intenta reparar ni
adivinar una operación incompleta. Ante el primer fallo se detiene el plan para
evitar publicaciones parciales o transformaciones posteriores sobre datos
incorrectos.
"""

from __future__ import annotations

import re
import shutil
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path, PurePosixPath
from typing import Any, ClassVar

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    TimestampType,
)

from motor_spark.conexiones.base_destino import ConfiguracionBaseDestino
from motor_spark.conexiones.modelos import (
    CatalogoConexiones,
    ConexionLocal,
    ConexionSftp,
)
from motor_spark.conexiones.secretos import AdministradorSecretos
from motor_spark.dataflow_script.ast import Expresion, TipoExpresion
from motor_spark.dataflow_script.expresiones import CompiladorExpresion
from motor_spark.dataflow_script.jdbc import leer_jdbc
from motor_spark.dataflow_script.publicacion import (
    PublicacionLocal,
    PublicacionSftp,
    StagingManager,
    UriLib,
    UriParseResult,
    decodificar_clave_privada_base64,
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
    OperacionPlan,
    PlanDataflow,
    Proyectar,
    Publicar,
    SeleccionPlan,
    TipoExpresionPlan,
    Unir,
)

PATRON_JOIN_SIMPLE = re.compile(
    r"^\s*(?:[A-Za-z_][A-Za-z0-9_]*\.)?"
    r"(?P<izquierda>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*"
    r"(?:[A-Za-z_][A-Za-z0-9_]*\.)?"
    r"(?P<derecha>[A-Za-z_][A-Za-z0-9_]*)\s*$"
)
PATRON_AGREGACION = re.compile(
    r"^\s*(?P<funcion>COUNT|SUM|AVG|MIN|MAX)\s*\(\s*"
    r"(?:(?P<distinct>DISTINCT)\s+)?"
    r"(?P<columna>\*|[A-Za-z_][A-Za-z0-9_]*)\s*\)\s+AS\s+"
    r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*$",
    re.IGNORECASE,
)


class ErrorEjecucionPlan(RuntimeError):
    """Fallo controlado que identifica la operación del plan responsable."""


class EjecutorPlanDataflow:
    """Mantiene el registro de tablas y despacha operaciones tipadas."""

    def __init__(
        self,
        *,
        spark: Any,
        catalogo: CatalogoConexiones,
        secretos: AdministradorSecretos,
        ejecucion_id: str,
        base_destino: ConfiguracionBaseDestino | None = None,
    ) -> None:
        if not ejecucion_id.strip():
            raise ValueError("ejecucion_id no puede estar vacío")
        self._spark = spark
        self._catalogo = catalogo
        self._secretos = secretos
        self._ejecucion_id = ejecucion_id
        self._base_destino = base_destino
        self._tablas: dict[str, Any] = {}
        self._publicaciones: list[dict[str, Any]] = []
        self._operaciones_ejecutadas = 0


    def registrar_tabla(
        self,
        nombre: str,
        dataframe: Any,
        *,
        reemplazar: bool = False,
    ) -> None:
        """Registra una tabla lógica sin sobrescribirla accidentalmente."""
        if nombre in self._tablas and not reemplazar:
            raise ErrorEjecucionPlan(
                f"La tabla {nombre!r} ya existe; se requiere reemplazo explícito"
            )
        self._tablas[nombre] = dataframe

    def obtener_tabla(self, nombre: str) -> Any | None:
        return self._tablas.get(nombre)

    def _exigir_tabla(self, nombre: str) -> Any:
        dataframe = self.obtener_tabla(nombre)
        if dataframe is None:
            raise ErrorEjecucionPlan(f"Tabla no encontrada: {nombre!r}")
        return dataframe

    def ejecutar(self, plan: PlanDataflow) -> dict[str, Any]:
        """Ejecuta en orden y detiene el plan ante el primer error."""
        errores_compilacion = tuple(plan.metadata.get("errores", ()))
        if errores_compilacion:
            raise ErrorEjecucionPlan(
                "El plan contiene errores de compilación: "
                + "; ".join(str(error) for error in errores_compilacion)
            )

        for operacion in plan.operaciones:
            try:
                self._ejecutar_operacion(operacion)
            except ErrorEjecucionPlan:
                raise
            except Exception as excepcion:
                # El mensaje evita serializar objetos completos que podrían
                # contener secretos. El traceback conserva la causa internamente.
                raise ErrorEjecucionPlan(
                    f"Operación {operacion.id!r} ({operacion.tipo.value}) falló: "
                    f"{type(excepcion).__name__}: {excepcion}"
                ) from excepcion
            self._operaciones_ejecutadas += 1

        return {
            "operaciones_ejecutadas": self._operaciones_ejecutadas,
            "tablas_disponibles": tuple(self._tablas),
            "publicaciones": tuple(self._publicaciones),
        }

    def _ejecutar_operacion(self, operacion: OperacionPlan) -> None:
        if isinstance(operacion, LeerJdbc):
            self._leer_jdbc(operacion)
        elif isinstance(operacion, CargarCsv):
            self._cargar_csv(operacion)
        elif isinstance(operacion, CargarLocal):
            self._cargar_local(operacion)
        elif isinstance(operacion, Proyectar):
            self._proyectar(operacion)
        elif isinstance(operacion, Filtrar):
            self._filtrar(operacion)
        elif isinstance(operacion, Concatenar):
            self._concatenar(operacion)
        elif isinstance(operacion, Unir):
            self._unir(operacion)
        elif isinstance(operacion, Agregar):
            self._agregar(operacion)
        elif isinstance(operacion, EliminarTabla):
            self._eliminar_tabla(operacion)
        elif isinstance(operacion, Publicar):
            self._publicar(operacion)
        else:  # pragma: no cover - la unión Pydantic impide llegar aquí.
            raise ErrorEjecucionPlan(
                f"Tipo de operación no implementado: {type(operacion).__name__}"
            )

    def _leer_jdbc(self, operacion: LeerJdbc) -> None:
        dataframe = leer_jdbc(
            spark=self._spark,
            nombre_conexion=operacion.conexion_nombre,
            esquema=operacion.esquema,
            tabla=operacion.tabla,
            columnas=operacion.campos,
            catalogo=self._catalogo,
            secretos=self._secretos,
        )
        self.registrar_tabla(operacion.nombre_tabla, dataframe)

    def _resolver_uri_local(
        self,
        ruta: str,
    ) -> tuple[ConexionLocal, UriParseResult, Path]:
        uri = UriLib.parsear(ruta)
        conexion = self._catalogo.buscar_local(uri.conexion)
        if conexion is None:
            if self._catalogo.buscar_sftp(uri.conexion):
                raise ErrorEjecucionPlan(
                    f"Lectura directa SFTP no soportada para {ruta!r}; "
                    "descargue primero a landing local"
                )
            raise ErrorEjecucionPlan(f"Conexión local no encontrada para URI {ruta!r}")

        permitidas = {item.tabla for item in conexion.allowlist}
        if uri.ruta not in permitidas:
            raise ErrorEjecucionPlan(
                f"Ruta {uri.ruta!r} no está en allowlist de {conexion.nombre!r}"
            )

        resuelta = UriLib.resolver_local(uri, Path(conexion.ruta_base))
        return conexion, uri, resuelta

    def _cargar_csv(self, operacion: CargarCsv) -> None:
        _, _, ruta = self._resolver_uri_local(operacion.ruta)
        if not ruta.is_file():
            raise ErrorEjecucionPlan(f"CSV de entrada no encontrado: {ruta}")

        # inferSchema conserva el comportamiento del prototipo. Para producción
        # bancaria se recomienda añadir esquemas explícitos al plan para evitar
        # un recorrido adicional del archivo y conversiones dependientes del lote.
        dataframe = (
            self._spark.read.option(
                "header",
                str(operacion.tiene_header).lower(),
            )
            .option("sep", operacion.delimitador)
            .option("inferSchema", "true")
            .csv(str(ruta))
        )
        self.registrar_tabla(operacion.nombre_tabla, dataframe)

    def _cargar_local(self, operacion: CargarLocal) -> None:
        # RESIDENT se representa como una referencia al nombre de otra tabla. No
        # se copia materialmente: Spark conserva un plan lógico inmutable.
        origen_resident = self.obtener_tabla(operacion.ruta)
        if origen_resident is not None:
            self.registrar_tabla(
                operacion.nombre_tabla,
                origen_resident,
            )
            return

        _, _, ruta = self._resolver_uri_local(operacion.ruta)
        if not ruta.is_file():
            raise ErrorEjecucionPlan(f"Archivo local no encontrado: {ruta}")
        if ruta.suffix.lower() not in {".csv", ".txt"}:
            raise ErrorEjecucionPlan(f"Formato local no soportado para {ruta.name!r}")
        dataframe = (
            self._spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(str(ruta))
        )
        self.registrar_tabla(operacion.nombre_tabla, dataframe)

    _MAPA_TIPOS_AST: ClassVar[dict] = {
        TipoExpresionPlan.COLUMNA: TipoExpresion.COLUMNA,
        TipoExpresionPlan.LITERAL_NUMERO: TipoExpresion.LITERAL_NUMERO,
        TipoExpresionPlan.LITERAL_STRING: TipoExpresion.LITERAL_STRING,
        TipoExpresionPlan.FUNCION: TipoExpresion.FUNCION,
        TipoExpresionPlan.OPERACION_BINARIA: TipoExpresion.OPERACION_BINARIA,
        TipoExpresionPlan.CONCATENACION: TipoExpresion.CONCATENACION,
        TipoExpresionPlan.ALIAS: TipoExpresion.ALIAS,
        TipoExpresionPlan.WINDOW: TipoExpresion.WINDOW,
        TipoExpresionPlan.WINDOW_RANK: TipoExpresion.WINDOW_RANK,
    }

    def _a_expresion_ast(self, expresion: ExpresionPlan) -> Expresion:
        """Reconstruye el AST validado que entiende CompiladorExpresion."""
        return Expresion(
            tipo=self._MAPA_TIPOS_AST[expresion.tipo],
            valor=expresion.valor,
            hijos=tuple(self._a_expresion_ast(hijo) for hijo in expresion.hijos),
        )

    def _compilar_seleccion(self, seleccion: SeleccionPlan) -> Any:
        """Compila una selección y exige alias para cálculos no triviales."""
        ast = self._a_expresion_ast(seleccion.expresion)
        columna = CompiladorExpresion().compilar(ast)
        alias = seleccion.alias
        if alias is None and seleccion.expresion.tipo == TipoExpresionPlan.COLUMNA:
            alias = seleccion.expresion.valor
        if not alias:
            raise ErrorEjecucionPlan(
                "Toda expresión calculada de LOAD requiere un alias explícito"
            )
        return columna.alias(alias)

    def _proyectar(self, operacion: Proyectar) -> None:
        from pyspark.sql import functions as F

        origen = self._exigir_tabla(operacion.tabla_origen)
        disponibles = set(origen.columns)
        faltantes = [campo for campo in operacion.campos if campo not in disponibles]
        if faltantes:
            raise ErrorEjecucionPlan(
                f"Columna {faltantes[0]!r} no existe en {operacion.tabla_origen!r}"
            )

        if operacion.selecciones:
            # La ruta tipada permite funciones, cálculos y ventanas sin pasar
            # texto arbitrario al analizador SQL de Spark.
            expresiones = [
                self._compilar_seleccion(seleccion)
                for seleccion in operacion.selecciones
            ]
        else:
            mapa_aliases = dict(operacion.aliases)
            aliases_desconocidos = set(mapa_aliases) - set(operacion.campos)
            if aliases_desconocidos:
                raise ErrorEjecucionPlan(
                    f"Alias definido para columna no proyectada: "
                    f"{min(aliases_desconocidos)!r}"
                )
            expresiones = [
                F.col(campo).alias(mapa_aliases.get(campo, campo))
                for campo in operacion.campos
            ]

        if not expresiones:
            raise ErrorEjecucionPlan("PROYECTAR requiere al menos una selección")
        resultado = origen.select(*expresiones)
        if operacion.distinct:
            resultado = resultado.distinct()

        destino = operacion.alias or operacion.tabla_origen
        self.registrar_tabla(
            destino,
            resultado,
            reemplazar=destino in self._tablas,
        )

    def _filtrar(self, operacion: Filtrar) -> None:
        from pyspark.sql import functions as F

        origen = self._exigir_tabla(operacion.tabla_origen)
        if operacion.expresion is not None:
            condicion = CompiladorExpresion().compilar_predicado(
                self._a_expresion_ast(operacion.expresion)
            )
        else:
            # Compatibilidad con planes v1. Los planes nuevos siempre usan el
            # árbol tipado y no atraviesan este camino de texto.
            if len(operacion.condicion) > 10_000:
                raise ErrorEjecucionPlan("Condición de filtro excede el límite seguro")
            if any(
                marcador in operacion.condicion for marcador in (";", "--", "/*", "*/")
            ):
                raise ErrorEjecucionPlan(
                    "Condición de filtro contiene separadores no permitidos"
                )
            condicion = F.expr(operacion.condicion)

        resultado = origen.filter(condicion)
        self.registrar_tabla(
            operacion.tabla_origen,
            resultado,
            reemplazar=True,
        )

    def _concatenar(self, operacion: Concatenar) -> None:
        objetivo = self._exigir_tabla(operacion.tabla_objetivo)
        origen = self._exigir_tabla(operacion.tabla_origen)

        # Qlik CONCATENATE alinea por nombre y crea columnas NULL para campos
        # ausentes. unionByName con allowMissingColumns reproduce esa semántica.
        resultado = objetivo.unionByName(
            origen,
            allowMissingColumns=True,
        )
        self.registrar_tabla(
            operacion.tabla_objetivo,
            resultado,
            reemplazar=True,
        )

    @staticmethod
    def _tipo_join_spark(tipo: str) -> str:
        normalizado = tipo.strip().upper()
        permitidos = {
            "LEFT": "left",
            "INNER": "inner",
        }
        if normalizado not in permitidos:
            raise ErrorEjecucionPlan(f"Tipo de JOIN no soportado: {tipo!r}")
        return permitidos[normalizado]

    def _unir(self, operacion: Unir) -> None:
        izquierda = self._exigir_tabla(operacion.tabla_izquierda)
        derecha = self._exigir_tabla(operacion.tabla_derecha)
        tipo_join = self._tipo_join_spark(operacion.tipo_join)

        if operacion.condicion_on.strip().upper() == "NATURAL":
            columnas_derecha = set(derecha.columns)
            comunes = [
                columna for columna in izquierda.columns if columna in columnas_derecha
            ]
            if not comunes:
                raise ErrorEjecucionPlan(
                    "JOIN natural sin columnas comunes; se bloqueó un "
                    "producto cartesiano"
                )
            # Pasar nombres a ``on`` elimina una sola copia de cada clave común.
            resultado = izquierda.join(
                derecha,
                on=comunes,
                how=tipo_join,
            )
        else:
            resultado = self._unir_condicion_simple(
                izquierda,
                derecha,
                operacion.condicion_on,
                tipo_join,
            )

        self.registrar_tabla(
            operacion.tabla_izquierda,
            resultado,
            reemplazar=True,
        )

    def _unir_condicion_simple(
        self,
        izquierda: Any,
        derecha: Any,
        condicion_texto: str,
        tipo_join: str,
    ) -> Any:
        """Ejecuta únicamente igualdad columna=columna sin evaluar SQL libre."""
        from pyspark.sql import functions as F

        coincidencia = PATRON_JOIN_SIMPLE.fullmatch(condicion_texto)
        if not coincidencia:
            raise ErrorEjecucionPlan(
                f"Condición JOIN no soportada: {condicion_texto!r}"
            )

        columna_izquierda = coincidencia.group("izquierda")
        columna_derecha = coincidencia.group("derecha")
        if columna_izquierda not in izquierda.columns:
            raise ErrorEjecucionPlan(
                f"Columna JOIN izquierda no encontrada: {columna_izquierda!r}"
            )
        if columna_derecha not in derecha.columns:
            raise ErrorEjecucionPlan(
                f"Columna JOIN derecha no encontrada: {columna_derecha!r}"
            )

        # Los aliases evitan que ``col('id')`` sea ambiguo cuando ambas tablas
        # contienen campos con el mismo nombre pero el JOIN usa claves distintas.
        izquierda_alias = izquierda.alias("__izquierda")
        derecha_alias = derecha.alias("__derecha")
        condicion = F.col(f"__izquierda.{columna_izquierda}") == F.col(
            f"__derecha.{columna_derecha}"
        )
        unido = izquierda_alias.join(
            derecha_alias,
            on=condicion,
            how=tipo_join,
        )

        # Qlik conserva una sola copia de campos homónimos. Se prioriza la tabla
        # izquierda y solo se anexan columnas nuevas de la derecha.
        nombres_izquierda = set(izquierda.columns)
        seleccion = [
            F.col(f"__izquierda.{nombre}").alias(nombre) for nombre in izquierda.columns
        ]
        seleccion.extend(
            F.col(f"__derecha.{nombre}").alias(nombre)
            for nombre in derecha.columns
            if nombre not in nombres_izquierda
        )
        return unido.select(*seleccion)

    def _compilar_agregacion(
        self,
        expresion: str,
        columnas_disponibles: set[str],
    ) -> Any:
        """Convierte una agregación declarativa a una función Spark conocida."""
        from pyspark.sql import functions as F

        coincidencia = PATRON_AGREGACION.fullmatch(expresion)
        if not coincidencia:
            raise ErrorEjecucionPlan(f"Agregación no soportada: {expresion!r}")

        funcion = coincidencia.group("funcion").upper()
        columna = coincidencia.group("columna")
        distinct = coincidencia.group("distinct") is not None
        alias = coincidencia.group("alias")

        if columna != "*" and columna not in columnas_disponibles:
            raise ErrorEjecucionPlan(
                f"Columna de agregación no encontrada: {columna!r}"
            )
        if distinct and funcion != "COUNT":
            raise ErrorEjecucionPlan(
                f"DISTINCT solo está permitido con COUNT, no con {funcion}"
            )
        if columna == "*" and funcion != "COUNT":
            raise ErrorEjecucionPlan(f"{funcion}(*) no está permitido")

        if funcion == "COUNT":
            if distinct:
                resultado = F.countDistinct(F.col(columna))
            elif columna == "*":
                resultado = F.count(F.lit(1))
            else:
                resultado = F.count(F.col(columna))
        elif funcion == "SUM":
            resultado = F.sum(F.col(columna))
        elif funcion == "AVG":
            resultado = F.avg(F.col(columna))
        elif funcion == "MIN":
            resultado = F.min(F.col(columna))
        elif funcion == "MAX":
            resultado = F.max(F.col(columna))
        else:  # pragma: no cover - la expresión regular cierra el conjunto.
            raise ErrorEjecucionPlan(
                f"Función de agregación no implementada: {funcion}"
            )

        return resultado.alias(alias)

    def _agregar(self, operacion: Agregar) -> None:
        from pyspark.sql import functions as F

        origen = self._exigir_tabla(operacion.tabla_origen)
        disponibles = set(origen.columns)
        faltantes = [
            columna for columna in operacion.grupo_por if columna not in disponibles
        ]
        if faltantes:
            raise ErrorEjecucionPlan(
                f"Columna GROUP BY no encontrada: {faltantes[0]!r}"
            )

        grupos = [F.col(columna) for columna in operacion.grupo_por]
        if operacion.selecciones:
            aliases_grupo: dict[str, str] = {}
            metricas: list[Any] = []
            aliases_metricas: list[str] = []
            for seleccion in operacion.selecciones:
                expresion = seleccion.expresion
                if (
                    expresion.tipo == TipoExpresionPlan.COLUMNA
                    and expresion.valor in operacion.grupo_por
                ):
                    aliases_grupo[expresion.valor] = seleccion.alias or expresion.valor
                    continue
                metrica = self._compilar_seleccion(seleccion)
                metricas.append(metrica)
                aliases_metricas.append(seleccion.alias or "")

            if metricas:
                resultado = origen.groupBy(*grupos).agg(*metricas)
            else:
                # GROUP BY sin métricas equivale a las combinaciones distintas
                # de las claves, sin inventar una columna COUNT auxiliar.
                resultado = origen.select(*grupos).distinct()

            seleccion_final = [
                F.col(columna).alias(aliases_grupo.get(columna, columna))
                for columna in operacion.grupo_por
            ]
            seleccion_final.extend(F.col(alias) for alias in aliases_metricas if alias)
            resultado = resultado.select(*seleccion_final)
        else:
            metricas = [
                self._compilar_agregacion(expresion, disponibles)
                for expresion in operacion.funciones
            ]
            if not metricas:
                raise ErrorEjecucionPlan(
                    "AGREGAR requiere selecciones tipadas o funciones v1"
                )
            resultado = origen.groupBy(*grupos).agg(*metricas)

        destino = operacion.tabla_resultado or operacion.tabla_origen
        self.registrar_tabla(
            destino,
            resultado,
            reemplazar=destino in self._tablas,
        )

    def _eliminar_tabla(self, operacion: EliminarTabla) -> None:
        if operacion.nombre not in self._tablas:
            raise ErrorEjecucionPlan(
                f"No se puede eliminar una tabla inexistente: {operacion.nombre!r}"
            )

        # Eliminar la referencia del registro libera el plan lógico para futuras
        # operaciones. Spark decidirá cuándo liberar datos cacheados explícitos.
        del self._tablas[operacion.nombre]

    def _materializar_csv_unico(
        self,
        dataframe: Any,
        directorio_staging: Path,
        nombre_archivo: str,
    ) -> Path:
        """Materializa exactamente un CSV para publicación fuera de Spark.

        Spark escribe directorios y nombres ``part-*``. ``coalesce(1)`` se usa
        únicamente en la frontera de publicación porque STORE espera un archivo
        único; todas las transformaciones anteriores permanecen distribuidas.
        """
        salida_spark = directorio_staging / "spark-output"
        (
            dataframe.coalesce(1)
            .write.mode("overwrite")
            .option("header", "true")
            .option("encoding", "UTF-8")
            .csv(salida_spark.resolve().as_uri())
        )
        partes = sorted(salida_spark.glob("part-*.csv"))
        if len(partes) != 1:
            raise ErrorEjecucionPlan(
                f"Spark generó {len(partes)} archivos CSV; se esperaba exactamente uno"
            )

        staged = directorio_staging / nombre_archivo
        shutil.move(str(partes[0]), staged)
        return staged

    @staticmethod
    def _ruta_permitida(conexion: Any, ruta: str) -> bool:
        """Comprueba la ruta completa, no solo el nombre final del archivo."""
        return any(item.tabla == ruta for item in conexion.allowlist)

    @staticmethod
    def _normalizar_tipo_sql(tipo: Any) -> str:
        """Normaliza un tipo Spark a una forma de DDL/Impala-Hive aceptable."""
        nombre_tipo = str(tipo).upper()
        if hasattr(tipo, "simpleString"):
            nombre_tipo = str(tipo.simpleString()).upper()
        elif hasattr(tipo, "typeName"):
            nombre_tipo = str(tipo.typeName()).upper()

        mapa = {
            "BYTE": "TINYINT",
            "SHORT": "SMALLINT",
            "INT": "INT",
            "INTEGER": "INT",
            "LONG": "BIGINT",
            "BIGINT": "BIGINT",
            "FLOAT": "FLOAT",
            "DOUBLE": "DOUBLE",
            "DECIMAL": "DECIMAL",
            "BOOLEAN": "BOOLEAN",
            "STRING": "STRING",
            "VARCHAR": "STRING",
            "CHAR": "STRING",
            "DATE": "DATE",
            "TIMESTAMP": "TIMESTAMP",
            "BINARY": "BINARY",
        }
        return mapa.get(nombre_tipo, nombre_tipo)

    def _crear_columna_types_hive(self, dataframe: Any) -> str | None:
        """Construye el string de `createTableColumnTypes` para Hive/Impala."""
        try:
            columnas = []
            for campo in dataframe.schema:
                nombre = str(campo.name).strip()
                tipo = self._normalizar_tipo_sql(campo.dataType)
                columnas.append(f"{nombre} {tipo}")
            if not columnas:
                return None
            return ", ".join(columnas)
        except Exception:
            return None

    def _publicar_base_destino_hive_compat(self, dataframe: Any, opciones: dict[str, str]) -> None:
        """Fallback de escritura para Hive/Impala cuando Spark JDBC no soporta addBatch().

        Se ejecuta en el driver para evitar serializar el ``SparkSession`` dentro de
        una lambda de partición, lo cual rompe con la restricción de Spark
        ``CONTEXT_ONLY_VALID_ON_DRIVER``.
        """
        columnas = [str(campo.name).strip() for campo in dataframe.schema]
        if not columnas:
            raise ErrorEjecucionPlan("No se pudieron identificar columnas para la escritura JDBC compatible")

        esquema_campos = tuple(dataframe.schema)
        nombres_sql = ", ".join(columnas)
        url = str(opciones["url"])
        usuario = str(opciones.get("user") or "")
        password = str(opciones.get("password") or "")
        driver = str(opciones.get("driver") or "")
        jvm = self._spark._jvm

        def _valor_fila(fila: Any, nombre_columna: str) -> Any:
            if hasattr(fila, "__getitem__"):
                try:
                    return fila[nombre_columna]
                except Exception:
                    pass
            return getattr(fila, nombre_columna, None)

        def _escapar_literal(valor: str) -> str:
            return str(valor).replace("'", "''")

        def _resolver_tipos_destino(conn: Any) -> dict[str, str]:
            tipos: dict[str, str] = {}
            try:
                metadata = conn.getMetaData()
                if metadata is not None:
                    schema = None
                    tabla = opciones["dbtable"]
                    if "." in tabla:
                        schema, tabla = tabla.split(".", 1)
                    columnas_rs = metadata.getColumns(None, schema, tabla, "%")
                    try:
                        while columnas_rs.next():
                            nombre = str(columnas_rs.getString("COLUMN_NAME") or "").strip()
                            tipo = str(columnas_rs.getString("TYPE_NAME") or "").upper()
                            if nombre:
                                tipos[nombre] = tipo
                    finally:
                        try:
                            columnas_rs.close()
                        except Exception:
                            pass
            except Exception:
                pass
            if tipos:
                return tipos

            try:
                stmt_meta = conn.createStatement()
                try:
                    rs_desc = stmt_meta.executeQuery(f"DESCRIBE {opciones['dbtable']}")
                    try:
                        while rs_desc.next():
                            nombre = str(rs_desc.getString(1) or "").strip()
                            tipo = str(rs_desc.getString(2) or "").upper()
                            if nombre:
                                tipos[nombre] = tipo
                    finally:
                        try:
                            rs_desc.close()
                        except Exception:
                            pass
                finally:
                    try:
                        stmt_meta.close()
                    except Exception:
                        pass
            except Exception:
                pass
            return tipos

        def _literal_sql(valor: Any, tipo: Any, tipo_destino: str | None = None) -> str:
            if valor is None:
                return "NULL"
            tipo_destino_normalizado = str(tipo_destino or "").upper()
            if tipo_destino_normalizado in {"STRING", "VARCHAR", "CHAR"}:
                return f"'{_escapar_literal(str(valor))}'"
            if tipo_destino_normalizado in {"TINYINT", "SMALLINT", "INT", "BIGINT"}:
                return str(int(valor))
            if tipo_destino_normalizado == "DECIMAL":
                precision = getattr(tipo, "precision", None) or 38
                scale = getattr(tipo, "scale", None) or 0
                return f"CAST('{_escapar_literal(str(valor))}' AS DECIMAL({precision},{scale}))"
            if tipo_destino_normalizado == "DATE":
                if isinstance(valor, date):
                    return f"CAST('{valor.isoformat()}' AS DATE)"
                return f"CAST('{_escapar_literal(str(valor))}' AS DATE)"
            if tipo_destino_normalizado == "TIMESTAMP":
                if isinstance(valor, datetime):
                    valor_iso = valor.isoformat(sep=" ", timespec="microseconds")
                    return f"CAST('{_escapar_literal(valor_iso)}' AS TIMESTAMP)"
                return f"CAST('{_escapar_literal(str(valor))}' AS TIMESTAMP)"
            if isinstance(tipo, DecimalType):
                precision = getattr(tipo, "precision", None) or 38
                scale = getattr(tipo, "scale", None) or 0
                return f"CAST('{_escapar_literal(str(valor))}' AS DECIMAL({precision},{scale}))"
            if isinstance(tipo, DateType):
                if isinstance(valor, date):
                    return f"CAST('{valor.isoformat()}' AS DATE)"
                return f"CAST('{_escapar_literal(str(valor))}' AS DATE)"
            if isinstance(tipo, TimestampType):
                if isinstance(valor, datetime):
                    valor_iso = valor.isoformat(sep=" ", timespec="microseconds")
                    return f"CAST('{_escapar_literal(valor_iso)}' AS TIMESTAMP)"
                return f"CAST('{_escapar_literal(str(valor))}' AS TIMESTAMP)"
            if isinstance(tipo, BooleanType):
                return "TRUE" if bool(valor) else "FALSE"
            if isinstance(tipo, (LongType, IntegerType, ShortType, FloatType, DoubleType)):
                return str(valor)
            return f"'{_escapar_literal(str(valor))}'"

        try:
            if driver:
                jvm.java.lang.Class.forName(driver)

            filas = dataframe.collect()
            conn = jvm.java.sql.DriverManager.getConnection(url, usuario, password)
            conn.setAutoCommit(True)
            tipos_destino = _resolver_tipos_destino(conn)
            stmt = conn.createStatement()
            try:
                for fila in filas:
                    valores_sql = []
                    for campo in esquema_campos:
                        valor = _valor_fila(fila, str(campo.name))
                        tipo = campo.dataType
                        tipo_destino = tipos_destino.get(str(campo.name).strip())
                        valores_sql.append(_literal_sql(valor, tipo, tipo_destino))
                    sql_insert = (
                        f"INSERT INTO {opciones['dbtable']} ({nombres_sql}) "
                        f"VALUES ({', '.join(valores_sql)})"
                    )
                    stmt.executeUpdate(sql_insert)
            finally:
                try:
                    stmt.close()
                finally:
                    conn.close()
        except Exception as exc:
            raise ErrorEjecucionPlan(
                f"Fallback JDBC Hive/Impala no pudo completar la inserción: {exc}"
            ) from exc

    def _publicar(self, operacion: Publicar) -> None:
        formato = operacion.formato.strip().lower()
        if formato not in {"txt", "csv"}:
            raise ErrorEjecucionPlan(
                f"Formato STORE no soportado: {operacion.formato!r}"
            )

        dataframe = self._exigir_tabla(operacion.tabla_origen)

        if self._base_destino is not None:
            try:
                uri = UriLib.parsear(operacion.destino)
                nombre_tabla = PurePosixPath(uri.ruta).stem
                if not nombre_tabla:
                    nombre_tabla = operacion.tabla_origen
            except Exception:
                nombre_tabla = operacion.tabla_origen

            nombre_tabla_limpio = re.sub(r"[^A-Za-z0-9_]", "_", nombre_tabla)
            opciones = self._base_destino.a_opciones_jdbc(nombre_tabla_limpio)

            writer = dataframe.write.mode(self._base_destino.modo).format("jdbc")
            for k, v in opciones.items():
                writer = writer.option(k, v)

            url_bd = str(self._base_destino.url).lower()
            driver_bd = str(self._base_destino.driver).lower()
            if "hive" in url_bd or "impala" in url_bd or "hive" in driver_bd:
                create_table_types = self._crear_columna_types_hive(dataframe)
                if create_table_types:
                    writer = writer.option("createTableColumnTypes", create_table_types)

            try:
                writer.save()
            except Exception as exc:
                texto_error = str(exc)
                if (
                    "hive" in url_bd
                    or "impala" in url_bd
                    or "hive" in driver_bd
                ) and (
                    "addBatch" in texto_error or "SQLFeatureNotSupportedException" in texto_error
                ):
                    self._publicar_base_destino_hive_compat(dataframe, opciones)
                    manifiesto_bd = {
                        "tipo": "base_destino",
                        "tabla": opciones["dbtable"],
                        "url": self._base_destino.url,
                        "tabla_origen": operacion.tabla_origen,
                    }
                    self._publicaciones.append(manifiesto_bd)
                    return
                raise

            manifiesto_bd = {
                "tipo": "base_destino",
                "tabla": opciones["dbtable"],
                "url": self._base_destino.url,
                "tabla_origen": operacion.tabla_origen,
            }
            self._publicaciones.append(manifiesto_bd)
            return

        nombre_conexion = UriLib.obtener_conexion(operacion.destino)
        conexion_local = self._catalogo.buscar_local(nombre_conexion)
        conexion_sftp = self._catalogo.buscar_sftp(nombre_conexion)
        conexion = conexion_local or conexion_sftp

        if conexion is None:
            raise ErrorEjecucionPlan(
                f"Conexión de publicación no encontrada: {nombre_conexion!r}"
            )

        uri = UriLib.parsear(
            operacion.destino,
            ruta_base=conexion.ruta_base,
        )

        if conexion_local is not None:
            manifiesto = self._publicar_local(
                dataframe,
                uri,
                conexion_local,
            )
        elif conexion_sftp is not None:
            manifiesto = self._publicar_sftp(
                dataframe,
                uri,
                conexion_sftp,
            )
        else:  # pragma: no cover - ``conexion`` ya descartó este caso.
            raise ErrorEjecucionPlan(
                f"Conexión de publicación no encontrada: {nombre_conexion!r}"
            )

        self._publicaciones.append(manifiesto.a_dict())


    def _publicar_local(
        self,
        dataframe: Any,
        uri: UriParseResult,
        conexion: ConexionLocal,
    ):
        """Publica localmente mediante copia parcial y promoción atómica."""
        if not self._ruta_permitida(conexion, uri.ruta):
            raise ErrorEjecucionPlan(
                f"Destino {uri.ruta!r} no está en allowlist de {conexion.nombre!r}"
            )

        base = Path(conexion.ruta_base)
        destino = UriLib.resolver_local(uri, base)
        destino.parent.mkdir(parents=True, exist_ok=True)
        staging = StagingManager(base)
        directorio_staging = staging.crear_staging_salida(
            self._ejecucion_id,
            f"{destino.name}-{len(self._publicaciones)}",
        )

        try:
            staged = self._materializar_csv_unico(
                dataframe,
                directorio_staging,
                destino.name,
            )
            return PublicacionLocal(base).publicar(staged, destino)
        finally:
            # Incluso cuando Spark o la promoción fallan, no quedan archivos que
            # una siguiente ejecución pueda confundir con una salida válida.
            staging.limpiar_staging(self._ejecucion_id)

    def _publicar_sftp(
        self,
        dataframe: Any,
        uri: UriParseResult,
        conexion: ConexionSftp,
    ):
        """Materializa localmente y sube por SFTP usando rename remoto."""
        import tempfile

        if not self._ruta_permitida(conexion, uri.ruta):
            raise ErrorEjecucionPlan(
                f"Destino {uri.ruta!r} no está en allowlist de {conexion.nombre!r}"
            )

        # La autenticación se resuelve antes de materializar el CSV para no
        # invertir tiempo de Spark si falta la clave o una credencial requerida.
        parametros_auth: dict[str, Any]
        if conexion.secreto_clave_privada_nombre:
            clave_b64 = self._secretos.obtener_obligatorio(
                conexion.secreto_clave_privada_nombre
            )
            passphrase = (
                self._secretos.obtener_obligatorio(conexion.secreto_passphrase_nombre)
                if conexion.secreto_passphrase_nombre
                else None
            )
            parametros_auth = {
                "usuario": conexion.usuario,
                "clave_privada_contenido": decodificar_clave_privada_base64(clave_b64),
                "passphrase": passphrase,
            }
        elif conexion.clave_privada:
            passphrase = (
                self._secretos.obtener_obligatorio(conexion.secreto_passphrase_nombre)
                if conexion.secreto_passphrase_nombre
                else None
            )
            parametros_auth = {
                "usuario": conexion.usuario,
                "clave_privada": Path(conexion.clave_privada).expanduser(),
                "passphrase": passphrase,
            }
        else:
            credencial = self._secretos.obtener_obligatorio(conexion.secreto_nombre)
            usuario, separador, password = credencial.partition(":")
            if not separador or not usuario or not password:
                raise ErrorEjecucionPlan(
                    f"Secreto {conexion.secreto_nombre!r} debe usar USUARIO:CLAVE"
                )
            parametros_auth = {
                "usuario": usuario,
                "password": password,
            }

        base_temporal = (
            Path(tempfile.gettempdir()) / "motor-spark-dataflow" / self._ejecucion_id
        )
        staging = StagingManager(base_temporal)
        nombre_archivo = PurePosixPath(uri.ruta).name
        directorio_staging = staging.crear_staging_salida(
            self._ejecucion_id,
            f"{nombre_archivo}-{len(self._publicaciones)}",
        )

        try:
            staged = self._materializar_csv_unico(
                dataframe,
                directorio_staging,
                nombre_archivo,
            )
            ruta_remota = PurePosixPath(conexion.ruta_base) / uri.ruta
            with PublicacionSftp(
                host=conexion.host,
                puerto=conexion.puerto,
                **parametros_auth,
            ) as publicador:
                return publicador.publicar(
                    staged,
                    Path(str(ruta_remota)),
                )
        finally:
            staging.limpiar_staging(self._ejecucion_id)
