# Motor Spark Modular Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convertir el motor Spark monolítico en un paquete modular, tipado con Pydantic v2 y extensible mediante un registro de transformaciones, manteniendo exactamente el comando `python motor.py`, los contratos JSON, los códigos de salida y los eventos de consola existentes.

**Architecture:** `motor.py` será un adaptador mínimo que delega a `motor_spark.aplicacion.ejecutor_motor`. La configuración se validará antes de crear Spark; las reglas de esquema y columnas vivirán en dominio; el acceso a Spark/Hadoop se aislará en infraestructura; las transformaciones se resolverán mediante un registro estático; y el flujo incremental será un servicio de aplicación independiente.

**Tech Stack:** Python 3.10+, PySpark compatible con el Remote Engine instalado, Pydantic `>=2,<3`, pytest, pytest-cov.

## Global Constraints

- Mantener exactamente los argumentos CLI actuales, incluidos obligatoriedad, valores predeterminados y nombres.
- Mantener los códigos de salida `0` para éxito y `1` para error.
- Mantener las recetas JSON actuales sin modificaciones.
- Mantener la estructura actual de los JSON de éxito y error.
- Mantener los eventos contractuales `CLAVE=valor` y su orden relativo.
- Mantener JSON directo o ruta de archivo como entrada de `--receta`.
- Mantener semántica incremental, validación Parquet, `_SUCCESS` y evolución de esquema.
- No actualizar PySpark como parte del refactor.
- Usar Pydantic v2 y traducir `ValidationError` a `ErrorReceta` estable.
- No implementar plugins dinámicos ni nuevas transformaciones.
- Mantener `motor.py` sin reglas Spark ni lógica de negocio.
- Objetivo inicial de cobertura: 85 % en módulos propios, excluyendo integraciones dependientes del servidor.

## Mapa de archivos

```text
motor.py                                      # Adaptador CLI compatible
motor_spark/aplicacion/ejecutor_motor.py      # Orquestación y ciclo de vida Spark
motor_spark/aplicacion/ejecutor_incremental.py# Deducción incremental y métricas
motor_spark/aplicacion/resultado_ejecucion.py # Construcción de JSON contractual
motor_spark/configuracion/argumentos.py       # argparse y ArgumentosEjecucion
motor_spark/configuracion/cargador_receta.py  # JSON/archivo + validación Pydantic
motor_spark/configuracion/modelos/*.py        # Modelos tipados de receta y pasos
motor_spark/dominio/errores.py                 # ErrorReceta
motor_spark/dominio/tipos_spark.py             # Alias y DecimalType
motor_spark/dominio/esquemas.py                # StructType y evolución
motor_spark/dominio/columnas.py                # Normalización y exigencia
motor_spark/infraestructura/spark/sesion.py    # Creación/configuración Spark
motor_spark/infraestructura/spark/lector.py    # Lectura de DataFrame
motor_spark/infraestructura/spark/escritor.py  # Escritura de DataFrame
motor_spark/infraestructura/spark/sistema_archivos.py # Hadoop/local y métricas
motor_spark/infraestructura/resultados/escritor_json.py # Escritura atómica
motor_spark/transformaciones/*.py              # Manejadores por responsabilidad
motor_spark/transformaciones/registro.py       # Mapa tipo -> manejador
motor_spark/transformaciones/ejecutor.py        # PASO_INICIO/PASO_FIN
motor_spark/compartido/booleanos.py             # Conversión compatible
motor_spark/compartido/eventos_consola.py       # Emisión contractual
legacy/motor_original.py                        # Referencia congelada para pruebas
```

---

### Task 1: Congelar comportamiento y crear el esqueleto instalable

**Files:**
- Create: `legacy/motor_original.py`
- Create: `pyproject.toml`
- Create: `requirements.txt`
- Create: `requirements-dev.txt`
- Create: `.gitignore`
- Create: `tests/unitarios/test_argumentos.py`
- Create: `tests/unitarios/test_resultado_json.py`
- Create: package `__init__.py` files shown in the architecture
- Copy from: `/mnt/data/Código pegado: py(2)`

**Interfaces:**
- Produces: package importable as `motor_spark`.
- Produces: `ArgumentosEjecucion` and `crear_argumentos()` in Task 2.
- Preserves: frozen original at `legacy/motor_original.py` for differential tests.

- [ ] **Step 1: Copy the original motor unchanged**

```bash
mkdir -p legacy
cp '/mnt/data/Código pegado: py(2)' legacy/motor_original.py
chmod +x legacy/motor_original.py
```

- [ ] **Step 2: Add packaging and dependency metadata**

```toml
[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[project]
name = "motor-spark-recetas"
version = "0.1.0"
requires-python = ">=3.10"
dependencies = [
  "pydantic>=2,<3",
  "pyspark",
]

[tool.pytest.ini_options]
pythonpath = ["."]
testpaths = ["tests"]
addopts = "-ra"

[tool.coverage.run]
source = ["motor_spark"]
omit = ["tests/*"]
```

```text
# requirements.txt
pydantic>=2,<3
pyspark
```

```text
# requirements-dev.txt
-r requirements.txt
pytest
pytest-cov
```

- [ ] **Step 3: Create package directories and empty initializers**

```bash
mkdir -p \
  motor_spark/aplicacion \
  motor_spark/configuracion/modelos \
  motor_spark/dominio \
  motor_spark/infraestructura/spark \
  motor_spark/infraestructura/resultados \
  motor_spark/transformaciones \
  motor_spark/compartido \
  tests/unitarios tests/integracion tests/compatibilidad \
  tests/recursos/recetas tests/recursos/datos
find motor_spark -type d -exec touch {}/__init__.py \;
```

- [ ] **Step 4: Add a parser characterization test before implementation**

```python
# tests/unitarios/test_argumentos.py
import pytest

from motor_spark.configuracion.argumentos import crear_argumentos


def test_parser_conserva_argumentos_obligatorios():
    parser = crear_argumentos()

    with pytest.raises(SystemExit) as error:
        parser.parse_args([])

    assert error.value.code == 2


def test_parser_acepta_contrato_actual():
    argumentos = crear_argumentos().parse_args([
        "--receta", "{}",
        "--entrada", "/tmp/in.csv",
        "--salida", "/tmp/out",
        "--esquema", "id:entero",
        "--resultado", "/tmp/result.json",
        "--ejecucion-id", "e-1",
    ])

    assert argumentos.receta == "{}"
    assert argumentos.entrada == "/tmp/in.csv"
    assert argumentos.salida == "/tmp/out"
    assert argumentos.esquema == "id:entero"
    assert argumentos.resultado == "/tmp/result.json"
    assert argumentos.ejecucion_id == "e-1"
```

- [ ] **Step 5: Run the characterization test and confirm it fails**

Run: `pytest tests/unitarios/test_argumentos.py -q`

Expected: collection failure because `motor_spark.configuracion.argumentos` does not exist.

- [ ] **Step 6: Commit the scaffold and frozen reference**

```bash
git add legacy pyproject.toml requirements*.txt .gitignore motor_spark tests

git commit -m "test: freeze original motor behavior"
```

---

### Task 2: Extraer CLI, errores, booleanos, eventos y persistencia JSON

**Files:**
- Create: `motor_spark/dominio/errores.py`
- Create: `motor_spark/configuracion/argumentos.py`
- Create: `motor_spark/compartido/booleanos.py`
- Create: `motor_spark/compartido/eventos_consola.py`
- Create: `motor_spark/infraestructura/resultados/escritor_json.py`
- Modify: `tests/unitarios/test_argumentos.py`
- Create: `tests/unitarios/test_booleanos.py`
- Create: `tests/unitarios/test_eventos_consola.py`
- Create: `tests/unitarios/test_escritor_json.py`

**Interfaces:**
- Produces: `class ErrorReceta(Exception)`.
- Produces: immutable `ArgumentosEjecucion(receta, entrada, salida, esquema, resultado, ejecucion_id)`.
- Produces: `crear_argumentos() -> argparse.ArgumentParser` and `analizar_argumentos(argv: Sequence[str] | None = None) -> ArgumentosEjecucion`.
- Produces: `convertir_booleano(valor: Any, predeterminado: bool) -> bool`.
- Produces: `emitir(evento: str, *, error: bool = False) -> None`.
- Produces: `guardar_resultado(ruta: str | None, contenido: dict[str, Any]) -> None`.

- [ ] **Step 1: Write tests for boolean aliases and invalid values**

```python
# tests/unitarios/test_booleanos.py
import pytest

from motor_spark.compartido.booleanos import convertir_booleano
from motor_spark.dominio.errores import ErrorReceta


@pytest.mark.parametrize("valor", [True, "1", "true", "si", "sí", "yes"])
def test_convertir_booleano_verdadero(valor):
    assert convertir_booleano(valor, False) is True


@pytest.mark.parametrize("valor", [False, "0", "false", "no"])
def test_convertir_booleano_falso(valor):
    assert convertir_booleano(valor, True) is False


def test_convertir_booleano_usa_predeterminado_para_none():
    assert convertir_booleano(None, True) is True


def test_convertir_booleano_rechaza_valor_desconocido():
    with pytest.raises(ErrorReceta, match="Valor booleano inválido"):
        convertir_booleano("quizas", False)
```

- [ ] **Step 2: Write tests for atomic JSON output and contract formatting**

```python
# tests/unitarios/test_escritor_json.py
import json

from motor_spark.infraestructura.resultados.escritor_json import guardar_resultado


def test_guardar_resultado_es_atomico_y_utf8(tmp_path):
    destino = tmp_path / "sub" / "resultado.json"
    guardar_resultado(str(destino), {"estado": "COMPLETADO", "texto": "sí"})

    assert json.loads(destino.read_text(encoding="utf-8")) == {
        "estado": "COMPLETADO",
        "texto": "sí",
    }
    assert not destino.with_suffix(".json.tmp").exists()


def test_guardar_resultado_ignora_ruta_none():
    guardar_resultado(None, {"estado": "COMPLETADO"})
```

- [ ] **Step 3: Implement the small compatibility modules**

```python
# motor_spark/dominio/errores.py
class ErrorReceta(Exception):
    """Error de validación o ejecución de una receta."""
```

```python
# motor_spark/compartido/eventos_consola.py
import sys


def emitir(evento: str, *, error: bool = False) -> None:
    print(evento, file=sys.stderr if error else sys.stdout, flush=True)
```

```python
# motor_spark/configuracion/argumentos.py
from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Sequence


@dataclass(frozen=True, slots=True)
class ArgumentosEjecucion:
    receta: str
    entrada: str
    salida: str
    esquema: str
    resultado: str | None
    ejecucion_id: str


def crear_argumentos() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Motor Spark mecánico dirigido por recetas JSON"
    )
    parser.add_argument("--receta", required=True)
    parser.add_argument("--entrada", required=True)
    parser.add_argument("--salida", required=True)
    parser.add_argument(
        "--esquema",
        required=False,
        default="",
        help=(
            "Esquema columna:tipo separado por |. "
            "Solo se usa cuando entrada.modo_esquema=estricto."
        ),
    )
    parser.add_argument("--resultado", default=None)
    parser.add_argument("--ejecucion-id", required=True)
    return parser


def analizar_argumentos(argv: Sequence[str] | None = None) -> ArgumentosEjecucion:
    valores = crear_argumentos().parse_args(argv)
    return ArgumentosEjecucion(
        receta=valores.receta,
        entrada=valores.entrada,
        salida=valores.salida,
        esquema=valores.esquema,
        resultado=valores.resultado,
        ejecucion_id=valores.ejecucion_id,
    )
```

- [ ] **Step 4: Run focused tests**

Run: `pytest tests/unitarios/test_argumentos.py tests/unitarios/test_booleanos.py tests/unitarios/test_escritor_json.py -q`

Expected: all tests pass without importing PySpark.

- [ ] **Step 5: Commit compatibility utilities**

```bash
git add motor_spark tests/unitarios

git commit -m "refactor: extract cli and shared compatibility utilities"
```

---

### Task 3: Modelar y cargar recetas con Pydantic v2

**Files:**
- Create: `motor_spark/configuracion/modelos/entrada.py`
- Create: `motor_spark/configuracion/modelos/salida.py`
- Create: `motor_spark/configuracion/modelos/incremental.py`
- Create: `motor_spark/configuracion/modelos/pasos.py`
- Create: `motor_spark/configuracion/modelos/receta.py`
- Create: `motor_spark/configuracion/cargador_receta.py`
- Create: `tests/unitarios/test_modelos_receta.py`
- Create: `tests/unitarios/test_cargador_receta.py`

**Interfaces:**
- Produces: `RecetaConfig`, `EntradaConfig`, `SalidaConfig`, `IncrementalConfig`, `SparkConfig`, `AuditoriaConfig`.
- Produces: discriminated `PasoConfig` with all eleven existing step types.
- Produces: `cargar_receta(valor: str) -> RecetaConfig`.
- Produces: `RecetaConfig.a_dict_compatible() -> dict[str, Any]` using `model_dump(exclude_none=True)` when raw dictionary semantics are needed.

- [ ] **Step 1: Write tests for current defaults and discriminated steps**

```python
# tests/unitarios/test_modelos_receta.py
import pytest

from motor_spark.configuracion.modelos.receta import RecetaConfig
from motor_spark.dominio.errores import ErrorReceta


def test_receta_aplica_predeterminados_actuales():
    receta = RecetaConfig.model_validate({
        "entrada": {},
        "salida": {},
        "pasos": [],
    })

    assert receta.nombre == "Motor Spark mecánico"
    assert receta.entrada.formato == "csv"
    assert receta.entrada.modo_esquema == "estricto"
    assert receta.salida.formato == "parquet"
    assert receta.salida.modo == "error"
    assert receta.salida.compresion == "snappy"
    assert receta.incremental.activo is False
    assert receta.auditoria.contar_registros is False


def test_receta_construye_paso_filtrar_tipado():
    receta = RecetaConfig.model_validate({
        "entrada": {},
        "salida": {},
        "pasos": [{"tipo": "filtrar", "expresion": "total > 0"}],
    })

    assert receta.pasos[0].tipo == "filtrar"
    assert receta.pasos[0].expresion == "total > 0"
```

- [ ] **Step 2: Write loader tests for direct JSON, file JSON and deterministic errors**

```python
# tests/unitarios/test_cargador_receta.py
import pytest

from motor_spark.configuracion.cargador_receta import cargar_receta
from motor_spark.dominio.errores import ErrorReceta


def test_cargar_receta_desde_json_directo():
    receta = cargar_receta('{"entrada": {}, "salida": {}, "pasos": []}')
    assert receta.entrada.formato == "csv"


def test_cargar_receta_desde_archivo(tmp_path):
    archivo = tmp_path / "receta.json"
    archivo.write_text('{"entrada": {}, "salida": {}, "pasos": []}', encoding="utf-8")
    assert cargar_receta(str(archivo)).salida.formato == "parquet"


def test_cargar_receta_reporta_linea_y_columna_json():
    with pytest.raises(ErrorReceta, match=r"línea=1, columna=2"):
        cargar_receta("{")


def test_cargar_receta_rechaza_raiz_no_objeto():
    with pytest.raises(ErrorReceta, match="La receta debe ser un objeto JSON"):
        cargar_receta("[]")
```

- [ ] **Step 3: Implement compatible Pydantic models**

Use `ConfigDict(extra="allow")` at the recipe-section boundary so previously ignored keys remain accepted. Use `extra="forbid"` only for individual step models after tests confirm no recipe relies on unknown step keys. Normalize `modo_esquema` in a `field_validator` and preserve the aliases `strict`, `schema`, `infer`, `inferido`, `dinamico`, and `dinámico`.

```python
class EntradaConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    formato: str = "csv"
    modo_esquema: Literal["estricto", "inferir"] = "estricto"
    opciones: dict[str, Any] = Field(default_factory=dict)
    inferir_tipos: bool = True
    normalizar_nombres_columnas: bool = True
    tipos_forzados: dict[str, str] = Field(default_factory=dict)
```

Each step must expose its original `tipo` literal and original fields; for example:

```python
class FiltrarPaso(BaseModel):
    model_config = ConfigDict(extra="allow")
    tipo: Literal["filtrar"]
    expresion: str


class ReparticionarPaso(BaseModel):
    model_config = ConfigDict(extra="allow")
    tipo: Literal["reparticionar"]
    cantidad: int
    columnas: list[str] = Field(default_factory=list)
```

- [ ] **Step 4: Translate Pydantic errors into ErrorReceta**

```python
def _mensaje_validacion(error: ValidationError) -> str:
    partes: list[str] = []
    for detalle in error.errors(include_url=False, include_context=False):
        ubicacion = ".".join(str(valor) for valor in detalle["loc"])
        partes.append(f"{ubicacion}: {detalle['msg']}")
    return "Receta inválida: " + "; ".join(partes)
```

- [ ] **Step 5: Run model and loader tests**

Run: `pytest tests/unitarios/test_modelos_receta.py tests/unitarios/test_cargador_receta.py -q`

Expected: all pass without creating a Spark session.

- [ ] **Step 6: Commit typed recipe configuration**

```bash
git add motor_spark/configuracion tests/unitarios

git commit -m "refactor: model recipes with pydantic"
```

---

### Task 4: Extraer tipos Spark, esquemas y reglas de columnas

**Files:**
- Create: `motor_spark/dominio/tipos_spark.py`
- Create: `motor_spark/dominio/esquemas.py`
- Create: `motor_spark/dominio/columnas.py`
- Create: `tests/unitarios/test_tipos_spark.py`
- Create: `tests/unitarios/test_esquemas.py`
- Create: `tests/unitarios/test_columnas.py`

**Interfaces:**
- Produces: `convertir_tipo_spark(tipo_crudo: str) -> DataType`.
- Produces: `construir_esquema(especificacion: str) -> StructType`.
- Produces: `resolver_esquema_entrada(especificacion: str | None, configuracion: EntradaConfig) -> tuple[str, StructType | None]`.
- Produces: `validar_evolucion_esquema(actual: StructType, nuevo: StructType) -> list[StructField]`.
- Produces: `normalizar_nombre_columna(nombre: str) -> str`.
- Produces: `normalizar_columnas_entrada(datos: DataFrame, activar: bool) -> DataFrame`.
- Produces: `aplicar_tipos_forzados_entrada(datos: DataFrame, configuracion: EntradaConfig) -> DataFrame`.
- Produces: `convertir_columnas_void_a_string(datos: DataFrame) -> DataFrame`.
- Produces: `exigir_columnas(datos: DataFrame, columnas: list[str], numero_paso: int) -> None`.

- [ ] **Step 1: Add PySpark-gated tests for simple and decimal types**

```python
# tests/unitarios/test_tipos_spark.py
import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql.types import DecimalType, IntegerType, StringType

from motor_spark.dominio.errores import ErrorReceta
from motor_spark.dominio.tipos_spark import convertir_tipo_spark


def test_convertir_tipo_spark_conserva_aliases():
    assert isinstance(convertir_tipo_spark("texto"), StringType)
    assert isinstance(convertir_tipo_spark("entero"), IntegerType)
    assert convertir_tipo_spark("decimal(12, 2)") == DecimalType(12, 2)


def test_convertir_tipo_spark_rechaza_precision_fuera_de_rango():
    with pytest.raises(ErrorReceta, match="rango 1-38"):
        convertir_tipo_spark("decimal(39,2)")
```

- [ ] **Step 2: Add schema and column normalization tests using local Spark**

```python
@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    sesion = SparkSession.builder.master("local[1]").appName("motor-tests").getOrCreate()
    yield sesion
    sesion.stop()
```

Test exact messages for duplicate schema columns, invalid names, normalized collisions, missing columns and invalid forced casts.

- [ ] **Step 3: Move the original implementations without semantic changes**

Move the bodies of the following original functions into their target modules, changing only imports and typed configuration access:

```text
convertir_tipo_spark
construir_esquema
obtener_modo_esquema
resolver_esquema_entrada
normalizar_nombre_columna
normalizar_columnas_entrada
aplicar_tipos_forzados_entrada
convertir_columnas_void_a_string
validar_evolucion_esquema
exigir_columnas
```

Replace direct `print(...)` calls with `emitir(...)` while keeping the emitted string byte-for-byte equivalent.

- [ ] **Step 4: Run the domain test suite**

Run: `pytest tests/unitarios/test_tipos_spark.py tests/unitarios/test_esquemas.py tests/unitarios/test_columnas.py -q`

Expected: pass when PySpark is installed; clean SKIP only for PySpark-dependent tests otherwise.

- [ ] **Step 5: Commit domain extraction**

```bash
git add motor_spark/dominio tests/unitarios

git commit -m "refactor: extract spark schema and column rules"
```

---

### Task 5: Reemplazar el if/elif de pasos por manejadores registrados

**Files:**
- Create: `motor_spark/transformaciones/contrato.py`
- Create: `motor_spark/transformaciones/columnas.py`
- Create: `motor_spark/transformaciones/conversion.py`
- Create: `motor_spark/transformaciones/texto.py`
- Create: `motor_spark/transformaciones/filtros.py`
- Create: `motor_spark/transformaciones/agregaciones.py`
- Create: `motor_spark/transformaciones/duplicados.py`
- Create: `motor_spark/transformaciones/particiones.py`
- Create: `motor_spark/transformaciones/registro.py`
- Create: `motor_spark/transformaciones/ejecutor.py`
- Create: `tests/unitarios/test_registro_transformaciones.py`
- Create: `tests/unitarios/test_transformaciones.py`

**Interfaces:**
- Produces: `ManejadorTransformacion = Callable[[DataFrame, PasoConfig, int], DataFrame]`.
- Produces: `REGISTRO_TRANSFORMACIONES: Mapping[str, ManejadorTransformacion]` containing exactly eleven current types.
- Produces: `aplicar_pasos(datos: DataFrame, pasos: Sequence[PasoConfig]) -> DataFrame`.

- [ ] **Step 1: Test registry completeness independently from Spark execution**

```python
# tests/unitarios/test_registro_transformaciones.py
from motor_spark.transformaciones.registro import REGISTRO_TRANSFORMACIONES


def test_registro_contiene_operaciones_actuales():
    assert set(REGISTRO_TRANSFORMACIONES) == {
        "seleccionar_columnas",
        "eliminar_columnas",
        "renombrar_columna",
        "convertir_tipo",
        "crear_columna",
        "filtrar",
        "rellenar_nulos",
        "normalizar_texto",
        "eliminar_duplicados",
        "agrupar",
        "reparticionar",
    }
```

- [ ] **Step 2: Implement one handler per transformation family**

Every handler has this shape:

```python
def aplicar_filtro(
    datos: DataFrame,
    paso: FiltrarPaso,
    numero_paso: int,
) -> DataFrame:
    del numero_paso
    return datos.filter(F.expr(paso.expresion))
```

Column-dependent handlers call `exigir_columnas` before transforming. `agrupar` uses the extracted `construir_agregacion`. `reparticionar` rejects `cantidad < 1` with the original message.

- [ ] **Step 3: Implement the static registry**

```python
REGISTRO_TRANSFORMACIONES: dict[str, ManejadorTransformacion] = {
    "seleccionar_columnas": aplicar_seleccion,
    "eliminar_columnas": aplicar_eliminacion,
    "renombrar_columna": aplicar_renombrado,
    "convertir_tipo": aplicar_conversion_tipo,
    "crear_columna": aplicar_creacion_columna,
    "filtrar": aplicar_filtro,
    "rellenar_nulos": aplicar_relleno_nulos,
    "normalizar_texto": aplicar_normalizacion_texto,
    "eliminar_duplicados": aplicar_eliminacion_duplicados,
    "agrupar": aplicar_agrupacion,
    "reparticionar": aplicar_reparticion,
}
```

- [ ] **Step 4: Implement the executor with contractual events**

```python
def aplicar_pasos(datos: DataFrame, pasos: Sequence[PasoConfig]) -> DataFrame:
    resultado = datos
    for numero, paso in enumerate(pasos, start=1):
        tipo = paso.tipo
        emitir(f"PASO_INICIO numero={numero} tipo={tipo}")
        manejador = REGISTRO_TRANSFORMACIONES.get(tipo)
        if manejador is None:
            raise ErrorReceta(
                f"Operación no soportada en el paso {numero}: {tipo}"
            )
        resultado = manejador(resultado, paso, numero)
        emitir(
            f"PASO_FIN numero={numero} "
            f"tipo={tipo} columnas={resultado.columns}"
        )
    return resultado
```

- [ ] **Step 5: Add Spark-local behavior tests for every handler**

Create one test per operation with a two-to-four-row DataFrame. Assert both schema/columns and collected rows. Capture stdout for `PASO_INICIO` and `PASO_FIN`.

- [ ] **Step 6: Run transformation tests**

Run: `pytest tests/unitarios/test_registro_transformaciones.py tests/unitarios/test_transformaciones.py -q`

Expected: registry test always passes; Spark tests pass when PySpark exists or skip cleanly otherwise.

- [ ] **Step 7: Commit transformation registry**

```bash
git add motor_spark/transformaciones tests/unitarios

git commit -m "refactor: register typed spark transformations"
```

---

### Task 6: Aislar sesión, lectura, escritura y Hadoop FileSystem

**Files:**
- Create: `motor_spark/infraestructura/spark/sesion.py`
- Create: `motor_spark/infraestructura/spark/lector.py`
- Create: `motor_spark/infraestructura/spark/escritor.py`
- Create: `motor_spark/infraestructura/spark/sistema_archivos.py`
- Create: `tests/unitarios/test_sistema_archivos.py`
- Create: `tests/integracion/test_lectura_escritura.py`

**Interfaces:**
- Produces: `crear_sesion_spark(nombre: str, ejecucion_id: str, configuracion: SparkConfig) -> SparkSession`.
- Produces: `leer_datos(spark, ruta, esquema, configuracion) -> DataFrame`.
- Produces: `escribir_datos(spark, datos, ruta, configuracion) -> dict[str, Any]`.
- Produces: `preparar_salida_local(ruta: str, modo: str) -> str`.
- Produces: `ruta_existe_hadoop(spark: SparkSession, ruta: str) -> bool`.
- Produces: `obtener_metricas_salida(spark, ruta_salida) -> dict[str, Any]`.

- [ ] **Step 1: Unit-test local path safety with mocked OS/group calls**

Test these exact branches:

```text
non-file scheme returns original mode
non-overwrite returns original mode
overwrite outside /srv/talend-motor/salida/ raises ErrorReceta
valid overwrite removes, creates, chowns, chmods and returns append
```

- [ ] **Step 2: Move Hadoop and local filesystem functions unchanged**

Move `preparar_salida_local`, `obtener_metricas_salida`, and `ruta_existe_hadoop` into `sistema_archivos.py`. Keep `_jvm` and `_jsc` access entirely inside this module.

- [ ] **Step 3: Implement Spark session factory**

```python
def crear_sesion_spark(nombre, ejecucion_id, configuracion):
    spark = SparkSession.builder.appName(
        f"{nombre} - {ejecucion_id}"
    ).getOrCreate()
    spark.sparkContext.setLogLevel(configuracion.nivel_log)
    if configuracion.shuffle_partitions:
        spark.conf.set(
            "spark.sql.shuffle.partitions",
            int(configuracion.shuffle_partitions),
        )
    return spark
```

- [ ] **Step 4: Move reader and writer bodies with typed configuration**

Preserve the original ordering of options, schema application, normalization, forced types, emitted events, repartitioning, partitioning, compression, Parquet requirement and output validation.

- [ ] **Step 5: Add local integration test**

Create a CSV with `id,nombre,total`, read it in strict mode, apply no transforms, write Parquet to a temporary `file://` path using mode `error`, and assert `_SUCCESS`, at least one `.parquet` file and positive bytes. Mark the test `pytest.mark.skipif` when PySpark or a Java runtime is absent.

- [ ] **Step 6: Run infrastructure tests**

Run: `pytest tests/unitarios/test_sistema_archivos.py tests/integracion/test_lectura_escritura.py -q`

Expected: unit tests pass everywhere; integration passes in a PySpark-capable environment.

- [ ] **Step 7: Commit Spark infrastructure**

```bash
git add motor_spark/infraestructura tests

git commit -m "refactor: isolate spark and hadoop infrastructure"
```

---

### Task 7: Extraer procesamiento incremental y construcción de resultados

**Files:**
- Create: `motor_spark/aplicacion/resultado_ejecucion.py`
- Create: `motor_spark/aplicacion/ejecutor_incremental.py`
- Create: `tests/unitarios/test_resultado_ejecucion.py`
- Create: `tests/integracion/test_incremental.py`

**Interfaces:**
- Produces: `ResultadoIncremental(datos_salida: DataFrame | None, total_registros: int, metricas_incrementales: dict[str, Any], metricas_salida: dict[str, Any])`.
- Produces: `ejecutar_incremental(spark, procesados, ruta_salida, configuracion_incremental, configuracion_salida) -> ResultadoIncremental`.
- Produces: `construir_resultado_exito(...) -> dict[str, Any]`.
- Produces: `construir_resultado_error(argumentos, excepcion) -> dict[str, Any]`.

- [ ] **Step 1: Test result dictionaries without Spark**

```python
def test_resultado_error_conserva_claves():
    argumentos = ArgumentosEjecucion("{}", "in", "out", "", None, "e-1")
    resultado = construir_resultado_error(argumentos, ErrorReceta("falló"))
    assert resultado == {
        "estado": "ERROR",
        "ejecucion_id": "e-1",
        "entrada": "in",
        "salida": "out",
        "tipo_error": "ErrorReceta",
        "mensaje": "falló",
    }
```

- [ ] **Step 2: Move incremental algorithm preserving every branch**

The service must preserve:

```text
only duplicados=ignorar
non-empty keys
non-null keys
persist/count of processed batch
dropDuplicates(keys)
destination existence check
parquet mergeSchema=true
schema evolution validation
left_anti against existing keys
append output mode
output validation when no new rows
same totals and INCREMENTAL_RESULTADO event
```

Use `try/finally` around each persisted DataFrame. Do not unpersist an object that was not persisted by the service.

- [ ] **Step 3: Add incremental integration cases**

Create tests for:

```text
new destination: 3 input, 2 unique -> 2 new, 1 duplicate
existing destination: one old key + two batch keys -> one new
all duplicates: no write, existing metrics still returned
null key: ErrorReceta
incompatible type change: ErrorReceta with CAMBIO_TIPO
```

- [ ] **Step 4: Run result and incremental tests**

Run: `pytest tests/unitarios/test_resultado_ejecucion.py tests/integracion/test_incremental.py -q`

Expected: dictionary tests always pass; integration passes with PySpark.

- [ ] **Step 5: Commit incremental service**

```bash
git add motor_spark/aplicacion tests

git commit -m "refactor: extract incremental execution service"
```

---

### Task 8: Construir el orquestador y reducir motor.py a adaptador

**Files:**
- Create: `motor_spark/aplicacion/ejecutor_motor.py`
- Create or replace: `motor.py`
- Create: `tests/unitarios/test_ejecutor_motor.py`
- Create: `tests/compatibilidad/test_cli_compatibilidad.py`

**Interfaces:**
- Produces: `ejecutar_motor(argumentos: ArgumentosEjecucion) -> int`.
- Produces: `main(argv: Sequence[str] | None = None) -> int` in `motor.py` for testability while preserving script execution.

- [ ] **Step 1: Write lifecycle tests with mocked collaborators**

Test these exact conditions:

```text
invalid recipe returns 1 and never creates Spark
successful full flow returns 0 and stops Spark once
unexpected failure returns 1, writes RESULTADO_MOTOR to stderr, prints traceback and stops Spark
schema ignored event emitted in infer mode when --esquema is non-empty
counting mode persists, counts and unpersists processed data
```

- [ ] **Step 2: Implement the orchestrator in the original event order**

```python
def ejecutar_motor(argumentos: ArgumentosEjecucion) -> int:
    spark: SparkSession | None = None
    try:
        receta = cargar_receta(argumentos.receta)
        modo_esquema, esquema = resolver_esquema_entrada(
            argumentos.esquema,
            receta.entrada,
        )
        spark = crear_sesion_spark(
            receta.nombre,
            argumentos.ejecucion_id,
            receta.spark,
        )
        # Emit events, read, transform, choose incremental/full,
        # construct result, persist optional result and emit RESULTADO_MOTOR.
        return 0
    except Exception as excepcion:
        resultado = construir_resultado_error(argumentos, excepcion)
        guardar_resultado(argumentos.resultado, resultado)
        emitir(
            "RESULTADO_MOTOR=" + json.dumps(resultado, ensure_ascii=False),
            error=True,
        )
        traceback.print_exc()
        return 1
    finally:
        if spark is not None:
            spark.stop()
```

The implementation step must contain the full original event and branch sequence; the comment above is explanatory only and must not remain in committed production code.

- [ ] **Step 3: Implement the thin compatible entry point**

```python
#!/usr/bin/env python3
from __future__ import annotations

from typing import Sequence

from motor_spark.aplicacion.ejecutor_motor import ejecutar_motor
from motor_spark.configuracion.argumentos import analizar_argumentos


def main(argv: Sequence[str] | None = None) -> int:
    return ejecutar_motor(analizar_argumentos(argv))


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Add a no-Spark CLI compatibility check**

Run both `python legacy/motor_original.py --help` and `python motor.py --help`, normalize only the `usage:` program name, and assert the remaining help text is identical.

- [ ] **Step 5: Run orchestrator and CLI tests**

Run: `pytest tests/unitarios/test_ejecutor_motor.py tests/compatibilidad/test_cli_compatibilidad.py -q`

Expected: all mocked tests pass without PySpark runtime; help compatibility passes.

- [ ] **Step 6: Commit the new entry point**

```bash
git add motor.py motor_spark/aplicacion tests

git commit -m "refactor: delegate motor cli to modular orchestrator"
```

---

### Task 9: Pruebas diferenciales, documentación y verificación final

**Files:**
- Create: `tests/compatibilidad/test_motor_diferencial.py`
- Create: `tests/recursos/recetas/estricta.json`
- Create: `tests/recursos/recetas/inferida.json`
- Create: `tests/recursos/recetas/incremental.json`
- Create: `tests/recursos/datos/ventas.csv`
- Create: `README.md`
- Modify: `docs/superpowers/specs/2026-07-28-motor-spark-refactor-design.md` only if implementation reveals a documented contradiction.

**Interfaces:**
- Verifies all public contracts.
- Documents installation, execution, recipes, architecture, tests and adding a transformation.

- [ ] **Step 1: Build representative fixtures**

Use a CSV with deterministic integer, text, date and decimal-like columns. Recipes must exercise strict schema, inferred schema with normalization and forced types, all eleven step types across the suite, full write and incremental write.

- [ ] **Step 2: Implement differential runner**

For each fixture, execute original and refactored motors in separate temporary output directories with the same arguments. Compare:

```text
exit code
RESULTADO_MOTOR JSON after replacing temporary roots
ordered contractual event names
output schema simpleString
record count
incremental totals
parquet file count > 0
_SUCCESS existence
```

Do not compare Spark noise, stack-trace file paths or generated part filenames.

- [ ] **Step 3: Write README with exact commands**

Document:

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements-dev.txt
python motor.py --receta receta.json --entrada entrada.csv --salida salida --esquema 'id:entero|nombre:texto' --ejecucion-id ejec-001
pytest -q
pytest --cov=motor_spark --cov-report=term-missing
```

Include the four files required to add a transformation: model, handler, registry entry and tests. State that `motor.py` is the stable external entry point.

- [ ] **Step 4: Run static verification**

Run:

```bash
python -m compileall -q motor.py motor_spark
python -m pytest -q
python -m pytest --cov=motor_spark --cov-report=term-missing
```

Expected: compile succeeds; all environment-independent tests pass; Spark-dependent tests pass on the Remote Engine or are explicitly skipped in a non-Spark development environment.

- [ ] **Step 5: Verify forbidden architecture regressions**

Run:

```bash
python - <<'PY'
from pathlib import Path
motor = Path('motor.py').read_text(encoding='utf-8')
assert 'pyspark.sql.functions' not in motor
assert '.read.' not in motor
assert '.write.' not in motor
assert 'left_anti' not in motor
assert len(motor.splitlines()) <= 30
print('Arquitectura de motor.py verificada')
PY
```

- [ ] **Step 6: Review plan/spec coverage and remove duplicate production code**

Confirm the production tree no longer contains the original monolithic function copies. Keep only `legacy/motor_original.py` as a test reference. Search for placeholder markers and fail if found:

```bash
! grep -RInE '\b(TODO|TBD|implement later)\b' motor.py motor_spark README.md
```

- [ ] **Step 7: Final commit**

```bash
git add README.md tests docs motor.py motor_spark legacy pyproject.toml requirements*.txt

git commit -m "refactor: complete modular spark recipe motor"
```
