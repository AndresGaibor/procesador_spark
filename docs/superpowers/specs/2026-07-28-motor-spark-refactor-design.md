# Diseño del refactor del Motor Spark dirigido por recetas

**Fecha:** 2026-07-28  
**Estado:** aprobado para planificación  
**Objetivo:** dividir el archivo monolítico actual en un proyecto modular, tipado, extensible y comprobable, sin romper ningún contrato externo existente.

## 1. Contexto

El motor actual concentra en un único archivo la CLI, validación de recetas, construcción de esquemas, lectura y escritura Spark, transformaciones, procesamiento incremental, métricas, persistencia del resultado y manejo global de errores.

El refactor debe reducir el acoplamiento interno y permitir agregar nuevas transformaciones sin modificar el núcleo del motor. Al mismo tiempo, el comportamiento observado por Talend, scripts shell, operadores y recetas existentes debe mantenerse exactamente.

## 2. Decisiones aprobadas

1. Se conservará el punto de entrada actual:

   ```bash
   python motor.py \
     --receta ... \
     --entrada ... \
     --salida ... \
     --esquema ... \
     --resultado ... \
     --ejecucion-id ...
   ```

2. Se utilizará Pydantic v2 para modelar y validar las recetas.
3. Las recetas JSON actuales seguirán siendo válidas.
4. Se conservarán exactamente:
   - argumentos y obligatoriedad de la CLI;
   - códigos de salida `0` y `1`;
   - mensajes de consola `CLAVE=valor` existentes;
   - estructura del JSON de resultado exitoso;
   - estructura del JSON de error;
   - comportamiento de lectura de receta desde JSON directo o archivo;
   - semántica del procesamiento incremental;
   - validaciones de salida Parquet e `_SUCCESS`;
   - cierre garantizado de `SparkSession`.
5. `motor.py` será un adaptador delgado, no contendrá lógica Spark ni reglas de negocio.
6. Las transformaciones se resolverán mediante un registro explícito, no mediante un bloque central creciente de `if/elif`.
7. No se implementará carga dinámica de plugins ni descubrimiento por `entry_points`; se prioriza simplicidad operativa.

## 3. Alcance

### Incluido

- Reorganización completa en módulos internos.
- Modelos Pydantic para receta, entrada, salida, Spark, auditoría, incremental y pasos.
- Registro extensible de transformaciones.
- Separación de lectura, escritura y sistema de archivos Hadoop/local.
- Separación del flujo incremental.
- Conservación de logs y resultados externos.
- Pruebas unitarias y de integración.
- Pruebas de compatibilidad contra el motor original.
- Documentación de desarrollo y extensión.

### No incluido

- Cambiar nombres de campos de las recetas.
- Cambiar formatos de logs.
- Cambiar algoritmos de deduplicación o evolución de esquema.
- Agregar nuevas transformaciones durante el refactor inicial.
- Cambiar el formato de salida requerido para publicación en Impala.
- Introducir interfaces web, API HTTP, base de datos o sistema de plugins dinámicos.
- Optimizar rendimiento mediante cambios funcionales no cubiertos por pruebas de equivalencia.

## 4. Arquitectura objetivo

```text
proyecto/
├── motor.py
├── pyproject.toml
├── requirements.txt
├── README.md
│
├── motor_spark/
│   ├── __init__.py
│   │
│   ├── aplicacion/
│   │   ├── __init__.py
│   │   ├── ejecutor_motor.py
│   │   ├── ejecutor_incremental.py
│   │   └── resultado_ejecucion.py
│   │
│   ├── configuracion/
│   │   ├── __init__.py
│   │   ├── argumentos.py
│   │   ├── cargador_receta.py
│   │   └── modelos/
│   │       ├── __init__.py
│   │       ├── receta.py
│   │       ├── entrada.py
│   │       ├── salida.py
│   │       ├── incremental.py
│   │       └── pasos.py
│   │
│   ├── dominio/
│   │   ├── __init__.py
│   │   ├── errores.py
│   │   ├── esquemas.py
│   │   ├── columnas.py
│   │   └── tipos_spark.py
│   │
│   ├── infraestructura/
│   │   ├── __init__.py
│   │   ├── spark/
│   │   │   ├── __init__.py
│   │   │   ├── sesion.py
│   │   │   ├── lector.py
│   │   │   ├── escritor.py
│   │   │   └── sistema_archivos.py
│   │   └── resultados/
│   │       ├── __init__.py
│   │       └── escritor_json.py
│   │
│   ├── transformaciones/
│   │   ├── __init__.py
│   │   ├── contrato.py
│   │   ├── registro.py
│   │   ├── ejecutor.py
│   │   ├── columnas.py
│   │   ├── conversion.py
│   │   ├── texto.py
│   │   ├── filtros.py
│   │   ├── agregaciones.py
│   │   ├── duplicados.py
│   │   └── particiones.py
│   │
│   └── compartido/
│       ├── __init__.py
│       ├── booleanos.py
│       └── eventos_consola.py
│
└── tests/
    ├── unitarios/
    ├── integracion/
    ├── compatibilidad/
    └── recursos/
        ├── recetas/
        └── datos/
```

## 5. Responsabilidades por módulo

### `motor.py`

- Ejecutar la CLI.
- Delegar el análisis de argumentos.
- Invocar el caso de uso principal.
- Finalizar con el código de salida devuelto.
- No importar directamente `pyspark.sql.functions` ni contener reglas de receta.

### `configuracion/argumentos.py`

- Crear el mismo `ArgumentParser` actual.
- Mantener nombres, valores predeterminados, argumentos obligatorios y textos de ayuda relevantes.
- Convertir el resultado de `argparse` a un objeto tipado `ArgumentosEjecucion`.

### `configuracion/cargador_receta.py`

- Aceptar JSON directo o ruta de archivo.
- Traducir errores de decodificación al mismo `ErrorReceta` observable.
- Validar la raíz como objeto JSON.
- Construir `RecetaConfig` mediante Pydantic.
- Convertir errores de Pydantic a mensajes deterministas y legibles sin exponer una traza interna como contrato.

### `configuracion/modelos`

- Representar todas las secciones de la receta.
- Aplicar valores predeterminados equivalentes a los actuales.
- Admitir aliases actuales en tipos y modos.
- Rechazar configuraciones incompatibles antes de iniciar Spark cuando la validación no depende del esquema real del DataFrame.
- Usar una unión discriminada por `tipo` para los pasos.

### `dominio/errores.py`

- Definir `ErrorReceta` como error funcional público del motor.
- Mantener una única excepción funcional para conservar el tipo de error esperado.
- No envolver errores inesperados de PySpark como si fueran validaciones de receta.

### `dominio/tipos_spark.py`

- Convertir nombres declarativos a tipos Spark.
- Validar `decimal(precision, escala)` con los límites actuales.
- Mantener aliases en español e inglés.

### `dominio/esquemas.py`

- Construir `StructType` a partir de `--esquema`.
- Resolver modo estricto o inferido.
- Validar evolución compatible del esquema.
- Convertir columnas `NullType` a `StringType`.

### `dominio/columnas.py`

- Normalizar nombres.
- Validar nombres y colisiones.
- Exigir existencia de columnas.
- Aplicar tipos forzados de entrada.

### `infraestructura/spark/sesion.py`

- Crear `SparkSession` usando nombre de receta e identificador de ejecución.
- Aplicar nivel de log y `spark.sql.shuffle.partitions`.
- No detener la sesión; su ciclo de vida pertenece al ejecutor principal.

### `infraestructura/spark/lector.py`

- Leer formatos soportados mediante opciones normalizadas.
- Aplicar esquema fijo o inferido.
- Ejecutar normalización y tipos forzados cuando corresponda.
- Emitir `LECTURA_COMPLETADA` con el mismo formato actual.

### `infraestructura/spark/escritor.py`

- Preparar reparticiones.
- Configurar formato, modo, compresión y particionado físico.
- Escribir datos.
- Validar el requisito Parquet actual.
- Retornar las mismas métricas de salida.

### `infraestructura/spark/sistema_archivos.py`

- Consultar existencia mediante Hadoop FileSystem.
- Preparar salidas locales seguras.
- Validar `_SUCCESS`, archivos Parquet y bytes escritos.
- Encapsular el acceso a `_jvm` y `_jsc` para que no se propague al resto del proyecto.

### `infraestructura/resultados/escritor_json.py`

- Guardar resultado de forma atómica mediante archivo temporal y reemplazo.
- Mantener JSON UTF-8, `ensure_ascii=False` e indentación actual.

### `transformaciones`

- Cada archivo agrupa transformaciones relacionadas.
- Cada manejador recibe `DataFrame`, modelo tipado del paso y número del paso.
- Cada manejador devuelve un nuevo `DataFrame`.
- Los manejadores no escriben archivos ni crean sesiones Spark.

### `aplicacion/ejecutor_incremental.py`

- Validar política incremental y claves.
- Rechazar claves nulas.
- Deduplicar el lote.
- Detectar destino existente.
- Validar evolución de esquema.
- Ejecutar `left_anti` contra claves existentes.
- Calcular las mismas métricas actuales.
- Gestionar correctamente `persist()` y `unpersist()`.

### `aplicacion/ejecutor_motor.py`

- Orquestar el caso de uso completo.
- Garantizar que Spark se detenga mediante `finally`.
- Mantener orden y contenido de los eventos externos.
- Elegir flujo completo o incremental.
- Construir resultado exitoso o de error.
- Retornar `0` o `1`.

## 6. Modelos Pydantic

Se utilizará Pydantic v2 con configuración estricta donde no rompa compatibilidad. Los modelos principales serán:

```text
RecetaConfig
├── nombre
├── version
├── spark: SparkConfig
├── entrada: EntradaConfig
├── pasos: list[PasoConfig]
├── incremental: IncrementalConfig
├── salida: SalidaConfig
└── auditoria: AuditoriaConfig
```

### Pasos como unión discriminada

```python
PasoConfig = Annotated[
    Union[
        SeleccionarColumnasPaso,
        EliminarColumnasPaso,
        RenombrarColumnaPaso,
        ConvertirTipoPaso,
        CrearColumnaPaso,
        FiltrarPaso,
        RellenarNulosPaso,
        NormalizarTextoPaso,
        EliminarDuplicadosPaso,
        AgruparPaso,
        ReparticionarPaso,
    ],
    Field(discriminator="tipo"),
]
```

Los modelos deberán aceptar los valores actuales y conservar sus valores predeterminados. Las comprobaciones que requieren conocer columnas reales seguirán ejecutándose después de la lectura.

## 7. Registro de transformaciones

El registro será explícito y estático:

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

El ejecutor:

1. obtiene el nombre discriminador del paso;
2. emite `PASO_INICIO`;
3. localiza el manejador;
4. ejecuta la transformación;
5. emite `PASO_FIN` con las columnas resultantes;
6. conserva la numeración iniciada en uno.

Agregar una transformación nueva requerirá crear su modelo, manejador, registro y pruebas, sin modificar el caso de uso principal.

## 8. Flujo de ejecución

```text
motor.py
  -> analizar argumentos
  -> cargar JSON o archivo
  -> validar RecetaConfig
  -> resolver esquema de entrada
  -> crear SparkSession
  -> configurar Spark
  -> leer DataFrame
  -> ejecutar pasos registrados
  -> elegir flujo:
       ├─ carga completa
       └─ carga incremental
  -> escribir o validar salida
  -> construir resultado
  -> guardar resultado opcional
  -> imprimir RESULTADO_MOTOR
  -> detener Spark
  -> devolver código de salida
```

La validación puramente estructural ocurrirá antes de crear Spark. La validación dependiente de datos o columnas ocurrirá después de leer el DataFrame.

## 9. Compatibilidad de consola y resultados

Los eventos actuales se tratarán como una interfaz pública. Se centralizarán en funciones pequeñas de `compartido/eventos_consola.py`, sin cambiar su texto.

No se sustituirán por logging convencional en esta fase porque Talend u otros procesos podrían analizar estas líneas. Se podrá usar logging interno adicional únicamente si no altera `stdout` ni `stderr` contractual.

Se escribirán pruebas de captura de salida para verificar, entre otros:

- `SALIDA_LOCAL_PREPARADA=`
- `SALIDA_VALIDADA=`
- `COLUMNAS_NORMALIZADAS=`
- `TIPOS_FORZADOS_ENTRADA=`
- `COLUMNAS_VOID_CONVERTIDAS_STRING=`
- `EVOLUCION_ESQUEMA_SPARK_VALIDADA=`
- `LECTURA_COMPLETADA=`
- `ESCRITURA_INICIO=`
- `ESCRITURA_FIN=`
- `EJECUCION_INICIO=`
- `MODO_ESQUEMA_ENTRADA=`
- `ESQUEMA_ENTRADA_DECLARADO=`
- `SCHEMA_SPEC_IGNORADO=true`
- `ESQUEMA_ENTRADA_REAL=`
- `PASO_INICIO`
- `PASO_FIN`
- `INCREMENTAL_RESULTADO=`
- `RESULTADO_MOTOR=`

## 10. Manejo de errores

### Errores funcionales

Las validaciones conocidas lanzarán `ErrorReceta` con mensajes compatibles. El ejecutor global las procesará igual que cualquier otra excepción para conservar la estructura actual de error:

```json
{
  "estado": "ERROR",
  "ejecucion_id": "...",
  "entrada": "...",
  "salida": "...",
  "tipo_error": "ErrorReceta",
  "mensaje": "..."
}
```

### Errores inesperados

- Conservarán su clase real en `tipo_error`.
- Se guardará e imprimirá primero `RESULTADO_MOTOR` en `stderr`.
- Después se imprimirá la traza mediante `traceback.print_exc()`.
- El proceso retornará `1`.
- Spark se detendrá siempre que haya sido creado.

### Recursos persistidos

Cada servicio que invoque `persist()` deberá liberar sus DataFrames mediante `try/finally` local o una estructura equivalente. El objetivo es evitar que una excepción intermedia omita un `unpersist()`.

## 11. Estrategia de pruebas

### Unitarias sin sesión Spark

- Carga de receta desde cadena y archivo.
- Errores JSON con línea y columna.
- Modelos Pydantic y valores predeterminados.
- Alias booleanos.
- Alias de modos de esquema.
- Registro completo de transformaciones.
- Construcción del parser y argumentos.
- Escritura atómica del resultado.

### Unitarias con Spark local

- Conversión de tipos.
- Construcción de esquemas.
- Normalización de columnas.
- Tipos forzados.
- Cada transformación individual.
- Validación de evolución de esquema.

### Integración

- CSV estricto a Parquet.
- CSV inferido a Parquet.
- Normalización y tipos forzados.
- Salida particionada.
- Carga incremental sin destino previo.
- Carga incremental con registros nuevos.
- Carga incremental compuesta solo por duplicados.
- Rechazo de claves nulas.
- Rechazo de evolución incompatible.

### Compatibilidad diferencial

Se conservará una copia del motor original únicamente dentro de los recursos de pruebas o se ejecutará desde una referencia congelada. Para un conjunto representativo de recetas se compararán:

- código de salida;
- JSON final normalizado;
- esquema de salida;
- cantidad de registros;
- métricas incrementales;
- archivos Parquet producidos;
- eventos contractuales de consola en su orden relativo.

Los valores inherentemente variables, como rutas temporales o trazas internas de Spark, se normalizarán únicamente en la prueba, no en el código productivo.

## 12. Dependencias y herramientas

Dependencias productivas mínimas:

```text
pyspark
pydantic>=2,<3
```

Dependencias de desarrollo:

```text
pytest
pytest-cov
```

La configuración concreta de la versión de PySpark deberá respetar la instalada en el Remote Engine; el refactor no actualizará PySpark por sí mismo.

## 13. Estrategia de migración

La implementación se hará por capas para reducir el riesgo:

1. Crear pruebas de caracterización del motor original.
2. Introducir paquete, errores, utilidades y CLI delegada.
3. Extraer modelos y cargador de receta.
4. Extraer tipos, esquemas y columnas.
5. Extraer transformaciones y registro.
6. Extraer lectura, escritura y sistema de archivos.
7. Extraer procesamiento incremental.
8. Reducir `ejecutor_motor.py` a orquestación.
9. Ejecutar pruebas diferenciales.
10. Retirar código duplicado del archivo original.

Durante la migración, cada paso debe dejar el comando actual funcional y las pruebas verdes.

## 14. Criterios de aceptación

El refactor se considera completo cuando:

1. `python motor.py` conserva todos los argumentos actuales.
2. Las recetas válidas actuales siguen ejecutándose sin modificación.
3. Las recetas inválidas conservan el tipo de error y mensajes compatibles en los casos caracterizados.
4. Los códigos de salida siguen siendo `0` y `1`.
5. El JSON final exitoso y de error conserva sus claves actuales.
6. Los eventos contractuales de consola conservan formato y orden relativo.
7. El flujo completo e incremental producen resultados equivalentes al motor original.
8. Ningún archivo productivo concentra simultáneamente CLI, I/O Spark, transformaciones y procesamiento incremental.
9. Agregar una transformación no requiere modificar `ejecutor_motor.py`.
10. Las pruebas unitarias, de integración y compatibilidad pasan.
11. La cobertura se concentra en reglas del motor, no en líneas triviales, con objetivo inicial mínimo de 85 % para módulos propios.
12. El README explica instalación, ejecución, estructura, creación de una transformación y ejecución de pruebas.

## 15. Riesgos y mitigaciones

### Pydantic cambia mensajes de error

**Mitigación:** traducir `ValidationError` a `ErrorReceta` mediante un adaptador estable y cubrir los mensajes contractuales con pruebas.

### Cambios accidentales en stdout/stderr

**Mitigación:** centralizar eventos, capturarlos en pruebas y no introducir logging adicional en los canales contractuales.

### Diferencias por extracción del flujo incremental

**Mitigación:** pruebas diferenciales con destino inexistente, destino existente, lote duplicado, columnas nuevas compatibles y cambios incompatibles.

### Uso incorrecto de persistencia

**Mitigación:** encapsular cada ciclo `persist/unpersist` y probar rutas de excepción.

### Dependencia del entorno Linux y grupo `spark`

**Mitigación:** aislar la preparación local en `sistema_archivos.py`, simular llamadas del sistema en unitarias y mantener una prueba de integración exclusiva para el servidor Linux.

## 16. Resultado esperado

El sistema conservará su comportamiento operativo, pero cada responsabilidad tendrá una ubicación clara. La validación será más temprana, los pasos estarán tipados, las transformaciones serán extensibles y el flujo incremental podrá probarse sin navegar por un archivo de más de mil ochocientas líneas.
