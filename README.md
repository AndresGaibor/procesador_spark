# Motor Spark dirigido por recetas

Motor PySpark que lee una entrada, ejecuta transformaciones declaradas en una receta JSON y publica resultados Parquet verificables. Este proyecto es el refactor modular del motor original y conserva como interfaz estable el comando `python motor.py`.

## Requisitos

- Python 3.10 o superior.
- Java compatible con la versión de PySpark del Remote Engine.
- PySpark ya alineado con el clúster o Remote Engine.
- Pydantic 2.

No se recomienda actualizar PySpark únicamente para instalar este proyecto. En Talend Remote Engine debe usarse la versión compatible con su instalación de Spark.

## Instalación

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements-dev.txt
```

En el servidor puede instalarse solo producción:

```bash
pip install -r requirements.txt
```

### Virtualenv propiedad de root

Si `.venv` ya existe y pertenece a `root`, el usuario que despliega no podrá
actualizar sus paquetes directamente. Sin cambiar el propietario, usa el
intérprete de esa virtualenv con `sudo`:

```bash
sudo /srv/talend-motor/motor/.venv/bin/python \
  -m pip install --upgrade pip

sudo /srv/talend-motor/motor/.venv/bin/python \
  -m pip install -r /srv/talend-motor/motor/requirements.txt
```

No uses `sudo pip install ...`, pues podría modificar el Python global en vez
de la virtualenv. Mientras `.venv` pertenezca a `root`, sus futuras
actualizaciones también requerirán `sudo`.

## Ejecución compatible

```bash
python motor.py \
  --receta tests/recursos/recetas/estricta.json \
  --entrada tests/recursos/datos/ventas.csv \
  --salida file:///srv/talend-motor/salida/ventas \
  --esquema 'id:entero|fecha:texto|cliente:texto|ciudad:texto|producto:texto|categoria:texto|cantidad:entero|precio:decimal(12,2)|descuento:decimal(12,2)|total:decimal(12,2)|metodo_pago:texto|vendedor:texto' \
  --resultado /tmp/resultado-ventas.json \
  --ejecucion-id ejec-001
```

`--receta` acepta una ruta JSON o un objeto JSON enviado directamente:

```bash
python motor.py \
  --receta '{"entrada":{"modo_esquema":"inferir","opciones":{"header":true}},"pasos":[],"salida":{"formato":"parquet","modo":"error"}}' \
  --entrada /datos/ventas.csv \
  --salida hdfs:///curado/ventas \
  --ejecucion-id ejec-002
```

## Scripts Qlik Dataflow

El motor también puede compilar y ejecutar scripts exportados por Qlik Dataflow mediante un plan Spark tipado, determinista y fail-closed. La guía completa de catálogo, seguridad, compilación y ejecución está en [`docs/dataflow.md`](docs/dataflow.md).

Para ejecutar sin guardar el script, el catálogo ni el resultado en archivos:

```bash
python motor.py \
  --dataflow-script-contenido "$SCRIPT_QLIK" \
  --conexiones-contenido "$CONEXIONES_JSON" \
  --secreto "POSTGRES=usuario:clave" \
  --ejecucion-id ejecucion-inline-001
```

Al omitir `--resultado`, el motor entrega únicamente `RESULTADO_MOTOR={...}` por consola.

El script puede enviarse como ruta con `--dataflow-script` o directamente como texto con `--dataflow-script-contenido`. Ambos modos son mutuamente excluyentes.

### Drivers JDBC

El wrapper de servidor `spark-talend-submit` detecta los drivers declarados en
`--conexiones-contenido` y añade automáticamente paquetes Maven aprobados antes
de iniciar Spark. El catálogo declara únicamente la clase, por ejemplo
`org.postgresql.Driver`; nunca define coordenadas Maven arbitrarias.

Los drivers aprobados son PostgreSQL, MySQL, MariaDB y SQL Server. El primer
arranque descarga el JAR desde Maven y Spark lo reutiliza desde su caché. Un
driver no registrado falla antes de ejecutar el Dataflow.

En una ejecución manual sin el wrapper, los drivers JDBC son JAR de la JVM y no
se instalan mediante `pip`. Para PostgreSQL, inicia el motor con el driver disponible para Spark:

```bash
PYSPARK_PYTHON="$PWD/.venv/bin/python" \
PYSPARK_DRIVER_PYTHON="$PWD/.venv/bin/python" \
./.venv/bin/spark-submit \
  --packages org.postgresql:postgresql:42.7.7 \
  motor.py \
  --dataflow-script-contenido "$SCRIPT_QLIK" \
  --conexiones-contenido "$CONEXIONES_JSON" \
  --secreto "POSTGRES_BANCO=$POSTGRES_BANCO" \
  --ejecucion-id ejecucion-001
```

`POSTGRES_BANCO` debe contener el valor `usuario:clave`. El primer arranque descarga el artefacto desde Maven; en entornos sin acceso a Internet, descarga el JAR durante el empaquetado y usa `--jars /ruta/postgresql-42.7.7.jar`.

## Estructura

```text
motor.py
motor_spark/
├── aplicacion/       # Orquestación, incremental y resultados
├── configuracion/    # CLI, carga JSON y modelos Pydantic
├── dominio/          # Tipos, esquemas, columnas y errores
├── infraestructura/  # Spark, Hadoop FileSystem y JSON atómico
├── transformaciones/ # Manejadores, registro y ejecutor
└── compartido/       # Booleanos y eventos contractuales
```

### Flujo

```text
CLI
 -> cargar y validar receta
 -> resolver esquema
 -> crear SparkSession
 -> leer DataFrame
 -> ejecutar pasos registrados
 -> carga completa o incremental
 -> escribir y validar Parquet
 -> guardar e imprimir RESULTADO_MOTOR
 -> detener SparkSession
```

## Receta

Secciones principales:

```json
{
  "nombre": "Ventas",
  "version": 1,
  "spark": {
    "nivel_log": "WARN",
    "shuffle_partitions": 8
  },
  "entrada": {
    "formato": "csv",
    "modo_esquema": "inferir",
    "opciones": {"header": true},
    "inferir_tipos": true,
    "normalizar_nombres_columnas": true,
    "tipos_forzados": {"id": "entero"}
  },
  "pasos": [],
  "incremental": {
    "activo": false,
    "duplicados": "ignorar",
    "claves": []
  },
  "salida": {
    "formato": "parquet",
    "modo": "error",
    "compresion": "snappy"
  },
  "auditoria": {
    "contar_registros": false
  }
}
```

Los modos de esquema válidos son `estricto` e `inferir`. También se conservan los aliases históricos `strict`, `schema`, `infer`, `inferido`, `dinamico` y `dinámico`.

## Transformaciones soportadas

- `seleccionar_columnas`
- `eliminar_columnas`
- `renombrar_columna`
- `convertir_tipo`
- `crear_columna`
- `filtrar`
- `rellenar_nulos`
- `normalizar_texto`
- `eliminar_duplicados`
- `agrupar`
- `reparticionar`

## Agregar una transformación

Una operación nueva requiere cuatro cambios explícitos:

1. Crear su modelo Pydantic en `motor_spark/configuracion/modelos/pasos.py` y añadirlo a `PasoConfig`.
2. Crear el manejador en el archivo temático de `motor_spark/transformaciones/`.
3. Registrar el nombre en `motor_spark/transformaciones/registro.py`.
4. Añadir pruebas unitarias y, cuando use Spark, una prueba de integración.

El manejador recibe `datos`, `paso` y `numero_paso`, y devuelve un nuevo DataFrame. No debe crear sesiones ni escribir archivos.

## Contratos preservados

- Punto de entrada `python motor.py`.
- Argumentos CLI actuales.
- Códigos de salida `0` y `1`.
- Resultado JSON exitoso y de error.
- Eventos `CLAVE=valor` consumibles por Talend.
- Lectura de receta desde JSON directo o archivo.
- Deducción incremental mediante `left_anti` y política `duplicados=ignorar`.
- Validación de `_SUCCESS`, cantidad de Parquet y bytes escritos.
- Cierre de Spark en `finally`.

`legacy/motor_original.py` es una referencia congelada para pruebas de compatibilidad; no debe usarse en producción.

## Pruebas

```bash
pytest -q
pytest --cov=motor_spark --cov-report=term-missing
```

Las pruebas unitarias que no requieren Spark pueden ejecutarse en cualquier entorno. Las pruebas bajo `tests/integracion/` se omiten automáticamente cuando PySpark o Java no están disponibles y deben ejecutarse en el Remote Engine antes del despliegue.

```bash
pytest -m spark -q
```

## Salidas locales con overwrite

Por seguridad, `overwrite` sobre rutas locales solo está permitido dentro de:

```text
/srv/talend-motor/salida/
```

El motor prepara esa carpeta con grupo `spark`, bit setgid y permisos `2770`, y luego usa `append` internamente para impedir que Spark elimine los permisos preparados.
