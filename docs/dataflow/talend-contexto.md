---
titulo: Configuracion Talend de JOB_20_PROCESAR_SPARK
identificador: TALEND-CTX-001
tipo: referencia-configuracion
estado: vigente
version: 1.0.0
propietario: por-asignar
audiencia:
  - integracion
  - operaciones
  - desarrollo
creado: 2026-08-06
ultima_revision: 2026-08-06
proxima_revision: 2026-11-06
fuente_de_verdad: true
confidencialidad: interna
relacionados:
  - ./runbook-talend.md
  - ./operacion.md
---

# Configuracion Talend de JOB_20_PROCESAR_SPARK

> Estado: **CONFIRMADO** para el entorno Talend `Prueba` validado el
> 2026-08-06.

## Componentes relevantes

| Componente | Responsabilidad |
| --- | --- |
| `tJava_4` | Validaciones no sensibles antes de la ejecución |
| `tSystem_LlamarPython` | Inicia Spark y el motor Python mediante array de argumentos |
| `tJava_5` | Detecta `RESULTADO_MOTOR` en la salida capturada |
| Rama de error | Debe terminar el Job con error cuando el motor devuelva fallo |

Los componentes históricos de lectura de resultados de recetas no forman parte
del flujo Dataflow actual. No usar `tFileInputJSON` para interpretar campos del
contrato antiguo de recetas.

## tSystem_LlamarPython

Seleccionar **Use Array Command**. Cada fila es un argumento independiente; no
construir una línea de shell.

```text
context.SPARK_SUBMIT
--master
context.SPARK_MASTER
--deploy-mode
client
--conf
spark.pyspark.python=<context.PYTHON_SPARK>
--conf
spark.pyspark.driver.python=<context.PYTHON_SPARK>
--conf
spark.sql.session.timeZone=America/Guayaquil
--conf
spark.sql.shuffle.partitions=<context.SPARK_SHUFFLE_PARTITIONS>
--packages
org.postgresql:postgresql:42.7.7
context.MOTOR_SCRIPT
--dataflow-script-contenido
context.DF_SCRIPT
--conexiones-contenido
context.CONEXIONES_JSON
--ejecucion-id
context.EJECUCION_ID
--resultado
context.RUTA_RESULTADO_JSON
--base-destino
context.BASE_DESTINO_JSON
```

`context.SPARK_SUBMIT` es el único ejecutable. No añadir una segunda fila con
`/opt/spark/bin/spark-submit`.

### Salidas

Configurar ambos campos de `tSystem` como **to both console and global
variable**:

| Campo | Motivo |
| --- | --- |
| Standard Output | Captura `RESULTADO_MOTOR` en `tSystem_1_OUTPUT` |
| Error Output | Mantiene errores y stack traces disponibles para el Job |

### Parámetros de entorno

| Nombre | Valor | Sensible |
| --- | --- | --- |
| `PYSPARK_PYTHON` | `context.PYTHON_SPARK` | no |
| `PYSPARK_DRIVER_PYTHON` | `context.PYTHON_SPARK` | no |
| `MOTOR_SECRETOS_JSON` | `context.SECRETOS_JSON` | sí |

No imprimir `MOTOR_SECRETOS_JSON`, `SECRETOS_JSON` ni crear argumentos CLI con
sus valores.

## Matriz de contextos

| Contexto | Valor o formato confirmado | Sensible | Uso |
| --- | --- | --- | --- |
| `SPARK_SUBMIT` | `/opt/spark/bin/spark-submit` | no | Ejecutable Spark 4.2 |
| `SPARK_MASTER` | `spark://209.50.245.140:7077` | no | Master Spark compatible |
| `PYTHON_SPARK` | Ruta al Python con PySpark compatible | no | Driver y workers Python |
| `SPARK_SHUFFLE_PARTITIONS` | Entero positivo | no | Configuración Spark |
| `MOTOR_SCRIPT` | `/srv/talend-motor/motor/motor.py` | no | Punto de entrada del motor |
| `DF_SCRIPT` | Script Qlik completo | interna | Argumento `--dataflow-script-contenido` |
| `CONEXIONES_JSON` | Objeto JSON de catálogo | interna | Argumento `--conexiones-contenido` |
| `BASE_DESTINO_JSON` | Objeto JSON de destino JDBC | interna | Argumento `--base-destino` |
| `SECRETOS_JSON` | Objeto JSON de secretos | sí | Variable de entorno del motor |
| `EJECUCION_ID` | Identificador único no vacío | no | Trazabilidad |
| `RUTA_RESULTADO_JSON` | Ruta local escribible por Remote Engine | interna | Persistencia de resultado |

`DF_SCRIPT` y `CONEXIONES_JSON` deben ser contenidos completos, no rutas de
archivo, porque se usan las opciones `--*-contenido`.

## Base destino PostgreSQL

`BASE_DESTINO_JSON` se pasa inline y no requiere un archivo en el servidor.
Debe contener los campos de conexión no secretos, el esquema, el modo de
escritura y `secreto_nombre`. El secreto referido debe existir en
`SECRETOS_JSON`.

> Estado: CONFIRMADO. El valor por defecto de modo es `overwrite` si no se
> especifica. Declarar el modo explícitamente para evitar sustituciones no
> intencionadas.

## Checklist antes de ejecutar

- [ ] El entorno seleccionado es `Prueba`.
- [ ] `SPARK_SUBMIT` y `SPARK_MASTER` apuntan a Spark 4.2 compatible.
- [ ] No hay un `spark-submit` duplicado en el array.
- [ ] `DF_SCRIPT` contiene `LIB CONNECT`, `SELECT`, `LOAD` y `STORE` cuando
  aplica.
- [ ] `CONEXIONES_JSON` es JSON válido y los nombres coinciden con Qlik.
- [ ] `SECRETOS_JSON` está configurado como secreto de Talend y no se imprime.
- [ ] `BASE_DESTINO_JSON` indica una tabla/esquema/mode aprobados.
- [ ] La salida estándar y de error se capturan globalmente.

## Checklist después de ejecutar

- [ ] `RESULTADO_CAPTURADO=true`.
- [ ] `RESULTADO_MOTOR.estado` es `COMPLETADO`.
- [ ] `operaciones` coincide con `operaciones_ejecutadas`.
- [ ] `publicaciones` contiene las cuatro tablas esperadas.
- [ ] Se retiró la instrumentación temporal `DF_SCRIPT_*` y
  `CONEXIONES_JSON_LONGITUD` de `tJava_4`.
