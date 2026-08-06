---
titulo: Operacion y observabilidad Dataflow
identificador: OPS-DATAFLOW-001
tipo: operacion
estado: vigente
version: 2.0.0
propietario: por-asignar
audiencia:
  - desarrollo
  - operaciones
  - integracion
creado: 2026-08-06
ultima_revision: 2026-08-06
proxima_revision: 2026-11-06
fuente_de_verdad: true
confidencialidad: interna
relacionados:
  - ./runbook-talend.md
  - ./talend-contexto.md
---

# Operacion y observabilidad Dataflow

> Estado: **CONFIRMADO** para el flujo Talend, Spark 4.2 y PostgreSQL validado.

## Fuente de verdad

El contrato operativo es la línea `RESULTADO_MOTOR=<json>` emitida por el
motor. Talend debe capturar la salida estándar para conservarla en
`tSystem_1_OUTPUT`.

## Resultado exitoso

Ejemplo estructural, sin datos sensibles:

```json
{
  "estado": "COMPLETADO",
  "ejecucion_id": "<id>",
  "origen_script": "parametro",
  "hash_script": "<sha-256>",
  "hash": "<sha-256-plan>",
  "operaciones": 72,
  "operaciones_ejecutadas": 72,
  "tablas_disponibles": ["<tabla>"],
  "publicaciones": [
    {"tipo": "base_destino", "tabla": "public.<tabla>"}
  ]
}
```

Campos de verificación:

| Campo | Criterio |
| --- | --- |
| `estado` | Debe ser `COMPLETADO` |
| `operaciones` | Número de operaciones compiladas |
| `operaciones_ejecutadas` | Debe coincidir con `operaciones` |
| `publicaciones` | Debe incluir todas las tablas destino esperadas |
| `hash_script` y `hash` | Permiten correlacionar script y plan sin registrar el contenido |

## Resultado de error

```json
{
  "estado": "ERROR",
  "ejecucion_id": "<id>",
  "errores": [
    {"codigo": "ERROREJECUCIONPLAN", "mensaje": "<mensaje redactado>"}
  ]
}
```

El motor redacta secretos conocidos de los mensajes. No completar los logs con
credenciales para intentar depurar una conexión.

## Señales de Spark

Una ejecución sana debe mostrar, en orden aproximado:

1. `Running Spark version 4.2.0`.
2. `Connected to Spark cluster`.
3. Un executor `RUNNING`.
4. Consultas `JDBCRDD` para fuentes permitidas.
5. Jobs `save` para las publicaciones JDBC.
6. `RESULTADO_MOTOR` con `estado: COMPLETADO`.

La existencia de un executor o de un código de salida Talend `0` no acredita
una publicación completa por sí sola.

## Incidentes conocidos

| Código o síntoma | Diagnóstico | Remediación |
| --- | --- | --- |
| `Failed to get main class in JAR` | `spark-submit` se añadió dos veces al array de Talend | Eliminar el ejecutable literal duplicado |
| `InvalidClassException` RPC | Master Spark diferente al Spark del driver | Usar driver y master Spark 4.2 compatibles |
| `Connection refused` al master | Puerto o dirección no escuchan | Usar el master confirmado del runbook |
| `FAILED_JDBC.CONNECTION` | No se puede abrir una fuente JDBC | Validar catálogo, secreto, red y driver sin registrar valores |
| `DFS_EMPTY_PLAN` | Script truncado o sin operaciones ejecutables | Verificar contenido Qlik en un único argumento |
| `RESULTADO_CAPTURADO=false` | Standard Output no se guardó en global | Configurar salida estándar a consola y variable global |

## Advertencias no bloqueantes observadas

| Advertencia | Estado | Acción |
| --- | --- | --- |
| PyArrow/Pandas no instalado | CONFIRMADO | Spark usa UDF sin optimización Arrow; no bloqueó la publicación |
| `VERSION_ESQUEMA` o `VERSION_RECETA` vacíos | CONFIRMADO | Contextos heredados de receta; no afectan Dataflow actual |
| Native Hadoop library no disponible | CONFIRMADO | Spark usa implementación incorporada; no bloqueó la ejecución |

## Instrumentación temporal Talend

Durante la investigación se añadieron indicadores seguros de longitud y
presencia de tokens en `tJava_4`: `DF_SCRIPT_LONGITUD`, `DF_SCRIPT_TIENE_*` y
`CONEXIONES_JSON_LONGITUD`. Tras validar la operación, retirarlos para evitar
ruido de log. No registrar contenido de script, catálogo ni secretos.

## Validación post-ejecución

1. Confirmar `RESULTADO_MOTOR` completo.
2. Confirmar las cuatro publicaciones esperadas.
3. Si el modo destino es `overwrite`, verificar el impacto funcional de la
   sustitución de tablas antes de habilitar automatización periódica.
4. Conservar el ID de ejecución y los hashes, no el JSON de secretos.
