---
titulo: Runbook Talend Remote Engine, Spark y PostgreSQL
identificador: RUNBOOK-TALEND-001
tipo: runbook
estado: vigente
version: 2.0.0
propietario: por-asignar
audiencia:
  - desarrollo
  - operaciones
  - integracion
creado: 2026-07-31
ultima_revision: 2026-08-06
proxima_revision: 2026-11-06
fuente_de_verdad: true
confidencialidad: interna
relacionados:
  - ./talend-contexto.md
  - ./operacion.md
  - ../dataflow.md
---

# Runbook Talend Remote Engine, Spark y PostgreSQL

> Estado de las afirmaciones: **CONFIRMADO** salvo indicación contraria.
> Este documento no contiene contraseñas, claves, tokens ni contenido de
> `SECRETOS_JSON`.

## Propósito y alcance

Este runbook describe la ejecución de `JOB_20_PROCESAR_SPARK` desde Talend
Remote Engine hacia Spark y PostgreSQL. El Job recibe el Qlik y el catálogo de
conexiones como contenido inline, compila un plan tipado y publica cada `STORE`
en la base destino configurada.

La referencia de componentes y contextos está en
[`talend-contexto.md`](./talend-contexto.md). El contrato observable está en
[`operacion.md`](./operacion.md).

## Arquitectura validada

```text
Talend Remote Engine (usuario talenduser)
  -> tSystem_LlamarPython
    -> /opt/spark/bin/spark-submit (Spark 4.2.0)
      -> spark://209.50.245.140:7077
        -> worker Spark (usuario spark)
          -> /srv/talend-motor/motor/motor.py
            -> fuentes PostgreSQL por JDBC
            -> tablas PostgreSQL por JDBC
```

El master de Spark 4.2 escucha en `spark://209.50.245.140:7077`.
`127.0.0.1:7077` fue rechazado con `Connection refused`; el master histórico
en `127.0.0.1:7177` usa otra distribución de Spark y produjo una
incompatibilidad de serialización. No combinar esas instalaciones.

## Estado validado

La ejecución Talend con ID `123456` confirmó:

- `estado: COMPLETADO`.
- 72 operaciones compiladas y 72 ejecutadas.
- Lecturas JDBC desde las fuentes permitidas del catálogo.
- Publicaciones JDBC: `public.ventas_rechazadas`, `public.ventas_curadas`,
  `public.muestra_calidad` y `public.resumen_mensual`.
- `RESULTADO_CAPTURADO=true`.

El resultado JSON y sus campos son la evidencia operativa; el código de salida
del Job Talend por sí solo no es suficiente.

## Repositorio y despliegue

El código desplegado vive en `/srv/talend-motor/motor` y se actualiza mediante
Git, no mediante copias manuales de código:

```bash
git -C /srv/talend-motor/motor status --short
git -C /srv/talend-motor/motor pull --ff-only origin main
git -C /srv/talend-motor/motor rev-parse HEAD origin/main
```

La virtualenv puede pertenecer a `root` y al grupo `spark`. No cambiar su
propietario. Si se requiere instalar dependencias en esa virtualenv, la
operación requiere autorización explícita y `sudo` sobre su intérprete, no
`sudo pip` global.

## Precondiciones

| Elemento | Estado | Verificación |
| --- | --- | --- |
| Motor actualizado por Git | CONFIRMADO | `HEAD` coincide con `origin/main` |
| Java 21 y Spark 4.2 | CONFIRMADO | `spark-submit --version` |
| Master Spark 4.2 | CONFIRMADO | conexión a `spark://209.50.245.140:7077` |
| Driver PostgreSQL | CONFIRMADO | `org.postgresql:postgresql:42.7.7` |
| Python y PySpark | CONFIRMADO | `.venv/bin/python` y PySpark 4.2 |
| Salida de resultado escribible | CONFIRMADO | `RUTA_RESULTADO_JSON` válida |
| Datos y secretos de Talend | CONFIRMADO por ejecución | contextos inline válidos |

## Seguridad y datos sensibles

`MOTOR_SECRETOS_JSON` se entrega como variable de entorno de `tSystem`:

```text
Nombre: "MOTOR_SECRETOS_JSON"
Valor: context.SECRETOS_JSON
```

No pasar secretos mediante `--secreto` en Talend ni imprimir
`SECRETOS_JSON`. `BASE_DESTINO_JSON` debe contener un nombre de secreto, no la
credencial. Ejemplo seguro:

```json
{
  "tipo": "postgres",
  "host": "<host>",
  "puerto": 5432,
  "database": "<base>",
  "esquema": "public",
  "secreto_nombre": "POSTGRES_DESTINO",
  "modo": "overwrite"
}
```

> Estado: CONFIRMADO. `modo: "overwrite"` puede reemplazar tablas. Confirmar
> explícitamente el destino y el modo antes de ejecutar en entornos con datos
> relevantes.

## Publicación JDBC

La publicación a PostgreSQL se activa con:

```text
--base-destino
context.BASE_DESTINO_JSON
```

El JAR JDBC debe ser una opción de `spark-submit` antes de `motor.py`:

```text
--packages
org.postgresql:postgresql:42.7.7
```

No instalar controladores JDBC con `pip`. Spark descarga o reutiliza el JAR en
la caché Ivy del usuario que ejecuta el Remote Engine.

## Ejecución y verificación

1. Comprobar que el entorno Talend seleccionado es `Prueba` y sus contextos
   están completos.
2. Ejecutar el Job desde Talend Remote Engine.
3. Localizar `RESULTADO_MOTOR=` en el log o en `tSystem_1_OUTPUT`.
4. Confirmar `estado: COMPLETADO`.
5. Confirmar que `operaciones` y `operaciones_ejecutadas` tienen el valor
   esperado.
6. Confirmar las tablas esperadas en `publicaciones`.

## Diagnóstico rápido

| Síntoma | Causa confirmada o probable | Acción |
| --- | --- | --- |
| `Failed to get main class in JAR` | Dos ejecutables `spark-submit` en el array | Dejar únicamente `context.SPARK_SUBMIT` |
| `InvalidClassException` al master | Driver y master Spark de versiones distintas | Usar el master Spark 4.2 configurado |
| `Connection refused` a `127.0.0.1:7077` | El master 4.2 no escucha en loopback | Usar `spark://209.50.245.140:7077` |
| `RESULTADO_CAPTURADO=false` | Standard Output no estaba almacenado globalmente | Enviar Standard Output a consola y variable global |
| `FAILED_JDBC.CONNECTION` | Catálogo, secreto o conectividad JDBC inválidos | Verificar nombre de secreto, URL, red y driver sin exponer valores |
| `DFS_EMPTY_PLAN` | Script Qlik truncado o solo directivas `SET` | Verificar longitud y presencia de `LIB CONNECT`, `SELECT`, `LOAD`, `STORE` |

## Recuperación y rollback

Si la publicación falla antes de `COMPLETADO`, el motor devuelve un resultado
con `estado: ERROR` y detiene la operación fallida. No reintentar una carga con
`overwrite` sin revisar las tablas ya publicadas y el error devuelto.

Para volver al código anterior, hacerlo con Git y una revisión explícita del
commit destino. No usar comandos destructivos de Git ni cambiar propietarios
del árbol o de `.venv`.

## Historia documental

Las versiones previas de este runbook mencionaban Spark 3.4.4, el wrapper
`spark-talend-submit`, publicaciones SFTP y el contrato de resultados de
recetas. Esas descripciones son históricas y no aplican a
`JOB_20_PROCESAR_SPARK` validado el 2026-08-06.
