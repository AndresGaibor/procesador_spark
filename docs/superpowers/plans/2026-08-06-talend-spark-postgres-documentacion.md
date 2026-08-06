# Talend Spark PostgreSQL Documentation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Dejar documentación operativa y verificable para ejecutar `JOB_20_PROCESAR_SPARK` desde Talend Remote Engine hacia PostgreSQL.

**Architecture:** Se sustituirá el runbook obsoleto por la configuración validada de Talend, Spark 4.2 y el motor Python. Un documento de operación definirá el contrato de observabilidad y fallos; una referencia de contexto separará los valores de componentes Talend de la infraestructura del servidor.

**Tech Stack:** Talend Remote Engine, tSystem, Spark 4.2, PySpark, PostgreSQL JDBC, Git y Markdown.

## Global Constraints

- No incluir secretos, contraseñas, claves, tokens ni valores de `SECRETOS_JSON`.
- Marcar toda afirmación como `CONFIRMADO`, `PROPUESTO` o `DESCONOCIDO`.
- No recomendar cambios de propietarios ni comandos `sudo` sin una necesidad explícita.
- Documentar solo rutas, versiones y resultados respaldados por ejecución o código.
- Mantener `RESULTADO_MOTOR` como contrato operativo primario.

---

### Task 1: Actualizar el runbook canónico

**Files:**
- Modify: `docs/dataflow/runbook-talend.md`

**Interfaces:**
- Consumes: `README.md`, `docs/dataflow.md`, `motor_spark/configuracion/argumentos.py`, `motor_spark/aplicacion/ejecutor_dataflow.py` y evidencia de ejecución remota.
- Produces: arquitectura, configuración de servidor, seguridad, despliegue y rollback para Talend/Spark/PostgreSQL.

- [ ] **Step 1: Reemplazar arquitectura histórica incompatible**

Documentar el flujo confirmado:

```text
Talend Remote Engine (talenduser)
  -> /opt/spark/bin/spark-submit (Spark 4.2)
    -> spark://209.50.245.140:7077
      -> worker Spark
        -> /srv/talend-motor/motor/motor.py
          -> fuentes JDBC
          -> publicaciones PostgreSQL JDBC
```

- [ ] **Step 2: Documentar despliegue y permisos**

Incluir `git pull --ff-only`, la ruta del repositorio, el grupo `spark`, la
virtualenv existente y el requisito de no modificar propietarios.

- [ ] **Step 3: Documentar publicación JDBC y secretos**

Describir `--base-destino`, el driver PostgreSQL aprobado y
`MOTOR_SECRETOS_JSON` como variable de entorno. Identificar `overwrite` como
operación potencialmente destructiva.

- [ ] **Step 4: Validar metadatos y enlaces internos**

Ejecutar:

```bash
git diff --check
```

Esperado: sin errores de espacios ni enlaces relativos rotos en el documento.

### Task 2: Actualizar operación y observabilidad

**Files:**
- Modify: `docs/dataflow/operacion.md`

**Interfaces:**
- Consumes: contrato emitido por `_guardar_y_emitir` en `motor_spark/aplicacion/ejecutor_dataflow.py`.
- Produces: interpretación de `RESULTADO_MOTOR`, checklist de verificación y diagnóstico de incidencias.

- [ ] **Step 1: Sustituir el contrato de recetas por Dataflow**

Documentar el resultado mínimo:

```json
{
  "estado": "COMPLETADO",
  "ejecucion_id": "<id>",
  "operaciones": 72,
  "operaciones_ejecutadas": 72,
  "publicaciones": []
}
```

- [ ] **Step 2: Añadir diagnóstico confirmado**

Incluir acciones para ejecutable `spark-submit` duplicado, master Spark no
compatible, master no escuchando en loopback, salida estándar no capturada,
error JDBC y plan vacío/truncado.

- [ ] **Step 3: Registrar las advertencias no bloqueantes**

Separar PyArrow ausente y parámetros heredados de receta vacíos de los errores
que detienen la publicación.

- [ ] **Step 4: Validar consistencia del contrato**

Comprobar que los nombres `estado`, `operaciones`, `operaciones_ejecutadas` y
`publicaciones` coinciden con `ejecutor_dataflow.py`.

### Task 3: Crear referencia de contexto y componentes Talend

**Files:**
- Create: `docs/dataflow/talend-contexto.md`

**Interfaces:**
- Consumes: configuración observada de `JOB_20_PROCESAR_SPARK` y CLI Dataflow.
- Produces: guía de configuración repetible para el entorno `Prueba`.

- [ ] **Step 1: Documentar `tSystem_LlamarPython`**

Incluir la secuencia de argumentos, en este orden:

```text
context.SPARK_SUBMIT
--master
context.SPARK_MASTER
--deploy-mode
client
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

- [ ] **Step 2: Documentar parámetros de entorno y salidas**

Especificar `PYSPARK_PYTHON`, `PYSPARK_DRIVER_PYTHON` y
`MOTOR_SECRETOS_JSON`; exigir que `Standard Output` y `Error Output` se envíen
a consola y variable global.

- [ ] **Step 3: Crear matriz de contextos**

Incluir `SPARK_SUBMIT`, `SPARK_MASTER`, `PYTHON_SPARK`, `MOTOR_SCRIPT`,
`DF_SCRIPT`, `CONEXIONES_JSON`, `BASE_DESTINO_JSON`, `SECRETOS_JSON`,
`EJECUCION_ID` y `RUTA_RESULTADO_JSON`, con formato, sensibilidad y propósito.

- [ ] **Step 4: Añadir checklist y limpieza**

Documentar la verificación de resultado, las cuatro tablas de la ejecución
validada y la eliminación de instrumentación temporal `DF_SCRIPT_*`.

### Task 4: Revisión documental final

**Files:**
- Modify: `docs/dataflow/runbook-talend.md`
- Modify: `docs/dataflow/operacion.md`
- Create: `docs/dataflow/talend-contexto.md`

**Interfaces:**
- Consumes: los documentos actualizados de las tareas 1 a 3.
- Produces: documentación coherente, sin contradicciones operativas históricas.

- [ ] **Step 1: Buscar secretos accidentales**

```bash
rg -n --glob '*.md' '(?i)(password|secretos_json|private_key|token)' docs/dataflow
```

Esperado: referencias de nombres y políticas, sin valores de secretos.

- [ ] **Step 2: Validar formato Markdown y espacios**

```bash
git diff --check
```

Esperado: salida vacía.

- [ ] **Step 3: Revisar contradicciones**

Confirmar que ninguna guía vigente recomiende Spark 3.4.4, el wrapper histórico
o el contrato de resultados de recetas para este Job Dataflow.

## Cobertura de especificación

- Arquitectura, despliegue, seguridad y rollback: Task 1.
- Contrato de ejecución, observabilidad e incidencias: Task 2.
- Componentes Talend, contextos y validación de salida: Task 3.
- Seguridad documental, formato y contradicciones: Task 4.
