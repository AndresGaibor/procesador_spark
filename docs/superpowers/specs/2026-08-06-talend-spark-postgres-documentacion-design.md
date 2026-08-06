---
titulo: Actualizacion documental Talend, Spark y PostgreSQL
identificador: SPEC-DOC-TALEND-001
tipo: diseno-documental
estado: en-revision
version: 1.0.0
audiencia:
  - desarrollo
  - operaciones
  - integracion
creado: 2026-08-06
ultima_revision: 2026-08-06
fuente_de_verdad: false
confidencialidad: interna
relacionados:
  - ../../dataflow/runbook-talend.md
  - ../../dataflow/operacion.md
---

# Actualizacion documental Talend, Spark y PostgreSQL

## Objetivo

Documentar la integracion validada entre `JOB_20_PROCESAR_SPARK`, Talend Remote
Engine, Spark 4.2, el motor Python y PostgreSQL. La documentacion debe permitir
configurar, desplegar, operar y diagnosticar el flujo sin exponer secretos.

## Alcance aprobado

Se actualizaran estos documentos canónicos:

1. `docs/dataflow/runbook-talend.md`: arquitectura, infraestructura,
   configuracion de Talend, contextos, permisos, despliegue, prueba y rollback.
2. `docs/dataflow/operacion.md`: contrato Dataflow, observabilidad y diagnostico
   de incidentes.
3. `docs/dataflow/talend-contexto.md`: referencia rapida del Job y su entorno
   `Prueba`.

## Fuentes de evidencia

| Fuente | Tipo | Uso |
| --- | --- | --- |
| `motor.py` y `motor_spark/` | canonica | CLI, resultado y ejecucion Dataflow |
| `README.md` y `docs/dataflow.md` | referencia | Contrato y uso soportado |
| Ejecuciones validadas en Talend y SSH | confirmada por usuario | Versiones, rutas, usuarios y resultado real |
| `JOB_20_PROCESAR_SPARK_0.1.item` | referencia de Talend | Componentes y argumentos del Job |

## Contenido requerido

### Arquitectura y despliegue

- Remote Engine ejecuta el Job como `talenduser`.
- El Job invoca `/opt/spark/bin/spark-submit` con Spark 4.2.
- El master compatible esta en `spark://209.50.245.140:7077`.
- El motor se sincroniza por Git en `/srv/talend-motor/motor`.
- La virtualenv puede pertenecer a `root`; no se modifica su propietario.

### Configuracion Talend

- Lista de argumentos de `tSystem_LlamarPython`, con un unico ejecutable
  `context.SPARK_SUBMIT`.
- `--packages org.postgresql:postgresql:42.7.7` antes de `context.MOTOR_SCRIPT`.
- Contenido Qlik y catalogo como argumentos independientes, nunca como una sola
  cadena de shell.
- `MOTOR_SECRETOS_JSON` como variable de entorno, nunca argumento CLI.
- `Standard Output` y `Error Output` hacia consola y variable global para captar
  `RESULTADO_MOTOR`.
- `--base-destino context.BASE_DESTINO_JSON` para publicar los `STORE` en
  PostgreSQL.

### Operacion y diagnostico

- Contrato `RESULTADO_MOTOR` para exito y error.
- Verificacion por `estado`, `operaciones`, `operaciones_ejecutadas` y
  `publicaciones`, no solo por el codigo final del Job Talend.
- Diagnosticos confirmados: ejecutable duplicado en `tSystem`, master Spark
  incompatible o inaccesible, salida estandar no capturada y JDBC sin conexion.
- Advertencias conocidas que no bloquearon la ejecucion: PyArrow ausente y
  contextos heredados de receta sin valor.

## Reglas de seguridad

- No registrar valores de `SECRETOS_JSON`, `MOTOR_SECRETOS_JSON`, claves,
  contraseñas o tokens.
- Usar nombres de secretos y ejemplos con marcadores.
- Identificar las operaciones JDBC con `overwrite` como potencialmente
  destructivas.

## Criterios de aceptacion

- Los tres documentos distinguen afirmaciones confirmadas de propuestas y
  desconocidas.
- No contienen secretos, credenciales ni valores privados.
- La configuracion permite reproducir la ejecucion confirmada de 72 operaciones
  y cuatro publicaciones PostgreSQL.
- Las contradicciones con los runbooks historicos quedan registradas.
