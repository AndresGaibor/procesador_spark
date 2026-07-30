# Runbook de Operación Dataflow

## Gates de Release

| Gate | Criterio | Responsable |
|------|----------|-------------|
| Tests unitarios | 100% passing | Dev |
| Tests integracion | 100% passing con Spark local | Dev |
| Tests diferenciales Qlik | `QLIK_GOLDEN_DIR` validation passed | Dev |
| Performance baseline | < umbral segun `DATAFLOW_PERF_ROWS` | Dev |
| Docker compose | `config --quiet` sin errores | Infra |
| SHA-256 manifest | Consistente con output | Dev |

## Observabilidad

### Métricas de Ejecución

El motor emite eventos contractuales a stdout/stderr en formato `CLAVE=valor`:

```
EJECUCION_INICIO id=<ejecucion_id>
LECTURA_COMPLETADA filas=<n>
PASO_INICIO numero=<n> tipo=<tipo>
PASO_FIN numero=<n> tipo=<tipo> columnas=<cols>
ESCRITURA_INICIO formato=<fmt>
ESCRITURA_FIN archivos=<n> bytes=<n>
RESULTADO_MOTOR=<json>
```

### Métricas Spark Disponibles

- `spark.executor.duration`: tiempo total del executor en ms
- `spark.job.duration`: tiempo por job
- `spark.sql.shuffle.partitions`: numero de particiones shuffle
- `spark.task.duration`: tiempo por tarea
- `spark.stage.duration`: tiempo por etapa

Para activar métricas detalladas:

```bash
export SPARK_CONF="spark.eventLog.enabled=true spark.eventLog.dir=/tmp/spark-events"
python motor.py --receta receta.json ...
```

### Logs de Errores

Errores de Spark y JDBC son sanitizados antes de emitirse. El resultado JSON en `RESULTADO_MOTOR` nunca contiene passwords ni secrets del entorno.

```json
{
  "estado": "ERROR",
  "ejecucion_id": "exec-001",
  "errores": [
    {
      "mensaje": "Error de conexion",
      "codigo": "CONN_ERROR",
      "ubicacion": null,
      "ayuda": null
    }
  ]
}
```

## Payload Talend / TMC

Para invocar el motor desde Talend o TMC:

```bash
python motor.py \
  --receta '/path/to/receta.json' \
  --entrada '/path/to/entrada.csv' \
  --salida 'file:///srv/talend-motor/salida/' \
  --esquema 'id:entero|nombre:texto|monto:decimal(12,2)' \
  --resultado '/tmp/resultado_exec001.json' \
  --ejecucion-id 'exec-001'
```

Variables de entorno requeridas en el engine:

| Variable | Descripcion | Ejemplo |
|----------|-------------|---------|
| `POSTGRES_PASSWORD` | Password de conexion JDBC | `secret:pass` |
| `SFTP_PASSWORD` | Password SFTP (si aplica) | `s3cr3t` |

### Contrato de Resultado

El motor escribe `/tmp/resultado_exec001.json` con:

```json
{
  "estado": "COMPLETADO",
  "ejecucion_id": "exec-001",
  "total_registros": 15000,
  "archivo_success": "/srv/talend-motor/salida/_SUCCESS",
  "cantidad_archivos_parquet": 4,
  "bytes_parquet": 2097152,
  "esquema_salida_simple": "..."
}
```

## Rollout Progresivo

### Fase 1: Canary (1%)
1. Seleccionar 1% de ejecuciones para usar motor Spark
2. Monitorizar `RESULTADO_MOTOR.estado` durante 24h
3. Si tasa de ERROR > 1%, abortar y rollback

### Fase 2: Beta (10%)
1. Ampliar a 10% de ejecuciones
2. Comparar métricas delatadas con Qlik:
   - Conteo de filas
   - SHA-256 de salida
   - Tiempo de ejecución
3. Si diff > 0, investigar antes de continuar

### Fase 3: Production (100%)
1. Cambiar todas las ejecuciones a motor Spark
2. Mantener Qlik como fallback durante 7 dias
3. Desplegar con feature flag `MOTOR_SPARK_ENABLED=true`

## Rollback a Qlik

Si se detecta degradación critica:

1. **Inmediato**: Desactivar motor Spark
   ```bash
   export MOTOR_SPARK_ENABLED=false
   ```

2. **Verificar**: Confirma que ejecuciones vuelven a Qlik
   ```bash
   grep "RESULTADO_MOTOR" /path/to/logs/*.log | grep -c "MOTOR_SPARK"
   ```

3. **Recompilar**: Si el problema es de datos, no de motor:
   ```bash
   rm -rf /tmp/.staging/*
   python motor.py --receta ...  # re-ejecutar con datos frescos
   ```

4. **Post-mortem**: Documentar en Jira con:
   - Timestamp de detección
   - Logs de error
   - SHA-256 del manifest que falló
   - Remediation action items

## Precondiciones

### Runtime
- [ ] Java 11+ instalado y en PATH
- [ ] Python 3.10+ con venv activa
- [ ] PySpark instalado en venv
- [ ] 8GB RAM mínimo para JVM Spark
- [ ] Acceso de lectura a `/path/to/entrada.csv`
- [ ] Acceso de escritura a `/srv/talend-motor/salida/`

### Variables de Entorno
- [ ] `POSTGRES_PASSWORD` configurado (formato: `user:password`)
- [ ] `SFTP_PASSWORD` configurado (si se usa SFTP staging)
- [ ] `MOTOR_SPARK_ENABLED=true` (defecto)

### Networking
- [ ] Acceso a endpoint JDBC si `leer_jdbc` está en receta
- [ ] Puerto 5432 PostgreSQL reachable si aplica
- [ ] Puerto 2222 SFTP reachable si aplica

### Datos
- [ ] Archivo de entrada existe y es legible
- [ ] Schema CSV coincide con `--esquema` declarado
- [ ] Espacio en disco > 2x tamaño de salida esperada

## Bloqueos Externos

| Recurso | Bloqueo | Accion |
|---------|---------|--------|
| Qlik Cloud | Licencia activa | Contactar Platform Ops si expira |
| PostgreSQL | Lock de transaccion | Esperar o matar session_id |
| SFTP | Upload en progreso | Esperar o limpiar /home/dataflow/upload |
| HDFS | Replicacion不一致 | `hdfs dfsadmin -refreshNodes` |
| Talend JobServer | Ejecucion activa | Esperar o cancelar job |

## Comandos de Debug

```bash
# Ver logs de Spark
cat /tmp/spark.log 2>/dev/null || echo "No Spark log"

# Ver staging
ls -la /tmp/.staging/ 2>/dev/null || echo "No staging"

# Ver resultado
cat /tmp/resultado_exec001.json | python -m json.tool

# Validar compose
POSTGRES_PASSWORD=test docker compose -f tests/infra/docker-compose.dataflow.yml config --quiet

# Test rapido sin Spark
python -c "from motor_spark.dataflow_script.publicacion import StagingManager; print('OK')"
```
