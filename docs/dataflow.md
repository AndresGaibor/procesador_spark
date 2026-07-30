# Ejecución de scripts Qlik Dataflow

El modo Dataflow traduce un subconjunto explícito de scripts Qlik a un plan tipado y después lo ejecuta con Spark. La compilación es **fail-closed**: una construcción sin equivalencia implementada detiene el proceso en lugar de producir una aproximación silenciosa.

## Flujo de ejecución

```text
script Qlik
 -> normalización preservando posiciones
 -> lexer y parser
 -> validación semántica
 -> plan Pydantic serializable
 -> ejecución Spark
 -> publicación local o SFTP
 -> RESULTADO_MOTOR en JSON
```

El plan intermedio conserva nombres lógicos, fuentes JDBC, proyecciones, expresiones, filtros, agregaciones, joins, concatenaciones, eliminaciones y publicaciones. Sus identificadores y hash son deterministas para el mismo script.

## Compilar sin ejecutar

```bash
python motor.py \
  --dataflow-script tests/recursos/dataflow/scripts/bancolombia_ventas_completo.qvs \
  --conexiones /ruta/segura/conexiones.json \
  --ejecucion-id bancolombia-compilacion-001 \
  --solo-compilar \
  --plan-salida /tmp/bancolombia-plan.json \
  --resultado /tmp/bancolombia-compilacion.json
```

`--solo-compilar` no crea una `SparkSession`, no abre JDBC y no resuelve secretos. El comando exige `--plan-salida`.

## Ejecutar

```bash
python motor.py \
  --dataflow-script /ruta/dataflow.qvs \
  --conexiones /ruta/segura/conexiones.json \
  --ejecucion-id bancolombia-ejecucion-001 \
  --resultado /tmp/bancolombia-resultado.json
```

La ejecución se detiene en la primera operación fallida. Spark se cierra exactamente una vez desde la capa de aplicación.

## Catálogo de conexiones

El script solo referencia nombres lógicos de `LIB CONNECT TO` y `lib://`. Las URLs, rutas base y allowlists se declaran fuera del script:

```json
{
  "version": 1,
  "jdbc": [
    {
      "nombre": "Bancolombia prueba:Postgres_BanColombia_Prueba",
      "url": "jdbc:postgresql://postgres.internal:5432/banco",
      "driver": "org.postgresql.Driver",
      "secreto_nombre": "POSTGRES_BANCOLOMBIA",
      "allowlist": [
        {
          "esquema": "demo_dataflow",
          "tabla": "ventas_2025",
          "campos": ["venta_id", "fecha_venta", "cliente_id"]
        }
      ],
      "propiedades": {}
    }
  ],
  "locales": [
    {
      "nombre": "Archivos",
      "ruta_base": "/srv/talend-motor/archivos",
      "allowlist": [
        {"esquema": "", "tabla": "entrada/ventas.csv", "campos": []}
      ]
    }
  ],
  "sftp": [
    {
      "nombre": "Bancolombia prueba:SFTP",
      "host": "sftp.internal",
      "puerto": 22,
      "secreto_nombre": "SFTP_BANCOLOMBIA",
      "ruta_base": "/upload",
      "allowlist": [
        {"esquema": "", "tabla": "ventas_curadas.csv", "campos": []}
      ]
    }
  ]
}
```

Los secretos no se guardan en el catálogo. El resolvedor admite variables de entorno o valores inyectados mediante el contrato seguro de ejecución. Para usuario y contraseña SFTP, el valor esperado es `usuario:password`.

## Construcciones soportadas

- `SET` y `LIB CONNECT TO`.
- Etiquetas Qlik simples o entre corchetes.
- `SELECT` JDBC con esquema, tabla, proyección, alias, `WHERE` y `GROUP BY`.
- `LOAD`, preceding LOAD y `LOAD RESIDENT`.
- `DISTINCT`, `COUNT(DISTINCT ...)`, `SUM`, `AVG`, `MIN` y `MAX`.
- Expresiones aritméticas y lógicas tipadas.
- `Trim`, `Match`, `Coalesce`, `IsNull`, `IndexRegEx`, `Num`, `Month`, `Year` e `IF`.
- `Window(WRank(1,1), ...)` con ranking competitivo.
- `CONCATENATE(...)`, `NOCONCATENATE` y `LEFT JOIN(...)` natural.
- `DROP TABLE` y `STORE ... INTO lib://...`.

Construcciones como `UNION`, `RIGHT JOIN`, `FULL JOIN`, `HAVING`, `CASE`, SQL libre o modos WRank no implementados se rechazan explícitamente.

## Seguridad

### JDBC

- La conexión y la tabla deben existir en el catálogo.
- El esquema se resuelve de forma exacta; una tabla homónima no selecciona automáticamente la primera coincidencia.
- Las columnas deben estar permitidas por la allowlist.
- Propiedades reservadas como `dbtable`, `query`, `url`, `user`, `password` y `driver` no pueden sobrescribirse desde el catálogo.
- El driver JDBC se configura explícitamente.

### Rutas `lib://`

- Se rechazan rutas absolutas, `..`, backslashes, NUL, componentes vacíos y codificación URL.
- Las rutas locales se resuelven y deben permanecer dentro de su directorio base, incluso si existen enlaces simbólicos.
- Los identificadores de staging y nombres de salida son componentes atómicos; no aceptan separadores de ruta.

### SFTP

- Paramiko carga las claves conocidas del sistema y usa `RejectPolicy`; nunca se usa `AutoAddPolicy`.
- El host debe estar registrado previamente en `known_hosts` del usuario que ejecuta el Remote Engine.
- La publicación carga primero `archivo.partial` con confirmación y luego lo renombra al nombre definitivo.
- Ante error se intenta eliminar el parcial y se cierran SFTP y SSH sin ocultar la causa original.
- El directorio remoto debe existir y estar autorizado por la allowlist.

Ejemplo para registrar una clave de host antes del despliegue:

```bash
ssh-keyscan -p 22 sftp.internal >> ~/.ssh/known_hosts
ssh-keygen -F sftp.internal
```

En producción, la huella obtenida debe compararse por un canal confiable antes de añadirla.

## Script de aceptación

El repositorio incluye `tests/recursos/dataflow/scripts/bancolombia_ventas_completo.qvs`, protegido por checksum. Su contrato actual compila a:

- 72 operaciones.
- 7 lecturas JDBC.
- 1 concatenación.
- 5 LEFT JOIN.
- 4 publicaciones.
- 2 filtros.
- 1 agregación.

Las pruebas verifican también el hash determinista y el round-trip JSON exacto del plan.

## Pruebas relevantes

```bash
pytest -q tests/unitarios/dataflow_script
pytest -q tests/integracion/test_dataflow_complejo_e2e.py
pytest -q tests/integracion/test_ejecutor_plan_dataflow.py
```

Las pruebas diferenciales requieren `QLIK_GOLDEN_DIR`. Las pruebas de rendimiento requieren `DATAFLOW_PERF_ROWS`; se omiten de forma visible cuando esos artefactos externos no están configurados.
