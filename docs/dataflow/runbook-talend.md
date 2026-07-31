---
titulo: Runbook Talend, Spark, JDBC y SFTP
identificador: RUNBOOK-TALEND-001
tipo: runbook
estado: vigente
version: 1.0.0
propietario: por-asignar
audiencia:
  - desarrollo
  - operaciones
creado: 2026-07-31
ultima_revision: 2026-07-31
fuente_de_verdad: true
confidencialidad: interna
relacionados:
  - ./operacion.md
  - ../dataflow.md
---

# Runbook Talend, Spark, JDBC y SFTP

> Estado: CONFIRMADO salvo cuando se indique otro estado.
>
> Este documento registra la puesta en marcha validada de un Dataflow Qlik
> desde Talend Remote Engine hacia Spark 3.4.4, PostgreSQL y SFTP. No contiene
> contrasenas, claves privadas, tokens ni endpoints productivos.

## Resultado validado

La ejecucion `123456` finalizo con `estado: COMPLETADO` el 2026-07-31.

- Plan: 72 operaciones compiladas y ejecutadas.
- Fuentes JDBC: ventas_2025, ventas_2026, clientes, productos, sucursales,
  vendedores y devoluciones.
- Publicaciones SFTP: cuatro archivos.

| Archivo | Bytes | SHA-256 |
| --- | ---: | --- |
| `ventas_rechazadas.csv` | 495367 | `08f6bfe3ee8d775e5983d5079462de95c93d8ffe35346d01fa332fc46a82cdb8` |
| `ventas_curadas.csv` | 58364181 | `092a14ec9305619f0b68cf2904485d0c9d57f18f2c34cb0487892baaf8c666a3` |
| `muestra_calidad.csv` | 58364181 | `092a14ec9305619f0b68cf2904485d0c9d57f18f2c34cb0487892baaf8c666a3` |
| `resumen_mensual.csv` | 52131 | `4ce33b2616cdcc72f7bc4af5b4a4ae133380c4104c84385ac5119864333b9afd` |

La fuente canonia del resultado de una ejecucion es el archivo enviado con
`--resultado`, por ejemplo `/srv/talend-motor/resultados/<ejecucion-id>.json`.
El codigo de salida de Talend por si solo no representa el estado del motor.

## Arquitectura y contrato

```text
Talend Remote Engine (talenduser)
  -> /usr/local/bin/spark-talend-submit
    -> sudo -u spark
      -> /opt/spark-talend/bin/spark-submit
        -> /srv/talend-motor/motor/motor.py
          -> PostgreSQL por JDBC
          -> staging local file:///
          -> SFTP
```

Talend invoca el motor en modo contenido:

```text
--dataflow-script-contenido <DF_SCRIPT>
--conexiones-contenido <CONEXIONES_JSON>
--ejecucion-id <id>
--resultado /srv/talend-motor/resultados/<id>.json
```

En `tSystem`, `MOTOR_SECRETOS_JSON` se debe configurar como parametro de
entorno, no como argumento de linea de comandos:

```text
Nombre: "MOTOR_SECRETOS_JSON"
Valor:  context.SECRETOS_JSON
```

No imprimir el valor de `SECRETOS_JSON`, `MOTOR_SECRETOS_JSON` ni de las
variables que contengan claves o contrasenas.

## Catalogo de conexiones

El catalogo se envia en `--conexiones-contenido`. El driver JDBC se declara por
clase, nunca por una coordenada Maven controlada por el usuario:

```json
{
  "version": 1,
  "jdbc": [
    {
      "nombre": "ConexionPostgres",
      "url": "jdbc:postgresql://<host>/<base>?sslmode=require",
      "driver": "org.postgresql.Driver",
      "secreto_nombre": "POSTGRES_BANCOLOMBIA",
      "allowlist": [
        { "esquema": "demo_dataflow", "tabla": "ventas_2025", "campos": [] }
      ],
      "propiedades": { "fetchsize": "10000" }
    }
  ],
  "locales": [],
  "sftp": [
    {
      "nombre": "ConexionSftp",
      "host": "<host-sftp-resoluble>",
      "puerto": 22,
      "usuario": "<usuario-sftp>",
      "secreto_clave_privada_nombre": "SFTP_PRIVATE_KEY_B64",
      "ruta_base": "/upload",
      "allowlist": [
        { "esquema": "", "tabla": "ventas_curadas.csv", "campos": [] }
      ]
    }
  ]
}
```

Los nombres de conexion en `LIB CONNECT TO` y `lib://` deben coincidir
exactamente con `nombre` del catalogo.

### Rutas `lib://`

Dataflow puede generar este destino:

```qlik
STORE [Tabla] INTO [lib://ConexionSftp//upload/ventas_curadas.csv] (txt);
```

Con `ruta_base: "/upload"`, el motor normaliza de forma segura la ruta a
`ventas_curadas.csv`; el destino final es `/upload/ventas_curadas.csv`.

Solo se normaliza el prefijo absoluto que coincide exactamente con
`ruta_base`. Se rechazan traversal (`..`), prefijos diferentes, rutas no
incluidas en la allowlist y extensiones distintas de `.csv` o `.txt`.

### Placeholder de host

`"host": "__SFTP_HOST__"` no se sustituye automaticamente dentro del JSON.
El catalogo enviado debe contener un host o IP resoluble. Si Talend conserva el
placeholder, se producira `gaierror: Name or service not known`.

> Estado: PROPUESTO
> Construir `CONEXIONES_JSON` desde una plantilla controlada por Talend que
> sustituya los placeholders antes de llamar a `tSystem`.

## Secretos

`MOTOR_SECRETOS_JSON` es un objeto JSON. Los valores no pueden incluir saltos
de linea, por lo que una clave privada debe codificarse como Base64 de una sola
linea.

```json
{
  "POSTGRES_BANCOLOMBIA": "<usuario>:<contrasena>",
  "SFTP_PRIVATE_KEY_B64": "<pem-completo-codificado-en-base64>"
}
```

El motor valida JSON, duplicados, nombres, valores vacios, NUL y saltos de
linea. El limite actual de `MOTOR_SECRETOS_JSON` es 10 MiB. La precedencia es:

1. `--secreto` explicito.
2. `MOTOR_SECRETOS_JSON`.
3. Variable de entorno individual con el mismo nombre.

## Wrapper y permisos

El wrapper confirmado en el servidor actual es
`/usr/local/bin/spark-talend-submit`. Si el usuario que recibe el Job no es
`spark`, debe relanzarse con este patron:

```bash
exec sudo -n \
  --preserve-env=MOTOR_SECRETOS_JSON \
  -u spark -H \
  /usr/local/bin/spark-talend-submit "$@"
```

La politica `sudoers` debe permitir conservar solo esa variable para el
usuario que lanza Talend. En la instalacion validada se uso un drop-in:

```sudoers
Defaults:talenduser env_keep += "MOTOR_SECRETOS_JSON"
```

Validar y editar siempre con `visudo`. No usar `sudo -E`, porque conservaria
todo el entorno del proceso Talend.

## Drivers JDBC automaticos

El wrapper ejecuta antes de Spark:

```text
python -m motor_spark.configuracion.paquetes_jdbc <argumentos-del-job>
```

El resolvedor lee el catalogo y agrega `--packages` a `spark-submit` solo para
drivers declarados en el registro de confianza. El catalogo no puede indicar
un artefacto Maven arbitrario.

| Clase JDBC | Paquete Maven aprobado |
| --- | --- |
| `org.postgresql.Driver` | `org.postgresql:postgresql:42.7.7` |
| `com.microsoft.sqlserver.jdbc.SQLServerDriver` | `com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11` |
| `com.mysql.cj.jdbc.Driver` | `com.mysql:mysql-connector-j:8.4.0` |
| `org.mariadb.jdbc.Driver` | `org.mariadb.jdbc:mariadb-java-client:3.5.2` |

La primera ejecucion con un driver requiere acceso HTTPS a Maven Central. Spark
guarda los JAR en la cache Ivy del usuario `spark`, validada en este servidor
en `/var/lib/spark/.ivy2`. Un driver no registrado falla antes de iniciar
Spark. Para un entorno aislado, preparar un repositorio Maven interno o usar
un JAR revisado mediante la configuracion de Spark.

## Compatibilidad Spark y staging

Spark 3.4.4 no expone `regexp_instr` en `pyspark.sql.functions`. `IndexRegEx`
se implementa como UDF tipada que conserva posicion 1-based, ocurrencia, cero
sin coincidencia y `NULL` de entrada.

Antes de subir por SFTP, el motor materializa un unico CSV. La ruta de staging
debe enviarse a Spark como `file:///...`; una ruta `/tmp/...` sin esquema se
interpreta como HDFS y puede fallar con `AccessControlException` para `spark`.

## Seguridad SFTP

El motor usa `paramiko.RejectPolicy`: un host SFTP desconocido o con clave
rotada falla de forma segura. Registrar la clave del host bajo el usuario
efectivo `spark`:

```bash
ssh-keyscan -T 10 -p 22 <host-sftp> > /tmp/sftp-known-hosts
ssh-keygen -lf /tmp/sftp-known-hosts

sudo install -d -o spark -g spark -m 700 /var/lib/spark/.ssh
sudo install -o spark -g spark -m 600 \
  /tmp/sftp-known-hosts /var/lib/spark/.ssh/known_hosts
sudo -u spark ssh-keygen -F <host-sftp> -f /var/lib/spark/.ssh/known_hosts
```

Antes de instalar una huella, compararla por un canal confiable. En el caso
validado, las huellas obtenidas por `ssh-keyscan` coincidieron con las claves
publicas locales del servidor SFTP.

La publicacion usa archivo `.partial` y promocion atomica cuando el servidor
soporta `posix_rename`; si no, usa backup y restauracion ante fallo.

## Instalacion en un servidor nuevo

### 1. Preparar runtime

> Estado: CONFIRMADO para el servidor validado; adaptar rutas y versiones al
> nuevo ambiente.

1. Instalar Java compatible con Spark, Python y Spark 3.4.4.
2. Crear usuarios separados para Talend y Spark. El usuario Spark necesita
   escritura en su cache Ivy y directorios locales de Spark.
3. Instalar el motor en `/srv/talend-motor/motor` desde el repositorio Git.
4. Crear el entorno virtual y dependencias del proyecto.
5. Instalar o configurar el cluster Spark y el `HADOOP_CONF_DIR` que aplique.
6. Crear `/srv/talend-motor/resultados` con escritura para el proceso que
   genere el contrato de resultado.

Ejemplo de obtencion de codigo:

```bash
sudo mkdir -p /srv/talend-motor
sudo chown <usuario-despliegue>:spark /srv/talend-motor
git clone <url-remoto-autorizada> /srv/talend-motor/motor
cd /srv/talend-motor/motor
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

### 2. Instalar el wrapper

Crear `/usr/local/bin/spark-talend-submit` como `root:spark`, modo `750`.
Debe hacer estas cuatro cosas:

1. Relanzarse como `spark` conservando solo `MOTOR_SECRETOS_JSON`.
2. Definir `JAVA_HOME`, `PYSPARK_PYTHON`, `PYSPARK_DRIVER_PYTHON` y
   `HADOOP_CONF_DIR` adecuados al nuevo servidor.
3. Ejecutar `motor_spark.configuracion.paquetes_jdbc` con los argumentos para
   obtener los paquetes JDBC aprobados.
4. Llamar a `spark-submit`, anteponiendo `--packages` cuando el resultado no
   sea vacio.

Tras instalarlo:

```bash
sudo bash -n /usr/local/bin/spark-talend-submit
stat -c 'MODO=%a PROPIETARIO=%U GRUPO=%G' /usr/local/bin/spark-talend-submit
```

El resultado esperado es propietario `root`, grupo `spark` y modo `750`.

### 3. Configurar sudo y secretos

1. Crear `/etc/sudoers.d/talend-motor-secretos` con el `env_keep` restringido.
2. Ejecutar `sudo visudo -cf /etc/sudoers.d/talend-motor-secretos`.
3. En Talend, marcar `SECRETOS_JSON` como `Password` y enviarlo a `tSystem`
   como `MOTOR_SECRETOS_JSON`.
4. Verificar presencia, no contenido, antes de Spark:

```java
System.out.println("SECRETOS_JSON_PRESENTE=" +
    (context.SECRETOS_JSON != null && !context.SECRETOS_JSON.trim().isEmpty()));
System.out.println("POSTGRES_SECRET_PRESENTE=" +
    (context.SECRETOS_JSON != null &&
     context.SECRETOS_JSON.contains("\"POSTGRES_BANCOLOMBIA\"")));
```

### 4. Configurar JDBC y red

1. Declarar en el catalogo el driver JDBC permitido y las allowlists minimas.
2. Confirmar la primera resolucion Maven desde `spark`; conservar la cache o
   configurar un repositorio interno para servidores sin Internet.
3. Validar red hacia PostgreSQL y SFTP desde el servidor nuevo.
4. Registrar y verificar `known_hosts` para `spark` antes de la primera carga.

### 5. Configurar Talend

El array de `tSystem` debe contener el script, catalogo, id y resultado. No
agregar `--packages` en Talend: el wrapper lo resuelve desde el catalogo.

```text
context.SPARK_SUBMIT
... configuracion Spark ...
context.MOTOR_SCRIPT
"--dataflow-script-contenido"
context.DF_SCRIPT
"--conexiones-contenido"
context.CONEXIONES_JSON
"--ejecucion-id"
context.EJECUCION_ID
"--resultado"
context.RESULTADOS_BASE + "/" + context.EJECUCION_ID + ".json"
```

### 6. Prueba de aceptacion

1. Usar un catalogo sin secretos embebidos y un Dataflow con un `STORE` SFTP.
2. Confirmar que Ivy informa el driver durante el primer arranque.
3. Revisar el JSON `--resultado`, no solo el exit code de Talend.
4. Confirmar `estado: COMPLETADO`, numero esperado de operaciones y manifiestos
   de publicaciones.
5. Validar nombres, bytes y SHA-256 contra el SFTP cuando sea necesario.

## Diagnostico de fallos observados

| Error | Causa | Accion |
| --- | --- | --- |
| `Secreto 'POSTGRES_BANCOLOMBIA' no encontrado` | `sudo` elimino `MOTOR_SECRETOS_JSON` | Configurar `env_keep` y `--preserve-env` para `talenduser -> spark`. |
| `ClassNotFoundException: org.postgresql.Driver` | Spark inicio sin JAR PostgreSQL | Verificar el resolvedor JDBC y acceso Ivy/Maven. |
| `functions has no attribute regexp_instr` | API PySpark 3.4 | Usar la version del motor con UDF `IndexRegEx`. |
| `AccessControlException ... inode="/"` | staging `/tmp` tratado como HDFS | Usar la version con `file:///` para staging local. |
| `gaierror: Name or service not known` | Host SFTP placeholder o no resoluble | Enviar host real en el catalogo. |
| `Server ... not found in known_hosts` | Falta clave SSH para `spark` | Registrar la huella validada en `/var/lib/spark/.ssh/known_hosts`. |

## Operacion y rollback

Para diagnostico, leer el JSON de resultado sin imprimir secretos:

```bash
.venv/bin/python -m json.tool \
  /srv/talend-motor/resultados/<ejecucion-id>.json
```

No modificar directamente el codigo de `/srv/talend-motor/motor` como mecanismo
normal de despliegue. Versionar los cambios, hacer push al remoto autorizado y
en el servidor ejecutar `git pull --ff-only` desde una copia limpia.

Si se debe revertir el wrapper, conservar una copia fechada antes de instalarlo
y restaurarla con `sudo install` despues de validar sintaxis con `bash -n`.
Para revertir el motor, volver a un commit conocido, ejecutar sus pruebas y
repetir la prueba de aceptacion antes de reactivar el Job.

## Evidencia y limites

- CONFIRMADO: el flujo anterior se ejecuto contra Spark 3.4.4 y completo los
  cuatro archivos enumerados en este documento.
- CONFIRMADO: las pruebas locales relevantes para JDBC, `IndexRegEx`, rutas y
  staging pasaron antes de los despliegues de la sesion.
- DESCONOCIDO: responsables operativos, ambientes, SLA, retencion de resultados
  y repositorio Maven interno para futuros servidores.
- PROPUESTO: automatizar una prueba de aceptacion del wrapper en CI con un SFTP
  aislado y un catalogo PostgreSQL de prueba.
