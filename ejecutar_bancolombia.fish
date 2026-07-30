#!/usr/bin/env fish

# ============================================================
# EJECUTOR DEL DATAFLOW QLIK SOBRE SPARK
#
# El script Qlik se toma del portapapeles de macOS.
# El catálogo de conexiones se mantiene únicamente en memoria.
# Las credenciales PostgreSQL se solicitan de forma interactiva.
# La clave SFTP se envía codificada como un parámetro en memoria.
# ============================================================

set -l PROJECT_DIR "$HOME/code/python/motor_spark"

cd "$PROJECT_DIR"
or begin
    echo "ERROR: No existe el proyecto en: $PROJECT_DIR" >&2
    exit 1
end


# ============================================================
# VALIDAR HERRAMIENTAS
# ============================================================

if not test -x "$PROJECT_DIR/.venv/bin/python"
    echo "ERROR: No existe .venv/bin/python" >&2
    exit 1
end

if not test -x "$PROJECT_DIR/.venv/bin/spark-submit"
    echo "ERROR: No existe .venv/bin/spark-submit" >&2
    echo "Instala PySpark dentro del entorno virtual." >&2
    exit 1
end

if not type -q pbpaste
    echo "ERROR: pbpaste no está disponible." >&2
    echo "Este ejecutor está preparado para macOS." >&2
    exit 1
end


# ============================================================
# OBTENER SCRIPT QLIK DESDE EL PORTAPAPELES
#
# Antes de ejecutar este archivo debes copiar únicamente el
# script Qlik completo. No copies 'set SCRIPT_QLIK' ni JSON.
#
# string collect hace que todo el contenido multilínea llegue
# a motor.py como un solo argumento.
# ============================================================

# ``string collect -N`` conserva el portapapeles completo como un único
# elemento de Fish, incluidos saltos de línea y el salto final. Sin esta
# protección, una sustitución de comandos puede convertir cada línea del
# Qlik en un argumento separado y corromper silenciosamente el script.
set -l SCRIPT_QLIK (pbpaste | string collect -N)

# Se usa ``string match --quiet`` porque ``test -z (string trim ...)`` vuelve
# a expandir la salida multilínea como muchos argumentos. Solo necesitamos
# comprobar que exista al menos un carácter no blanco.
if not string match --quiet --regex '\S' -- "$SCRIPT_QLIK"
    echo "ERROR: El portapapeles está vacío." >&2
    echo "Copia primero el script Qlik completo." >&2
    exit 1
end


# ============================================================
# NORMALIZAR LOS DESTINOS SFTP
#
# El script exportado por Qlik contiene:
#
# lib://Bancolombia prueba:SFTP//upload/archivo.csv
#
# La conexión ya define /upload como ruta_base. Por eso el
# script debe contener solamente:
#
# lib://Bancolombia prueba:SFTP/archivo.csv
#
# Esta sustitución se hace únicamente en memoria.
# ============================================================

# ``string replace`` escribe el resultado por stdout. El ``string collect``
# final vuelve a empaquetarlo como un solo valor para que las 954 líneas no
# se conviertan en una lista de Fish.
set SCRIPT_QLIK (
    string replace -a \
        'lib://Bancolombia prueba:SFTP//upload/' \
        'lib://Bancolombia prueba:SFTP/' \
        -- "$SCRIPT_QLIK" \
        | string collect -N
)


# ============================================================
# SOLICITAR CREDENCIALES
#
# No se incluyen en la línea de comandos para evitar que sean
# visibles mediante ps, top u otras herramientas del sistema.
# ============================================================

read -P "Usuario PostgreSQL de Neon: " POSTGRES_USER

read -s -P "Contraseña PostgreSQL de Neon: " POSTGRES_PASSWORD
echo

# La conexión SFTP usa clave privada, no contraseña. El archivo se lee una
# sola vez, se codifica como Base64 y su contenido se pasa al motor mediante
# --secreto. La ruta nunca aparece dentro del catálogo de conexiones.
set -l SFTP_HOST "209.50.245.140"
set -l SFTP_PORT 22
set -l SFTP_USER "sftpqlik"
set -l SFTP_PRIVATE_KEY_FILE "$HOME/Downloads/sftp_debian"

if test -z "$POSTGRES_USER" -o -z "$POSTGRES_PASSWORD"
    echo "ERROR: Las credenciales PostgreSQL son obligatorias." >&2
    exit 1
end

if not test -f "$SFTP_PRIVATE_KEY_FILE"
    echo "ERROR: No existe la clave privada: $SFTP_PRIVATE_KEY_FILE" >&2
    exit 1
end

set -l KEY_MODE (stat -f "%OLp" "$SFTP_PRIVATE_KEY_FILE")
if test "$KEY_MODE" != "600" -a "$KEY_MODE" != "400"
    echo "ERROR: La clave privada debe tener permisos 600 o 400." >&2
    echo "Ejecuta: chmod 600 $SFTP_PRIVATE_KEY_FILE" >&2
    exit 1
end

if not type -q openssl
    echo "ERROR: openssl es necesario para codificar la clave en Base64." >&2
    exit 1
end

# -A genera Base64 en una sola línea. Esto evita que argparse reciba saltos
# de línea dentro de --secreto y permite reconstruir el archivo en memoria.
set -l SFTP_PRIVATE_KEY_B64 (
    openssl base64 -A -in "$SFTP_PRIVATE_KEY_FILE" | string collect
)

if test -z "$SFTP_PRIVATE_KEY_B64"
    echo "ERROR: No se pudo leer la clave privada SFTP." >&2
    exit 1
end


# ============================================================
# VERIFICAR IDENTIDAD DEL SERVIDOR SFTP
#
# El motor usa RejectPolicy: no confía automáticamente en
# servidores desconocidos. La clave debe existir previamente
# en ~/.ssh/known_hosts.
# ============================================================

if not ssh-keygen -F "$SFTP_HOST" >/dev/null 2>&1
    echo
    echo "ERROR: El servidor SFTP no está registrado en known_hosts." >&2
    echo
    echo "Primero verifica su fingerprint por un canal confiable." >&2
    echo "Después puedes registrarlo con:" >&2
    echo
    echo "  ssh-keyscan -H -p 22 $SFTP_HOST >> ~/.ssh/known_hosts" >&2
    echo
    exit 1
end


# ============================================================
# SECRETOS QUE SE PASARÁN COMO PARÁMETROS
#
# PostgreSQL conserva el formato USUARIO:CONTRASEÑA.
# La clave SFTP ya está disponible en SFTP_PRIVATE_KEY_B64.
# Ninguno de estos valores se escribe en conexiones.json.
# ============================================================

set -l POSTGRES_BANCOLOMBIA \
    "$POSTGRES_USER:$POSTGRES_PASSWORD"


# ============================================================
# CATÁLOGO DE CONEXIONES INLINE
#
# Los nombres deben coincidir exactamente con los que aparecen
# en LIB CONNECT TO y lib:// dentro del script Qlik.
#
# campos=[] permite cualquier columna, pero únicamente dentro
# de las siete tablas declaradas. No autoriza otras tablas.
# ============================================================

set -l CONEXIONES_JSON '{
  "version": 1,
  "descripcion": "Dataflow Bancolombia ejecutado por Spark",

  "jdbc": [
    {
      "tipo": "jdbc",
      "nombre": "Bancolombia prueba:Postgres_BanColombia_Prueba",

      "url": "jdbc:postgresql://ep-blue-dust-atd8xd5k-pooler.c-9.us-east-1.aws.neon.tech/neondb?sslmode=require",

      "driver": "org.postgresql.Driver",
      "secreto_nombre": "POSTGRES_BANCOLOMBIA",

      "allowlist": [
        {
          "esquema": "demo_dataflow",
          "tabla": "ventas_2025",
          "campos": []
        },
        {
          "esquema": "demo_dataflow",
          "tabla": "ventas_2026",
          "campos": []
        },
        {
          "esquema": "demo_dataflow",
          "tabla": "clientes",
          "campos": []
        },
        {
          "esquema": "demo_dataflow",
          "tabla": "productos",
          "campos": []
        },
        {
          "esquema": "demo_dataflow",
          "tabla": "sucursales",
          "campos": []
        },
        {
          "esquema": "demo_dataflow",
          "tabla": "vendedores",
          "campos": []
        },
        {
          "esquema": "demo_dataflow",
          "tabla": "devoluciones",
          "campos": []
        }
      ],

      "propiedades": {
        "fetchsize": "10000"
      }
    }
  ],

  "locales": [],

  "sftp": [
    {
      "tipo": "sftp",
      "nombre": "Bancolombia prueba:SFTP",
      "host": "__SFTP_HOST__",
      "puerto": 22,
      "usuario": "sftpqlik",
      "secreto_clave_privada_nombre": "SFTP_PRIVATE_KEY_B64",
      "ruta_base": "/upload",

      "allowlist": [
        {
          "esquema": "",
          "tabla": "ventas_rechazadas.csv",
          "campos": []
        },
        {
          "esquema": "",
          "tabla": "ventas_curadas.csv",
          "campos": []
        },
        {
          "esquema": "",
          "tabla": "muestra_calidad.csv",
          "campos": []
        },
        {
          "esquema": "",
          "tabla": "resumen_mensual.csv",
          "campos": []
        }
      ]
    }
  ]
}'


# Insertar dinámicamente el host SFTP solicitado al usuario.
# La clave privada y las credenciales no se colocan dentro del JSON.

set CONEXIONES_JSON (
    string replace -a \
        '__SFTP_HOST__' \
        "$SFTP_HOST" \
        -- "$CONEXIONES_JSON"
)


# ============================================================
# CONFIGURACIÓN PYSPARK
#
# Driver y workers deben usar exactamente el mismo Python.
# ============================================================

set -lx PYSPARK_PYTHON \
    "$PROJECT_DIR/.venv/bin/python"

set -lx PYSPARK_DRIVER_PYTHON \
    "$PROJECT_DIR/.venv/bin/python"


# ============================================================
# EJECUCIÓN
#
# No se envía --resultado, por lo que no se crea resultado.json.
# El resultado se imprime como:
#
# RESULTADO_MOTOR={...}
# ============================================================

set -l EJECUCION_ID \
    "bancolombia-"(date "+%Y%m%d-%H%M%S")

echo
echo "Iniciando ejecución: $EJECUCION_ID"
echo

"$PROJECT_DIR/.venv/bin/spark-submit" \
    --packages org.postgresql:postgresql:42.7.7 \
    "$PROJECT_DIR/motor.py" \
    --dataflow-script-contenido "$SCRIPT_QLIK" \
    --conexiones-contenido "$CONEXIONES_JSON" \
    --secreto "POSTGRES_BANCOLOMBIA=$POSTGRES_BANCOLOMBIA" \
    --secreto "SFTP_PRIVATE_KEY_B64=$SFTP_PRIVATE_KEY_B64" \
    --ejecucion-id "$EJECUCION_ID"

set -l CODIGO_SALIDA $status


# ============================================================
# RESULTADO DEL PROCESO
# ============================================================

echo

if test "$CODIGO_SALIDA" -eq 0
    echo "Ejecución completada correctamente."
else
    echo "ERROR: Spark terminó con código $CODIGO_SALIDA." >&2
end


# Borrar referencias locales a credenciales antes de terminar.
set -e POSTGRES_PASSWORD
set -e POSTGRES_BANCOLOMBIA
set -e SFTP_PRIVATE_KEY_B64

exit "$CODIGO_SALIDA"
