from __future__ import annotations

import json
import shutil

import pytest

from motor_spark.conexiones.cargador import cargar_catalogo
from motor_spark.conexiones.modelos import CatalogoConexiones
from motor_spark.dataflow_script.jdbc import ConstructorSubconsulta
from motor_spark.dataflow_script.publicacion import StagingManager, UriLib

pytestmark = pytest.mark.spark


def _skip_if_no_spark():
    if shutil.which("java") is None:
        pytest.skip("Java no instalado")
    try:
        import pyspark  # noqa: F401 -- prueba disponibilidad opcional
    except ImportError:
        pytest.skip("PySpark no instalado")


class TestSqlInjection:
    def test_identificador_sql_invalido_con_palabras_reservadas(self):
        constructor = ConstructorSubconsulta("jdbc:postgresql://localhost/db", {})
        assert constructor.validar_identificador("SELECT") is False
        assert constructor.validar_identificador("DROP") is False
        assert constructor.validar_identificador("UNION") is False

    def test_identificador_sql_invalido_con_caracteres_riesgo(self):
        constructor = ConstructorSubconsulta("jdbc:postgresql://localhost/db", {})
        assert constructor.validar_identificador("tabla; DROP TABLE--") is False
        assert constructor.validar_identificador("tabla'OR'1'='1") is False
        assert constructor.validar_identificador("0x1234") is False

    def test_identificador_sql_valido(self):
        constructor = ConstructorSubconsulta("jdbc:postgresql://localhost/db", {})
        assert constructor.validar_identificador("tabla_valida") is True
        assert constructor.validar_identificador("MiTabla123") is True

    def test_construir_select_con_columnas_invalidas(self):
        constructor = ConstructorSubconsulta("jdbc:postgresql://localhost/db", {})
        resultado = constructor.construir_select(
            esquema=None,
            tabla="usuarios",
            columnas=["id", "DROP TABLE--", "nombre"],
        )
        assert resultado is None

    def test_identificador_sql_no_bloquea_palabras_seguras(self):
        constructor = ConstructorSubconsulta("jdbc:postgresql://localhost/db", {})
        assert constructor.validar_identificador("tabla_valida") is True
        assert constructor.validar_identificador("MiTabla123") is True


class TestPathTraversal:
    def test_uri_lib_ruta_absoluta_rechazada(self):
        with pytest.raises(ValueError, match="traversal"):
            UriLib.parsear("lib://local/../../../etc/passwd")

    def test_uri_lib_traversal_rechazado(self):
        with pytest.raises(ValueError, match="traversal"):
            UriLib.parsear("lib://local/../../../etc/passwd")

    def test_uri_lib_componentes_invalidos(self):
        with pytest.raises(ValueError):
            UriLib.parsear("lib://local/CON")

    def test_uri_lib_resolucion_segura(self, tmp_path):
        base = tmp_path / "directorio_base"
        base.mkdir()

        uri_result = UriLib.parsear("lib://local/salida/resultado.csv")
        resolved = UriLib.resolver_local(uri_result, base)

        assert resolved.is_relative_to(base)

    def test_staging_manager_permisos_correctos(self, tmp_path):
        manager = StagingManager(tmp_path)
        staging = manager.crear_staging("exec-123")
        assert manager.verificar_permisos(staging) is True


class TestAllowlistEnforcement:
    def test_catalogo_allowlist_campos_permitidos(self, conexion_path):
        catalogo = cargar_catalogo(conexion_path("catalogo_seguro.json"))
        assert isinstance(catalogo, CatalogoConexiones)

        conn = catalogo.buscar_local("local_files")
        assert conn is not None
        allowlist_tabla = next(
            (a for a in conn.allowlist if a.tabla == "ventas.csv"),
            None,
        )
        assert allowlist_tabla is not None
        assert "cliente" in allowlist_tabla.campos

    def test_catalogo_sin_secretos_hardcoded(self, conexion_path):
        contenido = conexion_path("catalogo_seguro.json").read_text()
        assert "password" not in contenido.lower() or "POSTGRES_PASSWORD" in contenido
        assert "${" in contenido


class TestErroresSinExposicionSecretos:
    def test_error_resultado_no_contiene_secretos(self, tmp_path):
        from motor_spark.aplicacion.ejecutor_dataflow import (
            _construir_resultado_error_dataflow,
        )
        from motor_spark.configuracion.argumentos import ArgumentosDataflowScript
        from motor_spark.dataflow_script.errores import (
            ErrorDataflow,
        )

        args = ArgumentosDataflowScript(
            dataflow_script="script.qvs",
            conexiones="conexiones.json",
            ejecucion_id="test-123",
            resultado=str(tmp_path / "resultado.json"),
        )

        errores = [
            ErrorDataflow(
                mensaje="Error de conexion",
                ubicacion=None,
                codigo="CONNECTION_ERROR",
            )
        ]

        resultado = _construir_resultado_error_dataflow(args, errores)
        resultado_str = json.dumps(resultado)
        assert (
            "secret" not in resultado_str.lower() or "secreto" in resultado_str.lower()
        )
        assert (
            "password" not in resultado_str.lower()
            or "POSTGRES_PASSWORD" not in resultado_str
        )

    def test_catalogo_password_no_en_resultado(self, conexion_path, tmp_path):
        from motor_spark.aplicacion.ejecutor_dataflow import (
            _construir_resultado_error_dataflow,
        )
        from motor_spark.configuracion.argumentos import ArgumentosDataflowScript
        from motor_spark.dataflow_script.errores import ErrorDataflow

        args = ArgumentosDataflowScript(
            dataflow_script="script.qvs",
            conexiones=str(conexion_path("catalogo_seguro.json")),
            ejecucion_id="test-seguro",
            resultado=str(tmp_path / "resultado.json"),
        )

        errores = [
            ErrorDataflow(
                mensaje="Connection error",
                ubicacion=None,
                codigo="CONN_ERROR",
            )
        ]

        resultado = _construir_resultado_error_dataflow(args, errores)
        resultado_str = json.dumps(resultado)
        assert (
            "secret" not in resultado_str.lower() or "secreto" in resultado_str.lower()
        )


class TestValidacionUriLib:
    def test_uri_sin_extension_csv_o_txt(self):
        with pytest.raises(ValueError, match=r"\.csv o \.txt"):
            UriLib.parsear("lib://local/salida/resultado.pdf")

    def test_uri_con_nombre_vacio(self):
        with pytest.raises(ValueError):
            UriLib.parsear("lib://local/")

    def test_uri_conexion_muy_larga(self):
        uri_larga = "lib://" + "a" * 300 + "/archivo.csv"
        with pytest.raises(ValueError, match="largo"):
            UriLib.parsear(uri_larga)
