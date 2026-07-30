from __future__ import annotations

import pytest

from motor_spark.dataflow_script.jdbc import (
    ConstructorSubconsulta,
    ErrorJdbc,
    construir_select,
    construir_reader_jdbc,
    leer_jdbc,
)


class TestConstructorSubconsulta:
    def setup_method(self) -> None:
        self._url = "jdbc:postgresql://localhost:5432/test"
        self._propiedades = {"schema": "public"}
        self._constructor = ConstructorSubconsulta(self._url, self._propiedades)

    def test_validar_identificador_ok(self) -> None:
        assert self._constructor.validar_identificador("tabla_test")
        assert self._constructor.validar_identificador("miTabla")
        assert self._constructor.validar_identificador("_privado")
        assert self._constructor.validar_identificador("T1")

    def test_validar_identificador_falla_vacio(self) -> None:
        assert not self._constructor.validar_identificador("")
        errores = self._constructor.errores
        assert len(errores) > 0
        assert errores[-1].codigo == "JDBC_INVALID_ID_EMPTY"

    def test_validar_identificador_falla_numero(self) -> None:
        assert not self._constructor.validar_identificador("1tabla")
        errores = self._constructor.errores
        assert len(errores) > 0
        assert errores[-1].codigo == "JDBC_INVALID_ID_FORMAT"

    def test_validar_identificador_falla_muy_largo(self) -> None:
        nombre_largo = "a" * 200
        assert not self._constructor.validar_identificador(nombre_largo)
        errores = self._constructor.errores
        assert len(errores) > 0
        assert errores[-1].codigo == "JDBC_INVALID_ID_TOO_LONG"

    def test_validar_identificador_falla_reservada(self) -> None:
        assert not self._constructor.validar_identificador("SELECT")
        errores = self._constructor.validar_identificador("WHERE")
        assert not errores
        assert len(self._constructor.errores) > 0

    def test_construir_select_sin_columnas_falla(self) -> None:
        resultado = self._constructor.construir_select(
            esquema="public",
            tabla="usuarios",
            columnas=[],
        )
        assert resultado is None
        assert any(e.codigo == "JDBC_COLUMNAS_REQUERIDAS" for e in self._constructor.errores)

    def test_construir_select_con_columnas(self) -> None:
        resultado = self._constructor.construir_select(
            esquema="public",
            tabla="usuarios",
            columnas=["id", "nombre", "email"],
        )
        assert resultado is not None
        assert '"id"' in resultado
        assert '"nombre"' in resultado
        assert '"email"' in resultado
        assert "SELECT" in resultado
        assert "FROM" in resultado
        assert '"public"."usuarios"' in resultado
        assert "SELECT *" not in resultado

    def test_construir_select_sin_esquema(self) -> None:
        resultado = self._constructor.construir_select(
            esquema=None,
            tabla="usuarios",
            columnas=["id"],
        )
        assert resultado is not None
        assert '"usuarios"' in resultado
        assert "SELECT *" not in resultado

    def test_construir_select_con_una_columna(self) -> None:
        resultado = self._constructor.construir_select(
            esquema="public",
            tabla="usuarios",
            columnas=["id"],
        )
        assert resultado is not None
        assert '"id"' in resultado
        assert '"usuarios"' in resultado

    def test_construir_select_tabla_invalida_retorna_none(self) -> None:
        resultado = self._constructor.construir_select(
            esquema="public",
            tabla="1tabla_invalida",
            columnas=["id"],
        )
        assert resultado is None

    def test_construir_reader_jdbc(self) -> None:
        resultado = self._constructor.construir_reader_jdbc(
            tabla="usuarios",
            columnas=["id", "nombre"],
        )
        assert resultado is not None
        assert "properties" in resultado
        assert "dbtable" in resultado["properties"]
        assert "SELECT" in resultado["properties"]["dbtable"]
        assert "SELECT *" not in resultado["properties"]["dbtable"]


class TestConstruirSelect:
    def test_funcion_constructora_select(self) -> None:
        resultado = construir_select(
            esquema="test",
            tabla="usuarios",
            columnas=["id", "nombre"],
            url="jdbc:mysql://localhost:3306/test",
            propiedades={"schema": "test"},
        )
        assert resultado is not None
        assert "SELECT" in resultado
        assert '"id"' in resultado

    def test_funcion_constructora_reader_jdbc(self) -> None:
        resultado = construir_reader_jdbc(
            tabla="usuarios",
            columnas=["id", "nombre"],
            url="jdbc:mysql://localhost:3306/test",
            propiedades={"schema": "test"},
        )
        assert resultado is not None
        assert "properties" in resultado


class TestErrorJdbc:
    def test_error_jdbc_tiene_formato_estable(self) -> None:
        from motor_spark.dataflow_script.errores import SourceLocation, SourceSpan

        ubicacion = SourceLocation(
            inicio=SourceSpan(linea=1, columna=1, offset=0),
            fin=SourceSpan(linea=1, columna=10, offset=10),
        )
        error = ErrorJdbc(
            mensaje="Error de prueba",
            ubicacion=ubicacion,
            codigo="TEST_ERROR",
        )
        formato = error.formato_estable()
        assert "TEST_ERROR" in formato
        assert "Error de prueba" in formato
        assert "ubicacion=" in formato


class TestSinInterpolarValoresUsuario:
    def test_no_permite_sql_injection_en_tabla(self) -> None:
        constructor = ConstructorSubconsulta(
            url="jdbc:postgresql://localhost:5432/test",
            propiedades={"schema": "public"},
        )

        resultado = constructor.construir_select(
            esquema="public",
            tabla="usuarios; DROP TABLE usuarios;--",
            columnas=["id"],
        )
        assert resultado is None

    def test_no_permite_comentarios_sql(self) -> None:
        constructor = ConstructorSubconsulta(
            url="jdbc:postgresql://localhost:5432/test",
            propiedades={"schema": "public"},
        )

        assert not constructor.validar_identificador("tabla--comentario")
        assert not constructor.validar_identificador("tabla/*comentario*/")
        assert not constructor.validar_identificador("tabla;")


class TestLeerJdbc:
    def test_leer_jdbc_catalogo_invalido_falla(self) -> None:
        with pytest.raises(ValueError, match="catalogo debe ser CatalogoConexiones"):
            leer_jdbc(
                spark=None,
                nombre_conexion="mi_conn",
                tabla="usuarios",
                columnas=["id"],
                catalogo={"jdbc": []},
            )

    def test_leer_jdbc_conexion_no_existe_falla(self) -> None:
        from motor_spark.conexiones.modelos import CatalogoConexiones

        catalogo = CatalogoConexiones()
        with pytest.raises(ValueError, match="no encontrada en catalogo"):
            leer_jdbc(
                spark=None,
                nombre_conexion="inexistente",
                tabla="usuarios",
                columnas=["id"],
                catalogo=catalogo,
            )

    def test_leer_jdbc_tabla_no_en_allowlist_falla(self) -> None:
        from motor_spark.conexiones.modelos import (
            CatalogoConexiones,
            ConexionJdbc,
            CampoAllowlist,
        )

        conn = ConexionJdbc(
            nombre="test_conn",
            url="jdbc:postgresql://localhost:5432/test",
            driver="org.postgresql.Driver",
            secreto_nombre="TEST_SECRETO",
            allowlist=(
                CampoAllowlist(esquema="public", tabla="usuarios", campos=("id", "nombre")),
            ),
        )
        catalogo = CatalogoConexiones(jdbc=(conn,))

        with pytest.raises(ValueError, match="no esta en allowlist"):
            leer_jdbc(
                spark=None,
                nombre_conexion="test_conn",
                tabla="ventas",
                columnas=["id"],
                catalogo=catalogo,
            )

    def test_leer_jdbc_columna_no_en_allowlist_falla(self) -> None:
        from motor_spark.conexiones.modelos import (
            CatalogoConexiones,
            ConexionJdbc,
            CampoAllowlist,
        )

        conn = ConexionJdbc(
            nombre="test_conn",
            url="jdbc:postgresql://localhost:5432/test",
            driver="org.postgresql.Driver",
            secreto_nombre="TEST_SECRETO",
            allowlist=(
                CampoAllowlist(esquema="public", tabla="usuarios", campos=("id",)),
            ),
        )
        catalogo = CatalogoConexiones(jdbc=(conn,))

        with pytest.raises(ValueError, match="no esta en allowlist"):
            leer_jdbc(
                spark=None,
                nombre_conexion="test_conn",
                tabla="usuarios",
                columnas=["nombre"],
                catalogo=catalogo,
            )

    def test_leer_jdbc_secreto_no_en_environ_falla(self) -> None:
        import os

        from motor_spark.conexiones.modelos import (
            CatalogoConexiones,
            ConexionJdbc,
            CampoAllowlist,
        )

        conn = ConexionJdbc(
            nombre="test_conn",
            url="jdbc:postgresql://localhost:5432/test",
            driver="org.postgresql.Driver",
            secreto_nombre="SECRETO_INEXISTENTE_12345",
            allowlist=(
                CampoAllowlist(esquema="public", tabla="usuarios", campos=()),
            ),
        )
        catalogo = CatalogoConexiones(jdbc=(conn,))

        clave = "SECRETO_INEXISTENTE_12345"
        if clave in os.environ:
            del os.environ[clave]

        with pytest.raises(ValueError, match="no encontrado en entorno"):
            leer_jdbc(
                spark=None,
                nombre_conexion="test_conn",
                tabla="usuarios",
                columnas=["id"],
                catalogo=catalogo,
            )
