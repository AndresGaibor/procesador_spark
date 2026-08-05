from __future__ import annotations

import json
import unittest
from unittest.mock import MagicMock

from motor_spark.conexiones.base_destino import (
    ConfiguracionBaseDestino,
    cargar_json_base_destino,
    resolver_base_destino,
)
from motor_spark.conexiones.secretos import AdministradorSecretos
from pyspark.sql.types import LongType, StringType, StructField, StructType

from motor_spark.configuracion.argumentos import analizar_argumentos
from motor_spark.configuracion.paquetes_jdbc import resolver_paquetes_jdbc
from motor_spark.plan.ejecutor import EjecutorPlanDataflow
from motor_spark.plan.modelos import Publicar


class TestBaseDestino(unittest.TestCase):
    def test_cargar_json_base_destino_valido(self):
        json_str = '{"tipo": "postgres", "host": "localhost", "database": "db"}'
        res = cargar_json_base_destino(json_str)
        self.assertEqual(res["host"], "localhost")
        self.assertEqual(res["database"], "db")

    def test_cargar_json_base_destino_invalido(self):
        with self.assertRaises(ValueError):
            cargar_json_base_destino("{json_invalido")

    def test_resolver_base_destino_completo(self):
        json_str = json.dumps({
            "tipo": "postgres",
            "host": "ep-blue-dust.neon.tech",
            "puerto": 5432,
            "database": "neondb",
            "schema": "resultado",
            "usuario": "neondb_owner",
            "secreto_nombre": "BASE_DESTINO"
        })
        secretos = AdministradorSecretos({"BASE_DESTINO": "secreto123"})
        config = resolver_base_destino(json_str, secretos=secretos)

        self.assertIsInstance(config, ConfiguracionBaseDestino)
        self.assertEqual(config.url, "jdbc:postgresql://ep-blue-dust.neon.tech:5432/neondb")
        self.assertEqual(config.driver, "org.postgresql.Driver")
        self.assertEqual(config.usuario, "neondb_owner")
        self.assertEqual(config.password, "secreto123")
        self.assertEqual(config.esquema, "resultado")

        opciones = config.a_opciones_jdbc("ventas_curadas")
        self.assertEqual(opciones["dbtable"], "resultado.ventas_curadas")
        self.assertEqual(opciones["url"], config.url)
        self.assertEqual(opciones["user"], "neondb_owner")
        self.assertEqual(opciones["password"], "secreto123")

    def test_resolver_base_destino_particionado_usuario_clave(self):
        json_str = json.dumps({
            "tipo": "postgres",
            "host": "localhost",
            "database": "mydb",
            "secreto_nombre": "POSTGRES_BANCOLOMBIA"
        })
        secretos = AdministradorSecretos({"POSTGRES_BANCOLOMBIA": "myuser:mypassword"})
        config = resolver_base_destino(json_str, secretos=secretos)

        self.assertEqual(config.usuario, "myuser")
        self.assertEqual(config.password, "mypassword")

    def test_analizar_argumentos_base_destino(self):
        argv = [
            "--dataflow-script-contenido", "LOAD 1 RESIDENT x;",
            "--conexiones-contenido", '{"version": 1, "descripcion": "d", "jdbc": [], "locales": [], "sftp": []}',
            "--ejecucion-id", "ej-001",
            "--base-destino", '{"host": "h", "database": "d", "secreto_nombre": "S"}'
        ]
        args = analizar_argumentos(argv)
        self.assertEqual(args.base_destino, '{"host": "h", "database": "d", "secreto_nombre": "S"}')

    def test_resolver_paquetes_jdbc_base_destino(self):
        argv = [
            "--base-destino", '{"tipo": "postgres", "host": "h", "database": "d"}'
        ]
        paquetes = resolver_paquetes_jdbc(argv)
        self.assertIn("org.postgresql:postgresql:42.7.7", paquetes)

    def test_resolver_paquetes_jdbc_base_destino_hive(self):
        argv = [
            "--base-destino",
            '{"tipo": "hive2", "url": "jdbc:hive2://host:21050/default;auth=noSasl", "driver": "org.apache.hive.jdbc.HiveDriver"}'
        ]
        paquetes = resolver_paquetes_jdbc(argv)
        self.assertIn("org.apache.hive:hive-jdbc:4.0.0", paquetes)

    def test_a_opciones_jdbc_hive_usa_db_de_url(self):
        config = ConfiguracionBaseDestino(
            url="jdbc:hive2://209.50.245.140:21050/default;auth=noSasl",
            driver="org.apache.hive.jdbc.HiveDriver",
            usuario="",
            password="",
            esquema="default",
        )

        opciones = config.a_opciones_jdbc("Filtro_1_REJECT")
        self.assertEqual(opciones["dbtable"], "filtro_1_reject")
        self.assertEqual(opciones["driver"], "org.apache.hive.jdbc.HiveDriver")
        self.assertEqual(opciones["batchsize"], "1")

    def test_resolver_base_destino_hive_default_append(self):
        json_str = json.dumps({
            "tipo": "hive2",
            "url": "jdbc:hive2://209.50.245.140:21050/default;auth=noSasl",
            "driver": "org.apache.hive.jdbc.HiveDriver",
            "esquema": "default"
        })
        config = resolver_base_destino(json_str)
        self.assertEqual(config.modo, "append")

    def test_ejecutor_plan_publicar_base_destino_hive_usa_create_table_column_types(self):
        spark_mock = MagicMock()
        df_mock = MagicMock()
        writer_mock = MagicMock()
        df_mock.write.mode.return_value = writer_mock
        writer_mock.format.return_value = writer_mock
        writer_mock.option.return_value = writer_mock
        df_mock.schema = StructType([
            StructField("venta_id", LongType(), True),
            StructField("fecha_venta", StringType(), True),
        ])

        config_bd = ConfiguracionBaseDestino(
            url="jdbc:hive2://209.50.245.140:21050/default;auth=noSasl",
            driver="org.apache.hive.jdbc.HiveDriver",
            usuario="",
            password="",
            esquema="default"
        )

        catalogo_mock = MagicMock()
        secretos_mock = AdministradorSecretos()

        ejecutor = EjecutorPlanDataflow(
            spark=spark_mock,
            catalogo=catalogo_mock,
            secretos=secretos_mock,
            ejecucion_id="ej-001",
            base_destino=config_bd
        )
        ejecutor.registrar_tabla("Ranking", df_mock)

        operacion = Publicar(
            id="pub-1",
            tabla_origen="Ranking",
            destino="lib://Bancolombia prueba//upload/ventas_curadas.csv",
            formato="csv"
        )
        ejecutor._publicar(operacion)

        writer_mock.option.assert_any_call(
            "createTableColumnTypes",
            "venta_id BIGINT, fecha_venta STRING",
        )
        writer_mock.save.assert_called_once()

    def test_ejecutor_plan_publicar_base_destino_hive_fallback_add_batch(self):
        spark_mock = MagicMock()
        df_mock = MagicMock()
        writer_mock = MagicMock()
        conn_mock = MagicMock()
        stmt_mock = MagicMock()
        metadata_mock = MagicMock()
        columns_rs_mock = MagicMock()
        describe_rs_mock = MagicMock()

        df_mock.write.mode.return_value = writer_mock
        writer_mock.format.return_value = writer_mock
        writer_mock.option.return_value = writer_mock
        writer_mock.save.side_effect = Exception(
            "SQLFeatureNotSupportedException: Method not supported in HivePreparedStatement.addBatch"
        )
        df_mock.schema = StructType([
            StructField("venta_id", LongType(), True),
            StructField("fecha_venta", StringType(), True),
        ])
        df_mock.collect.return_value = [MagicMock(venta_id=1, fecha_venta="a")]
        spark_mock._jvm.java.sql.DriverManager.getConnection.return_value = conn_mock
        conn_mock.getMetaData.return_value = metadata_mock
        metadata_mock.getColumns.return_value = columns_rs_mock
        columns_rs_mock.next.return_value = False
        conn_mock.createStatement.return_value = stmt_mock
        stmt_mock.executeQuery.return_value = describe_rs_mock
        describe_rs_mock.next.return_value = False
        stmt_mock.executeUpdate.return_value = 1

        config_bd = ConfiguracionBaseDestino(
            url="jdbc:hive2://209.50.245.140:21050/default;auth=noSasl",
            driver="org.apache.hive.jdbc.HiveDriver",
            usuario="",
            password="",
            esquema="default"
        )

        catalogo_mock = MagicMock()
        secretos_mock = AdministradorSecretos()

        ejecutor = EjecutorPlanDataflow(
            spark=spark_mock,
            catalogo=catalogo_mock,
            secretos=secretos_mock,
            ejecucion_id="ej-001",
            base_destino=config_bd
        )
        ejecutor.registrar_tabla("Ranking", df_mock)

        operacion = Publicar(
            id="pub-1",
            tabla_origen="Ranking",
            destino="lib://Bancolombia prueba//upload/ventas_curadas.csv",
            formato="csv"
        )
        ejecutor._publicar(operacion)

        df_mock.collect.assert_called_once()
        stmt_mock.executeUpdate.assert_called_once()
        conn_mock.createStatement.assert_called_once()
        self.assertEqual(len(ejecutor._publicaciones), 1)
        self.assertEqual(ejecutor._publicaciones[0]["tabla"], "ventas_curadas")

    def test_ejecutor_plan_publicar_base_destino(self):
        spark_mock = MagicMock()
        df_mock = MagicMock()
        writer_mock = MagicMock()
        df_mock.write.mode.return_value = writer_mock
        writer_mock.format.return_value = writer_mock
        writer_mock.option.return_value = writer_mock

        config_bd = ConfiguracionBaseDestino(
            url="jdbc:postgresql://host:5432/db",
            driver="org.postgresql.Driver",
            usuario="user",
            password="pwd",
            esquema="resultado"
        )

        catalogo_mock = MagicMock()
        secretos_mock = AdministradorSecretos()

        ejecutor = EjecutorPlanDataflow(
            spark=spark_mock,
            catalogo=catalogo_mock,
            secretos=secretos_mock,
            ejecucion_id="ej-001",
            base_destino=config_bd
        )
        ejecutor.registrar_tabla("Ranking", df_mock)

        operacion = Publicar(
            id="pub-1",
            tabla_origen="Ranking",
            destino="lib://Bancolombia prueba//upload/ventas_curadas.csv",
            formato="csv"
        )
        ejecutor._publicar(operacion)

        df_mock.write.mode.assert_called_with("overwrite")
        writer_mock.format.assert_called_with("jdbc")
        writer_mock.save.assert_called_once()
        self.assertEqual(len(ejecutor._publicaciones), 1)
        self.assertEqual(ejecutor._publicaciones[0]["tipo"], "base_destino")
        self.assertEqual(ejecutor._publicaciones[0]["tabla"], "resultado.ventas_curadas")


if __name__ == "__main__":
    unittest.main()
