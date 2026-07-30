import io
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from motor_spark.dataflow_script.publicacion import (
    HAS_PARAMIKO,
    CsvWriter,
    ManifiestoPublicacion,
    PublicacionLocal,
    PublicacionSftp,
    StagingManager,
    UriLib,
    UriParseResult,
    calcular_sha256,
)


class TestUriLib:
    def test_parsear_uri_valida(self):
        result = UriLib.parsear("lib://mi_conexion/datos/salida.csv")
        assert result.conexion == "mi_conexion"
        assert result.ruta == "datos/salida.csv"

    def test_parsear_uri_con_espacios(self):
        result = UriLib.parsear("lib://mi conexion con espacios/datos/salida.csv")
        assert result.conexion == "mi conexion con espacios"
        assert result.ruta == "datos/salida.csv"

    def test_parsear_uri_con_colon(self):
        result = UriLib.parsear("lib://mi:conexion:datos/datos/salida.txt")
        assert result.conexion == "mi:conexion:datos"
        assert result.ruta == "datos/salida.txt"

    def test_parsear_uri_sin_scheme_falla(self):
        with pytest.raises(ValueError, match="URI invalida"):
            UriLib.parsear("mi_conexion/datos/salida.csv")

    def test_parsear_uri_sin_ruta_falla(self):
        with pytest.raises(ValueError, match="URI invalida"):
            UriLib.parsear("lib://mi_conexion")

    def test_parsear_uri_sin_conexion_falla(self):
        with pytest.raises(ValueError, match="URI invalida"):
            UriLib.parsear("lib:///datos/salida.csv")

    def test_parsear_uri_url_encoding_rechazado(self):
        with pytest.raises(ValueError, match="URL encoding no permitido"):
            UriLib.parsear("lib://mi_conexion/datos%20con%20espacios.csv")

    def test_parsear_uri_traversal_rechazado(self):
        with pytest.raises(ValueError, match="no puede contener traversal"):
            UriLib.parsear("lib://mi_conexion/../etc/passwd.csv")

    def test_parsear_uri_backslash_rechazado(self):
        with pytest.raises(ValueError, match="no puede contener backslash"):
            UriLib.parsear("lib://mi_conexion/datos\\salida.csv")

    def test_parsear_uri_nul_rechazado(self):
        with pytest.raises(ValueError, match="no puede contener NUL"):
            UriLib.parsear("lib://mi_conexion/datos\x00salida.csv")

    def test_parsear_uri_ruta_absoluta_rechazada(self):
        with pytest.raises(ValueError, match="no puede ser absoluta"):
            UriLib.parsear("lib://mi_conexion//etc/passwd.csv")

    def test_parsear_uri_destino_no_csv_o_txt_rechazado(self):
        with pytest.raises(ValueError, match="Destino debe ser .csv o .txt"):
            UriLib.parsear("lib://mi_conexion/datos/salida.pdf")

    def test_parsear_uri_txt_valido(self):
        result = UriLib.parsear("lib://conn/datos/salida.txt")
        assert result.ruta == "datos/salida.txt"

    def test_parsear_uri_con_punto_en_nombre(self):
        result = UriLib.parsear("lib://conn.123/datos/salida.csv")
        assert result.conexion == "conn.123"

    def test_parsear_uri_no_string_falla(self):
        with pytest.raises(ValueError, match="URI debe ser string"):
            UriLib.parsear(123)  # type: ignore

    def test_parsear_uri_nombre_conexion_largo_rechazado(self):
        nombre_largo = "a" * 300
        with pytest.raises(ValueError, match="demasiado largo"):
            UriLib.parsear(f"lib://{nombre_largo}/datos/salida.csv")

    def test_parsear_uri_componente_punto_rechazado(self):
        with pytest.raises(ValueError, match="no puede terminar en punto"):
            UriLib.parsear("lib://conn/datos./salida.csv")


class TestUriLibTraversal:
    def test_resolver_local_dentro_directorio(self, tmp_path):
        uri = UriParseResult(conexion="conn", ruta="sub/dir/salida.csv")
        resultado = UriLib.resolver_local(uri, tmp_path)
        assert resultado == (tmp_path / "sub/dir/salida.csv").resolve()

    def test_resolver_local_traversal_bloqueado(self, tmp_path):
        subdir = tmp_path / "sub"
        subdir.mkdir()
        uri = UriParseResult(conexion="conn", ruta="../etc/passwd.csv")
        with pytest.raises(ValueError, match="Traversal detectado"):
            UriLib.resolver_local(uri, subdir)

    def test_resolver_local_con_symlink_externo_bloqueado(self, tmp_path):
        external = tmp_path / "external"
        external.mkdir()
        linked = tmp_path / "link"
        linked.symlink_to(external)
        uri = UriParseResult(conexion="conn", ruta="link/../../../etc/passwd.csv")
        with pytest.raises(ValueError, match="Traversal detectado"):
            UriLib.resolver_local(uri, tmp_path)


class TestStagingManager:
    def test_crear_staging_crea_directorio(self, tmp_path):
        manager = StagingManager(tmp_path)
        staging = manager.crear_staging("exec123")
        assert staging.exists()
        assert staging.is_dir()

    def test_crear_staging_permisos_0700(self, tmp_path):
        manager = StagingManager(tmp_path)
        staging = manager.crear_staging("exec456")
        perms = os.stat(staging).st_mode & 0o777
        assert perms == 0o700

    def test_crear_staging_salida_permisos_0700(self, tmp_path):
        manager = StagingManager(tmp_path)
        manager.crear_staging("exec789")
        salida = manager.crear_staging_salida("exec789", "mi_salida")
        perms = os.stat(salida).st_mode & 0o777
        assert perms == 0o700

    def test_limpiar_staging_elimina_directorio(self, tmp_path):
        manager = StagingManager(tmp_path)
        staging = manager.crear_staging("exec000")
        assert staging.exists()
        manager.limpiar_staging("exec000")
        assert not staging.exists()

    def test_verificar_permisos_0700_correcto(self, tmp_path):
        manager = StagingManager(tmp_path)
        staging = manager.crear_staging("exec111")
        assert manager.verificar_permisos(staging) is True

    def test_verificar_permisos_incorrecto(self, tmp_path):
        manager = StagingManager(tmp_path)
        staging = manager.crear_staging("exec222")
        os.chmod(staging, 0o755)
        assert manager.verificar_permisos(staging) is False

    def test_staging_no_reutilizable(self, tmp_path):
        manager = StagingManager(tmp_path)
        staging1 = manager.crear_staging("exec333")
        (staging1 / "archivo_temporal.txt").write_text("test")
        manager.limpiar_staging("exec333")
        manager.crear_staging("exec333")
        assert not (staging1 / "archivo_temporal.txt").exists()

    def test_staging_salida_no_reutilizable_misma_ejecucion(self, tmp_path):
        manager = StagingManager(tmp_path)
        manager.crear_staging_salida("exec444", "salida_a")
        with pytest.raises(ValueError, match="ya fue creada"):
            manager.crear_staging_salida("exec444", "salida_a")

    def test_staging_salida_rechaza_directorio_existente(self, tmp_path):
        manager = StagingManager(tmp_path)
        staging = manager.crear_staging("exec555")
        salida_existente = staging / "ya_existe"
        salida_existente.mkdir()
        with pytest.raises(ValueError, match="ya existe"):
            manager.crear_staging_salida("exec555", "ya_existe")

    def test_staging_salida_diferentes_son_independientes(self, tmp_path):
        manager = StagingManager(tmp_path)
        salida1 = manager.crear_staging_salida("exec666", "salida_x")
        salida2 = manager.crear_staging_salida("exec666", "salida_y")
        assert salida1 != salida2
        assert salida1.name == "salida_x"
        assert salida2.name == "salida_y"


class TestCsvWriter:
    def test_escribir_csv_con_bom_utf8(self, tmp_path):
        path = tmp_path / "test.csv"
        writer = CsvWriter(path, ["col1", "col2", "col3"])
        writer.escribir_fila(["a", "b", "c"])
        writer.escribir_fila(["1", "2", "3"])
        writer.cerrar()

        with open(path, "rb") as f:
            contenido = f.read()
        assert contenido[:3] == b"\xef\xbb\xbf"

    def test_escribir_csv_con_encabezado(self, tmp_path):
        path = tmp_path / "test.csv"
        CsvWriter.escribir_desde_iterable(
            path,
            ["col1", "col2"],
            [["a", "b"], ["c", "d"]],
        )

        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            rows = list(reader)
        assert rows[0] == ["col1", "col2"]

    def test_escribir_csv_crlf_line_terminator(self, tmp_path):
        path = tmp_path / "test.csv"
        CsvWriter.escribir_desde_iterable(
            path,
            ["col1"],
            [["a"], ["b"]],
        )

        with open(path, "rb") as f:
            contenido = f.read()
        assert b"\r\n" in contenido

    def test_escribir_csv_quoting_minimal(self, tmp_path):
        path = tmp_path / "test.csv"
        CsvWriter.escribir_desde_iterable(
            path,
            ["col1", "col2"],
            [["sin comillas", "tambien sin"], ["a,b", "con coma"]],
        )

        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            contenido = f.read()
        assert '"a,b"' in contenido

    def test_escribir_csv_desde_iterable(self, tmp_path):
        path = tmp_path / "iterable.csv"
        filas = [["a", "1"], ["b", "2"], ["c", "3"]]
        CsvWriter.escribir_desde_iterable(path, ["letra", "num"], filas)

        assert path.exists()
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            rows = list(reader)
        assert len(rows) == 4
        assert rows[0] == ["letra", "num"]
        assert rows[1] == ["a", "1"]

    def test_csv_sin_spark(self, tmp_path):
        path = tmp_path / "sin_spark.csv"
        writer = CsvWriter(path, ["id", "nombre"])
        writer.escribir_fila([1, "juan"])  # type: ignore
        writer.escribir_fila([2, "ana"])  # type: ignore
        writer.cerrar()
        assert path.exists()

    def test_csv_escritura_perezosa_no_abre_hasta_primera_fila(self, tmp_path):
        path = tmp_path / "perezoso.csv"
        writer = CsvWriter(path, ["col1"])
        assert not path.exists()
        writer.escribir_fila(["valor"])
        assert path.exists()
        writer.cerrar()

    def test_csv_context_manager_cierra_correctamente(self, tmp_path):
        path = tmp_path / "context.csv"
        with CsvWriter(path, ["col1", "col2"]) as writer:
            writer.escribir_fila(["a", "b"])
        assert path.exists()
        with open(path, "r", encoding="utf-8-sig") as f:
            assert "col1" in f.read()

    def test_csv_no_acumula_en_memoria(self, tmp_path):
        path = tmp_path / "grande.csv"
        writer = CsvWriter(path, ["num"])
        for i in range(10000):
            writer.escribir_fila([str(i)])
        writer.cerrar()
        with open(path, "r", encoding="utf-8-sig") as f:
            lineas = f.readlines()
        assert len(lineas) == 10001

    def test_csv_cerrar_sin_datos_no_crea_encabezado_vacio(self, tmp_path):
        path = tmp_path / "vacio.csv"
        writer = CsvWriter(path, ["col1"])
        writer.cerrar()
        assert not path.exists()

    def test_csv_context_manager_llama_cerrar_en_excepcion(self, tmp_path):
        path = tmp_path / "excepcion.csv"
        with pytest.raises(RuntimeError):
            with CsvWriter(path, ["col1", "col2"]) as writer:
                writer.escribir_fila(["a", "b"])
                raise RuntimeError("simulada")

        assert path.exists()
        contenido = path.read_text(encoding="utf-8-sig")
        assert "col1" in contenido

    def test_csv_escribir_despues_de_cerrar_falla(self, tmp_path):
        path = tmp_path / "cerrado.csv"
        writer = CsvWriter(path, ["col1"])
        writer.escribir_fila(["valor"])
        writer.cerrar()
        with pytest.raises(RuntimeError, match="ya fue cerrado"):
            writer.escribir_fila(["nuevo"])


class TestCalcularSha256:
    def test_sha256_calculado_correctamente(self, tmp_path):
        archivo = tmp_path / "archivo.txt"
        archivo.write_bytes(b"contenido de prueba")
        sha = calcular_sha256(archivo)
        assert len(sha) == 64
        assert sha == "f3a7a67ab20351ddf47e87ecbf0e5a0868fc0e257d0aea65d018b0405b9a34f3"

    def test_sha256_bloques(self, tmp_path):
        archivo = tmp_path / "bloques.bin"
        archivo.write_bytes(b"a" * 100000)
        sha = calcular_sha256(archivo, bloque_tamano=1024)
        assert len(sha) == 64


class TestPublicacionLocal:
    def test_publicar_atomicamente(self, tmp_path):
        staged = tmp_path / "staged.csv"
        staged.write_text("contenido staged")
        destino = tmp_path / "destino.csv"

        pub = PublicacionLocal(tmp_path)
        resultado = pub.publicar(staged, destino)

        assert destino.exists()
        assert not (tmp_path / "destino.csv.partial").exists()
        assert resultado.estado.value == "publicado"
        assert resultado.sha256 is not None

    def test_publicar_no_sobreescribe_hash_distinto(self, tmp_path):
        staged = tmp_path / "staged.csv"
        staged.write_text("contenido nuevo")
        destino = tmp_path / "destino.csv"
        destino.write_text("contenido existente")

        pub = PublicacionLocal(tmp_path)
        with pytest.raises(ValueError, match="Hash diff"):
            pub.publicar(staged, destino)

        assert destino.read_text() == "contenido existente"

    def test_publicar_archivo_no_existente(self, tmp_path):
        staged = tmp_path / "no_existe.csv"
        destino = tmp_path / "destino.csv"

        pub = PublicacionLocal(tmp_path)
        with pytest.raises(FileNotFoundError):
            pub.publicar(staged, destino)

    def test_publicar_idempotente_hash_igual(self, tmp_path):
        staged = tmp_path / "staged.csv"
        staged.write_text("contenido estable")
        destino = tmp_path / "destino.csv"

        pub = PublicacionLocal(tmp_path)
        r1 = pub.publicar(staged, destino)
        assert destino.exists()
        sha_v1 = destino.read_text()

        staged.write_text("contenido estable")
        r2 = pub.publicar(staged, destino)

        assert destino.read_text() == sha_v1
        assert r1.sha256 == r2.sha256
        assert not (tmp_path / "destino.csv.partial").exists()

    def test_publicar_limpia_partial_siempre(self, tmp_path):
        staged = tmp_path / "staged.csv"
        staged.write_text("contenido")
        destino = tmp_path / "destino.csv"
        parcial = tmp_path / "destino.csv.partial"
        parcial.write_text("sobrescribeme")

        pub = PublicacionLocal(tmp_path)
        pub.publicar(staged, destino)

        assert not parcial.exists()
        assert destino.exists()


class TestManifiestoPublicacion:
    def test_a_dict_sin_secretos(self):
        m = ManifiestoPublicacion(
            archivo="salida.csv",
            bytes=1024,
            sha256="abc123",
            filas=100,
        )
        d = m.a_dict()
        assert d["archivo"] == "salida.csv"
        assert d["bytes"] == 1024
        assert d["sha256"] == "abc123"
        assert d["filas"] == 100
        assert d["estado"] == "pendiente"
        assert "password" not in d
        assert "clave" not in d


class TestPublicacionSftpMocks:
    @pytest.mark.skipif(not HAS_PARAMIKO, reason="paramiko no instalado")
    def test_sftp_load_host_keys(self):
        with patch(
            "motor_spark.dataflow_script.publicacion.paramiko.SSHClient"
        ) as mock_ssh:
            mock_instance = MagicMock()
            mock_ssh.return_value = mock_instance

            pub = PublicacionSftp(
                host="servidor.example.com",
                puerto=22,
                usuario="testuser",
                password="testpassword",
            )
            pub._crear_cliente()
            mock_instance.load_system_host_keys.assert_called_once_with()

    @pytest.mark.skipif(not HAS_PARAMIKO, reason="paramiko no instalado")
    def test_sftp_put_confirm_true(self):
        with patch(
            "motor_spark.dataflow_script.publicacion.paramiko.SSHClient"
        ) as mock_ssh:
            mock_instance = MagicMock()
            mock_sftp = MagicMock()
            mock_instance.open_sftp.return_value = mock_sftp
            mock_ssh.return_value = mock_instance

            pub = PublicacionSftp(
                host="servidor.example.com",
                puerto=22,
                usuario="testuser",
                password="testpassword",
            )
            pub.conectar()

            mock_instance.connect.assert_called_once()
            mock_instance.open_sftp.assert_called_once()

            archivo_local = Path("/tmp/test_put.csv")
            archivo_local.touch()
            ruta_remota = Path("/remote/path/test.csv")

            pub.publicar(archivo_local, ruta_remota)

            mock_sftp.put.assert_called_once()
            call_args = mock_sftp.put.call_args
            assert call_args[1]["confirm"] is True

    @pytest.mark.skipif(not HAS_PARAMIKO, reason="paramiko no instalado")
    def test_sftp_cierre_en_finally(self):
        with patch(
            "motor_spark.dataflow_script.publicacion.paramiko.SSHClient"
        ) as mock_ssh:
            mock_instance = MagicMock()
            mock_sftp = MagicMock()
            mock_instance.open_sftp.return_value = mock_sftp
            mock_ssh.return_value = mock_instance

            pub = PublicacionSftp(
                host="servidor.example.com",
                puerto=22,
                usuario="testuser",
                password="testpassword",
            )
            pub.conectar()
            pub.cerrar()

            mock_sftp.close.assert_called_once()
            mock_instance.close.assert_called_once()

    @pytest.mark.skipif(not HAS_PARAMIKO, reason="paramiko no instalado")
    def test_sftp_context_manager(self):
        with patch(
            "motor_spark.dataflow_script.publicacion.paramiko.SSHClient"
        ) as mock_ssh:
            mock_instance = MagicMock()
            mock_sftp = MagicMock()
            mock_instance.open_sftp.return_value = mock_sftp
            mock_ssh.return_value = mock_instance

            with PublicacionSftp(
                host="servidor.example.com",
                puerto=22,
                usuario="testuser",
                password="testpassword",
            ):
                pass

            mock_sftp.close.assert_called()
            mock_instance.close.assert_called()

    @pytest.mark.skipif(not HAS_PARAMIKO, reason="paramiko no instalado")
    def test_sftp_rename_after_put(self):
        with patch(
            "motor_spark.dataflow_script.publicacion.paramiko.SSHClient"
        ) as mock_ssh:
            mock_instance = MagicMock()
            mock_sftp = MagicMock()
            mock_instance.open_sftp.return_value = mock_sftp
            mock_ssh.return_value = mock_instance

            pub = PublicacionSftp(
                host="servidor.example.com",
                puerto=22,
                usuario="testuser",
                password="testpassword",
            )
            pub.conectar()

            archivo_local = Path("/tmp/test.csv")
            archivo_local.touch()
            ruta_remota = Path("/remote/path/test.csv")

            pub.publicar(archivo_local, ruta_remota)

            mock_sftp.rename.assert_called_once()
            rename_args = mock_sftp.rename.call_args[0]
            assert rename_args[1] == str(ruta_remota)

    @pytest.mark.skipif(not HAS_PARAMIKO, reason="paramiko no instalado")
    def test_sftp_timeout_configurado(self):
        with patch(
            "motor_spark.dataflow_script.publicacion.paramiko.SSHClient"
        ) as mock_ssh:
            mock_instance = MagicMock()
            mock_sftp = MagicMock()
            mock_instance.open_sftp.return_value = mock_sftp
            mock_ssh.return_value = mock_instance

            pub = PublicacionSftp(
                host="servidor.example.com",
                puerto=22,
                usuario="testuser",
                password="testpassword",
                timeout=60.0,
            )
            pub.conectar()

            connect_call = mock_instance.connect.call_args
            assert connect_call[1]["timeout"] == 60.0

    @pytest.mark.skipif(not HAS_PARAMIKO, reason="paramiko no instalado")
    def test_sftp_no_auto_add_policy(self):
        with patch(
            "motor_spark.dataflow_script.publicacion.paramiko.SSHClient"
        ) as mock_ssh:
            mock_instance = MagicMock()
            mock_ssh.return_value = mock_instance

            pub = PublicacionSftp(
                host="servidor.example.com",
                puerto=22,
                usuario="testuser",
                password="testpassword",
            )
            pub._crear_cliente()

            for call in mock_instance.method_calls:
                if "AutoAddPolicy" in str(call):
                    pytest.fail("AutoAddPolicy no debe ser usado")

    @pytest.mark.skipif(not HAS_PARAMIKO, reason="paramiko no instalado")
    def test_sftp_cierra_en_excepcion_put(self):
        with patch(
            "motor_spark.dataflow_script.publicacion.paramiko.SSHClient"
        ) as mock_ssh:
            mock_instance = MagicMock()
            mock_sftp = MagicMock()
            mock_sftp.put.side_effect = RuntimeError("put fallo")
            mock_instance.open_sftp.return_value = mock_sftp
            mock_ssh.return_value = mock_instance

            pub = PublicacionSftp(
                host="servidor.example.com",
                puerto=22,
                usuario="testuser",
                password="testpassword",
            )
            pub.conectar()

            archivo_local = Path("/tmp/test.csv")
            archivo_local.write_text("datos")
            ruta_remota = Path("/remote/path/test.csv")

            with pytest.raises(RuntimeError, match="put fallo"):
                pub.publicar(archivo_local, ruta_remota)

            mock_sftp.close.assert_called_once()
            mock_instance.close.assert_called_once()


class TestStagingManagerConcurrencia:
    def test_staging_ids_diferentes_son_independientes(self, tmp_path):
        manager = StagingManager(tmp_path)
        ids = [f"exec-{i}" for i in range(5)]
        staging_dirs = [manager.crear_staging(id_) for id_ in ids]
        assert len(set(staging_dirs)) == 5
        for s in staging_dirs:
            assert s.exists()

    def test_staging_limpieza_multiple_no_interfieren(self, tmp_path):
        manager = StagingManager(tmp_path)
        ids = [f"exec-limpieza-{i}" for i in range(3)]
        for id_ in ids:
            staging = manager.crear_staging(id_)
            (staging / f"archivo_{id_}.txt").write_text("dato")
            manager.limpiar_staging(id_)
            assert not staging.exists()

    def test_staging_no_hay_condiciones_de_carrera(self, tmp_path):
        import queue
        import threading

        manager = StagingManager(tmp_path)
        resultados = queue.Queue()
        ids_por_hilo = [[] for _ in range(4)]

        def crear_staging_por_hilo(hilo_id, num_ids):
            for i in range(num_ids):
                id_ = f"h{hilo_id}-i{i}"
                ids_por_hilo[hilo_id].append(id_)
                s = manager.crear_staging(id_)
                resultados.put(("creado", id_, s))
                (s / "marca.txt").write_text(f"hilo {hilo_id}")

        hilos = [
            threading.Thread(target=crear_staging_por_hilo, args=(i, 10))
            for i in range(4)
        ]
        for h in hilos:
            h.start()
        for h in hilos:
            h.join()

        while not resultados.empty():
            _op, id_, path = resultados.get()
            assert path.exists(), f"{id_} deberia existir"

        manager2 = StagingManager(tmp_path)
        for hilo_ids in ids_por_hilo:
            for id_ in hilo_ids:
                s = manager2.crear_staging(id_)
                assert s.exists()
                marca = s / "marca.txt"
                assert marca.exists(), f"{id_} marca deberia existir tras recrear"


import csv


@pytest.mark.skipif(not HAS_PARAMIKO, reason="paramiko no instalado")
def test_sftp_carga_host_keys_del_sistema_y_rechaza_desconocidos():
    with patch(
        "motor_spark.dataflow_script.publicacion.paramiko.SSHClient"
    ) as mock_ssh:
        mock_instance = MagicMock()
        mock_ssh.return_value = mock_instance

        pub = PublicacionSftp(
            host="servidor.example.com",
            puerto=22,
            usuario="testuser",
            password="testpassword",
        )
        pub._crear_cliente()

        mock_instance.load_system_host_keys.assert_called_once_with()
        mock_instance.set_missing_host_key_policy.assert_called_once()
        policy = mock_instance.set_missing_host_key_policy.call_args.args[0]
        assert policy.__class__.__name__ == "RejectPolicy"


@pytest.mark.parametrize("valor", ["../escape", "a/b", ".", "", ".."])
def test_staging_rechaza_identificadores_no_atomicos(tmp_path, valor):
    manager = StagingManager(tmp_path)
    with pytest.raises(ValueError, match="identificador"):
        manager.crear_staging(valor)


@pytest.mark.parametrize("valor", ["../salida", "a/b", ".", "", ".."])
def test_staging_rechaza_nombres_de_salida_no_atomicos(tmp_path, valor):
    manager = StagingManager(tmp_path)
    manager.crear_staging("exec-segura")
    with pytest.raises(ValueError, match="nombre de salida"):
        manager.crear_staging_salida("exec-segura", valor)


@pytest.mark.skipif(not HAS_PARAMIKO, reason="paramiko no instalado")
def test_sftp_clave_privada_usa_key_filename_y_passphrase(tmp_path):
    clave = tmp_path / "sftp_debian"
    clave.write_text("clave-ficticia", encoding="utf-8")
    clave.chmod(0o600)

    with patch(
        "motor_spark.dataflow_script.publicacion.paramiko.SSHClient"
    ) as mock_ssh:
        cliente = MagicMock()
        cliente.open_sftp.return_value = MagicMock()
        mock_ssh.return_value = cliente

        PublicacionSftp(
            host="209.50.245.140",
            puerto=22,
            usuario="sftpqlik",
            clave_privada=clave,
            passphrase="frase-secreta",
        ).conectar()

        kwargs = cliente.connect.call_args.kwargs
        assert kwargs["key_filename"] == str(clave)
        assert kwargs["passphrase"] == "frase-secreta"
        assert kwargs["look_for_keys"] is False
        assert kwargs["allow_agent"] is False
        assert "password" not in kwargs


@pytest.mark.skipif(not HAS_PARAMIKO, reason="paramiko no instalado")
def test_sftp_clave_privada_no_depende_de_rsa_key(tmp_path):
    clave = tmp_path / "id_ed25519"
    clave.write_text("clave-ficticia", encoding="utf-8")
    clave.chmod(0o600)

    with (
        patch("motor_spark.dataflow_script.publicacion.paramiko.SSHClient") as mock_ssh,
        patch(
            "motor_spark.dataflow_script.publicacion.paramiko.RSAKey.from_private_key_file"
        ) as rsa_loader,
    ):
        cliente = MagicMock()
        cliente.open_sftp.return_value = MagicMock()
        mock_ssh.return_value = cliente

        PublicacionSftp(
            host="209.50.245.140",
            puerto=22,
            usuario="sftpqlik",
            clave_privada=clave,
        ).conectar()

        rsa_loader.assert_not_called()


@pytest.mark.skipif(not HAS_PARAMIKO, reason="paramiko no instalado")
def test_sftp_contenido_clave_usa_pkey_sin_archivo():
    contenido = "-----BEGIN OPENSSH PRIVATE KEY-----\nfalso\n-----END OPENSSH PRIVATE KEY-----\n"
    pkey = MagicMock()

    with (
        patch("motor_spark.dataflow_script.publicacion.paramiko.SSHClient") as mock_ssh,
        patch.object(
            PublicacionSftp,
            "_cargar_clave_desde_contenido",
            return_value=pkey,
        ) as cargar_clave,
    ):
        cliente = MagicMock()
        cliente.open_sftp.return_value = MagicMock()
        mock_ssh.return_value = cliente

        PublicacionSftp(
            host="209.50.245.140",
            puerto=22,
            usuario="sftpqlik",
            clave_privada_contenido=contenido,
        ).conectar()

        cargar_clave.assert_called_once_with(contenido, None)
        kwargs = cliente.connect.call_args.kwargs
        assert kwargs["pkey"] is pkey
        assert "key_filename" not in kwargs
        assert "password" not in kwargs


def test_decodificar_clave_privada_base64_valida_contenido():
    import base64

    from motor_spark.dataflow_script.publicacion import (
        decodificar_clave_privada_base64,
    )

    clave = (
        "-----BEGIN OPENSSH PRIVATE KEY-----\nabc\n-----END OPENSSH PRIVATE KEY-----\n"
    )
    valor = base64.b64encode(clave.encode("utf-8")).decode("ascii")

    assert decodificar_clave_privada_base64(valor) == clave


def test_decodificar_clave_privada_base64_rechaza_valor_invalido():
    from motor_spark.dataflow_script.publicacion import (
        decodificar_clave_privada_base64,
    )

    with pytest.raises(ValueError, match="Base64"):
        decodificar_clave_privada_base64("no-es-base64!!!")


@pytest.mark.skipif(not HAS_PARAMIKO, reason="paramiko no instalado")
def test_cargar_clave_desde_contenido_detecta_rsa_real():
    from motor_spark.dataflow_script.publicacion import paramiko

    contenido = io.StringIO()
    original = paramiko.RSAKey.generate(1024)
    original.write_private_key(contenido)

    cargada = PublicacionSftp._cargar_clave_desde_contenido(
        contenido.getvalue(),
        None,
    )

    assert isinstance(cargada, paramiko.RSAKey)


def test_publicacion_sftp_rechaza_modos_auth_ambiguos():
    with pytest.raises(ValueError, match="exactamente"):
        PublicacionSftp(
            host="209.50.245.140",
            puerto=22,
            usuario="sftpqlik",
            password="clave",
            clave_privada_contenido=(
                "-----BEGIN OPENSSH PRIVATE KEY-----\nabc\n"
                "-----END OPENSSH PRIVATE KEY-----\n"
            ),
        )
