from pathlib import Path
from unittest.mock import MagicMock, patch

from motor_spark.conexiones.modelos import (
    CampoAllowlist,
    CatalogoConexiones,
    ConexionSftp,
)
from motor_spark.conexiones.secretos import AdministradorSecretos
from motor_spark.plan.ejecutor import EjecutorPlanDataflow


def test_publicar_sftp_con_clave_no_exige_secreto_usuario_password(tmp_path):
    clave = tmp_path / "sftp_debian"
    clave.write_text("clave-ficticia", encoding="utf-8")
    clave.chmod(0o600)
    conexion = ConexionSftp(
        nombre="Banco:SFTP",
        host="209.50.245.140",
        puerto=22,
        usuario="sftpqlik",
        clave_privada=str(clave),
        ruta_base="/upload",
        allowlist=(CampoAllowlist(esquema="", tabla="salida.csv"),),
    )
    ejecutor = EjecutorPlanDataflow(
        spark=MagicMock(),
        catalogo=CatalogoConexiones(sftp=(conexion,)),
        secretos=AdministradorSecretos(),
        ejecucion_id="sftp-key-test",
    )
    dataframe = MagicMock()
    uri = MagicMock(ruta="salida.csv")
    staged = tmp_path / "salida.csv"
    staged.write_text("id\n1\n", encoding="utf-8")

    with (
        patch.object(ejecutor, "_materializar_csv_unico", return_value=staged),
        patch("motor_spark.plan.ejecutor.PublicacionSftp") as publicador_cls,
    ):
        publicador = publicador_cls.return_value.__enter__.return_value
        publicador.publicar.return_value = MagicMock()
        ejecutor._publicar_sftp(dataframe, uri, conexion)

    kwargs = publicador_cls.call_args.kwargs
    assert kwargs["usuario"] == "sftpqlik"
    assert kwargs["clave_privada"] == Path(clave)
    assert kwargs["passphrase"] is None
    assert "password" not in kwargs


def test_publicar_sftp_decodifica_clave_desde_secreto_base64(tmp_path):
    import base64

    clave = (
        "-----BEGIN OPENSSH PRIVATE KEY-----\nabc\n-----END OPENSSH PRIVATE KEY-----\n"
    )
    clave_b64 = base64.b64encode(clave.encode("utf-8")).decode("ascii")
    conexion = ConexionSftp(
        nombre="Banco:SFTP",
        host="209.50.245.140",
        puerto=22,
        usuario="sftpqlik",
        secreto_clave_privada_nombre="SFTP_PRIVATE_KEY_B64",
        ruta_base="/upload",
        allowlist=(CampoAllowlist(esquema="", tabla="salida.csv"),),
    )
    ejecutor = EjecutorPlanDataflow(
        spark=MagicMock(),
        catalogo=CatalogoConexiones(sftp=(conexion,)),
        secretos=AdministradorSecretos({"SFTP_PRIVATE_KEY_B64": clave_b64}),
        ejecucion_id="sftp-key-content-test",
    )
    dataframe = MagicMock()
    uri = MagicMock(ruta="salida.csv")
    staged = tmp_path / "salida.csv"
    staged.write_text("id\n1\n", encoding="utf-8")

    with (
        patch.object(ejecutor, "_materializar_csv_unico", return_value=staged),
        patch("motor_spark.plan.ejecutor.PublicacionSftp") as publicador_cls,
    ):
        publicador = publicador_cls.return_value.__enter__.return_value
        publicador.publicar.return_value = MagicMock()
        ejecutor._publicar_sftp(dataframe, uri, conexion)

    kwargs = publicador_cls.call_args.kwargs
    assert kwargs["usuario"] == "sftpqlik"
    assert kwargs["clave_privada_contenido"] == clave
    assert kwargs["passphrase"] is None
    assert "clave_privada" not in kwargs
    assert "password" not in kwargs
