from types import SimpleNamespace

import pytest

from motor_spark.dominio.errores import ErrorReceta
from motor_spark.infraestructura.spark.sistema_archivos import preparar_salida_local


def test_preparar_salida_local_ignora_esquema_hdfs():
    assert preparar_salida_local("hdfs:///datos/salida", "overwrite") == "overwrite"


def test_preparar_salida_local_ignora_modo_distinto_overwrite():
    assert preparar_salida_local("file:///tmp/salida", "append") == "append"


def test_preparar_salida_local_rechaza_directorio_fuera_del_permitido():
    with pytest.raises(ErrorReceta, match="Se rechazó overwrite"):
        preparar_salida_local("file:///tmp/salida", "overwrite")


def test_preparar_salida_local_configura_grupo_y_permisos(monkeypatch, capsys):
    llamadas = []
    monkeypatch.setattr("os.path.lexists", lambda ruta: True)
    monkeypatch.setattr("shutil.rmtree", lambda ruta: llamadas.append(("rmtree", ruta)))
    monkeypatch.setattr(
        "os.makedirs",
        lambda ruta, mode, exist_ok: llamadas.append(("makedirs", ruta, mode, exist_ok)),
    )
    monkeypatch.setattr("grp.getgrnam", lambda nombre: SimpleNamespace(gr_gid=777))
    monkeypatch.setattr("os.chown", lambda ruta, uid, gid: llamadas.append(("chown", ruta, uid, gid)))
    monkeypatch.setattr("os.chmod", lambda ruta, mode: llamadas.append(("chmod", ruta, mode)))

    modo = preparar_salida_local(
        "file:///srv/talend-motor/salida/ventas",
        "overwrite",
    )

    assert modo == "append"
    assert llamadas == [
        ("rmtree", "/srv/talend-motor/salida/ventas"),
        ("makedirs", "/srv/talend-motor/salida/ventas", 0o2770, False),
        ("chown", "/srv/talend-motor/salida/ventas", -1, 777),
        ("chmod", "/srv/talend-motor/salida/ventas", 0o2770),
    ]
    assert capsys.readouterr().out == (
        "SALIDA_LOCAL_PREPARADA=/srv/talend-motor/salida/ventas "
        "modo_spark=append\n"
    )
