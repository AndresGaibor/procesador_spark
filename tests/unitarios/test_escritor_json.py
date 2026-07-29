import json

from motor_spark.infraestructura.resultados.escritor_json import guardar_resultado


def test_guardar_resultado_es_atomico_y_utf8(tmp_path):
    destino = tmp_path / "sub" / "resultado.json"
    guardar_resultado(str(destino), {"estado": "COMPLETADO", "texto": "sí"})
    assert json.loads(destino.read_text(encoding="utf-8")) == {
        "estado": "COMPLETADO",
        "texto": "sí",
    }
    assert not destino.with_suffix(".json.tmp").exists()


def test_guardar_resultado_ignora_ruta_none():
    guardar_resultado(None, {"estado": "COMPLETADO"})
