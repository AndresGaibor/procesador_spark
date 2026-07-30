"""Prueba vertical del subconjunto Qlik usado por Dataflow."""

from __future__ import annotations

import csv

import pytest

from motor_spark.conexiones.modelos import (
    CampoAllowlist,
    CatalogoConexiones,
    ConexionLocal,
)
from motor_spark.conexiones.secretos import AdministradorSecretos
from motor_spark.dataflow_script import normalizador, parsear, tokenizar
from motor_spark.dataflow_script.validador import validar_semantico
from motor_spark.plan.compilador import compilar
from motor_spark.plan.ejecutor import EjecutorPlanDataflow
from motor_spark.plan.serializador import deserializar_plan, serializar_plan

pytestmark = pytest.mark.spark


def test_script_complejo_compila_serializa_y_ejecuta(spark_local, tmp_path):
    entrada = tmp_path / "entrada.csv"
    with entrada.open("w", newline="", encoding="utf-8") as archivo:
        escritor = csv.writer(archivo)
        escritor.writerows(
            [
                ["id", "region", "estado", "monto", "texto"],
                [1, "Norte", "OK", 100, "  Uno  "],
                [2, "Norte", "OK", 100, " Dos "],
                [3, "Norte", "ERROR", 50, " Tres "],
                [4, "Sur", "OK", 80, " Cuatro "],
                [5, "Sur", "OK", -1, "Descartar"],
            ]
        )

    script = """
    [Base]:
    LOAD [id], [region], [estado], [monto], [texto]
    FROM [lib://Archivos/entrada.csv];

    [Curada]:
    LOAD
        [id],
        [region],
        Trim([texto]) AS [texto_limpio],
        [monto] * 2 AS [doble],
        IF(Match([estado], 'OK'), 'SI', 'NO') AS [valida]
    RESIDENT [Base]
    WHERE [monto] > 0;

    [Ranking]:
    LOAD
        [id], [region], [doble],
        Window(WRank(1, 1), [region], 'DESC', [doble]) AS [ranking]
    RESIDENT [Curada];

    [Resumen]:
    LOAD
        [region],
        Sum([doble]) AS [total],
        Count(DISTINCT [id]) AS [cantidad]
    RESIDENT [Ranking]
    GROUP BY [region];
    """

    contenido, errores_normalizacion = normalizador.normalizar(script)
    tokens, errores_lexicos = tokenizar(contenido)
    programa, errores_parser = parsear(tokens)
    errores_semanticos = validar_semantico(programa)
    plan = compilar(programa)

    assert errores_normalizacion == []
    assert errores_lexicos == []
    assert errores_parser == []
    assert errores_semanticos == []
    assert plan.metadata["errores"] == ()

    # El round-trip comprueba que ninguna expresión se pierde al persistir el
    # plan para auditoría, aprobación o ejecución posterior.
    plan = deserializar_plan(serializar_plan(plan))
    catalogo = CatalogoConexiones(
        locales=(
            ConexionLocal(
                nombre="Archivos",
                ruta_base=str(tmp_path),
                allowlist=(
                    CampoAllowlist(
                        esquema="",
                        tabla="entrada.csv",
                    ),
                ),
            ),
        )
    )
    ejecutor = EjecutorPlanDataflow(
        spark=spark_local,
        catalogo=catalogo,
        secretos=AdministradorSecretos(),
        ejecucion_id="complejo-e2e",
    )
    resultado = ejecutor.ejecutar(plan)

    assert resultado["operaciones_ejecutadas"] == len(plan.operaciones)
    ranking = {
        fila.id: fila.ranking for fila in ejecutor.obtener_tabla("Ranking").collect()
    }
    assert ranking == {1: 1, 2: 1, 3: 3, 4: 1}

    resumen = {
        fila.region: fila.asDict()
        for fila in ejecutor.obtener_tabla("Resumen").collect()
    }
    assert resumen["Norte"]["total"] == 500
    assert resumen["Norte"]["cantidad"] == 3
    assert resumen["Sur"]["total"] == 160
    assert resumen["Sur"]["cantidad"] == 1
