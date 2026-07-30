"""Pruebas de integración de la única ruta de ejecución del plan compilado."""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from motor_spark.conexiones.modelos import (
    CampoAllowlist,
    CatalogoConexiones,
    ConexionLocal,
)
from motor_spark.conexiones.secretos import AdministradorSecretos
from motor_spark.plan.ejecutor import EjecutorPlanDataflow, ErrorEjecucionPlan
from motor_spark.plan.modelos import (
    Agregar,
    CargarCsv,
    Concatenar,
    EliminarTabla,
    Filtrar,
    PlanDataflow,
    Proyectar,
    Publicar,
    Unir,
)

pytestmark = pytest.mark.spark


def _escribir_csv(ruta: Path, filas: list[list[object]]) -> None:
    ruta.parent.mkdir(parents=True, exist_ok=True)
    with ruta.open("w", newline="", encoding="utf-8") as archivo:
        csv.writer(archivo).writerows(filas)


def _catalogo_local(base: Path, *rutas: str) -> CatalogoConexiones:
    permitidas = tuple(CampoAllowlist(esquema="", tabla=ruta) for ruta in rutas)
    return CatalogoConexiones(
        locales=(
            ConexionLocal(
                nombre="Archivos",
                ruta_base=str(base),
                allowlist=permitidas,
            ),
        )
    )


def test_carga_proyecta_filtra_y_publica_csv_local(spark_local, tmp_path):
    _escribir_csv(
        tmp_path / "entrada.csv",
        [
            ["id", "nombre", "monto"],
            [1, "Ana", 150],
            [2, "Carlos", 80],
            [3, "María", 220],
        ],
    )
    catalogo = _catalogo_local(tmp_path, "entrada.csv", "salida.csv")
    plan = PlanDataflow(
        operaciones=(
            CargarCsv(
                id="cargar",
                nombre_tabla="Ventas",
                ruta="lib://Archivos/entrada.csv",
            ),
            Proyectar(
                id="proyectar",
                tabla_origen="Ventas",
                campos=("id", "nombre", "monto"),
                aliases=(("nombre", "cliente"),),
                alias="VentasCuradas",
            ),
            Filtrar(
                id="filtrar",
                tabla_origen="VentasCuradas",
                condicion="monto > 100",
            ),
            Publicar(
                id="publicar",
                tabla_origen="VentasCuradas",
                destino="lib://Archivos/salida.csv",
            ),
        ),
        tabla_resultado="VentasCuradas",
    )

    resultado = EjecutorPlanDataflow(
        spark=spark_local,
        catalogo=catalogo,
        secretos=AdministradorSecretos(),
        ejecucion_id="e2e-local",
    ).ejecutar(plan)

    assert resultado["operaciones_ejecutadas"] == 4
    assert resultado["publicaciones"][0]["archivo"] == "salida.csv"
    salida = spark_local.read.csv(
        str(tmp_path / "salida.csv"),
        header=True,
        inferSchema=True,
    )
    assert salida.columns == ["id", "cliente", "monto"]
    assert {fila.id for fila in salida.collect()} == {1, 3}


def test_concatenate_alinea_columnas_por_nombre(spark_local, tmp_path):
    _escribir_csv(
        tmp_path / "parte_a.csv",
        [["id", "nombre"], [1, "Ana"]],
    )
    _escribir_csv(
        tmp_path / "parte_b.csv",
        [["id", "monto"], [2, 90]],
    )
    catalogo = _catalogo_local(
        tmp_path,
        "parte_a.csv",
        "parte_b.csv",
    )
    plan = PlanDataflow(
        operaciones=(
            CargarCsv(
                id="a",
                nombre_tabla="Acumulada",
                ruta="lib://Archivos/parte_a.csv",
            ),
            CargarCsv(
                id="b",
                nombre_tabla="Nueva",
                ruta="lib://Archivos/parte_b.csv",
            ),
            Concatenar(
                id="concat",
                tabla_objetivo="Acumulada",
                tabla_origen="Nueva",
            ),
        )
    )

    ejecutor = EjecutorPlanDataflow(
        spark=spark_local,
        catalogo=catalogo,
        secretos=AdministradorSecretos(),
        ejecucion_id="concat-columnas",
    )
    ejecutor.ejecutar(plan)
    resultado = ejecutor.obtener_tabla("Acumulada")

    assert resultado.columns == ["id", "nombre", "monto"]
    filas = {fila.id: fila.asDict() for fila in resultado.collect()}
    assert filas[1]["monto"] is None
    assert filas[2]["nombre"] is None


def test_join_natural_usa_todos_los_campos_comunes(spark_local, tmp_path):
    catalogo = _catalogo_local(tmp_path)
    ejecutor = EjecutorPlanDataflow(
        spark=spark_local,
        catalogo=catalogo,
        secretos=AdministradorSecretos(),
        ejecucion_id="join-natural",
    )
    ejecutor.registrar_tabla(
        "Izquierda",
        spark_local.createDataFrame(
            [(1, "Norte", "venta-a"), (1, "Sur", "venta-b")],
            ["id", "region", "venta"],
        ),
    )
    ejecutor.registrar_tabla(
        "Derecha",
        spark_local.createDataFrame(
            [(1, "Norte", "cliente-norte")],
            ["id", "region", "cliente"],
        ),
    )
    plan = PlanDataflow(
        operaciones=(
            Unir(
                id="join",
                tabla_izquierda="Izquierda",
                tabla_derecha="Derecha",
                condicion_on="NATURAL",
            ),
        )
    )

    ejecutor.ejecutar(plan)
    filas = ejecutor.obtener_tabla("Izquierda").orderBy("region").collect()

    assert filas[0].cliente == "cliente-norte"
    assert filas[1].cliente is None


def test_agregacion_permitida_con_aliases(spark_local, tmp_path):
    ejecutor = EjecutorPlanDataflow(
        spark=spark_local,
        catalogo=_catalogo_local(tmp_path),
        secretos=AdministradorSecretos(),
        ejecucion_id="agregacion",
    )
    ejecutor.registrar_tabla(
        "Ventas",
        spark_local.createDataFrame(
            [("Norte", 100.0), ("Norte", 200.0), ("Sur", 50.0)],
            ["region", "monto"],
        ),
    )
    plan = PlanDataflow(
        operaciones=(
            Agregar(
                id="agregar",
                tabla_origen="Ventas",
                grupo_por=("region",),
                funciones=(
                    "SUM(monto) AS total",
                    "COUNT(*) AS cantidad",
                    "AVG(monto) AS promedio",
                ),
                tabla_resultado="Resumen",
            ),
        )
    )

    ejecutor.ejecutar(plan)
    resumen = {
        fila.region: fila.asDict()
        for fila in ejecutor.obtener_tabla("Resumen").collect()
    }

    assert resumen["Norte"]["total"] == 300.0
    assert resumen["Norte"]["cantidad"] == 2
    assert resumen["Norte"]["promedio"] == 150.0


def test_falla_detiene_operaciones_posteriores(spark_local, tmp_path):
    catalogo = _catalogo_local(tmp_path, "no_debe_crearse.csv")
    ejecutor = EjecutorPlanDataflow(
        spark=spark_local,
        catalogo=catalogo,
        secretos=AdministradorSecretos(),
        ejecucion_id="fail-fast",
    )
    ejecutor.registrar_tabla(
        "Ventas",
        spark_local.createDataFrame([(1,)], ["id"]),
    )
    plan = PlanDataflow(
        operaciones=(
            Proyectar(
                id="proyeccion-invalida",
                tabla_origen="Ventas",
                campos=("columna_inexistente",),
            ),
            Publicar(
                id="publicacion-prohibida",
                tabla_origen="Ventas",
                destino="lib://Archivos/no_debe_crearse.csv",
            ),
            EliminarTabla(id="drop", nombre="Ventas"),
        )
    )

    with pytest.raises(ErrorEjecucionPlan, match="columna_inexistente"):
        ejecutor.ejecutar(plan)

    assert not (tmp_path / "no_debe_crearse.csv").exists()
    assert ejecutor.obtener_tabla("Ventas") is not None
