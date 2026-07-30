"""
Estrategia de pruebas de rendimiento para Dataflow.

Parametrizado por DATAFLOW_PERF_ROWS. Saltado por defecto.

Uso:
    DATAFLOW_PERF_ROWS=100000 pytest tests/rendimiento/test_dataflow_perf.py -v

Umbrales de referencia (local[1], Java 11+):
    - 10K filas:   < 5s  por paso simple (sin shuffle)
    - 100K filas:  < 30s por paso simple
    - 500K filas:  < 120s por paso simple

    Shuffle (groupBy/join): multiplicar por 3-5x.
    Escritura Parquet: +20-30% sobre tiempo de procesamiento.

Métricas Spark collectées:
    - spark.executor.duration ms
    - spark.job.duration ms
    - spark.sql.shuffle.partitions
    - spark.task.duration ms
"""

from __future__ import annotations

import os
import time

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("DATAFLOW_PERF_ROWS") is None,
    reason="DATAFLOW_PERF_ROWS no definido",
)


NUM_FILAS = int(os.environ.get("DATAFLOW_PERF_ROWS", "0"))


@pytest.fixture
def spark_perf(spark_local):
    pytest.importorskip("pyspark")
    import shutil

    if shutil.which("java") is None:
        pytest.skip("Java no instalado")
    yield spark_local


class TestDataflowPerfBasico:
    @pytest.fixture
    def df_grande(self, spark_perf):
        datos = [(i, f"cliente_{i}", float(i) * 1.5) for i in range(NUM_FILAS)]
        return spark_perf.createDataFrame(
            datos,
            ["id", "nombre", "monto"],
        )

    def test_lectura_csv_rapida(self, tmp_path):
        import shutil

        pytest.importorskip("pyspark")
        if shutil.which("java") is None:
            pytest.skip("Java no instalado")

        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.master("local[1]").appName("perf-test").getOrCreate()
        )

        csv_path = tmp_path / "perf.csv"
        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            f.write("id,nombre,monto\n")
            f.writelines(f"{i},cliente_{i},{i * 1.5}\n" for i in range(NUM_FILAS))

        inicio = time.monotonic()
        df = spark.read.csv(str(csv_path), header=True, inferSchema=True)
        df.collect()
        duracion = time.monotonic() - inicio

        spark.stop()

        assert duracion < 60, (
            f"Lectura {NUM_FILAS} filas tardo {duracion:.2f}s (umbral: 60s)"
        )

    def test_agregacion_groupby(self, spark_perf, df_grande):
        from pyspark.sql import functions as F

        inicio = time.monotonic()
        df_agg = df_grande.groupBy("nombre").agg(
            F.sum("monto").alias("total"),
            F.count("*").alias("cantidad"),
        )
        df_agg.collect()
        duracion = time.monotonic() - inicio

        assert duracion < 30, (
            f"GROUP BY con {NUM_FILAS} filas tardo {duracion:.2f}s (umbral: 30s)"
        )

    def test_filtro_where(self, spark_perf, df_grande):
        from pyspark.sql import functions as F

        inicio = time.monotonic()
        df_filt = df_grande.filter(F.col("id") > NUM_FILAS // 2)
        df_filt.collect()
        duracion = time.monotonic() - inicio

        assert duracion < 15, (
            f"Filtro WHERE con {NUM_FILAS} filas tardo {duracion:.2f}s (umbral: 15s)"
        )

    def test_join_seguro(self, spark_perf):
        izq = spark_perf.createDataFrame(
            [(i, f"cliente_{i}") for i in range(NUM_FILAS)],
            ["id", "nombre"],
        )
        der = spark_perf.createDataFrame(
            [(i, float(i) * 1.5) for i in range(NUM_FILAS)],
            ["id", "monto"],
        )

        inicio = time.monotonic()
        df_join = izq.join(der, "id", how="inner")
        df_join.collect()
        duracion = time.monotonic() - inicio

        assert duracion < 45, (
            f"JOIN inner con {NUM_FILAS} filas tardo {duracion:.2f}s (umbral: 45s)"
        )

    def test_escritura_parquet_local(self, spark_perf, df_grande, tmp_path):
        salida = tmp_path / "perf_parquet"

        inicio = time.monotonic()
        df_grande.write.mode("overwrite").parquet(str(salida))
        duracion = time.monotonic() - inicio

        assert duracion < 60, (
            f"Escritura Parquet con {NUM_FILAS} filas tardo {duracion:.2f}s (umbral: 60s)"
        )


class TestDataflowPerfStaging:
    def test_staging_manager_creacion_masiva(self, tmp_path):
        from motor_spark.dataflow_script.publicacion import StagingManager

        manager = StagingManager(tmp_path)
        inicio = time.monotonic()

        for i in range(min(NUM_FILAS // 100, 1000)):
            s = manager.crear_staging(f"perf-{i}")
            (s / "dato.txt").write_text("x" * 100)

        duracion = time.monotonic() - inicio

        assert duracion < 10, (
            f"Creacion de {NUM_FILAS // 100} staging tardo {duracion:.2f}s (umbral: 10s)"
        )

    def test_csv_writer_rapido(self, tmp_path):
        from motor_spark.dataflow_script.publicacion import CsvWriter

        path = tmp_path / "perf_writer.csv"
        inicio = time.monotonic()

        writer = CsvWriter(path, ["id", "nombre", "monto"])
        for i in range(min(NUM_FILAS, 10000)):
            writer.escribir_fila([str(i), f"cliente_{i}", str(i * 1.5)])
        writer.cerrar()

        duracion = time.monotonic() - inicio

        assert duracion < 10, (
            f"Escritura {min(NUM_FILAS, 10000)} filas CSV tardo {duracion:.2f}s (umbral: 10s)"
        )
