from __future__ import annotations

from motor_spark.plan.modelos import (
    Agregar,
    CargarCsv,
    CargarLocal,
    Concatenar,
    EliminarTabla,
    Filtrar,
    LeerJdbc,
    Operacion,
    PlanDataflow,
    Proyectar,
    Publicar,
    TipoOperacion,
    Unir,
)


class ExplicadorPlan:
    def __init__(self, plan: PlanDataflow) -> None:
        self._plan = plan

    def explicar_operacion(self, operacion: Operacion) -> str:
        if isinstance(operacion, LeerJdbc):
            return (
                f"LEER JDBC desde '{operacion.esquema}.{operacion.tabla}' "
                f"usando conexion '{operacion.conexion_nombre}'"
            )
        elif isinstance(operacion, Proyectar):
            campos = ", ".join(operacion.campos)
            return f"PROYECTAR campos [{campos}] desde '{operacion.tabla_origen}'"
        elif isinstance(operacion, Filtrar):
            return f"FILTRAR '{operacion.tabla_origen}' WHERE {operacion.condicion}"
        elif isinstance(operacion, Concatenar):
            modo = "NOCONCATENATE" if operacion.noconcatenate else "CONCATENATE"
            return f"{modo} tabla '{operacion.tabla_origen}' hacia '{operacion.tabla_objetivo}'"
        elif isinstance(operacion, Unir):
            return (
                f"UNIR '{operacion.tabla_izquierda}' {operacion.tipo_join} JOIN "
                f"'{operacion.tabla_derecha}' ON {operacion.condicion_on}"
            )
        elif isinstance(operacion, Agregar):
            grupos = ", ".join(operacion.grupo_por)
            funcs = ", ".join(operacion.funciones)
            return f"AGRUPAR '{operacion.tabla_origen}' GROUP BY [{grupos}] {funcs}"
        elif isinstance(operacion, EliminarTabla):
            return f"ELIMINAR TABLA '{operacion.nombre}'"
        elif isinstance(operacion, Publicar):
            return f"PUBLICAR '{operacion.tabla_origen}' hacia {operacion.destino} ({operacion.formato})"
        elif isinstance(operacion, CargarCsv):
            return f"CARGAR CSV desde '{operacion.ruta}'"
        elif isinstance(operacion, CargarLocal):
            return f"CARGAR LOCAL '{operacion.ruta}' como '{operacion.nombre_tabla}'"
        return f"Operacion tipo {operacion.tipo}"

    def explicar_plan(self) -> str:
        lineas = [
            f"Plan Dataflow v{self._plan.version}",
            f"Hash: {self._plan.hash_determista()}",
            f"Tabla resultado: {self._plan.tabla_resultado or 'N/A'}",
            "",
            "Operaciones:",
        ]

        for i, op in enumerate(self._plan.operaciones):
            lineas.append(f"  {i + 1}. [{op.id}] {self.explicar_operacion(op)}")

        if self._plan.metadata.get("errores"):
            lineas.append("")
            lineas.append("Errores:")
            for err in self._plan.metadata["errores"]:
                lineas.append(f"  - {err}")

        return "\n".join(lineas)

    def explicar_breve(self) -> str:
        ops = [f"{op.tipo.value}" for op in self._plan.operaciones]
        return f"Plan con {len(ops)} operaciones: {' -> '.join(ops)}"


def explicar_plan(plan: PlanDataflow) -> str:
    return ExplicadorPlan(plan).explicar_plan()


def explicar_breve(plan: PlanDataflow) -> str:
    return ExplicadorPlan(plan).explicar_breve()
