from __future__ import annotations

import hashlib
import json

from motor_spark.plan.modelos import Operacion, PlanDataflow


class SerializadorPlan:
    @staticmethod
    def serializar(plan: PlanDataflow) -> str:
        import json

        datos = plan.model_dump(mode="json")
        return json.dumps(datos, indent=2, sort_keys=True, default=str)

    @staticmethod
    def deserializar(json_str: str) -> PlanDataflow:
        return PlanDataflow.model_validate_json(json_str)

    @staticmethod
    def hash_plano(plan: PlanDataflow) -> str:
        return plan.hash_determista()

    @staticmethod
    def fingerprint_operacion(operacion: Operacion) -> str:
        datos = {
            "tipo": operacion.tipo.value,
            "id": operacion.id,
        }
        if hasattr(operacion, "tabla_origen"):
            datos["tabla_origen"] = getattr(operacion, "tabla_origen", None)
        if hasattr(operacion, "tabla"):
            datos["tabla"] = getattr(operacion, "tabla", None)
        if hasattr(operacion, "campos"):
            datos["campos"] = getattr(operacion, "campos", None)

        contenido = json.dumps(datos, sort_keys=True, default=str)
        return hashlib.sha256(contenido.encode()).hexdigest()

    @staticmethod
    def verificar_integridad(plan: PlanDataflow, hash_esperado: str) -> bool:
        return plan.hash_determista() == hash_esperado


def serializar_plan(plan: PlanDataflow) -> str:
    return SerializadorPlan.serializar(plan)


def deserializar_plan(json_str: str) -> PlanDataflow:
    return SerializadorPlan.deserializar(json_str)
