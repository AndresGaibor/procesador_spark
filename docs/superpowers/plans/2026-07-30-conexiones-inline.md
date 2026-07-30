# Conexiones Inline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Aceptar el catálogo de conexiones como JSON inline sin guardar archivos.

**Architecture:** El CLI resolverá una única fuente del catálogo. `cargador.py` expondrá funciones separadas para ruta y contenido que convergen en un único constructor tipado. El orquestador elegirá la fuente después de compilar el script.

**Tech Stack:** Python 3.10+, argparse, json, Pydantic v2, pytest.

## Global Constraints

- Mantener `--conexiones` compatible.
- No mostrar el catálogo ni secretos en resultados o logs.
- Aplicar TDD y ejecutar la suite completa.

### Task 1: Contrato CLI

- [ ] Añadir pruebas de `--conexiones-contenido`, conflicto y ausencia.
- [ ] Hacer fallar las pruebas.
- [ ] Añadir el campo al dataclass y grupo mutuamente excluyente.
- [ ] Ejecutar pruebas dirigidas.

### Task 2: Cargador inline

- [ ] Añadir pruebas para JSON válido, vacío, inválido y raíz no objeto.
- [ ] Extraer construcción común del catálogo.
- [ ] Añadir `cargar_catalogo_contenido(contenido: str)`.
- [ ] Ejecutar pruebas dirigidas.

### Task 3: Orquestación y seguridad

- [ ] Añadir pruebas de ejecución con catálogo inline y sin archivo de resultado.
- [ ] Resolver archivo/contenido de forma fail-closed.
- [ ] Verificar que el contenido no aparece en salida.
- [ ] Actualizar documentación.

### Task 4: Verificación

- [ ] Ejecutar Ruff y formato.
- [ ] Ejecutar suite completa con cobertura mínima 75%.
- [ ] Construir wheel/sdist y hacer smoke test.
- [ ] Commit del cambio.
