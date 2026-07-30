# Script Dataflow enviado como contenido

## Objetivo

Permitir que el modo Dataflow reciba el script Qlik completo como argumento, sin eliminar el contrato existente basado en archivo.

## Contrato

- `--dataflow-script` recibe una ruta.
- `--dataflow-script-contenido` recibe el texto completo.
- Son mutuamente excluyentes.
- El límite se calcula en bytes UTF-8 para ambos orígenes.
- El resultado no incluye el texto: solo origen, referencia segura y SHA-256.
- Las llamadas programáticas que establezcan ambos orígenes fallan de forma cerrada.
- El nombre de Spark usa `contenido-parametro` cuando no existe una ruta.

## Pruebas

Se cubren CLI, multilínea, comillas, contenido vacío, conflicto de orígenes, exceso de tamaño, ausencia de filtración y compatibilidad con ruta.
