# Conexiones inline para Dataflow

## Objetivo

Permitir que el modo Dataflow reciba el catálogo completo mediante `--conexiones-contenido`, sin crear archivos temporales ni persistir JSON en disco.

## Contrato

- `--conexiones` conserva el comportamiento actual de ruta a archivo.
- `--conexiones-contenido` recibe un objeto JSON completo como un único argumento.
- Ambos parámetros son mutuamente excluyentes y exactamente uno es obligatorio.
- El texto inline se valida con el mismo modelo Pydantic que el archivo.
- El catálogo inline nunca se imprime ni se incorpora al resultado.
- `--resultado` continúa siendo opcional; al omitirlo solo se emite `RESULTADO_MOTOR`.
- Las credenciales continúan fuera del catálogo, mediante `--secreto` o variables de entorno.

## Flujo

CLI -> resolver origen del catálogo -> parsear JSON -> construir CatalogoConexiones -> ejecutar plan.

La compilación con `--solo-compilar` no carga el catálogo, igual que antes.

## Pruebas

Se cubren JSON inline válido, vacío, malformado, raíz no objeto, conflicto con ruta, ausencia de ambos, compatibilidad de archivo y ausencia del contenido en resultados/logs.
