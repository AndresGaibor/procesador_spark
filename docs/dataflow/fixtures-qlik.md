# Fixtures Qlik para Suite Diferencial

## Bloqueo: Qlik Cloud No Disponible en Tests Locales

La suite de pruebas diferenciales de Dataflow requiere exportar "golden outputs" desde Qlik para comparar contra las salidas del motor Spark. Este documento explica por que no es posible generar esos artefactos y como se mitiga.

### Restricciones Identificadas

1. **Qlik Cloud no expuesto**: No existe credencial ni endpoint de Qlik Cloud en el entorno de CI/local
2. **Qlik Desktop no instalado**: El motor de evaluacion Qlik no esta disponible
3. **Datos reales no disponibles**: Por politica de seguridad, no se pueden usar datos de produccion

### Mitigacion: Synthetic Golden Files

Para permitir la ejecucion de tests sin Qlik, se generan artefactos sintéticos basados en la especificacion del lenguaje Qlik:

```
tests/recursos/dataflow/
├── scripts/
│   ├── script_valido_load_csv.qvs       # Script QVS objetivo
│   └── script_valido_expresiones.qvs    # Script con funciones
├── conexiones/
│   └── catalogo_seguro.json             # Catalogo sin secretos
└── datos/
    ├── entrada.csv                       # Datos de entrada
    └── golden_output_esperado.json       # Salida golden sintética
```

### Formato de Golden Output Sintetico

```json
{
  "version": "1.0-synthetic",
  "hash_sha256": "abc123...",
  "schema_hash": "def456...",
  "generated_from": "script_valido_expresiones.qvs",
  "generated_at": "2026-07-30T00:00:00Z",
  "nota": "Generado sinteticamente - no出自 Qlik real",
  "estructura": {
    "columnas": ["id", "nombre", "total"],
    "tipos": ["int", "string", "double"],
    "filas_esperadas": 5
  },
  "datos": []
}
```

### Por Que No Hay Hash Reales de Qlik

- **Sin acceso a Qlik**: No hay forma de ejecutar un script QVS en el entorno
- **Sin datos reales**: Los datos de produccion no pueden usarse
- **Sin licencia**: Qlik Cloud API no disponible

### Estrategia de Verificacion Alternativa

En lugar de comparar golden outputs bit-a-bit con Qlik, la suite actual verifica:

1. **Parsing**: El script QVS se parsea sin errores
2. **AST**: El AST generado es estructuralmente correcto
3. **Compilacion**: El plan se compila correctamente
4. **Propiedades**: Hash determinista, idempotencia, serializacion
5. **Seguridad**: SQL injection, path traversal, allowlist

### Para Habilitar Suite Diferencial (Futuro)

Cuando Qlik este disponible, ejecutar:

```bash
# 1. Exportar desde Qlik Cloud
qlik context use production
qlik script export script.qvs --output tests/recursos/dataflow/scripts/

# 2. Generar golden outputs
qlik eval --script script.qvs --output golden_output.json

# 3. Actualizar hashes en fixtures
sha256sum golden_output.json
# Actualizar fixture con hash nuevo
```

### Nota de Seguridad

- Nunca commitear datos reales de Qlik
- Nunca hardcodear credenciales de Qlik Cloud
- Usar siempre variables de entorno
