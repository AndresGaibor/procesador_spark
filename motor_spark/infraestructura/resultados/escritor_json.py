from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def guardar_resultado(
    ruta: str | None,
    contenido: dict[str, Any],
) -> None:
    if not ruta:
        return

    archivo = Path(ruta)
    archivo.parent.mkdir(parents=True, exist_ok=True)
    temporal = archivo.with_suffix(archivo.suffix + ".tmp")
    temporal.write_text(
        json.dumps(contenido, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temporal.replace(archivo)
