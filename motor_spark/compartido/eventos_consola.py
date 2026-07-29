from __future__ import annotations

import sys
from typing import TextIO


def emitir(evento: str, *, error: bool = False, archivo: TextIO | None = None) -> None:
    destino = archivo if archivo is not None else (sys.stderr if error else sys.stdout)
    print(evento, file=destino, flush=True)
