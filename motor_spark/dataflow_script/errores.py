from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SourceSpan:
    linea: int
    columna: int
    offset: int

    def __str__(self) -> str:
        return f"{self.linea}:{self.columna}"


@dataclass(frozen=True, slots=True)
class SourceLocation:
    inicio: SourceSpan
    fin: SourceSpan

    def __str__(self) -> str:
        return f"{self.inicio}-{self.fin}"


@dataclass(frozen=True, slots=True)
class ErrorDataflow:
    mensaje: str
    ubicacion: SourceLocation | None
    ayuda: str | None = None
    codigo: str | None = None

    def formato_estable(self) -> str:
        partes = [self.mensaje]
        if self.codigo:
            partes.insert(0, f"[{self.codigo}]")
        if self.ubicacion:
            partes.append(f" ubicacion={self.ubicacion}")
        if self.ayuda:
            partes.append(f" ayuda={self.ayuda}")
        return "".join(partes)
