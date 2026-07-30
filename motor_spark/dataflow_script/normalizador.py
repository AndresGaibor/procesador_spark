from __future__ import annotations

import re

from motor_spark.dataflow_script.errores import (
    ErrorDataflow,
    SourceLocation,
    SourceSpan,
)

BOM: str = "\ufeff"
CRLF: str = "\r\n"
LF: str = "\n"
CR: str = "\r"

PATRON_BOM: re.Pattern[str] = re.compile(r"^\ufeff")
PATRON_CRLF: re.Pattern[str] = re.compile(r"\r\n")
PATRON_COMENTARIO_LINEA: re.Pattern[str] = re.compile(r"//[^\n]*")
PATRON_COMENTARIO_SQL: re.Pattern[str] = re.compile(r"--[^\n]*")
PATRON_COMENTARIO_BLOQUE: re.Pattern[str] = re.compile(r"/\*[\s\S]*?\*/")


class Normalizador:
    def __init__(self, contenido: str) -> None:
        self._original: str = contenido
        self._procesado: str = contenido
        self._errores: list[ErrorDataflow] = []

    def _crear_ubicacion(self, inicio: int, fin: int) -> SourceLocation:
        linea_inicio, col_inicio = self._offset_a_linea_col(inicio)
        linea_fin, col_fin = self._offset_a_linea_col(fin)
        return SourceLocation(
            inicio=SourceSpan(linea=linea_inicio, columna=col_inicio, offset=inicio),
            fin=SourceSpan(linea=linea_fin, columna=col_fin, offset=fin),
        )

    def _offset_a_linea_col(self, offset: int) -> tuple[int, int]:
        linea = 1
        columna = 1
        for i, char in enumerate(self._original):
            if i >= offset:
                break
            if char in (LF, CRLF):
                linea += 1
                columna = 1
                if char == CR and self._original[i + 1 : i + 2] == LF:
                    continue
            else:
                columna += 1
        return linea, columna

    def quitar_bom(self) -> Normalizador:
        self._procesado = self._procesado.removeprefix(BOM)
        return self

    def normalizar_saltos_linea(self) -> Normalizador:
        self._procesado = PATRON_CRLF.sub(LF, self._procesado)
        self._procesado = self._procesado.replace(CR, LF)
        return self

    def _es_dentro_string(self, texto: str, posicion: int) -> bool:
        comillas_simple: bool = False
        comillas_doble: bool = False
        escape: bool = False
        for i, char in enumerate(texto):
            if i >= posicion:
                break
            if escape:
                escape = False
                continue
            if char == "\\":
                escape = True
                continue
            if char == "'" and not comillas_doble:
                comillas_simple = not comillas_simple
            elif char == '"' and not comillas_simple:
                comillas_doble = not comillas_doble
        return comillas_simple or comillas_doble

    def _es_dentro_delimitadores(
        self,
        texto: str,
        posicion: int,
        apertura: str,
        cierre: str,
    ) -> bool:
        """Indica si ``posicion`` está dentro de delimitadores aún abiertos.

        Se usa para no confundir el ``//`` de ``[lib://...]`` con un
        comentario. También conserva el comportamiento histórico que protege
        comentarios escritos dentro de paréntesis. No pretende validar toda la
        sintaxis; esa responsabilidad pertenece al lexer y al parser.
        """
        balance = 0
        for indice, caracter in enumerate(texto):
            if indice >= posicion:
                break
            if caracter == apertura:
                balance += 1
            elif caracter == cierre:
                balance = max(0, balance - 1)
        return balance > 0

    def _esta_en_region_protegida(self, texto: str, posicion: int) -> bool:
        """Evita eliminar secuencias que parecen comentarios pero son datos."""
        return (
            self._es_dentro_string(texto, posicion)
            or self._es_dentro_delimitadores(texto, posicion, "[", "]")
            or self._es_dentro_delimitadores(texto, posicion, "(", ")")
        )

    def quitar_comentarios(self) -> Normalizador:
        def reemplazar_comentario_linea(match: re.Match[str]) -> str:
            inicio = match.start()
            fin = match.end()
            if self._esta_en_region_protegida(self._procesado, inicio):
                return match.group(0)
            return " " * (fin - inicio)

        def reemplazar_comentario_bloque(match: re.Match[str]) -> str:
            inicio = match.start()
            fin = match.end()
            if self._esta_en_region_protegida(self._procesado, inicio):
                return match.group(0)
            return " " * (fin - inicio)

        # Qlik usa ``//``; algunos exportadores y fixtures también emiten
        # ``--``. Ambos se reemplazan por espacios para conservar offsets.
        self._procesado = PATRON_COMENTARIO_LINEA.sub(
            reemplazar_comentario_linea, self._procesado
        )
        self._procesado = PATRON_COMENTARIO_SQL.sub(
            reemplazar_comentario_linea, self._procesado
        )
        self._procesado = PATRON_COMENTARIO_BLOQUE.sub(
            reemplazar_comentario_bloque, self._procesado
        )
        return self

    def procesar(self) -> tuple[str, list[ErrorDataflow]]:
        self.quitar_bom()
        self.normalizar_saltos_linea()
        self.quitar_comentarios()
        return self._procesado, self._errores


def normalizar(contenido: str) -> tuple[str, list[ErrorDataflow]]:
    return Normalizador(contenido).procesar()
