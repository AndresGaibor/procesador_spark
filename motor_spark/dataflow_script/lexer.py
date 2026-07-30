from __future__ import annotations

import re

from motor_spark.dataflow_script.errores import (
    ErrorDataflow,
    SourceLocation,
    SourceSpan,
)
from motor_spark.dataflow_script.limites import (
    LIMITE_CANTIDAD_TOKENES,
    LIMITE_LONGITUD_IDENTIFICADOR,
    LIMITE_LONGITUD_LINEA,
)

RESERVADAS: frozenset[str] = frozenset(
    {
        "SET",
        "LIB",
        "CONNECT",
        "TO",
        "SELECT",
        "FROM",
        "WHERE",
        "LEFT",
        "JOIN",
        "LOAD",
        "RESIDENT",
        "DROP",
        "TABLE",
        "STORE",
        "INTO",
        "CONCATENATE",
        "NOCONCATENATE",
        "AND",
        "OR",
        "NOT",
        "AS",
        "IN",
        "LIKE",
        "IS",
        "NULL",
        "ON",
        "TRUE",
        "FALSE",
        "CONCAT",
        "GROUP",
        "BY",
        "DISTINCT",
        "WINDOW",
        "WRANK",
    }
)

SIMBOLOS_SIMPLE: frozenset[str] = frozenset(
    {
        "(",
        ")",
        "[",
        "]",
        ",",
        ";",
        "=",
        "<",
        ">",
        "+",
        "-",
        "*",
        "/",
        ":",
        ".",
    }
)

PATRON_SIMBOLO_MULTICAR: re.Pattern[str] = re.compile(r"(<=|>=|<>|=|<|>)")
PATRON_IDENTIFICADOR: re.Pattern[str] = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
PATRON_NUMERO: re.Pattern[str] = re.compile(r"\d+(\.\d+)?")
PATRON_STRING_DOBLE: re.Pattern[str] = re.compile(r'"(?:[^"\\]|\\.)*"')
PATRON_STRING_SIMPLE: re.Pattern[str] = re.compile(r"'(?:[^'\\]|\\.)*'")

PATRON_LIB_URI: re.Pattern[str] = re.compile(r"\[lib://[^\]]+\]")
PATRON_LIB_URI_SIN_CORCHETES: re.Pattern[str] = re.compile(r"lib://[^\];)]+")


class LexerError(Exception):
    def __init__(self, mensaje: str, ubicacion: SourceLocation | None = None) -> None:
        self.mensaje = mensaje
        self.ubicacion = ubicacion
        super().__init__(mensaje)


class Token:
    def __init__(
        self,
        tipo: str,
        valor: str,
        linea: int,
        columna: int,
        offset: int,
    ) -> None:
        self.tipo = tipo
        self.valor = valor
        self.linea = linea
        self.columna = columna
        self.offset = offset

    def __repr__(self) -> str:
        return f"Token({self.tipo!r}, {self.valor!r}, {self.linea}:{self.columna})"


class Lexer:
    def __init__(self, contenido: str) -> None:
        self._contenido: str = contenido
        self._offset: int = 0
        self._linea: int = 1
        self._columna: int = 1
        self._tokens: list[Token] = []
        self._errores: list[ErrorDataflow] = []

    def _crear_ubicacion(self, offset: int) -> SourceLocation:
        linea, columna = self._offset_a_linea_col(offset)
        return SourceLocation(
            inicio=SourceSpan(linea=linea, columna=columna, offset=offset),
            fin=SourceSpan(linea=linea, columna=columna + 1, offset=offset + 1),
        )

    def _offset_a_linea_col(self, offset: int) -> tuple[int, int]:
        linea = 1
        columna = 1
        for i, char in enumerate(self._contenido):
            if i >= offset:
                break
            if char == "\n":
                linea += 1
                columna = 1
            else:
                columna += 1
        return linea, columna

    def _avanzar(self, cantidad: int = 1) -> None:
        for _ in range(cantidad):
            if self._offset < len(self._contenido):
                char = self._contenido[self._offset]
                if char == "\n":
                    self._linea += 1
                    self._columna = 1
                else:
                    self._columna += 1
                self._offset += 1

    def _quedar(self) -> str:
        if self._offset >= len(self._contenido):
            return ""
        return self._contenido[self._offset]

    def _quedar_multi(self, longitud: int) -> str:
        fin = min(self._offset + longitud, len(self._contenido))
        return self._contenido[self._offset : fin]

    def _es_fin(self) -> bool:
        return self._offset >= len(self._contenido)

    def _saltar_espacios(self) -> None:
        while not self._es_fin():
            char = self._quedar()
            if char in " \t" or char == "\n":
                self._avanzar()
            else:
                break

    def _error_fatal(self, mensaje: str, codigo: str = "DFS_SYNTAX_GENERIC") -> None:
        ubicacion = self._crear_ubicacion(self._offset)
        self._errores.append(
            ErrorDataflow(
                mensaje=f"Lexico [{codigo}]: {mensaje}",
                ubicacion=ubicacion,
                codigo=codigo,
                ayuda="Verificar sintaxis del script dataflow",
            )
        )

    def _token_simbolo(self) -> Token:
        inicio_offset = self._offset
        inicio_linea = self._linea
        inicio_columna = self._columna

        restante = self._quedar_multi(2)
        match = PATRON_SIMBOLO_MULTICAR.match(restante)
        if match and len(match.group(0)) == 2:
            self._avanzar(2)
            valor = match.group(0)
        else:
            valor = self._quedar()
            self._avanzar(1)

        return Token("SIMBOLO", valor, inicio_linea, inicio_columna, inicio_offset)

    def _tokenBracketId(self) -> Token:
        inicio_offset = self._offset
        inicio_linea = self._linea
        inicio_columna = self._columna

        self._avanzar(1)

        if self._quedar_multi(6) == "lib://":
            self._avanzar(6)
            uri_parts = ["lib://"]
            while not self._es_fin():
                char = self._quedar()
                if char == "]":
                    self._avanzar(1)
                    break
                if char == "\n":
                    self._error_fatal(
                        "URI lib no cerrada", "DFS_SYNTAX_LIB_URI_UNCLOSED"
                    )
                    break
                uri_parts.append(char)
                self._avanzar(1)
            valor = "".join(uri_parts)
            if len(valor) > LIMITE_LONGITUD_IDENTIFICADOR:
                self._error_fatal(
                    f"URI demasiado larga: {valor}", "DFS_SYNTAX_URI_TOO_LONG"
                )
            return Token("LIB_URI", valor, inicio_linea, inicio_columna, inicio_offset)

        valor_parts: list[str] = []
        while not self._es_fin():
            char = self._quedar()
            if char == "]":
                self._avanzar(1)
                break
            if char == "\n":
                self._error_fatal(
                    "Bracket identifier no cerrado", "DFS_SYNTAX_BRACKET_UNCLOSED"
                )
                break
            valor_parts.append(char)
            self._avanzar(1)

        valor = "".join(valor_parts)

        if len(valor) > LIMITE_LONGITUD_IDENTIFICADOR:
            self._error_fatal(
                f"Identificador demasiado largo: {valor}", "DFS_SYNTAX_ID_TOO_LONG"
            )

        return Token("BRACKET_ID", valor, inicio_linea, inicio_columna, inicio_offset)

    def _token_lib_uri(self) -> Token:
        inicio_offset = self._offset
        inicio_linea = self._linea
        inicio_columna = self._columna

        match = PATRON_LIB_URI.match(self._contenido[self._offset :])
        if match:
            valor = match.group(0)
            self._avanzar(len(valor))
            return Token(
                "LIB_URI", valor[1:-1], inicio_linea, inicio_columna, inicio_offset
            )

        match_simple = PATRON_LIB_URI_SIN_CORCHETES.match(
            self._contenido[self._offset :]
        )
        if match_simple:
            valor = match_simple.group(0)
            self._avanzar(len(valor))
            return Token("LIB_URI", valor, inicio_linea, inicio_columna, inicio_offset)

        self._error_fatal("URI lib no valido", "DFS_SYNTAX_LIB_URI_INVALID")
        self._avanzar(1)
        return Token(
            "DESCONOCIDO",
            self._quedar_multi(1),
            inicio_linea,
            inicio_columna,
            inicio_offset,
        )

    def _token_identificador(self) -> Token:
        inicio_offset = self._offset
        inicio_linea = self._linea
        inicio_columna = self._columna

        match = PATRON_IDENTIFICADOR.match(self._contenido[self._offset :])
        if not match:
            self._error_fatal(
                f"Identificador inesperado en posicion {self._offset}",
                "DFS_SYNTAX_INVALID_ID",
            )
            self._avanzar(1)
            return Token(
                "DESCONOCIDO",
                self._quedar_multi(1),
                inicio_linea,
                inicio_columna,
                inicio_offset,
            )

        valor = match.group(0)
        self._avanzar(len(valor))

        if len(valor) > LIMITE_LONGITUD_IDENTIFICADOR:
            self._error_fatal(
                f"Identificador demasiado largo: {valor}", "DFS_SYNTAX_ID_TOO_LONG"
            )

        valor_upper = valor.upper()
        if valor_upper in RESERVADAS:
            tipo = "PALABRA_RESERVADA"
            if valor_upper == "GROUP" and self._quedar() == " ":
                pass
            return Token(tipo, valor_upper, inicio_linea, inicio_columna, inicio_offset)
        else:
            tipo = "IDENTIFICADOR"

        return Token(tipo, valor, inicio_linea, inicio_columna, inicio_offset)

    def _token_numero(self) -> Token:
        inicio_offset = self._offset
        inicio_linea = self._linea
        inicio_columna = self._columna

        match = PATRON_NUMERO.match(self._contenido[self._offset :])
        if not match:
            self._error_fatal(
                f"Numero inesperado en posicion {self._offset}",
                "DFS_SYNTAX_INVALID_NUMBER",
            )
            self._avanzar(1)
            return Token(
                "DESCONOCIDO",
                self._quedar_multi(1),
                inicio_linea,
                inicio_columna,
                inicio_offset,
            )

        valor = match.group(0)
        self._avanzar(len(valor))
        return Token("NUMERO", valor, inicio_linea, inicio_columna, inicio_offset)

    def _token_string(self) -> Token:
        inicio_offset = self._offset
        inicio_linea = self._linea
        inicio_columna = self._columna

        quote = self._quedar()
        patron = PATRON_STRING_DOBLE if quote == '"' else PATRON_STRING_SIMPLE
        match = patron.match(self._contenido[self._offset :])

        if not match:
            self._error_fatal(
                f"String mal formado en posicion {self._offset}",
                "DFS_SYNTAX_INVALID_STRING",
            )
            self._avanzar(1)
            return Token(
                "DESCONOCIDO", quote, inicio_linea, inicio_columna, inicio_offset
            )

        valor = match.group(0)
        self._avanzar(len(valor))
        return Token("STRING", valor, inicio_linea, inicio_columna, inicio_offset)

    def tokenizar(self) -> tuple[list[Token], list[ErrorDataflow]]:
        cantidad_tokens = 0

        while not self._es_fin():
            self._saltar_espacios()

            if self._es_fin():
                break

            if cantidad_tokens > LIMITE_CANTIDAD_TOKENES:
                self._error_fatal(
                    f"Demasiados tokens, limite: {LIMITE_CANTIDAD_TOKENES}",
                    "DFS_SYNTAX_TOO_MANY_TOKENS",
                )
                break

            linea = self._linea
            if linea > LIMITE_LONGITUD_LINEA:
                self._error_fatal(
                    f"Linea demasiado larga, limite: {LIMITE_LONGITUD_LINEA}",
                    "DFS_SYNTAX_LINE_TOO_LONG",
                )
                break

            char = self._quedar()

            if self._quedar_multi(7).startswith("lib://"):
                self._tokens.append(self._token_lib_uri())
            elif char == "[":
                self._tokens.append(self._tokenBracketId())
            elif char in SIMBOLOS_SIMPLE or PATRON_SIMBOLO_MULTICAR.match(
                self._quedar_multi(2)
            ):
                self._tokens.append(self._token_simbolo())
            elif char.isdigit():
                self._tokens.append(self._token_numero())
            elif char in "\"'":
                self._tokens.append(self._token_string())
            elif char.isalpha() or char == "_":
                self._tokens.append(self._token_identificador())
            else:
                self._error_fatal(
                    f"Caracter desconocido: {char!r}", "DFS_SYNTAX_UNKNOWN_CHAR"
                )
                self._avanzar(1)
                cantidad_tokens += 1
                continue

            cantidad_tokens += 1

        self._tokens.append(Token("FIN", "", self._linea, self._columna, self._offset))
        return self._tokens, self._errores


def tokenizar(contenido: str) -> tuple[list[Token], list[ErrorDataflow]]:
    return Lexer(contenido).tokenizar()
