"""Backend-neutral, structural compiled-program descriptors."""

from __future__ import annotations

import struct
from dataclasses import dataclass
from hashlib import sha256
from typing import Any

from ..expressions.program import ExprProgram
from ..expressions.row_ir import (
    Binary,
    Call,
    Cast,
    Coalesce,
    Field,
    GetItem,
    IfElse,
    Index,
    InputRow,
    IsNull,
    Path,
    PythonUDF,
    Unary,
)
from ..expressions.row_ir import (
    Literal as RowLiteral,
)
from ..expressions.scalar import Expr, FExpr
from ..expressions.typed_ir import Effect


@dataclass(frozen=True, slots=True)
class ProgramFingerprint:
    """SHA-256 identity of a serializable compiled expression or stage list."""

    value: str

    @classmethod
    def from_expression(cls, expression: ExprProgram) -> ProgramFingerprint:
        """Hash supported IR without callable identity, addresses, or randomized hashes."""
        if expression.effect is Effect.PYTHON_CALLBACK:
            raise ValueError("callback expressions cannot have compiled fingerprints")
        return cls(sha256(_encode(expression.root)).hexdigest())


def _encode(value: Any) -> bytes:
    """Encode only explicit supported IR and exact built-in literals."""
    if isinstance(value, Expr):
        return _encode_integer_instructions(value.native_instructions())
    if isinstance(value, FExpr):
        return _encode_float_instructions(value.native_instructions())
    leaf = _encode_row_leaf(value)
    if leaf is not None:
        return leaf
    compound = _encode_row_compound(value)
    if compound is not None:
        return compound
    if isinstance(value, (PythonUDF, Cast)):
        raise ValueError("callback expressions cannot have compiled fingerprints")
    return _literal(value)


def _encode_integer_instructions(
    instructions: tuple[tuple[int, int], ...],
) -> bytes:
    """Encode integer opcodes and arbitrary-size operands without textual delimiters."""
    return _record(
        b"expr",
        *(
            _record(b"instruction", _encode_integer(opcode), _encode_integer(operand))
            for opcode, operand in instructions
        ),
    )


def _encode_float_instructions(
    instructions: tuple[tuple[int, float], ...],
) -> bytes:
    """Encode float instructions while retaining each IEEE-754 payload bit."""
    return _record(
        b"fexpr",
        *(
            _record(b"instruction", _encode_integer(opcode), struct.pack("!d", operand))
            for opcode, operand in instructions
        ),
    )


def _encode_row_leaf(value: Any) -> bytes | None:
    """Encode row IR leaves, returning None when recursion or literal handling is needed."""
    if isinstance(value, InputRow):
        return _record(b"input")
    if isinstance(value, Field):
        return _record(b"field", value.name.encode("utf-8"))
    if isinstance(value, Path):
        return _record(
            b"path",
            _literal(value.selector),
            *(part.encode("utf-8") for part in value.parts),
        )
    if isinstance(value, Index):
        return _record(b"index", _encode_integer(value.index))
    if isinstance(value, RowLiteral):
        return _record(b"literal", _literal(value.value))
    return None


def _encode_row_compound(value: Any) -> bytes | None:
    """Recursively encode callback-free compound row IR nodes."""
    if isinstance(value, Unary):
        return _record(b"unary", value.kind.encode("utf-8"), _encode(value.operand))
    if isinstance(value, Binary):
        return _record(
            b"binary",
            value.kind.encode("utf-8"),
            _encode(value.left),
            _encode(value.right),
        )
    if isinstance(value, GetItem):
        return _record(b"getitem", _encode(value.value), _encode(value.key))
    if isinstance(value, IsNull):
        return _record(b"isnull", _literal(value.negate), _encode(value.value))
    if isinstance(value, Coalesce):
        return _record(b"coalesce", *(_encode(item) for item in value.values))
    if isinstance(value, Call):
        return _record(
            b"call",
            value.kind.encode("utf-8"),
            *(_encode(item) for item in value.arguments),
        )
    if isinstance(value, IfElse):
        return _record(
            b"if",
            _encode(value.condition),
            _encode(value.yes),
            _encode(value.no),
        )
    return None


def _record(tag: bytes, *parts: bytes) -> bytes:
    """Frame a tagged structural record so arbitrary payload bytes cannot collide."""
    return _frame(tag) + b"".join(_frame(part) for part in parts)


def _frame(value: bytes) -> bytes:
    """Prefix one byte payload with its fixed-width unsigned length."""
    return len(value).to_bytes(8, "big") + value


def _encode_integer(value: int) -> bytes:
    """Encode an arbitrary-size signed integer as sign plus unsigned magnitude."""
    magnitude = abs(value)
    width = max(1, (magnitude.bit_length() + 7) // 8)
    return (b"\x01" if value < 0 else b"\x00") + magnitude.to_bytes(width, "big")


def _literal(value: object) -> bytes:
    """Encode exact immutable built-ins without process-specific representation data."""
    if value is None:
        return _record(b"none")
    if type(value) is bool:
        return _record(b"bool", b"\x01" if value else b"\x00")
    if type(value) is int:
        return _record(b"int", _encode_integer(value))
    if type(value) is float:
        return _record(b"float", struct.pack("!d", value))
    if type(value) is str:
        return _record(b"str", value.encode("utf-8"))
    if type(value) is tuple:
        return _record(b"tuple", *(_literal(item) for item in value))
    raise ValueError(f"literal {type(value).__name__} cannot have a compiled fingerprint")
