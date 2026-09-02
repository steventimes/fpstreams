"""Lazy CSV and JSON Lines sources with explicit handle ownership."""

from __future__ import annotations

import codecs
import csv
import json
import os
import sys
from collections.abc import Callable, Iterator
from typing import Any, BinaryIO, TextIO, TypeAlias, cast

from ..errors import BufferLimitError, DuplicateKeyError, SelectionError
from ..io_safety import validate_max_record_bytes
from ..planning.semantics import facts_from_capabilities
from ..planning.source import Source, SourceCapabilities
from ..runtime.iterators import closing_iterators
from ..streams.flow import Flow, flow
from .records import _require_unique_names

CSVHandle: TypeAlias = TextIO
CSVOpener: TypeAlias = Callable[[], CSVHandle]
CSVInput: TypeAlias = str | os.PathLike[str] | CSVHandle | CSVOpener

JSONLHandle: TypeAlias = TextIO | BinaryIO
JSONLOpener: TypeAlias = Callable[[], JSONLHandle]
JSONLInput: TypeAlias = str | os.PathLike[str] | JSONLHandle | JSONLOpener

_WIDE_JSONL_CODECS: dict[str, tuple[int, str | None]] = {
    "utf-16": (2, None),
    "utf-16-le": (2, "little"),
    "utf-16-be": (2, "big"),
    "utf-32": (4, None),
    "utf-32-le": (4, "little"),
    "utf-32-be": (4, "big"),
}


def _csv_records(
    handle: CSVHandle,
    format_parameters: dict[str, Any],
) -> Iterator[dict[str, Any]]:
    """Read one caller-positioned text handle without taking ownership of it."""
    yield from _csv_reader(handle, format_parameters)


def _csv_reader(
    handle: CSVHandle,
    format_parameters: dict[str, Any],
) -> csv.DictReader[str]:
    """Build the shared parser only after validating its materialized header."""
    reader = csv.DictReader(handle, **format_parameters)
    _require_unique_names(reader.fieldnames or (), operation="CSV header")
    return reader


class CSVRowSource:
    """Retain one project-owned CSV adapter beside its canonical lazy opener."""

    __slots__ = ("encoding", "format_parameters", "kind", "source")

    def __init__(
        self,
        source: CSVInput,
        *,
        encoding: str,
        format_parameters: dict[str, Any],
    ) -> None:
        self.source = source
        self.encoding = encoding
        self.format_parameters = format_parameters
        self.kind = (
            "path"
            if isinstance(source, (str, os.PathLike))
            else "opener"
            if callable(source)
            else "handle"
        )

    def open_records(self) -> Iterator[dict[str, Any]]:
        """Open one lazy iterator with the adapter's established ownership rules."""
        source = self.source
        if self.kind == "path":
            path_handle = open(  # noqa: SIM115 - cleanup must preserve parser failures
                cast("str | os.PathLike[str]", source),
                encoding=self.encoding,
                newline="",
            )
            with closing_iterators((path_handle,)):
                yield from _csv_records(path_handle, self.format_parameters)
            return
        if self.kind == "opener":
            opened_handle = cast("CSVOpener", source)()
            with closing_iterators((opened_handle,)):
                yield from _csv_records(opened_handle, self.format_parameters)
            return
        yield from _csv_records(cast("CSVHandle", source), self.format_parameters)

    def materialize(self) -> list[dict[str, Any]]:
        """Collect through the same parser and header guard without per-row forwarding."""
        source = self.source
        try:
            if self.kind == "path":
                path_handle = open(  # noqa: SIM115 - cleanup must preserve parser failures
                    cast("str | os.PathLike[str]", source),
                    encoding=self.encoding,
                    newline="",
                )
                with closing_iterators((path_handle,)):
                    return list(_csv_reader(path_handle, self.format_parameters))
            if self.kind == "opener":
                opened_handle = cast("CSVOpener", source)()
                with closing_iterators((opened_handle,)):
                    return list(_csv_reader(opened_handle, self.format_parameters))
            return list(_csv_reader(cast("CSVHandle", source), self.format_parameters))
        except StopIteration as error:
            # The ordinary adapter opens inside a generator, where PEP 479 translates an
            # opener's escaped StopIteration.  Keep that observable contract on this sink.
            raise RuntimeError("generator raised StopIteration") from error


def csv_flow(
    source: CSVInput,
    *,
    encoding: str,
    format_parameters: dict[str, Any],
) -> Flow[dict[str, Any]]:
    """Build a path-, handle-, or opener-backed CSV flow with truthful replayability."""
    descriptor = CSVRowSource(
        source,
        encoding=encoding,
        format_parameters=format_parameters,
    )
    reiterable = descriptor.kind != "handle"
    return Flow(
        Source(
            descriptor.open_records,
            SourceCapabilities(reiterable=reiterable, exact_size=None, ordered=True),
            native_data=descriptor,
            facts=facts_from_capabilities(
                reiterable=reiterable,
                exact_size=None,
                ordered=True,
                reopenable=reiterable,
            ),
        )
    )


def _wide_binary_jsonl_lines(  # noqa: C901 - byte-aligned framing is one state machine
    handle: BinaryIO,
    initial: bytes,
    *,
    codec: str,
    unit_width: int,
    explicit_byte_order: str | None,
    record_limit: int | None,
) -> Iterator[tuple[int, str]]:
    """Split UTF-16/32 binary records without mistaking one byte for a newline."""
    line_number = 1
    buffer = bytearray(initial)

    def check_limit() -> None:
        size = len(buffer)
        if record_limit is not None and size > record_limit:
            raise BufferLimitError(
                f"JSON Lines record {line_number} bytes {size} exceed "
                f"max_record_bytes={record_limit}"
            )

    check_limit()
    while len(buffer) < unit_width:
        read_size = unit_width - len(buffer)
        if record_limit is not None:
            read_size = min(read_size, record_limit + 1 - len(buffer))
        chunk = handle.read(read_size)
        if not chunk:
            yield line_number, bytes(buffer).decode(codec)
            return
        buffer.extend(chunk)
        check_limit()

    strip_initial_bom = False
    byte_order = explicit_byte_order
    if byte_order is None:
        prefix = bytes(buffer[:unit_width])
        little_bom = codecs.BOM_UTF16_LE if unit_width == 2 else codecs.BOM_UTF32_LE
        big_bom = codecs.BOM_UTF16_BE if unit_width == 2 else codecs.BOM_UTF32_BE
        if prefix == little_bom:
            byte_order = "little"
            strip_initial_bom = True
        elif prefix == big_bom:
            byte_order = "big"
            strip_initial_bom = True
        else:
            byte_order = sys.byteorder

    suffix = "le" if byte_order == "little" else "be"
    decode_codec = f"utf-{unit_width * 8}-{suffix}"
    delimiter = "\n".encode(decode_codec)
    search_from = 0

    while True:
        candidate = buffer.find(b"\n", search_from)
        if candidate < 0:
            read_size = -1
            if record_limit is not None:
                read_size = record_limit + 1 - len(buffer)
            chunk = handle.readline(read_size)
            if not chunk:
                decoded_line = bytes(buffer).decode(decode_codec)
                if strip_initial_bom:
                    decoded_line = decoded_line[1:]
                yield line_number, decoded_line
                return
            buffer.extend(chunk)
            check_limit()
            continue

        unit_start = candidate - (candidate % unit_width)
        unit_end = unit_start + unit_width
        if unit_end > len(buffer):
            read_size = unit_end - len(buffer)
            if record_limit is not None:
                read_size = min(read_size, record_limit + 1 - len(buffer))
            chunk = handle.read(read_size)
            if chunk:
                buffer.extend(chunk)
                check_limit()
            if unit_end > len(buffer):
                decoded_line = bytes(buffer).decode(decode_codec)
                if strip_initial_bom:
                    decoded_line = decoded_line[1:]
                yield line_number, decoded_line
                return

        if bytes(buffer[unit_start:unit_end]) != delimiter:
            search_from = candidate + 1
            continue

        encoded_line = bytes(buffer[:unit_end])
        del buffer[:unit_end]
        decoded_line = encoded_line.decode(decode_codec)
        if strip_initial_bom:
            decoded_line = decoded_line[1:]
            strip_initial_bom = False
        yield line_number, decoded_line
        line_number += 1
        search_from = 0
        if buffer:
            check_limit()
            continue

        read_size = -1 if record_limit is None else record_limit + 1
        chunk = handle.readline(read_size)
        if not chunk:
            return
        buffer.extend(chunk)
        check_limit()


def _jsonl_line_decoder(decoder: json.JSONDecoder) -> Callable[[str], Any]:
    """Preserve ``JSONDecoder.decode`` semantics while skipping common whitespace scans."""
    decode = decoder.decode
    raw_decode = decoder.raw_decode

    def decode_line(line: str) -> Any:
        if type(line) is not str or not line or line[0] != "{":
            return decode(line)

        value, end = raw_decode(line)
        length = len(line)
        if end == length or (end + 1 == length and line[end] == "\n"):
            return value
        if end + 2 == length and line[end] == "\r" and line[end + 1] == "\n":
            return value
        while end < length and line[end] in " \t\r\n":
            end += 1
        if end != length:
            raise json.JSONDecodeError("Extra data", line, end)
        return value

    return decode_line


def _jsonl_records(  # noqa: C901 - keep line-numbered decode checks in one hot loop
    handle: JSONLHandle,
    *,
    encoding: str,
    record_limit: int | None,
) -> Iterator[dict[str, Any]]:
    """Decode bounded JSON objects from one caller-positioned text or binary handle."""
    line_number = 0

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for name, item in pairs:
            if name in value:
                raise DuplicateKeyError(
                    f"JSON Lines record {line_number} contains duplicate key {name!r}"
                )
            value[name] = item
        return value

    decode_line = _jsonl_line_decoder(json.JSONDecoder(object_pairs_hook=unique_object))

    wide_codec = codecs.lookup(encoding).name
    wide_details = _WIDE_JSONL_CODECS.get(wide_codec)
    if wide_details is not None:
        read_size = -1 if record_limit is None else record_limit + 1
        wide_physical_line = handle.readline(read_size)
        if not wide_physical_line:
            return
        if isinstance(wide_physical_line, bytes):
            unit_width, explicit_byte_order = wide_details
            for line_number, wide_decoded_line in _wide_binary_jsonl_lines(
                cast("BinaryIO", handle),
                wide_physical_line,
                codec=wide_codec,
                unit_width=unit_width,
                explicit_byte_order=explicit_byte_order,
                record_limit=record_limit,
            ):
                if not wide_decoded_line or wide_decoded_line.isspace():
                    continue
                value = decode_line(wide_decoded_line)
                if type(value) is not dict:
                    raise SelectionError(f"JSON Lines record {line_number} is not an object")
                yield value
            return

        while wide_physical_line:
            line_number += 1
            wide_decoded_line = cast("str", wide_physical_line)
            if record_limit is not None:
                encoded = wide_decoded_line.encode(encoding)
                if len(encoded) > record_limit:
                    raise BufferLimitError(
                        f"JSON Lines record {line_number} bytes {len(encoded)} exceed "
                        f"max_record_bytes={record_limit}"
                    )
            if wide_decoded_line and not wide_decoded_line.isspace():
                value = decode_line(wide_decoded_line)
                if type(value) is not dict:
                    raise SelectionError(f"JSON Lines record {line_number} is not an object")
                yield value
            wide_physical_line = handle.readline(read_size)
        return

    if record_limit is None:
        for line_number, physical_line in enumerate(handle, start=1):
            if isinstance(physical_line, bytes):
                line = physical_line.decode(encoding)
            else:
                line = cast("str", physical_line)
            if not line or line.isspace():
                continue
            value = decode_line(line)
            if type(value) is not dict:
                raise SelectionError(f"JSON Lines record {line_number} is not an object")
            yield value
        return

    read_size = record_limit + 1
    while True:
        physical_line = handle.readline(read_size)
        if not physical_line:
            return
        line_number += 1
        decoded_line: str | None
        if isinstance(physical_line, bytes):
            encoded = physical_line
            decoded_line = None
        else:
            decoded_line = physical_line
            encoded = decoded_line.encode(encoding)
        if len(encoded) > record_limit:
            raise BufferLimitError(
                f"JSON Lines record {line_number} bytes {len(encoded)} exceed "
                f"max_record_bytes={record_limit}"
            )
        if decoded_line is None:
            decoded_line = encoded.decode(encoding)
        if not decoded_line or decoded_line.isspace():
            continue

        value = decode_line(decoded_line)
        if type(value) is not dict:
            raise SelectionError(f"JSON Lines record {line_number} is not an object")
        yield value


def jsonl_flow(
    source: JSONLInput,
    *,
    encoding: str,
    max_record_bytes: int | None,
) -> Flow[dict[str, Any]]:
    """Build a path-, handle-, or opener-backed JSON Lines flow."""
    record_limit = validate_max_record_bytes(max_record_bytes)
    if isinstance(source, (str, os.PathLike)):

        def path_records() -> Iterator[dict[str, Any]]:
            if record_limit is None:
                text_handle = open(  # noqa: SIM115 - cleanup must preserve parser failures
                    source, encoding=encoding
                )
                with closing_iterators((text_handle,)):
                    yield from _jsonl_records(
                        text_handle,
                        encoding=encoding,
                        record_limit=record_limit,
                    )
                return
            binary_handle = open(  # noqa: SIM115 - cleanup must preserve parser failures
                source, "rb"
            )
            with closing_iterators((binary_handle,)):
                yield from _jsonl_records(
                    binary_handle,
                    encoding=encoding,
                    record_limit=record_limit,
                )

        return flow.defer(path_records)

    if callable(source):
        opener = source

        def reopened_records() -> Iterator[dict[str, Any]]:
            handle = opener()
            with closing_iterators((handle,)):
                yield from _jsonl_records(
                    handle,
                    encoding=encoding,
                    record_limit=record_limit,
                )

        return flow.defer(reopened_records)

    return flow(
        _jsonl_records(
            source,
            encoding=encoding,
            record_limit=record_limit,
        )
    )
