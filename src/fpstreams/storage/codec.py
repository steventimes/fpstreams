"""Versioned framed pickle codec for query-temporary spill runs."""

from __future__ import annotations

import pickle
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, BinaryIO, SupportsIndex

_MAGIC = b"FPSTRM\x00\x01"


class SpillFormatError(ValueError):
    """Raised when a spill run is truncated, has an unknown version, or is malformed."""


class SpillSerializationError(TypeError):
    """Raised only when the spill codec cannot serialize a record payload."""


class _CodecSerializationError(SpillSerializationError):
    """Mark an error that was created at the codec boundary itself.

    ``SpillSerializationError`` is public, so a source or callback may legitimately
    raise an instance of it.  Internal recovery catches this private subtype instead
    of guessing from an exception's class or message; user exceptions therefore keep
    their original identity while real pickle failures remain recoverable.
    """


@dataclass(frozen=True, slots=True)
class _SpillBatch:
    """A bounded group of logical records stored in one physical pickle frame."""

    records: tuple[object, ...]

    def __reduce_ex__(self, _protocol: SupportsIndex) -> tuple[object, tuple[object, ...]]:
        """Avoid the reflective state walker generated for frozen slot dataclasses."""
        return type(self), (self.records,)


class SpillCodec:
    """Read and write length-prefixed pickle records with a stable file signature."""

    def write_header(self, handle: BinaryIO) -> None:
        """Write the exact format/version signature before any record frames."""
        handle.write(_MAGIC)

    def read_records(self, handle: BinaryIO) -> Iterator[Any]:
        """Validate framing and lazily decode every complete pickle record."""
        magic = handle.read(len(_MAGIC))
        if magic != _MAGIC:
            if len(magic) < len(_MAGIC):
                raise SpillFormatError("truncated spill header")
            raise SpillFormatError("unsupported spill version")
        while True:
            length_data = handle.read(4)
            if not length_data:
                return
            if len(length_data) != 4:
                raise SpillFormatError("truncated spill frame length")
            length = int.from_bytes(length_data, "big")
            payload = handle.read(length)
            if len(payload) != length:
                raise SpillFormatError("truncated spill frame payload")
            try:
                value = pickle.loads(payload)
            except (pickle.UnpicklingError, EOFError, ValueError, TypeError) as error:
                raise SpillFormatError(f"invalid spill payload: {error}") from error
            if isinstance(value, _SpillBatch):
                # Batching is deliberately invisible above the codec.  Consumers
                # continue to see the same lazy logical record stream, including
                # files containing a mixture of old single frames and new batches.
                yield from value.records
            else:
                yield value

    def write_record(self, handle: BinaryIO, value: object) -> int:
        """Encode one value, write its length-prefixed frame, and return bytes written."""
        frame = self.encode_record(value)
        handle.write(frame)
        return len(frame)

    def write_records(self, handle: BinaryIO, values: tuple[object, ...]) -> int:
        """Encode a non-empty bounded batch as one frame and return bytes written."""
        if not values:
            return 0
        frame = self.encode_record(_SpillBatch(values))
        handle.write(frame)
        return len(frame)

    def encode_record(self, value: object) -> bytes:
        """Serialize one complete frame without opening or retaining a file handle.

        Partition writers use this boundary to surface pickle errors at the input
        row that caused them while buffering only immutable bytes for later,
        descriptor-bounded flushes.
        """
        payload = self._encode_payload(value)
        return len(payload).to_bytes(4, "big") + payload

    def encoded_size(self, value: object) -> int:
        """Return the exact framed size after performing the real pickle encoding."""
        return 4 + len(self._encode_payload(value))

    @staticmethod
    def _encode_payload(value: object) -> bytes:
        """Encode one pickle payload with the shared spill error normalization."""
        try:
            return pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        except (pickle.PicklingError, AttributeError, TypeError, ValueError) as error:
            raise _CodecSerializationError(f"spill record is not serializable: {error}") from error
