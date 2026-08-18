"""Export immutable `Option` values and explicit `Ok`/`Err` results."""

from .option import Option
from .result import Err, Ok, Result

__all__ = ["Err", "Ok", "Option", "Result"]
