from typing import TypeVar, Protocol, Any

T = TypeVar("T")
R = TypeVar("R")

class SupportsRichComparison(Protocol):
    def __lt__(self, other: Any) -> bool: ...