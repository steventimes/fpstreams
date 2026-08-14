from typing import TypeAlias

__all__ = [
    "aggregate_f64",
    "aggregate_f64_range",
    "aggregate_i64",
    "aggregate_i64_range",
    "count_f64",
    "count_f64_range",
    "execute_f64",
    "execute_f64_range",
    "execute_i64",
    "execute_i64_range",
    "statistics_f64",
    "statistics_f64_range",
    "statistics_i64",
    "statistics_i64_range",
    "terminal_f64",
    "terminal_f64_range",
    "terminal_i64",
    "terminal_i64_range",
    "version",
]

_Instruction: TypeAlias = tuple[int, int]
_Stage: TypeAlias = tuple[int, tuple[_Instruction, ...]]
_FloatInstruction: TypeAlias = tuple[int, float]
_FloatStage: TypeAlias = tuple[int, tuple[_FloatInstruction, ...]]

def version() -> str: ...
def build_profile() -> str: ...
def execute_i64(
    values: list[int] | tuple[int, ...],
    program: list[_Stage],
) -> list[int]: ...
def execute_i64_range(
    start: int,
    stop: int,
    step: int,
    program: list[_Stage],
) -> list[int]: ...
def terminal_i64(
    values: list[int] | tuple[int, ...],
    program: list[_Stage],
    terminal: int,
) -> int | None: ...
def terminal_i64_range(
    start: int,
    stop: int,
    step: int,
    program: list[_Stage],
    terminal: int,
) -> int | None: ...
def statistics_i64(
    values: list[int] | tuple[int, ...],
    program: list[_Stage],
) -> tuple[int, float, float]: ...
def statistics_i64_range(
    start: int,
    stop: int,
    step: int,
    program: list[_Stage],
) -> tuple[int, float, float]: ...
def aggregate_i64(
    values: list[int] | tuple[int, ...],
    program: list[_Stage],
) -> tuple[int, int, int | None, int | None, int | None, int | None, float, float]: ...
def aggregate_i64_range(
    start: int,
    stop: int,
    step: int,
    program: list[_Stage],
) -> tuple[int, int, int | None, int | None, int | None, int | None, float, float]: ...
def execute_f64(
    values: list[float] | tuple[float, ...],
    program: list[_FloatStage],
) -> list[float]: ...
def execute_f64_range(
    start: int,
    stop: int,
    step: int,
    program: list[_FloatStage],
) -> list[float]: ...
def terminal_f64(
    values: list[float] | tuple[float, ...],
    program: list[_FloatStage],
    terminal: int,
) -> float | None: ...
def terminal_f64_range(
    start: int,
    stop: int,
    step: int,
    program: list[_FloatStage],
    terminal: int,
) -> float | None: ...
def statistics_f64(
    values: list[float] | tuple[float, ...],
    program: list[_FloatStage],
) -> tuple[int, float, float]: ...
def statistics_f64_range(
    start: int,
    stop: int,
    step: int,
    program: list[_FloatStage],
) -> tuple[int, float, float]: ...
def aggregate_f64(
    values: list[float] | tuple[float, ...],
    program: list[_FloatStage],
) -> tuple[int, float, float | None, float | None, float | None, float | None, float, float]: ...
def aggregate_f64_range(
    start: int,
    stop: int,
    step: int,
    program: list[_FloatStage],
) -> tuple[int, float, float | None, float | None, float | None, float | None, float, float]: ...
def count_f64(
    values: list[float] | tuple[float, ...],
    program: list[_FloatStage],
) -> int: ...
def count_f64_range(
    start: int,
    stop: int,
    step: int,
    program: list[_FloatStage],
) -> int: ...
