from typing import TypeAlias

Instruction: TypeAlias = tuple[int, int]
Stage: TypeAlias = tuple[int, tuple[Instruction, ...]]
FloatInstruction: TypeAlias = tuple[int, float]
FloatStage: TypeAlias = tuple[int, tuple[FloatInstruction, ...]]

def version() -> str: ...
def execute_i64(
    values: list[int] | tuple[int, ...],
    program: list[Stage],
) -> list[int]: ...
def execute_i64_range(
    start: int,
    stop: int,
    step: int,
    program: list[Stage],
) -> list[int]: ...
def terminal_i64(
    values: list[int] | tuple[int, ...],
    program: list[Stage],
    terminal: int,
) -> int | None: ...
def terminal_i64_range(
    start: int,
    stop: int,
    step: int,
    program: list[Stage],
    terminal: int,
) -> int | None: ...
def statistics_i64(
    values: list[int] | tuple[int, ...],
    program: list[Stage],
) -> tuple[int, float, float]: ...
def statistics_i64_range(
    start: int,
    stop: int,
    step: int,
    program: list[Stage],
) -> tuple[int, float, float]: ...
def aggregate_i64(
    values: list[int] | tuple[int, ...],
    program: list[Stage],
) -> tuple[int, int, int | None, int | None, int | None, int | None, float, float]: ...
def aggregate_i64_range(
    start: int,
    stop: int,
    step: int,
    program: list[Stage],
) -> tuple[int, int, int | None, int | None, int | None, int | None, float, float]: ...
def execute_f64(
    values: list[float] | tuple[float, ...],
    program: list[FloatStage],
) -> list[float]: ...
def execute_f64_range(
    start: int,
    stop: int,
    step: int,
    program: list[FloatStage],
) -> list[float]: ...
def terminal_f64(
    values: list[float] | tuple[float, ...],
    program: list[FloatStage],
    terminal: int,
) -> float | None: ...
def terminal_f64_range(
    start: int,
    stop: int,
    step: int,
    program: list[FloatStage],
    terminal: int,
) -> float | None: ...
def statistics_f64(
    values: list[float] | tuple[float, ...],
    program: list[FloatStage],
) -> tuple[int, float, float]: ...
def statistics_f64_range(
    start: int,
    stop: int,
    step: int,
    program: list[FloatStage],
) -> tuple[int, float, float]: ...
def aggregate_f64(
    values: list[float] | tuple[float, ...],
    program: list[FloatStage],
) -> tuple[int, float, float | None, float | None, float | None, float | None, float, float]: ...
def aggregate_f64_range(
    start: int,
    stop: int,
    step: int,
    program: list[FloatStage],
) -> tuple[int, float, float | None, float | None, float | None, float | None, float, float]: ...
def count_f64(
    values: list[float] | tuple[float, ...],
    program: list[FloatStage],
) -> int: ...
def count_f64_range(
    start: int,
    stop: int,
    step: int,
    program: list[FloatStage],
) -> int: ...
