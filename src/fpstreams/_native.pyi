from collections.abc import Callable
from typing import TypeAlias

__all__ = [
    "aggregate_f64",
    "aggregate_f64_masked",
    "aggregate_f64_range",
    "aggregate_f64_range_masked",
    "aggregate_i64",
    "aggregate_i64_masked",
    "aggregate_i64_range",
    "aggregate_i64_range_masked",
    "all_exact_dict_rows_v1",
    "count_f64",
    "count_f64_range",
    "direct_dict_field_key_v1",
    "exact_container_extraction_v1",
    "execute_f64",
    "execute_f64_range",
    "execute_i64",
    "execute_i64_range",
    "group_count_sum_callable_key_dict_rows_v1",
    "group_count_sum_callable_value_dict_rows_v1",
    "group_fixed_i64_dict_rows_v1",
    "group_fixed_i64_rows_v1",
    "group_sum_i64_dict_rows",
    "group_sum_i64_dict_rows_v1",
    "group_sum_i64_exact_pairs_v1",
    "group_sum_i64_pairs",
    "group_sum_i64_rows_v1",
    "join_hashable_many_direct_records_v1",
    "join_hashable_many_records_v1",
    "join_hashable_many_records_v2",
    "join_hashable_unique_direct_records_v1",
    "join_hashable_unique_records_v1",
    "join_hashable_unique_records_v2",
    "join_i64_many_dict_rows_v1",
    "join_i64_unique_dict_rows_v1",
    "materialize_f64",
    "materialize_f64_range",
    "materialize_i64",
    "materialize_i64_range",
    "record_group_sum_max_fields",
    "record_join_v1_max_fields",
    "standard_namedtuple_record_adapter_v1",
    "statistics_f64",
    "statistics_f64_range",
    "statistics_i64",
    "statistics_i64_range",
    "terminal_f64",
    "terminal_f64_probe",
    "terminal_f64_range",
    "terminal_i64",
    "terminal_i64_probe",
    "terminal_i64_range",
    "version",
]

_Instruction: TypeAlias = tuple[int, int]
_Stage: TypeAlias = tuple[int, tuple[_Instruction, ...]]
_FloatInstruction: TypeAlias = tuple[int, float]
_FloatStage: TypeAlias = tuple[int, tuple[_FloatInstruction, ...]]
record_group_sum_max_fields: int
record_join_v1_max_fields: int

def version() -> str: ...
def build_profile() -> str: ...
def all_exact_dict_rows_v1(source: object) -> bool: ...
def direct_dict_field_key_v1(
    field: str, selection_error_type: type[BaseException]
) -> Callable[[object], object]: ...
def exact_container_extraction_v1() -> bool: ...
def group_count_sum_callable_key_dict_rows_v1(
    source: object,
    key_selector: Callable[[object], object],
    value_field: str,
    value_accessor: Callable[[object], object],
) -> list[tuple[object, int, object]] | None: ...
def group_count_sum_callable_value_dict_rows_v1(
    source: object,
    key_field: str,
    key_accessor: Callable[[object], object],
    value_selector: Callable[[object], object],
) -> list[tuple[object, int, object]] | None: ...
def group_fixed_i64_rows_v1(
    source: object,
    key_index: int,
    value_index_or_none: int | None,
    key_name: str,
    count_name: str,
    sum_name_or_none: str | None,
) -> (
    tuple[
        bool,
        list[tuple[object, int]] | list[tuple[object, int, int]] | list[dict[str, object]],
    ]
    | None
): ...
def group_fixed_i64_dict_rows_v1(
    source: object,
    key_field: str,
    value_field_or_none: str | None,
    key_name: str,
    count_name: str,
    sum_name_or_none: str | None,
) -> (
    tuple[
        bool,
        list[tuple[object, int]] | list[tuple[object, int, int]] | list[dict[str, object]],
    ]
    | None
): ...
def group_sum_i64_exact_pairs_v1(source: object) -> list[tuple[int, int]] | None: ...
def group_sum_i64_pairs(
    source: object, key_index: int, value_index: int
) -> list[tuple[int, int]] | None: ...
def group_sum_i64_rows_v1(
    source: object,
    key_index: int,
    value_index: int,
    key_name: str,
    output_name: str,
) -> tuple[bool, list[tuple[int, int]] | list[dict[str, object]]] | None: ...
def group_sum_i64_dict_rows(
    source: object, key_field: str, value_field: str
) -> list[tuple[object, int]] | None: ...
def group_sum_i64_dict_rows_v1(
    source: object,
    key_field: str,
    value_field: str,
    key_name: str,
    output_name: str,
) -> tuple[bool, list[tuple[object, int]] | list[dict[str, object]]] | None: ...
def join_hashable_many_records_v1(
    left: object,
    right: object,
    left_key: Callable[[object], object],
    right_key: Callable[[object], object],
    record_adapter: Callable[[object], dict[str, object]],
    left_join: bool,
    suffix: str,
    shared_names: frozenset[str],
) -> list[dict[str, object]] | None: ...
def join_hashable_many_records_v2(
    left: object,
    right: object,
    left_key: Callable[[object], object],
    right_key: Callable[[object], object],
    record_adapter: Callable[[object], dict[str, object]],
    left_join: bool,
    suffix: str,
    shared_names: frozenset[str],
    allowed_record_types: tuple[type, ...],
) -> list[dict[str, object]] | None: ...
def join_hashable_many_direct_records_v1(
    left: object,
    right: object,
    left_field: str,
    right_field: str,
    left_join: bool,
    suffix: str,
    shared_names: frozenset[str],
    allowed_record_types: tuple[type, ...],
) -> list[dict[str, object]] | None: ...
def join_hashable_unique_records_v1(
    left: object,
    right: object,
    left_key: Callable[[object], object],
    right_key: Callable[[object], object],
    record_adapter: Callable[[object], dict[str, object]],
    left_join: bool,
    suffix: str,
    shared_names: frozenset[str],
) -> list[dict[str, object]] | None: ...
def join_hashable_unique_records_v2(
    left: object,
    right: object,
    left_key: Callable[[object], object],
    right_key: Callable[[object], object],
    record_adapter: Callable[[object], dict[str, object]],
    left_join: bool,
    suffix: str,
    shared_names: frozenset[str],
    allowed_record_types: tuple[type, ...],
) -> list[dict[str, object]] | None: ...
def join_hashable_unique_direct_records_v1(
    left: object,
    right: object,
    left_field: str,
    right_field: str,
    left_join: bool,
    suffix: str,
    shared_names: frozenset[str],
    allowed_record_types: tuple[type, ...],
) -> list[dict[str, object]] | None: ...
def standard_namedtuple_record_adapter_v1(
    record_types: tuple[type, ...],
    fallback_adapter: Callable[[object], dict[str, object]],
    get_cache_token: Callable[[], object],
    abc_token: object,
    canonical_namedtuple_factory: Callable[..., type[tuple[object, ...]]],
    code_type: type,
    mapping_abc: type,
    record_continuations: tuple[Callable[[object], dict[str, object]], ...],
    record_globals: dict[str, object],
    gettrace: Callable[[], object | None],
    getprofile: Callable[[], object | None],
) -> Callable[[object], dict[str, object]] | None: ...
def join_i64_many_dict_rows_v1(
    left: object,
    right: object,
    left_field: str,
    right_field: str,
    left_join: bool,
) -> list[dict[str, object]] | None: ...
def join_i64_unique_dict_rows_v1(
    left: object,
    right: object,
    left_field: str,
    right_field: str,
    left_join: bool,
) -> list[dict[str, object]] | None: ...
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
def materialize_i64(
    values: list[int] | tuple[int, ...],
    program: list[_Stage],
    target: int,
) -> list[int] | tuple[int, ...] | set[int]: ...
def materialize_i64_range(
    start: int,
    stop: int,
    step: int,
    program: list[_Stage],
    target: int,
) -> list[int] | tuple[int, ...] | set[int]: ...
def terminal_i64(
    values: list[int] | tuple[int, ...],
    program: list[_Stage],
    terminal: int,
) -> int | None: ...
def terminal_i64_probe(
    values: list[int] | tuple[int, ...],
    program: list[_Stage],
    terminal: int,
    max_items: int,
) -> tuple[bool, int | None]: ...
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
def aggregate_i64_masked(
    values: list[int] | tuple[int, ...],
    program: list[_Stage],
    mask: int,
) -> tuple[int, int, int | None, int | None, int | None, int | None, float, float]: ...
def aggregate_i64_range_masked(
    start: int,
    stop: int,
    step: int,
    program: list[_Stage],
    mask: int,
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
def materialize_f64(
    values: list[float] | tuple[float, ...],
    program: list[_FloatStage],
    target: int,
) -> list[float] | tuple[float, ...] | set[float]: ...
def materialize_f64_range(
    start: int,
    stop: int,
    step: int,
    program: list[_FloatStage],
    target: int,
) -> list[float] | tuple[float, ...] | set[float]: ...
def terminal_f64(
    values: list[float] | tuple[float, ...],
    program: list[_FloatStage],
    terminal: int,
) -> float | None: ...
def terminal_f64_probe(
    values: list[float] | tuple[float, ...],
    program: list[_FloatStage],
    terminal: int,
    max_items: int,
) -> tuple[bool, float | None]: ...
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
def aggregate_f64_masked(
    values: list[float] | tuple[float, ...],
    program: list[_FloatStage],
    mask: int,
) -> tuple[int, float, float | None, float | None, float | None, float | None, float, float]: ...
def aggregate_f64_range_masked(
    start: int,
    stop: int,
    step: int,
    program: list[_FloatStage],
    mask: int,
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
