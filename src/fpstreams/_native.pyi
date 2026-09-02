from collections.abc import Callable, Iterator
from typing import TypeAlias, final

__all__ = [
    "NumpyGroupPartial",
    "NumpyGroupState",
    "aggregate_f64",
    "aggregate_f64_buffer_masked_v1",
    "aggregate_f64_buffer_masked_v2",
    "aggregate_f64_masked",
    "aggregate_f64_range",
    "aggregate_f64_range_masked",
    "aggregate_i64",
    "aggregate_i64_buffer_masked_v1",
    "aggregate_i64_buffer_masked_v2",
    "aggregate_i64_masked",
    "aggregate_i64_range",
    "aggregate_i64_range_masked",
    "all_exact_dict_rows_v1",
    "count_f64",
    "count_f64_range",
    "direct_dict_field_key_v1",
    "drop_nulls_exact_dict_prefix_v1",
    "exact_container_extraction_v1",
    "execute_f64",
    "execute_f64_buffer_v1",
    "execute_f64_range",
    "execute_i64",
    "execute_i64_buffer_v1",
    "execute_i64_range",
    "filter_i64_expr_exact_dict_prefix_v1",
    "frequencies_i64_exact_v1",
    "global_multi_i64_dict_rows_v1",
    "global_multi_i64_rows_v1",
    "global_sum_i64_dict_rows_v1",
    "group_fixed_i64_dict_rows_v1",
    "group_fixed_i64_rows_v1",
    "group_multi_i64_dict_rows_v1",
    "group_multi_i64_exact_pairs_v1",
    "group_multi_i64_rows_v1",
    "group_sum_i64_dict_rows",
    "group_sum_i64_dict_rows_v1",
    "group_sum_i64_exact_pairs_v1",
    "group_sum_i64_exact_pairs_v2",
    "group_sum_i64_pair_expr_rows_v1",
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
    "join_i64_unique_dict_rows_v2",
    "materialize_f64",
    "materialize_f64_buffer_v1",
    "materialize_f64_range",
    "materialize_i64",
    "materialize_i64_buffer_v1",
    "materialize_i64_filter_exact_list_v1",
    "materialize_i64_map_exact_list_v1",
    "materialize_i64_range",
    "mean_exact_iterator_chunk_v1",
    "mean_exact_numbers_v1",
    "mean_f64",
    "mean_f64_buffer_v1",
    "mean_f64_buffer_v2",
    "mean_f64_range",
    "mean_i64",
    "mean_i64_buffer_v1",
    "mean_i64_buffer_v2",
    "mean_i64_range",
    "numpy_group_commit_v1",
    "numpy_group_finalize_v1",
    "numpy_group_partial_v1",
    "numpy_group_state_v1",
    "numpy_group_strided_partial_v2",
    "pack_i64_exact_sequence_v1",
    "pair_f64_value_filter_to_dict_exact_prefix_v1",
    "pair_f64_value_map_to_dict_exact_prefix_v1",
    "pair_i64_row_filter_to_dict_exact_prefix_v1",
    "pair_i64_value_filter_to_dict_exact_prefix_v1",
    "pair_i64_value_map_to_dict_exact_prefix_v1",
    "pair_unique_exact_prefix_v1",
    "pivot_exact_dict_rows_v1",
    "record_group_sum_max_fields",
    "record_join_v1_max_fields",
    "records_from_exact_columns_v1",
    "select_exact_dict_prefix_v1",
    "sequential_f64_aggregate_total_v1",
    "sort_i64_exact_sequence_v1",
    "standard_namedtuple_record_adapter_v1",
    "statistics_f64",
    "statistics_f64_range",
    "statistics_i64",
    "statistics_i64_range",
    "terminal_f64",
    "terminal_f64_buffer_v1",
    "terminal_f64_buffer_v2",
    "terminal_f64_probe",
    "terminal_f64_range",
    "terminal_i64",
    "terminal_i64_probe",
    "terminal_i64_range",
    "unique_i64_exact_prefix_cached_v1",
    "unique_i64_exact_prefix_identity_cached_v1",
    "unique_i64_exact_prefix_v1",
    "unnest_exact_dict_prefix_v1",
    "unpivot_exact_dict_prefix_v1",
    "update_mean_f64_buffer_v1",
    "update_mean_i64_buffer_v1",
    "update_sum_f64_buffer_v1",
    "version",
]

_Instruction: TypeAlias = tuple[int, int]
_Stage: TypeAlias = tuple[int, tuple[_Instruction, ...]]
_FloatInstruction: TypeAlias = tuple[int, float]
_FloatStage: TypeAlias = tuple[int, tuple[_FloatInstruction, ...]]
record_group_sum_max_fields: int
record_join_v1_max_fields: int

@final
class NumpyGroupState: ...

@final
class NumpyGroupPartial: ...

def version() -> str: ...
def sequential_f64_aggregate_total_v1() -> bool: ...
def build_profile() -> str: ...
def all_exact_dict_rows_v1(source: object) -> bool: ...
def direct_dict_field_key_v1(
    field: str, selection_error_type: type[BaseException]
) -> Callable[[object], object]: ...
def drop_nulls_exact_dict_prefix_v1(
    output: list[object],
    source: Iterator[object],
    field: str,
) -> tuple[object | None, bool] | None: ...
def filter_i64_expr_exact_dict_prefix_v1(
    output: list[object],
    source: Iterator[object],
    field: str,
    instructions: tuple[_Instruction, ...],
    negate: bool,
) -> tuple[object | None, bool] | None: ...
def select_exact_dict_prefix_v1(
    output: list[object],
    source: Iterator[object],
    output_names: tuple[str, ...],
    input_fields: tuple[str, ...],
    selection_error_type: type[BaseException],
) -> tuple[object | None, bool] | None: ...
def unnest_exact_dict_prefix_v1(
    output: list[object],
    source: Iterator[object],
    column: str,
    prefix: str,
) -> tuple[object | None, bool] | None: ...
def unpivot_exact_dict_prefix_v1(
    output: list[object],
    source: Iterator[object],
    columns: tuple[str, ...],
    names_to: str,
    values_to: str,
) -> tuple[object | None, bool] | None: ...
def pair_unique_exact_prefix_v1(
    output: dict[object, object],
    source: Iterator[object],
) -> tuple[object | None, bool] | None: ...
def pair_i64_value_filter_to_dict_exact_prefix_v1(
    output: dict[object, object],
    source: Iterator[object],
    instructions: tuple[_Instruction, ...],
    keep_first: bool,
) -> tuple[object | None, bool] | None: ...
def pair_i64_row_filter_to_dict_exact_prefix_v1(
    output: dict[object, object],
    source: Iterator[object],
    instructions: tuple[_Instruction, ...],
    keep_first: bool,
) -> tuple[object | None, bool] | None: ...
def pair_f64_value_filter_to_dict_exact_prefix_v1(
    output: dict[object, object],
    source: Iterator[object],
    instructions: tuple[_FloatInstruction, ...],
    keep_first: bool,
) -> tuple[object | None, bool] | None: ...
def pair_i64_value_map_to_dict_exact_prefix_v1(
    output: dict[object, object],
    source: Iterator[object],
    instructions: tuple[_Instruction, ...],
    keep_first: bool,
    result_is_bool: bool,
) -> tuple[object | None, bool] | None: ...
def pair_f64_value_map_to_dict_exact_prefix_v1(
    output: dict[object, object],
    source: Iterator[object],
    instructions: tuple[_FloatInstruction, ...],
    keep_first: bool,
    result_is_bool: bool,
) -> tuple[object | None, bool] | None: ...
def pivot_exact_dict_rows_v1(
    source: object,
    index_fields: tuple[str, ...],
    column_field: str,
    value_field: str,
    key_names: tuple[str, ...],
    fill: object,
    duplicate_error_type: type[BaseException],
) -> list[dict[str, object]] | None: ...
def exact_container_extraction_v1() -> bool: ...
def frequencies_i64_exact_v1(
    source: object,
) -> dict[int, int] | tuple[dict[int, int], Iterator[object]] | None: ...
def mean_exact_iterator_chunk_v1(
    source: Iterator[object],
    count: int,
    total: float,
    compensation: float,
    dependency_bindings: tuple[
        tuple[dict[str, object], dict[str, object] | None, str, object], ...
    ],
    mean_function: Callable[..., object],
    mean_code: object,
) -> tuple[int, int, float, float, object | None]: ...
def mean_exact_numbers_v1(source: object) -> tuple[bool, float | None]: ...
def pack_i64_exact_sequence_v1(source: object) -> bytes | None: ...
def records_from_exact_columns_v1(
    names: tuple[str, ...], columns: tuple[list[object], ...]
) -> list[dict[str, object]] | None: ...
def numpy_group_state_v1(lane_mask: int) -> NumpyGroupState: ...
def numpy_group_partial_v1(
    keys: object,
    values: object | None,
    lane_mask: int,
) -> NumpyGroupPartial | None: ...
def numpy_group_strided_partial_v2(
    keys: object,
    values: object | None,
    lane_mask: int,
) -> NumpyGroupPartial | None: ...
def numpy_group_commit_v1(
    state: NumpyGroupState,
    partial: NumpyGroupPartial,
) -> None: ...
def numpy_group_finalize_v1(
    state: NumpyGroupState,
) -> tuple[
    list[bool | int],
    list[int] | None,
    list[int] | None,
    list[bool | int] | None,
    list[bool | int] | None,
]: ...
def update_mean_i64_buffer_v1(
    values: object,
    count: int,
    total: float,
    compensation: float,
) -> tuple[int, float, float]: ...
def update_mean_f64_buffer_v1(
    values: object,
    count: int,
    total: float,
    compensation: float,
) -> tuple[int, float, float]: ...
def update_sum_f64_buffer_v1(values: object, total: float) -> float: ...
def sort_i64_exact_sequence_v1(source: object, reverse: bool) -> list[object] | None: ...
def global_multi_i64_rows_v1(
    source: object,
    lanes: tuple[tuple[int, int | None, str], ...],
) -> dict[str, object] | None: ...
def global_multi_i64_dict_rows_v1(
    source: object,
    lanes: tuple[tuple[int, str | None, str], ...],
) -> dict[str, object] | None: ...
def global_sum_i64_dict_rows_v1(source: object, value_field: str) -> int | None: ...
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
def group_multi_i64_rows_v1(
    source: object,
    key_index: int,
    key_name: str,
    lanes: tuple[tuple[int, int | None, str], ...],
) -> list[dict[str, object]] | None: ...
def group_multi_i64_exact_pairs_v1(
    source: object,
    key_name: str,
    lanes: tuple[tuple[int, int | None, str], ...],
) -> list[dict[str, object]] | None: ...
def group_multi_i64_dict_rows_v1(
    source: object,
    key_field: str,
    key_name: str,
    lanes: tuple[tuple[int, str | None, str], ...],
) -> list[dict[str, object]] | None: ...
def group_sum_i64_exact_pairs_v1(source: object) -> list[tuple[int, int]] | None: ...
def group_sum_i64_exact_pairs_v2(
    source: object, output_name: str
) -> tuple[bool, list[tuple[int, int]] | dict[int, dict[str, int]]] | None: ...
def group_sum_i64_pair_expr_rows_v1(
    source: object,
    key_instructions: tuple[_Instruction, ...],
    value_instructions: tuple[_Instruction, ...],
    key_name: str,
    output_name: str,
) -> tuple[bool, list[tuple[int, int]] | list[dict[str, object]]] | None: ...
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
def join_i64_unique_dict_rows_v2(
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
def execute_i64_buffer_v1(values: object, program: list[_Stage]) -> list[int]: ...
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
def materialize_i64_buffer_v1(
    values: object,
    program: list[_Stage],
    target: int,
) -> list[int] | tuple[int, ...] | set[int]: ...
def materialize_i64_filter_exact_list_v1(
    source: list[object] | tuple[object, ...],
    instructions: tuple[_Instruction, ...],
    negated: bool,
) -> list[object] | None: ...
def materialize_i64_map_exact_list_v1(
    source: list[object] | tuple[object, ...],
    instructions: tuple[_Instruction, ...],
) -> list[int] | None: ...
def materialize_i64_range(
    start: int,
    stop: int,
    step: int,
    program: list[_Stage],
    target: int,
) -> list[int] | tuple[int, ...] | set[int]: ...
def unique_i64_exact_prefix_v1(
    output: list[object],
    source: Iterator[object],
) -> tuple[object | None, bool] | None: ...
def unique_i64_exact_prefix_cached_v1(
    output: list[object],
    source: Iterator[object],
) -> tuple[object | None, bool] | None: ...
def unique_i64_exact_prefix_identity_cached_v1(
    output: list[object],
    source: Iterator[object],
) -> tuple[object | None, bool] | None: ...
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
def mean_i64(
    values: list[int] | tuple[int, ...],
    program: list[_Stage],
) -> float | None: ...
def mean_i64_buffer_v1(values: object, program: list[_Stage]) -> float | None: ...
def mean_i64_buffer_v2(values: object, program: list[_Stage]) -> float | None: ...
def mean_i64_range(
    start: int,
    stop: int,
    step: int,
    program: list[_Stage],
) -> float | None: ...
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
def aggregate_i64_buffer_masked_v1(
    values: object,
    program: list[_Stage],
    mask: int,
) -> tuple[int, int, int | None, int | None, int | None, int | None, float, float]: ...
def aggregate_i64_buffer_masked_v2(
    values: object,
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
def execute_f64_buffer_v1(values: object, program: list[_FloatStage]) -> list[float]: ...
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
def materialize_f64_buffer_v1(
    values: object,
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
def terminal_f64_buffer_v1(
    values: object,
    program: list[_FloatStage],
    terminal: int,
) -> tuple[int, float | None]: ...
def terminal_f64_buffer_v2(
    values: object,
    program: list[_FloatStage],
    terminal: int,
) -> tuple[int, float | None]: ...
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
def mean_f64(
    values: list[float] | tuple[float, ...],
    program: list[_FloatStage],
) -> float | None: ...
def mean_f64_buffer_v1(values: object, program: list[_FloatStage]) -> float | None: ...
def mean_f64_buffer_v2(values: object, program: list[_FloatStage]) -> float | None: ...
def mean_f64_range(
    start: int,
    stop: int,
    step: int,
    program: list[_FloatStage],
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
def aggregate_f64_buffer_masked_v1(
    values: object,
    program: list[_FloatStage],
    mask: int,
) -> tuple[int, float, float | None, float | None, float | None, float | None, float, float]: ...
def aggregate_f64_buffer_masked_v2(
    values: object,
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
