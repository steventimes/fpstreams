"""Small identity snapshots for mutable Python callables used by fast paths."""

from __future__ import annotations

import builtins as _builtins
from types import FunctionType
from typing import Any

_BUILTIN_ALL = _builtins.all
_BUILTIN_ANY = _builtins.any
_BUILTIN_DICT = _builtins.dict
_BUILTIN_GETATTR = _builtins.getattr
_BUILTIN_INT = _builtins.int
_BUILTIN_LEN = _builtins.len
_BUILTIN_STR = _builtins.str
_BUILTIN_TUPLE: type[tuple[Any, ...]] = _builtins.tuple
_BUILTIN_TYPE = _builtins.type
_BUILTIN_ZIP = _builtins.zip
_MISSING_BINDING = object()
_EMPTY_CELL = object()
_INTRINSIC_TYPE = (0).__class__.__class__
_INTRINSIC_BUILTIN_FUNCTION_TYPE: type[object] = [].append.__class__

BuiltinEndpoint = tuple[str, object, str]
BuiltinEndpointManifest = tuple[bool, bool, tuple[BuiltinEndpoint, ...]]


def builtin_endpoint_is_canonical(endpoint: object, name: str) -> bool:
    """Recognize an original builtins function or type even after pre-import patching."""
    endpoint_type = _INTRINSIC_TYPE(endpoint)
    if endpoint_type is _INTRINSIC_BUILTIN_FUNCTION_TYPE:
        return (
            endpoint.__self__ is _builtins  # type: ignore[attr-defined]
            and endpoint.__module__ == "builtins"
            and endpoint.__name__ == name  # type: ignore[attr-defined]
        )
    if endpoint_type is not _INTRINSIC_TYPE:  # type: ignore[comparison-overlap]
        return False
    return (
        endpoint.__flags__ & (1 << 9) == 0  # type: ignore[attr-defined]
        and endpoint.__module__ == "builtins"
        and endpoint.__name__ == name  # type: ignore[attr-defined]
    )


_PROVENANCE_BOOTSTRAP_TRUSTED = True
for _builtin_name, _builtin_endpoint in (
    ("all", _BUILTIN_ALL),
    ("any", _BUILTIN_ANY),
    ("dict", _BUILTIN_DICT),
    ("getattr", _BUILTIN_GETATTR),
    ("int", _BUILTIN_INT),
    ("len", _BUILTIN_LEN),
    ("str", _BUILTIN_STR),
    ("tuple", _BUILTIN_TUPLE),
    ("type", _BUILTIN_TYPE),
    ("zip", _BUILTIN_ZIP),
):
    if not builtin_endpoint_is_canonical(_builtin_endpoint, _builtin_name):
        _PROVENANCE_BOOTSTRAP_TRUSTED = False
        break


def capture_builtin_endpoints(
    *endpoints: BuiltinEndpoint,
    revalidate: bool = False,
) -> BuiltinEndpointManifest:
    """Freeze builtin aliases and whether they were canonical when imported."""
    trusted = _PROVENANCE_BOOTSTRAP_TRUSTED and _BUILTIN_ALL(
        builtin_endpoint_is_canonical(endpoint, builtin_name)
        for _alias_name, endpoint, builtin_name in endpoints
    )
    return trusted, revalidate, endpoints


def builtin_endpoints_are_live(
    module_globals: dict[str, Any],
    _manifest_name: str,
    manifest: BuiltinEndpointManifest,
) -> bool:
    """Validate the public builtins whose semantics a fast path would bypass."""
    bootstrap_trusted, revalidate, endpoints = manifest
    if not bootstrap_trusted:
        return False
    for alias_name, endpoint, builtin_name in endpoints:
        if module_globals.get(alias_name) is not endpoint or (
            revalidate and not builtin_endpoint_is_canonical(endpoint, builtin_name)
        ):
            return False
    return True


def _effective_function_binding(function: FunctionType, name: str) -> tuple[int, object]:
    """Return the exact global-or-builtin object resolved for one code name."""
    globals_ = function.__globals__
    if name in globals_:
        return 1, globals_[name]
    builtins_ = _BUILTIN_GETATTR(function, "__builtins__", None)
    if _BUILTIN_TYPE(builtins_) is _BUILTIN_DICT and name in builtins_:
        return 2, builtins_[name]
    return 0, _MISSING_BINDING


def function_environment_snapshot(function: FunctionType) -> tuple[Any, ...]:
    """Capture mutable function state without copying user-owned values."""
    closure = function.__closure__ or ()
    cells: list[tuple[object, object]] = []
    for cell in closure:
        try:
            value = cell.cell_contents
        except ValueError:
            value = _EMPTY_CELL
        cells.append((cell, value))
    return (
        function,
        function.__code__,
        function.__globals__,
        _BUILTIN_GETATTR(function, "__builtins__", None),
        function.__defaults__,
        function.__kwdefaults__,
        _BUILTIN_TUPLE(
            (name, *_effective_function_binding(function, name))
            for name in function.__code__.co_names
        ),
        _BUILTIN_TUPLE(cells),
    )


def row_expression_environment_snapshot(function: FunctionType) -> tuple[Any, ...]:
    """Capture a generated evaluator and the exact function slots it may call."""
    root = function_environment_snapshot(function)
    nested: list[tuple[Any, ...]] = []
    for _name, _source, value in root[6]:
        if _BUILTIN_TYPE(value) is FunctionType:
            nested.append(function_environment_snapshot(value))
        elif _BUILTIN_TYPE(value) is _BUILTIN_TUPLE:
            nested.extend(
                function_environment_snapshot(item)
                for item in value
                if _BUILTIN_TYPE(item) is FunctionType
            )
    return root, _BUILTIN_TUPLE(nested)


def function_environment_is_current(snapshot: tuple[Any, ...]) -> bool:
    """Return whether a callable still resolves the captured live objects."""
    function, code, globals_, builtins_, defaults, kwdefaults, bindings, cells = snapshot
    if (
        function.__code__ is not code
        or function.__globals__ is not globals_
        or _BUILTIN_GETATTR(function, "__builtins__", None) is not builtins_
        or function.__defaults__ is not defaults
        or function.__kwdefaults__ is not kwdefaults
    ):
        return False
    names = code.co_names
    if _BUILTIN_LEN(bindings) != _BUILTIN_LEN(names):
        return False
    for expected_name, binding in _BUILTIN_ZIP(names, bindings, strict=True):
        name, source, value = binding
        if name != expected_name or (
            source == 2 and not builtin_endpoint_is_canonical(value, name)
        ):
            return False
        live_source, live_value = _effective_function_binding(function, name)
        if live_source != source or live_value is not value:
            return False
    closure = function.__closure__ or ()
    if _BUILTIN_LEN(closure) != _BUILTIN_LEN(cells):
        return False
    for cell, captured in _BUILTIN_ZIP(closure, cells, strict=True):
        trusted_cell, trusted_value = captured
        if cell is not trusted_cell:
            return False
        try:
            live_value = cell.cell_contents
        except ValueError:
            live_value = _EMPTY_CELL
        if live_value is not trusted_value:
            return False
    return True


def row_expression_environment_is_current(snapshot: tuple[Any, ...]) -> bool:
    """Validate a generated evaluator together with every captured function slot."""
    root, nested = snapshot
    if not function_environment_is_current(root):
        return False
    expected: list[FunctionType] = []
    for _name, _source, value in root[6]:
        if _BUILTIN_TYPE(value) is FunctionType:
            expected.append(value)
        elif _BUILTIN_TYPE(value) is _BUILTIN_TUPLE:
            expected.extend(item for item in value if _BUILTIN_TYPE(item) is FunctionType)
    if _BUILTIN_LEN(nested) != _BUILTIN_LEN(expected):
        return False
    for function, captured in _BUILTIN_ZIP(expected, nested, strict=True):
        if captured[0] is not function or not function_environment_is_current(captured):
            return False
    return True
