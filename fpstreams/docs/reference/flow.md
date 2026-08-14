# Flow

`Flow[T]` is the synchronous lazy pipeline. Transformations return a new flow;
terminal methods execute its plan.

Use `flow(source)` for an existing iterable and `flow.defer(factory)` when every
execution must create a fresh iterable.

## Explaining terminal execution

`explain()` defaults to ordinary iteration. Pass a terminal name when you want
to inspect `to_list()`, `count()`, `sum()`, statistics, aggregation, or a
short-circuiting terminal. The same planner is used by the explanation and the
terminal itself.

```python
from fpstreams import flow

flow([1, 2, 3]).explain(terminal="count").to_dict()
```

The actual result is:

```python
{
    "terminal": "count",
    "source": {"reiterable": True, "exact_size": 3, "ordered": True},
    "requested_engine": "auto",
    "selected_engine": "python",
    "streaming_engine": "python",
    "materializing_engine": "python",
    "selection_reason": "identity list/tuple stays in Python to avoid a type scan and Rust copy",
    "data_movement": {
        "scans_source": False,
        "copies_source": False,
        "materializes": False,
    },
    "complexity": "O(1)",
    "operations": [],
    "stages": [],
}
```

An identity list or tuple remains in Python under `auto` when a terminal would
otherwise scan and copy it. An identity range can still use native numeric
reduction. Exact-size `count()` is O(1) only for an unchanged, safely reiterable
source; operations and one-shot inputs are consumed normally.

## CSV safety

`to_csv(..., spreadsheet_safe=False)` preserves raw values for machine
interchange. Set it to `True` when untrusted strings will be opened in Excel,
Sheets, or similar software. Suspect formula prefixes are neutralized with a
leading single quote. The method writes the file and returns `None`.

## Creating a flow

| Call | Behavior |
| --- | --- |
| `flow(source)` | Wrap an iterable without reading it immediately |
| `flow.defer(factory)` | Call the factory for each execution |
| `flow.empty()` | Create a flow with no items |
| `flow.of_nullable(value)` | Emit one value, or nothing when it is `None` |
| `flow.iterate(seed, function)` | Repeatedly derive the next value from the previous one |
| `flow.generate(supplier)` | Call a supplier for every emitted value |
| `flow.concat(*sources)` | Read several sources in order |

## Methods

::: fpstreams.Flow
    options:
      members_order: source
      show_root_heading: true
      show_source: false
