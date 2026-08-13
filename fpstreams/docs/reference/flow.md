# Flow

`Flow[T]` is the synchronous lazy pipeline. Transformations return a new flow;
terminal methods execute its plan.

Use `flow(source)` for an existing iterable and `flow.defer(factory)` when every
execution must create a fresh iterable.

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
