# Gatherers

A `Gatherer` is a stateful intermediate operation. Unlike a `Collector`, it can
emit zero or more values while the rest of the pipeline is still running.

Use the regular constructor for the iterable-returning interface:

~~~python
from fpstreams import Gatherer, flow

pairs = Gatherer(
    initializer=lambda: None,
    integrator=lambda _state, value: (value, -value),
)

assert flow([1, 2]).gather(pairs).to_list() == [1, -1, 2, -2]
~~~

`Gatherer.of()` uses a push interface. Its integrator receives the current state,
the input item, and a `Downstream`. Return `False` to stop reading the source.
`Gatherer.of_sequential()` declares that its state cannot be combined in parallel.
`and_then()` composes two gatherers without collecting the intermediate output.

## Gatherer

::: fpstreams.Gatherer
    options:
      members_order: source
      show_root_heading: true
      show_source: false

## Downstream

`Downstream.push(value)` emits a value and reports whether downstream still wants
more. Check `is_rejecting()` before expensive work when downstream may have
short-circuited.

::: fpstreams.Downstream
    options:
      members_order: source
      show_root_heading: true
      show_source: false
