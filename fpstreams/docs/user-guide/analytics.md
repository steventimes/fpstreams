# Streaming analytics recipes

These examples use v2 APIs; sections marked unreleased require the development
version. Normalize identifiers, dates, units, and
duplicate-record policy before aggregation. Keep the record grain explicit:
a player-match row and a player-season row answer different questions.

## Select records by version (unreleased)

`agg.max_by` retains the whole record with the largest selected key. This lets
you resolve a finite batch of out-of-order updates without collecting and
sorting each group. `agg.min_by` selects the smallest key instead.

```python
from fpstreams import agg, rows

changes = [
    {"id": "a", "revision": 3, "deleted": True, "value": None},
    {"id": "b", "revision": 2, "deleted": False, "value": 12},
    {"id": "a", "revision": 1, "deleted": False, "value": 7},
    {"id": "b", "revision": 4, "deleted": False, "value": 19},
]
current = (
    rows(changes)
    .group_by("id")
    .aggregate(record=agg.max_by("revision"))
    .map(lambda group: group["record"])
    .filter(lambda record: not record["deleted"])
    .to_list()
)
assert current == [changes[3]]
```

Choose the winner before discarding deletion records; filtering deletions first
would restore an older value for `a`. Equal keys keep the first record. Use a
composite key such as `lambda row: (row["revision"], row["sequence"])` when your
data defines a tie breaker. Missing and incomparable keys need an explicit
normalization policy. Hash grouping retains one key and one candidate per group,
along with its group index; sorted grouping retains the current group plus
lookahead. This computes a batch result. Persisting a change log and reconciling
partial updates require application-specific storage and merge rules.

## Rank records within each group (unreleased)

Use `agg.top` or `agg.bottom` when each group needs several ranked records.
The aggregator retains up to the requested number instead of collecting the
whole group. Equal keys keep input order.

```python
from fpstreams import agg, rows

players = [
    {"team": "a", "name": "Jo", "score": 8},
    {"team": "b", "name": "Sam", "score": 6},
    {"team": "a", "name": "Lee", "score": 11},
    {"team": "a", "name": "Kai", "score": 8},
]
leaders = rows(iter(players)).group_by("team").aggregate(
    best=agg.top(2, key="score"), count=agg.count(),
).to_list()
assert leaders == [
    {"team": "a", "best": [players[2], players[0]], "count": 3},
    {"team": "b", "best": [players[1]], "count": 1},
]
```

`agg.bottom(2, key="score")` returns ascending scores. Both factories accept
the usual field, path, index, expression, and callable selectors. Normalize
missing values and use keys with a consistent order. Each state retains at
most K records; hash grouping also retains its group index, and spill follows
the existing serialized-record contract. The heap takes O(log K) comparisons
per item and final ordering takes O(K log K). A single winner can use
`agg.min_by` or `agg.max_by`.

Bounded state does not guarantee lower latency or allocation for small groups.
Collecting a small group and ranking it with Python's standard library can be
faster. Measure the full query when choosing between those approaches.

## Feed normalized records without another list

For each normalized match, generate one record for each team:

```python
from fpstreams import agg, flow


def team_records(matches):
    for match in matches:
        for team, scored, conceded in (
            (match["home_team"], match["home_goals"], match["away_goals"]),
            (match["away_team"], match["away_goals"], match["home_goals"]),
        ):
            yield {
                "team": team,
                "points": 3 if scored > conceded else int(scored == conceded),
                "goals_for": scored,
                "goals_against": conceded,
            }


matches = [
    {"home_team": 7, "away_team": 8, "home_goals": 2, "away_goals": 0},
    {"home_team": 8, "away_team": 7, "home_goals": 1, "away_goals": 1},
]
summary = (
    flow(team_records(matches))
    .group_by("team")
    .aggregate(
        matches=agg.count(),
        points=agg.sum("points"),
        goals_for=agg.sum("goals_for"),
        goals_against=agg.sum("goals_against"),
    )
    .sort_by("team")
    .to_list()
)
assert summary[0] == {
    "team": 7, "matches": 2, "points": 4,
    "goals_for": 3, "goals_against": 1,
}
```

The projection generates records as they are consumed. Any upstream list,
deduplication dictionary, or sorting step still has its own cost. Hash grouping
keeps state for every distinct team. A retained dict list may qualify for a
native route that a generator does not; measure both shapes before choosing.

## Compose a weighted collector (unreleased)

Use the existing collector combinators when a metric needs its own filter and
finisher. Named aggregation now accepts these collectors directly, so the same
definition works for global totals and individual groups:

```python
from fpstreams import Collectors, agg, rows

observations = [
    {"player": 1, "speed": 0, "seconds": 30},
    {"player": 1, "speed": 6, "seconds": 90},
    {"player": 1, "speed": None, "seconds": 300},
    {"player": 2, "speed": None, "seconds": 60},
]
weighted_speed = Collectors.filtering(
    lambda row: row["speed"] is not None,
    Collectors.teeing(
        Collectors.summing(lambda row: row["speed"] * row["seconds"]),
        Collectors.summing("seconds"),
        lambda numerator, denominator: numerator / denominator if denominator else None,
    ),
)
summary = rows(iter(observations)).group_by("player").aggregate(
    mean_speed=weighted_speed, observations=agg.count(),
).to_list()
assert summary == [
    {"player": 1, "mean_speed": 4.5, "observations": 3},
    {"player": 2, "mean_speed": None, "observations": 1},
]
```

The filter treats a measured zero as valid and excludes an unknown speed from
both sums. The finisher chooses `None` for a zero denominator. Normalization,
finite-value checks, weight validation, and duplicate handling remain part of
the metric definition. Summing uses ordinary Python arithmetic. Keep the two
totals as separate outputs when they are needed for auditing or later merging.

## Keep valid weighted denominators

Accumulate the numerator and its valid denominator together. A missing value
contributes to neither, while a measured zero remains a valid observation.

```python
observations = [
    {"player": 1, "speed": 3.0, "observed_seconds": 60},
    {"player": 1, "speed": 6.0, "observed_seconds": 120},
    {"player": 2, "speed": None, "observed_seconds": 90},
]
weighted = (
    flow(observations)
    .with_columns(
        numerator=lambda row: (
            row["speed"] * row["observed_seconds"]
            if row["speed"] is not None else 0.0
        ),
        denominator=lambda row: (
            row["observed_seconds"] if row["speed"] is not None else 0.0
        ),
    )
    .group_by("player")
    .aggregate(
        numerator=agg.sum("numerator"),
        denominator=agg.sum("denominator"),
        observations=agg.count(),
    )
    .with_columns(
        mean_speed=lambda row: (
            row["numerator"] / row["denominator"]
            if row["denominator"] else None
        )
    )
    .sort_by("player")
    .to_list()
)
assert weighted[0]["mean_speed"] == 5.0
assert weighted[1]["mean_speed"] is None
```

Validate finite values and nonnegative weights when normalizing the source.
Choose weights for the metric: action coordinates use valid action counts,
speed observations use observed time, and rates per 90 minutes use the minutes
for which the event count is known. Repeated matches contribute at their stated
grain. A weighted mean of match-level percentiles is not a pooled percentile.

## Exact grouped percentiles (unreleased)

Compose a collector when a finite set of speed observations needs several
percentiles. This example computes unweighted p50 and p90 values using the
standard library's [inclusive quantiles](https://docs.python.org/3/library/statistics.html#statistics.quantiles).
Normalize speeds to finite numbers before this step; only `None` is missing,
so a measured zero remains part of the distribution.

```python
from statistics import quantiles
from fpstreams import Collectors, rows


def speed_percentiles(values):
    count = len(values)
    if count == 0:
        return {"observations": 0, "p50": None, "p90": None}
    if count == 1:
        return {"observations": 1, "p50": values[0], "p90": values[0]}
    cuts = quantiles(values, n=10, method="inclusive")
    return {"observations": count, "p50": cuts[4], "p90": cuts[8]}


speed_distribution = Collectors.filtering(
    lambda row: row["speed"] is not None,
    Collectors.mapping(
        "speed",
        Collectors.collecting_and_then(Collectors.to_list(), speed_percentiles),
    ),
)
observations = [
    {"player": 1, "speed": 0.0},
    {"player": 1, "speed": 4.0},
    {"player": 1, "speed": 8.0},
    {"player": 1, "speed": 10.0},
    {"player": 1, "speed": None},
    {"player": 2, "speed": None},
    {"player": 3, "speed": 7.0},
]
summary = rows(iter(observations)).group_by("player").aggregate(
    speed=speed_distribution,
).to_list()
assert summary == [
    {"player": 1, "speed": {"observations": 4, "p50": 6.0, "p90": 9.4}},
    {"player": 2, "speed": {"observations": 0, "p50": None, "p90": None}},
    {"player": 3, "speed": {"observations": 1, "p50": 7.0, "p90": 7.0}},
]
```

Inclusive quantiles interpolate between ordered observations, treating the
minimum and maximum as the endpoints. The explicit empty and single-value
policies above also keep this recipe consistent across supported Python versions.
`observations` counts the non-missing speeds in this result.
Each group receives its own list, and one `quantiles` call supplies both outputs.
Exact percentiles retain every valid observation in each active group; spilling
partitions does not bound that list's size. Keep source, player and observation
grain consistent when pooling observations.

## Filter SQLite rows before conversion

Project only needed columns and bind selection values separately from SQL:

```python
from fpstreams import Rows

selected = Rows.from_sqlite(
    "matches.sqlite",
    """SELECT team_id, points FROM matches
       WHERE source = ? AND version = ? AND match_date >= ?""",
    ("tracking", 2, "2026-01-01"),
    batch_size=1000,
    timeout=5.0,
)
totals = selected.group_by("team_id").aggregate(points=agg.sum("points"))
```

Use a consistent ISO date representation for this text comparison. The source
uses `fetchmany()` and opens one connection for each iteration. Early exit or
failure closes the owned cursor and connection. Measure reading, record
conversion, aggregation, and output separately before attributing a change to
the stream engine. This query selects the input explicitly; it does not depend
on automatic SQL pushdown.

## Write batches in one transaction

```python
written = totals.to_sqlite(
    "results.sqlite", "team_totals", if_exists="replace", batch_size=1000,
)
```

`batch_size` controls each `executemany()` call. The sink commits once after the
complete write succeeds. A failed later batch rolls back earlier batches, and
replace-mode schema changes roll back with the inserts. Choose append/conflict
policies for your application's keys. If independent import tasks need separate
recovery checkpoints, manage those task boundaries in the application. Retrying
a failed atomic write and retrying an already committed append have different
duplicate-record consequences.

## Resume file partitions (unreleased)

For a collection of stable JSONL files, checkpoint each partition separately.
The callback writes one result file; later stages receive its path instead of
the full result payload.

```python
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path
from fpstreams import BoundedExecutor, FileCheckpoint, agg, flow, rows


def summarize(input_path, output_path):
    rows.from_jsonl(input_path).group_by("team").aggregate(
        points=agg.sum("points"), matches=agg.count(),
    ).to_jsonl(output_path)


checkpoint = FileCheckpoint("completed", version="team-totals-v1")
process_partition = partial(checkpoint.run, function=summarize)
inputs = sorted(Path("partitions").glob("*.jsonl"))
with ThreadPoolExecutor(max_workers=4) as pool:
    with BoundedExecutor(pool, max_pending=8) as budget:
        result_files = flow(inputs).map_parallel(
            process_partition, executor=budget, buffer=4,
        ).to_list()

totals = (
    flow(result_files).flat_map(rows.from_jsonl).group_by("team")
    .aggregate(points=agg.sum("points"), matches=agg.sum("matches"))
    .to_list()
)
```

Repeat the same invocation after a failure. Completed partitions are verified
and reused; partitions without a committed result run again. The existing
`attempt()` operation can collect individual failures while allowing other
partitions to finish. Async `retry()` already provides attempt limits, backoff,
jitter, and exception filtering for async callbacks.

The final aggregation sums each partition's counts because the same team can
appear in several files. `flat_map(rows.from_jsonl)` reads the result files
lazily and closes each reader when it finishes or the consumer stops.

The resolved input path, its SHA256, and `version` identify a partition. Change
`version` when callback code, options, or external dependencies can change its
result. Inputs must stay unchanged during processing. Cache hits read input
and output bytes to verify their hashes, so they still incur file I/O. A corrupt
completed entry raises `ValueError` without silently rerunning side effects.

Keep the checkpoint directory on trusted local storage. Each successful run
publishes the output and JSON metadata together using a same-filesystem rename.
The callback must close its regular output file before returning. Partial runs
are not reusable, and publication does not guarantee durability after power
loss. A concurrently running duplicate raises `BlockingIOError`. OS locks are
released on process exit; their small lock files remain in the directory.
Forced termination can leave `.stage-*` directories, which you can remove after
active jobs have stopped. Completed entries remain until you remove them.

This checkpoints the output file. Database writes, network calls, and other
callback side effects need their own idempotency. Arbitrary iterators are not
replayed. Use a separate application transaction when multiple partition
outputs must become visible together.
