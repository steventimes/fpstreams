# Streaming analytics recipes

These examples use existing v2 APIs. Normalize identifiers, dates, units, and
duplicate-record policy before aggregation. Keep the record grain explicit:
a player-match row and a player-season row answer different questions.

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
