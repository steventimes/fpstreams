"""QA-style source factory tests using realistic event stream scenarios."""

from __future__ import annotations

from itertools import count

from fpstreams import Stream


def test_stream_of_preserves_business_event_order() -> None:
    events = [
        {"event_id": "evt-100", "type": "signup"},
        {"event_id": "evt-101", "type": "verify_email"},
        {"event_id": "evt-102", "type": "first_login"},
    ]

    result = Stream.of(*events).to_list()

    assert result == events


def test_stream_generate_can_model_live_health_checks() -> None:
    probe = {"status": "ok", "region": "us-east-1"}

    result = Stream.generate(lambda: probe).limit(4).to_list()

    assert len(result) == 4
    assert all(item["status"] == "ok" for item in result)
    assert all(item["region"] == "us-east-1" for item in result)


def test_stream_iterate_models_monotonic_invoice_ids() -> None:
    result = Stream.iterate(10_000, lambda invoice_id: invoice_id + 10).limit(5).to_list()

    assert result == [10_000, 10_010, 10_020, 10_030, 10_040]


def test_stream_generate_with_stateful_supplier_matches_counter() -> None:
    sequence = count(start=501)

    result = Stream.generate(lambda: next(sequence)).limit(3).to_list()

    assert result == [501, 502, 503]
