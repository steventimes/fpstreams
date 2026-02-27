"""QA-style Option tests focused on null-safety in data ingestion pipelines."""

from __future__ import annotations

import pytest

from fpstreams import Option


def test_option_of_rejects_none_to_prevent_silent_data_contract_break() -> None:
    with pytest.raises(ValueError, match=r"Option\.of\(\) cannot be called with None"):
        Option.of(None)  # type: ignore[arg-type]


def test_option_pipeline_extracts_nested_profile_email() -> None:
    payload = {"customer": {"profile": {"email": "alice@example.com", "locale": "en-US"}}}

    email = (
        Option.of_nullable(payload)
        .map(lambda p: p.get("customer"))
        .map(lambda c: c.get("profile") if c else None)
        .map(lambda profile: profile.get("email") if profile else None)
        .filter(lambda value: value.endswith("@example.com"))
        .or_else("missing@example.com")
    )

    assert email == "alice@example.com"


def test_option_pipeline_falls_back_when_upstream_field_missing() -> None:
    payload = {"customer": {"profile": {"locale": "en-US"}}}

    email = (
        Option.of_nullable(payload)
        .map(lambda p: p.get("customer"))
        .map(lambda c: c.get("profile") if c else None)
        .map(lambda profile: profile.get("email") if profile else None)
        .or_else_get(lambda: "fallback@company.test")
    )

    assert email == "fallback@company.test"


def test_option_or_else_throw_raises_domain_error_for_required_value() -> None:
    with pytest.raises(RuntimeError, match="primary key missing"):
        Option.empty().or_else_throw(lambda: RuntimeError("primary key missing"))
