"""QA-style Result tests around realistic network/payment workflows."""

from __future__ import annotations

from fpstreams import Result


class GatewayTimeoutError(RuntimeError):
    """Simulates a transient network timeout from a payment gateway."""


def test_result_success_path_with_side_effect_hook() -> None:
    audit_log: list[str] = []

    result = (
        Result.success({"transaction_id": "tx-1", "amount": 49.99})
        .on_success(lambda payload: audit_log.append(payload["transaction_id"]))
    )

    assert result.is_success()
    assert result.get_or_else({"transaction_id": "missing"})["transaction_id"] == "tx-1"
    assert audit_log == ["tx-1"]


def test_result_of_captures_gateway_error_and_maps_domain_exception() -> None:
    def charge_card() -> str:
        raise GatewayTimeoutError("gateway timed out after 30s")

    result = Result.of(charge_card).map_error(lambda err: RuntimeError(f"payment retry needed: {err}"))

    assert result.is_failure()
    assert "payment retry needed" in str(result.error)


def test_result_flat_map_short_circuits_on_failure() -> None:
    called = False

    def reserve_inventory(_: dict[str, object]) -> Result[str]:
        nonlocal called
        called = True
        return Result.success("reserved")

    failed_payment = Result.failure(ValueError("insufficient funds"))
    out = failed_payment.flat_map(reserve_inventory)

    assert out.is_failure()
    assert called is False


def test_result_get_or_throw_bubbles_stored_exception() -> None:
    failure = Result.failure(ValueError("invalid invoice state"))

    try:
        failure.get_or_throw()
        assert False, "Expected ValueError to be raised"
    except ValueError as exc:
        assert "invalid invoice state" in str(exc)
