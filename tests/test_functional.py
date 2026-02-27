"""QA-style tests for functional helpers in production-like workflows."""

from __future__ import annotations

from fpstreams.functional import curry, pipe


def test_pipe_transforms_order_payload_end_to_end() -> None:
    payload = {
        "line_items": [
            {"sku": "A-1", "qty": 2, "unit_price": 12.5},
            {"sku": "B-2", "qty": 1, "unit_price": 30.0},
        ]
    }

    result = pipe(
        payload,
        lambda p: p["line_items"],
        lambda rows: [row["qty"] * row["unit_price"] for row in rows],
        sum,
        lambda total: f"USD {total:.2f}",
    )

    assert result == "USD 55.00"


def test_curry_supports_partial_and_full_invocation_for_tax_calculation() -> None:
    @curry
    def apply_tax(subtotal: float, tax_rate: float, shipping_fee: float) -> float:
        return round(subtotal * (1 + tax_rate) + shipping_fee, 2)

    us_checkout = apply_tax(100.0)(0.0825)

    assert us_checkout(7.99) == 116.24
    assert apply_tax(100.0, 0.0825, 7.99) == 116.24
