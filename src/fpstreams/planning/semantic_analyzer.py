"""Propagate registered stream facts and diagnose completion, ordering, and state risks."""

from __future__ import annotations

from typing import Any

from .async_ import AsyncLogicalPlan
from .logical import Pipeline
from .semantic_rules import ASYNC_OPERATOR_RULES, SYNC_OPERATOR_RULES, OperatorRule
from .semantics import (
    AsyncTerminalName,
    CompletionCondition,
    DiagnosticSeverity,
    OperatorAnalysis,
    PlanDiagnostic,
    PlanSemantics,
    StateKind,
    StreamFacts,
    TerminationEvidence,
)


def _terminal_condition(terminal: str) -> CompletionCondition:
    """Map a terminal to the event that lets it finish or short-circuit."""
    if terminal in {"first", "any"}:
        return CompletionCondition.FIRST_ITEM_OR_SOURCE_END
    if terminal == "all":
        return CompletionCondition.WITNESS_OR_SOURCE_END
    if terminal == "iterate":
        return CompletionCondition.CONSUMER_STOP
    return CompletionCondition.SOURCE_END


def _analyse(
    source: StreamFacts,
    operations: tuple[object, ...],
    rules: dict[type[Any], OperatorRule],
) -> tuple[tuple[OperatorAnalysis, ...], StreamFacts]:
    """Apply exact-type rules in plan order and capture each input-to-output fact transition."""
    current = source
    analyses: list[OperatorAnalysis] = []
    for index, operation in enumerate(operations):
        rule = rules.get(type(operation))
        if rule is None:
            raise TypeError(f"no semantic rule registered for {type(operation).__name__}")
        output = rule.transfer(current, operation)
        analyses.append(
            OperatorAnalysis(
                index=index,
                name=str(getattr(operation, "name", rule.name)),
                input=current,
                output=output,
                progress=rule.progress,
                state=rule.state(operation),
                completion_dependencies=rule.completion_dependencies(current, operation),
                requires_order=rule.requires_order,
            )
        )
        current = output
    return tuple(analyses), current


def _diagnostics(
    operations: tuple[OperatorAnalysis, ...], output: StreamFacts, terminal: str
) -> tuple[PlanDiagnostic, ...]:
    """Report unordered inputs, potentially unbounded state, and unproven completion.

    Proven-infinite dependencies and source-ending terminals are errors; unknown termination is
    a warning because the analyzer cannot prove that evaluation completes.
    """
    diagnostics: list[PlanDiagnostic] = []
    for operation in operations:
        if operation.requires_order and operation.input.ordering.value == "unordered":
            diagnostics.append(
                PlanDiagnostic(
                    "ORDER_NOT_PRESERVED",
                    DiagnosticSeverity.WARNING,
                    "operator "
                    f"{operation.name} requires ordered input, but upstream ordering is unordered",
                    operation.index,
                )
            )
        if (
            operation.state.kind
            in {
                StateKind.GROWS_WITH_INPUT,
                StateKind.GROWS_WITH_KEYS,
                StateKind.UNKNOWN,
            }
            and operation.input.termination is not TerminationEvidence.PROVEN_FINITE
        ):
            diagnostics.append(
                PlanDiagnostic(
                    "STATE_MAY_GROW",
                    DiagnosticSeverity.WARNING,
                    f"operator {operation.name} may retain unbounded state until input completion",
                    operation.index,
                )
            )
        for dependency in operation.completion_dependencies:
            if dependency.condition is not CompletionCondition.SOURCE_END:
                continue
            if dependency.facts.termination is TerminationEvidence.PROVEN_INFINITE:
                diagnostics.append(
                    PlanDiagnostic(
                        "NON_TERMINATING_PLAN",
                        DiagnosticSeverity.ERROR,
                        "operator "
                        f"{operation.name} waits for {dependency.label}, which is proven infinite",
                        operation.index,
                    )
                )
            elif dependency.facts.termination is not TerminationEvidence.PROVEN_FINITE:
                diagnostics.append(
                    PlanDiagnostic(
                        "COMPLETION_NOT_PROVEN",
                        DiagnosticSeverity.WARNING,
                        "operator "
                        f"{operation.name} completion depends on an input with unknown termination",
                        operation.index,
                    )
                )
    condition = _terminal_condition(terminal)
    if condition is CompletionCondition.SOURCE_END:
        if output.termination is TerminationEvidence.PROVEN_INFINITE:
            diagnostics.append(
                PlanDiagnostic(
                    "NON_TERMINATING_PLAN",
                    DiagnosticSeverity.ERROR,
                    f"terminal {terminal} waits for source end",
                )
            )
        elif output.termination is not TerminationEvidence.PROVEN_FINITE:
            diagnostics.append(
                PlanDiagnostic(
                    "COMPLETION_NOT_PROVEN",
                    DiagnosticSeverity.WARNING,
                    f"terminal {terminal} completion is not proven",
                )
            )
    return tuple(diagnostics)


def analyze_sync_plan(plan: Pipeline, terminal: str = "iterate") -> PlanSemantics:
    """Analyze a synchronous plan with the sync rule registry and terminal diagnostics."""
    source = plan.source.current_facts()
    operations, output = _analyse(source, plan.operations, SYNC_OPERATOR_RULES)
    return PlanSemantics(
        source,
        operations,
        output,
        _terminal_condition(terminal),
        _diagnostics(operations, output, terminal),
    )


def analyze_async_plan(
    plan: AsyncLogicalPlan[object], terminal: AsyncTerminalName = "iterate"
) -> PlanSemantics:
    """Analyze an asynchronous plan with the async rule registry and terminal diagnostics."""
    operations, output = _analyse(plan.source.facts, plan.operations, ASYNC_OPERATOR_RULES)
    return PlanSemantics(
        plan.source.facts,
        operations,
        output,
        _terminal_condition(terminal),
        _diagnostics(operations, output, terminal),
    )
