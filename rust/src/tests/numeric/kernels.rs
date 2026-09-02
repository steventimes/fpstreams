//! Expression preparation, fused pipeline, frequency, and terminal coverage.

use super::*;

#[test]
fn division_and_modulo_match_python_for_negative_operands() {
    assert_eq!(floor_div(-7, 3).unwrap(), -3);
    assert_eq!(floor_div(7, -3).unwrap(), -3);
    assert_eq!(modulo(-7, 3).unwrap(), 2);
    assert_eq!(modulo(7, -3).unwrap(), -2);
}

#[test]
fn common_integer_programs_prepare_specialized_kernels() {
    let affine = prepare_expression(vec![(0, 0), (1, 3), (4, 0), (1, 1), (2, 0)]);
    let divisible = prepare_expression(vec![(0, 0), (1, 8), (6, 0), (1, 0), (8, 0)]);

    assert!(matches!(
        affine,
        PreparedExpression::Affine {
            multiplier: 3,
            offset: 1
        }
    ));
    assert!(matches!(
        divisible,
        PreparedExpression::DivisibleByPowerOfTwo { mask: 7 }
    ));
}

#[test]
fn direct_integer_affine_programs_prepare_specialized_kernels() {
    let cases = [
        (vec![(0, 0), (1, 3), (4, 0)], 3, 0),
        (vec![(1, 3), (0, 0), (4, 0)], 3, 0),
        (vec![(0, 0), (1, 7), (2, 0)], 1, 7),
        (vec![(1, 7), (0, 0), (2, 0)], 1, 7),
        (vec![(0, 0), (1, 7), (3, 0)], 1, -7),
        (vec![(1, 3), (0, 0), (4, 0), (1, 7), (2, 0)], 3, 7),
        (vec![(1, 7), (1, 3), (0, 0), (4, 0), (2, 0)], 3, 7),
    ];

    for (code, multiplier, offset) in cases {
        assert!(matches!(
            prepare_expression(code),
            PreparedExpression::Affine {
                multiplier: actual_multiplier,
                offset: actual_offset,
            } if actual_multiplier == multiplier && actual_offset == offset
        ));
    }
}

#[test]
fn direct_integer_affine_programs_preserve_checked_overflow() {
    let cases = [
        (
            vec![(0, 0), (1, 2), (4, 0)],
            [(i64::MAX, None), (i64::MIN, None), (7, Some(14))],
        ),
        (
            vec![(1, 7), (0, 0), (2, 0)],
            [
                (i64::MAX, None),
                (i64::MIN, Some(i64::MIN + 7)),
                (7, Some(14)),
            ],
        ),
        (
            vec![(0, 0), (1, 7), (3, 0)],
            [
                (i64::MAX, Some(i64::MAX - 7)),
                (i64::MIN, None),
                (7, Some(0)),
            ],
        ),
    ];

    for (code, values) in cases {
        let prepared = prepare_expression(code);
        let mut stack = Vec::new();
        for (value, expected) in values {
            let actual = prepared.evaluate(value, &mut stack);
            match expected {
                Some(expected) => assert_eq!(actual.unwrap(), expected),
                None => assert!(matches!(actual, Err(crate::common::KernelError::Overflow))),
            }
        }
    }

    let minimum_subtrahend = prepare_expression(vec![(0, 0), (1, i64::MIN), (3, 0)]);
    assert!(matches!(
        minimum_subtrahend,
        PreparedExpression::Bytecode(_)
    ));
    assert_eq!(
        minimum_subtrahend
            .evaluate(i64::MIN, &mut Vec::new())
            .unwrap(),
        0
    );
    assert_eq!(
        minimum_subtrahend.evaluate(-1, &mut Vec::new()).unwrap(),
        i64::MAX
    );
}

#[test]
fn integer_affine_preparation_rejects_changed_overflow_order() {
    let programs = [
        vec![(0, 0), (1, 1), (2, 0), (1, -1), (2, 0)],
        vec![(0, 0), (1, 2), (4, 0), (1, 0), (4, 0)],
        vec![(1, i64::MIN), (0, 0), (3, 0)],
        vec![(0, 0), (1, i64::MIN), (3, 0)],
    ];

    for program in programs {
        assert!(matches!(
            prepare_expression(program),
            PreparedExpression::Bytecode(_)
        ));
    }

    let nested_addition = prepare_expression(vec![(0, 0), (1, 1), (2, 0), (1, -1), (2, 0)]);
    assert!(matches!(
        nested_addition.evaluate(i64::MAX, &mut Vec::new()),
        Err(crate::common::KernelError::Overflow)
    ));

    let multiply_then_zero = prepare_expression(vec![(0, 0), (1, 2), (4, 0), (1, 0), (4, 0)]);
    assert!(matches!(
        multiply_then_zero.evaluate(i64::MAX, &mut Vec::new()),
        Err(crate::common::KernelError::Overflow)
    ));

    let constant_minus_item = prepare_expression(vec![(1, i64::MIN), (0, 0), (3, 0)]);
    assert_eq!(
        constant_minus_item
            .evaluate(i64::MIN, &mut Vec::new())
            .unwrap(),
        0
    );
}

#[test]
fn stateless_integer_map_filter_preserves_consumers_negation_and_short_circuiting() {
    let map_then_filter = vec![
        (0, vec![(0, 0), (1, 7), (4, 0), (1, 1), (2, 0)]),
        (1, vec![(0, 0), (1, 8), (6, 0), (1, 0), (8, 0)]),
    ];
    assert_eq!(
        run_values(0..10, map_then_filter.clone()).unwrap(),
        vec![8, 64]
    );
    assert_eq!(
        run_terminal(0..10, map_then_filter.clone(), 1).unwrap(),
        Some(72)
    );
    assert_eq!(
        run_i64_aggregate_masked(0..10, map_then_filter, AGGREGATE_COUNT | AGGREGATE_TOTAL)
            .unwrap(),
        (2, 72, None, None, None, None, 0.0, 0.0)
    );

    let map_then_reject = vec![
        (0, vec![(0, 0), (1, 7), (4, 0), (1, 1), (2, 0)]),
        (2, vec![(0, 0), (1, 8), (6, 0), (1, 0), (8, 0)]),
    ];
    assert_eq!(
        run_values(0..5, map_then_reject).unwrap(),
        vec![1, 15, 22, 29]
    );

    let stateful_fallback = vec![
        (0, vec![(0, 0), (1, 7), (4, 0), (1, 1), (2, 0)]),
        (1, vec![(0, 0), (1, 8), (6, 0), (1, 0), (8, 0)]),
        (3, vec![(1, 1)]),
    ];
    assert_eq!(run_values(0..10, stateful_fallback).unwrap(), vec![8]);

    let first_before_error = vec![
        (0, vec![(1, 10), (0, 0), (5, 0)]),
        (1, vec![(0, 0), (1, 0), (12, 0)]),
    ];
    assert_eq!(
        run_terminal([2, 0], first_before_error.clone(), 5).unwrap(),
        Some(5)
    );
    assert!(run_values([2, 0], first_before_error).is_err());
}

#[test]
fn common_float_expression_shapes_preserve_numeric_edges_and_errors() {
    let affine = vec![(0, vec![(0, 0.0), (1, 2.0), (4, 0.0), (1, -0.0), (2, 0.0)])];
    let mapped = run_f64(vec![-0.0, 0.0, 1.5, f64::NAN], affine).unwrap();
    for (actual, expected) in mapped.into_iter().zip([-0.0, 0.0, 3.0, f64::NAN]) {
        assert_same_f64(actual, expected);
    }

    let interval_and = vec![(
        1,
        vec![
            (0, 0.0),
            (1, -0.0),
            (12, 0.0),
            (0, 0.0),
            (1, f64::INFINITY),
            (10, 0.0),
            (14, 0.0),
        ],
    )];
    assert_eq!(
        run_f64(vec![f64::NAN, -0.0, 0.0, 1.0, f64::INFINITY], interval_and,).unwrap(),
        vec![1.0]
    );

    let interval_or = vec![(
        1,
        vec![
            (0, 0.0),
            (1, 0.0),
            (10, 0.0),
            (0, 0.0),
            (1, -0.0),
            (8, 0.0),
            (15, 0.0),
        ],
    )];
    let selected = run_f64(vec![f64::NAN, -0.0, 0.0, 1.0], interval_or).unwrap();
    assert_eq!(
        selected
            .iter()
            .map(|value| value.to_bits())
            .collect::<Vec<_>>(),
        vec![(-0.0_f64).to_bits(), 0.0_f64.to_bits()]
    );

    let division_by_zero = vec![(0, vec![(0, 0.0), (1, 0.0), (18, 0.0)])];
    assert!(run_f64(vec![1.0], division_by_zero).is_err());
}

#[test]
fn common_float_programs_prepare_specialized_kernels() {
    let affine = prepare_float_expression(vec![(0, 0.0), (1, 1.5), (4, 0.0), (1, 0.25), (2, 0.0)]);
    let comparison = prepare_float_expression(vec![(0, 0.0), (1, 2.5), (12, 0.0)]);
    let interval_and = prepare_float_expression(vec![
        (0, 0.0),
        (1, -1.0),
        (12, 0.0),
        (1, 3.0),
        (0, 0.0),
        (10, 0.0),
        (14, 0.0),
    ]);
    let interval_or = prepare_float_expression(vec![
        (0, 0.0),
        (1, 0.0),
        (10, 0.0),
        (0, 0.0),
        (1, -0.0),
        (8, 0.0),
        (15, 0.0),
    ]);
    let fallback = prepare_float_expression(vec![(0, 0.0), (1, 2.0), (18, 0.0)]);

    assert!(matches!(
        affine,
        PreparedFloatExpression::Affine {
            multiplier: 1.5,
            offset: 0.25,
        }
    ));
    assert!(matches!(
        comparison,
        PreparedFloatExpression::Comparison(comparison)
            if comparison.item_left
                && comparison.opcode == 12
                && comparison.operand.to_bits() == 2.5_f64.to_bits()
    ));
    assert!(matches!(
        interval_and,
        PreparedFloatExpression::ComparisonPair {
            left,
            right,
            boolean_opcode: 14,
        } if left.item_left
            && left.opcode == 12
            && left.operand.to_bits() == (-1.0_f64).to_bits()
            && !right.item_left
            && right.opcode == 10
            && right.operand.to_bits() == 3.0_f64.to_bits()
    ));
    assert!(matches!(
        interval_or,
        PreparedFloatExpression::ComparisonPair {
            left,
            right,
            boolean_opcode: 15,
        } if left.item_left
            && left.opcode == 10
            && left.operand.to_bits() == 0.0_f64.to_bits()
            && right.item_left
            && right.opcode == 8
            && right.operand.to_bits() == (-0.0_f64).to_bits()
    ));
    assert!(matches!(fallback, PreparedFloatExpression::Bytecode(_)));
}

#[test]
fn stateless_float_map_filter_preserves_numeric_edges_and_terminal_order() {
    let map_then_filter = vec![
        (0, vec![(0, 0.0), (1, 0.75), (4, 0.0), (1, -2.5), (2, 0.0)]),
        (
            1,
            vec![
                (0, 0.0),
                (1, -1.0),
                (12, 0.0),
                (0, 0.0),
                (1, 1.0),
                (10, 0.0),
                (14, 0.0),
            ],
        ),
    ];
    assert_eq!(
        run_f64(vec![0.0, 2.0, 4.0, 6.0], map_then_filter.clone()).unwrap(),
        vec![0.5]
    );
    assert_eq!(
        run_f64_terminal(vec![0.0, 2.0, 4.0, 6.0], map_then_filter, 1).unwrap(),
        Some(0.5)
    );

    let map_then_reject = vec![
        (0, vec![(0, 0.0), (1, 0.75), (4, 0.0), (1, -2.5), (2, 0.0)]),
        (
            2,
            vec![
                (0, 0.0),
                (1, -1.0),
                (12, 0.0),
                (0, 0.0),
                (1, 1.0),
                (10, 0.0),
                (14, 0.0),
            ],
        ),
    ];
    assert_eq!(
        run_f64(vec![0.0, 2.0, 4.0, 6.0], map_then_reject).unwrap(),
        vec![-2.5, -1.0, 2.0]
    );

    let stateful_fallback = vec![
        (0, vec![(0, 0.0), (1, 0.75), (4, 0.0), (1, -2.5), (2, 0.0)]),
        (1, vec![(0, 0.0), (1, -1.0), (12, 0.0)]),
        (3, vec![(1, 1.0)]),
    ];
    assert_eq!(
        run_f64(vec![0.0, 2.0, 4.0, 6.0], stateful_fallback).unwrap(),
        vec![0.5]
    );

    let keep_truthy = vec![
        (0, vec![(0, 0.0), (1, 1.0), (4, 0.0), (1, -0.0), (2, 0.0)]),
        (1, vec![(0, 0.0)]),
    ];
    let selected = run_f64(vec![f64::NAN, -0.0, 0.0, 1.0], keep_truthy).unwrap();
    assert!(selected[0].is_nan());
    assert_eq!(selected[1].to_bits(), 1.0_f64.to_bits());

    let keep_falsey = vec![
        (0, vec![(0, 0.0), (1, 1.0), (4, 0.0), (1, -0.0), (2, 0.0)]),
        (2, vec![(0, 0.0)]),
    ];
    let zeros = run_f64(vec![-0.0, 0.0, 1.0], keep_falsey).unwrap();
    assert_eq!(
        zeros
            .iter()
            .map(|value| value.to_bits())
            .collect::<Vec<_>>(),
        vec![(-0.0_f64).to_bits(), 0.0_f64.to_bits()]
    );
}

#[test]
fn specialized_float_expressions_match_the_canonical_bytecode_evaluator() {
    let values = [
        f64::NEG_INFINITY,
        -1.25,
        -0.0,
        0.0,
        1.25,
        f64::INFINITY,
        f64::NAN,
    ];
    let mut expressions = vec![vec![(0, 0.0), (1, 1.5), (4, 0.0), (1, -0.0), (2, 0.0)]];
    for opcode in 8..=13 {
        expressions.push(vec![(0, 0.0), (1, -0.0), (opcode, 0.0)]);
        expressions.push(vec![(1, -0.0), (0, 0.0), (opcode, 0.0)]);
    }
    expressions.extend([
        vec![
            (0, 0.0),
            (1, -1.0),
            (12, 0.0),
            (0, 0.0),
            (1, 1.0),
            (10, 0.0),
            (14, 0.0),
        ],
        vec![
            (1, -1.0),
            (0, 0.0),
            (12, 0.0),
            (0, 0.0),
            (1, 1.0),
            (13, 0.0),
            (15, 0.0),
        ],
    ]);

    for code in expressions {
        let mut stack = Vec::new();
        let expected = values
            .iter()
            .map(|value| evaluate_f64(*value, &code, &mut stack).unwrap())
            .collect::<Vec<_>>();
        let actual = run_f64(values, vec![(0, code)]).unwrap();
        for (actual, expected) in actual.into_iter().zip(expected) {
            assert_same_f64(actual, expected);
        }
    }
}

#[test]
fn exact_i64_frequencies_preserve_first_key_identity_and_order() {
    Python::initialize();
    Python::attach(|py| {
        let first = pyo3::types::PyInt::new(py, 1_000_000_i64);
        let equal = pyo3::types::PyInt::new(py, 1_000_000_i64);
        let later = pyo3::types::PyInt::new(py, -7_i64);
        assert!(!first.is(&equal));
        let source = PyList::new(py, [&first, &later, &equal]).unwrap();

        let result = frequencies_i64_exact_v1(source.as_any()).unwrap().unwrap();
        let counts = result.bind(py).cast_exact::<PyDict>().unwrap();

        assert_eq!(counts.len(), 2);
        assert!(counts.keys().get_item(0).unwrap().is(&first));
        assert!(counts.keys().get_item(1).unwrap().is(&later));
        assert_eq!(
            counts
                .get_item(&first)
                .unwrap()
                .unwrap()
                .extract::<usize>()
                .unwrap(),
            2
        );
        assert_eq!(
            counts
                .get_item(&later)
                .unwrap()
                .unwrap()
                .extract::<usize>()
                .unwrap(),
            1
        );
    });
}

#[test]
fn exact_i64_frequencies_decline_protocol_values_without_hashing() {
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"class Trap:\n    calls = 0\n    def __hash__(self):\n        type(self).calls += 1\n        raise AssertionError('native probe hashed custom key')\ntrap = Trap()\n",
            c"native_frequencies_safety.py",
            c"native_frequencies_safety",
        )
        .unwrap();
        let trap = fixture.getattr("trap").unwrap();
        let source = PyList::new(py, [&trap]).unwrap();

        let partial = frequencies_i64_exact_v1(source.as_any())
            .unwrap()
            .expect("an exact container should return resumable state");
        let partial = partial.bind(py).cast::<PyTuple>().unwrap();
        let remainder = partial.get_item(1).unwrap();
        assert!(remainder.call_method0("__next__").unwrap().is(&trap));
        assert_eq!(
            fixture
                .getattr("Trap")
                .unwrap()
                .getattr("calls")
                .unwrap()
                .extract::<usize>()
                .unwrap(),
            0
        );

        let boolean_source = PyList::new(py, [true]).unwrap();
        assert!(
            frequencies_i64_exact_v1(boolean_source.as_any())
                .unwrap()
                .unwrap()
                .bind(py)
                .is_instance_of::<PyTuple>()
        );
        let huge = py.eval(c"1 << 100", None, None).unwrap();
        let huge_source = PyList::new(py, [&huge]).unwrap();
        assert!(
            frequencies_i64_exact_v1(huge_source.as_any())
                .unwrap()
                .unwrap()
                .bind(py)
                .is_instance_of::<PyTuple>()
        );
    });
}

#[test]
fn exact_i64_frequencies_handle_256_distinct_groups() {
    Python::initialize();
    Python::attach(|py| {
        let source = PyList::new(py, 0_i64..256).unwrap();
        let counts = frequencies_i64_exact_v1(source.as_any())
            .unwrap()
            .expect("a bounded exact-i64 distribution should stay native");

        assert_eq!(counts.bind(py).len().unwrap(), 256);
    });
}

#[test]
fn exact_i64_frequencies_return_resumable_state_at_the_257th_group() {
    Python::initialize();
    Python::attach(|py| {
        let source = PyList::new(py, 0_i64..258).unwrap();
        let partial = frequencies_i64_exact_v1(source.as_any())
            .unwrap()
            .expect("an exact container should return resumable state");
        let partial = partial.bind(py).cast::<PyTuple>().unwrap();

        assert_eq!(partial.get_item(0).unwrap().len().unwrap(), 256);
        let remainder = partial.get_item(1).unwrap();
        assert_eq!(
            remainder
                .call_method0("__next__")
                .unwrap()
                .extract::<i64>()
                .unwrap(),
            256
        );
        assert_eq!(
            remainder
                .call_method0("__next__")
                .unwrap()
                .extract::<i64>()
                .unwrap(),
            257
        );
    });
}

#[test]
fn specialized_integer_programs_preserve_negative_and_overflow_semantics() {
    let program = vec![
        (0, vec![(0, 0), (1, 3), (4, 0), (1, 1), (2, 0)]),
        (1, vec![(0, 0), (1, 8), (6, 0), (1, 0), (8, 0)]),
    ];

    assert_eq!(run_values(-8..=8, program.clone()).unwrap(), vec![-8, 16]);
    assert!(run_values(vec![i64::MAX], program).is_err());
}

#[test]
fn pipeline_stages_are_fused_in_encounter_order() {
    let program = vec![
        (0, vec![(0, 0), (1, 3), (4, 0), (1, 1), (2, 0)]),
        (1, vec![(0, 0), (1, 2), (6, 0), (1, 0), (8, 0)]),
        (3, vec![(1, 4)]),
    ];

    assert_eq!(run_values(0..100, program).unwrap(), vec![4, 10, 16, 22]);
}

#[test]
fn terminals_consume_the_fused_pipeline_without_materializing() {
    let program = vec![(0, vec![(0, 0), (1, 2), (4, 0)])];

    assert_eq!(run_terminal(1..=4, program.clone(), 0).unwrap(), Some(4));
    assert_eq!(run_terminal(1..=4, program.clone(), 1).unwrap(), Some(20));
    assert_eq!(run_terminal(1..=4, program.clone(), 2).unwrap(), Some(2));
    assert_eq!(run_terminal(1..=4, program, 3).unwrap(), Some(8));
}

#[test]
fn terminals_retain_take_drop_and_filter_state_between_source_values() {
    let program = vec![
        (4, vec![(1, 2)]),
        (1, vec![(0, 0), (1, 2), (6, 0), (1, 0), (8, 0)]),
        (3, vec![(1, 2)]),
        (0, vec![(0, 0), (1, 1), (2, 0)]),
    ];

    assert_eq!(run_terminal(0..20, program.clone(), 5).unwrap(), Some(3));
    assert_eq!(run_terminal(0..20, program, 7).unwrap(), Some(1));
}

#[test]
fn short_circuit_terminals_never_pull_an_unneeded_tail() {
    let identity = vec![(0, vec![(0, 0)])];
    let dangerous_tail = || std::iter::once_with(|| panic!("tail was evaluated"));

    assert_eq!(
        run_terminal(
            std::iter::once(7).chain(dangerous_tail()),
            identity.clone(),
            5
        )
        .unwrap(),
        Some(7)
    );
    assert_eq!(
        run_terminal(
            std::iter::once(1).chain(dangerous_tail()),
            identity.clone(),
            6
        )
        .unwrap(),
        Some(1)
    );
    assert_eq!(
        run_terminal(
            std::iter::once(0).chain(dangerous_tail()),
            identity.clone(),
            7
        )
        .unwrap(),
        Some(0)
    );
    assert_eq!(
        run_terminal(vec![1, 2, 3], identity.clone(), 4).unwrap(),
        Some(3)
    );
    assert_eq!(
        run_terminal(Vec::new(), identity.clone(), 6).unwrap(),
        Some(0)
    );
    assert_eq!(run_terminal(Vec::new(), identity, 7).unwrap(), Some(1));

    let float_identity = vec![(0, vec![(0, 0.0)])];
    assert_eq!(
        run_f64_terminal(vec![1.5, 2.5], float_identity.clone(), 5).unwrap(),
        Some(1.5)
    );
    assert_eq!(
        run_f64_terminal(vec![0.0, 2.5], float_identity, 7).unwrap(),
        Some(0.0)
    );
}

#[test]
fn online_statistics_are_fused_and_use_a_compensated_mean() {
    let identity = vec![(0, vec![(0, 0)])];
    let (count, mean, squared_deviations) = run_i64_statistics(1..=4, identity).unwrap();
    assert_eq!(count, 4);
    assert_eq!(mean, 2.5);
    assert_eq!(squared_deviations, 5.0);

    let float_identity = vec![(0, vec![(0, 0.0)])];
    let (count, mean, _squared_deviations) =
        run_f64_statistics(vec![1e16, 1.0, -1e16], float_identity.clone()).unwrap();
    assert_eq!(count, 3);
    assert!((mean - 1.0 / 3.0).abs() < f64::EPSILON);
    assert_eq!(
        run_f64_statistics(Vec::new(), float_identity).unwrap(),
        (0, 0.0, 0.0)
    );
}

#[test]
fn dedicated_mean_terminals_preserve_fused_and_compensated_semantics() {
    let doubled = vec![(0, vec![(0, 0), (1, 2), (4, 0)])];
    assert_eq!(run_i64_mean(1..=4, doubled).unwrap(), Some(5.0));
    assert_eq!(run_i64_mean(Vec::new(), Vec::new()).unwrap(), None);

    let float_identity = vec![(0, vec![(0, 0.0)])];
    let cancellation = run_f64_mean(vec![1e16, 1.0, -1e16], float_identity).unwrap();
    assert_eq!(cancellation, Some(1.0 / 3.0));
    assert_eq!(run_f64_mean(Vec::new(), Vec::new()).unwrap(), None);
}

#[test]
fn aggregate_snapshot_computes_every_terminal_in_one_pipeline_pass() {
    let snapshot = run_i64_aggregate(1..=4, Vec::new()).unwrap();

    assert_eq!(
        snapshot,
        (4, 10, Some(1), Some(4), Some(1), Some(4), 2.5, 5.0)
    );
}

#[test]
fn aggregate_masks_compute_only_requested_fields_without_narrowing_integer_totals() {
    assert!(run_i64_aggregate_masked(Vec::new(), Vec::new(), 0).is_err());

    let total =
        run_i64_aggregate_masked(vec![i64::MAX, i64::MAX], Vec::new(), AGGREGATE_TOTAL).unwrap();
    assert_eq!(total.0, 0);
    assert_eq!(total.1, i128::from(i64::MAX) * 2);
    assert_eq!(total.2, None);
    assert_eq!(total.6, 0.0);

    let endpoints = run_i64_aggregate_masked(
        vec![4, -2, 7],
        Vec::new(),
        AGGREGATE_COUNT | AGGREGATE_MINIMUM | AGGREGATE_LAST,
    )
    .unwrap();
    assert_eq!(endpoints, (3, 0, Some(-2), None, None, Some(7), 0.0, 0.0));
}

#[test]
fn float_aggregate_masks_preserve_sequential_sum_and_compensated_statistics_fields() {
    let values = vec![1e16, 1.0, -1e16];
    let total = run_f64_aggregate_masked(values.clone(), Vec::new(), AGGREGATE_TOTAL).unwrap();
    assert_eq!(total, (0, 0.0, None, None, None, None, 0.0, 0.0));

    let statistics = run_f64_aggregate_masked(
        values,
        Vec::new(),
        AGGREGATE_COUNT | AGGREGATE_MEAN | AGGREGATE_M2,
    )
    .unwrap();
    assert_eq!(statistics.0, 3);
    assert!((statistics.6 - 1.0 / 3.0).abs() < f64::EPSILON);
    assert!(statistics.7 > 0.0);
}

#[test]
fn stable_distinct_composes_with_other_stages_and_terminals() {
    let program = vec![
        (0, vec![(0, 0), (1, 5), (6, 0)]),
        (5, vec![]),
        (1, vec![(0, 0), (1, 0), (12, 0)]),
    ];
    let values = vec![8, 3, 8, 5, 3, 2, 5, 9, 2, 9, 1, 8];

    assert_eq!(
        run_values(values.clone(), program.clone()).unwrap(),
        vec![3, 2, 4, 1]
    );
    assert_eq!(run_terminal(values, program, 1).unwrap(), Some(10));
}

#[test]
fn stable_distinct_preserves_signed_integer_boundaries() {
    let values = vec![
        i64::MIN,
        -1_000_003,
        0,
        i64::MAX,
        i64::MIN,
        0,
        i64::MAX,
        -1_000_003,
    ];

    assert_eq!(
        run_values(values, vec![(5, vec![])]).unwrap(),
        vec![i64::MIN, -1_000_003, 0, i64::MAX]
    );
}

#[test]
fn while_stages_preserve_longest_prefix_semantics() {
    let program = vec![
        (7, vec![(0, 0), (1, 3), (10, 0)]),
        (6, vec![(0, 0), (1, 6), (10, 0)]),
    ];

    assert_eq!(run_values(0..100, program.clone()).unwrap(), vec![3, 4, 5]);
    assert_eq!(run_terminal(0..100, program, 1).unwrap(), Some(12));

    let float_program = vec![
        (7, vec![(0, 0.0), (1, 1.5), (10, 0.0)]),
        (6, vec![(0, 0.0), (1, 3.0), (10, 0.0)]),
    ];
    assert_eq!(
        run_f64(vec![0.0, 1.0, 1.5, 2.0, 3.0, 4.0], float_program).unwrap(),
        vec![1.5, 2.0]
    );
}

#[test]
fn inclusive_take_sends_its_boundary_through_downstream_stages_then_stops() {
    let program = vec![
        (8, vec![(0, 0), (1, 6), (10, 0)]),
        (1, vec![(0, 0), (1, 4), (6, 0), (1, 0), (8, 0)]),
    ];

    assert_eq!(
        run_values(vec![0, 2, 4, 6, 8], program).unwrap(),
        vec![0, 4]
    );
}

#[test]
fn boolean_and_absolute_value_opcodes_compose() {
    let absolute_is_three = vec![(0, 0), (17, 0), (1, 3), (8, 0)];
    let nonzero_and_not_three = vec![
        (0, 0),
        (1, 0),
        (9, 0),
        (0, 0),
        (17, 0),
        (1, 3),
        (8, 0),
        (16, 0),
        (14, 0),
    ];

    assert_eq!(
        evaluate(-3, &absolute_is_three, &mut Vec::new()).unwrap(),
        1
    );
    assert_eq!(
        evaluate(-3, &nonzero_and_not_three, &mut Vec::new()).unwrap(),
        0
    );
    assert_eq!(
        evaluate(4, &nonzero_and_not_three, &mut Vec::new()).unwrap(),
        1
    );
}

#[test]
fn float_pipeline_and_terminals_are_fused() {
    let program = vec![
        (0, vec![(0, 0.0), (1, 1.5), (4, 0.0)]),
        (1, vec![(0, 0.0), (1, 3.0), (12, 0.0)]),
    ];

    assert_eq!(
        run_f64(vec![1.0, 2.0, 3.0], program.clone()).unwrap(),
        vec![4.5]
    );
    assert_eq!(
        run_f64_count(vec![1.0, 2.0, 3.0], program.clone()).unwrap(),
        1
    );
    assert_eq!(
        run_f64_terminal(vec![1.0, 2.0, 3.0], program, 1).unwrap(),
        Some(4.5)
    );
    let identity = vec![(0, vec![(0, 0.0)])];
    assert_eq!(
        run_f64_terminal(vec![1e16, 1.0, -1e16], identity.clone(), 1).unwrap(),
        Some(1.0)
    );
    assert_eq!(
        run_f64_terminal(vec![1e16, 1.0, -1e16], identity, 8).unwrap(),
        Some(0.0)
    );
}
