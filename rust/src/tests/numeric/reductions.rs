//! Numeric reduction edge cases and buffer-specialization coverage.

use super::*;

pub(super) fn legacy_compensated_total(values: &[f64], compensate: bool) -> f64 {
    let mut total: f64 = 0.0;
    let mut compensation = 0.0;
    for &value in values {
        let combined = total + value;
        if compensate && total.is_finite() && value.is_finite() && combined.is_finite() {
            compensation += if total.abs() >= value.abs() {
                total - combined + value
            } else {
                value - combined + total
            };
        } else if compensate {
            compensation = 0.0;
        }
        total = combined;
    }
    if compensate {
        total + compensation
    } else {
        total
    }
}

fn legacy_online_statistics(values: &[f64]) -> (u64, f64, f64) {
    let mut count = 0_u64;
    let mut total: f64 = 0.0;
    let mut compensation = 0.0;
    let mut rolling_mean = 0.0;
    let mut squared_deviations = 0.0;
    for &value in values {
        count += 1;
        let combined = total + value;
        if total.is_finite() && value.is_finite() && combined.is_finite() {
            compensation += if total.abs() >= value.abs() {
                total - combined + value
            } else {
                value - combined + total
            };
        } else {
            compensation = 0.0;
        }
        total = combined;

        let delta = value - rolling_mean;
        rolling_mean += delta / (count as f64);
        squared_deviations += delta * (value - rolling_mean);
    }
    let mean = if count == 0 {
        0.0
    } else {
        (total + compensation) / (count as f64)
    };
    (count, mean, squared_deviations)
}

#[test]
fn compensated_finite_check_preserves_legacy_numeric_edges_and_sum_opcodes() {
    let cases = [
        Vec::new(),
        vec![f64::NAN],
        vec![1.0, f64::NAN, 2.0],
        vec![f64::INFINITY],
        vec![f64::NEG_INFINITY],
        vec![f64::INFINITY, f64::NEG_INFINITY],
        vec![f64::MAX, f64::MAX],
        vec![0.0, -0.0],
        vec![-0.0, 0.0],
        vec![f64::from_bits(1), -f64::from_bits(1)],
        vec![1e16, 1.0, -1e16],
    ];
    let identity = vec![(0, vec![(0, 0.0)])];

    for values in cases {
        let expected_compensated = legacy_compensated_total(&values, true);
        let expected_sequential = legacy_compensated_total(&values, false);

        let mut compensated = CompensatedSum::default();
        for &value in &values {
            compensated.accept(value);
        }
        assert_same_f64(compensated.value(), expected_compensated);

        let mut statistics = OnlineStatistics::default();
        for &value in &values {
            statistics.accept(value).unwrap();
        }
        let expected_statistics = legacy_online_statistics(&values);
        let actual_statistics = statistics.snapshot();
        assert_eq!(actual_statistics.0, expected_statistics.0);
        assert_same_f64(actual_statistics.1, expected_statistics.1);
        assert_same_f64(actual_statistics.2, expected_statistics.2);

        for (terminal, expected) in [(1, expected_compensated), (8, expected_sequential)] {
            let actual_identity = run_f64_identity_terminal(values.clone(), terminal).unwrap();
            assert_eq!(actual_identity.0, values.len() as u64);
            assert_same_f64(actual_identity.1.unwrap(), expected);

            let actual_staged =
                run_f64_terminal(values.clone(), identity.clone(), terminal).unwrap();
            assert_same_f64(actual_staged.unwrap(), expected);
        }
    }
}

#[test]
fn affine_comparison_pair_sum_specialization_preserves_float_terminal_semantics() {
    let map = vec![(0, 0.0), (1, 1.0), (4, 0.0), (1, 0.0), (2, 0.0)];
    let comparison = |item_left: bool, opcode: u8, operand: f64| {
        if item_left {
            vec![(0, 0.0), (1, operand), (opcode, 0.0)]
        } else {
            vec![(1, operand), (0, 0.0), (opcode, 0.0)]
        }
    };
    let program = |left: Vec<FloatInstruction>, right: Vec<FloatInstruction>, filter_kind| {
        let mut predicate = left;
        predicate.extend(right);
        predicate.push((14, 0.0));
        vec![(0, map.clone()), (filter_kind, predicate)]
    };

    let values = vec![-2.0, -1.0, -0.0, 0.0, 1.0, 2.0, f64::NAN];
    let cases = [
        (
            program(comparison(true, 12, -1.0), comparison(true, 10, 2.0), 1),
            3,
            1.0,
        ),
        (
            program(comparison(false, 10, -1.0), comparison(false, 12, 2.0), 1),
            3,
            1.0,
        ),
        (
            program(comparison(true, 13, -1.0), comparison(true, 11, 2.0), 1),
            5,
            2.0,
        ),
        (
            program(comparison(false, 11, -1.0), comparison(false, 13, 2.0), 1),
            5,
            2.0,
        ),
        (
            program(comparison(true, 9, 0.0), comparison(true, 8, 1.0), 1),
            1,
            1.0,
        ),
        (
            program(comparison(false, 9, 0.0), comparison(false, 8, 1.0), 1),
            1,
            1.0,
        ),
        (
            program(comparison(true, 8, 0.0), comparison(true, 9, 1.0), 1),
            2,
            0.0,
        ),
        (
            program(comparison(true, 10, 2.0), comparison(true, 12, -1.0), 1),
            3,
            1.0,
        ),
        (
            program(comparison(true, 11, 2.0), comparison(true, 13, -1.0), 1),
            5,
            2.0,
        ),
    ];
    for (program, expected_count, expected_total) in cases {
        let actual = run_f64_affine_comparison_pair_sum::<_, true>(values.clone(), &program)
            .expect("the direct comparison pair must be recognized")
            .unwrap();
        assert_eq!(actual.0, expected_count);
        assert_same_optional_f64(actual.1, Some(expected_total));
    }

    let finite = program(
        comparison(true, 12, f64::NEG_INFINITY),
        comparison(true, 10, f64::INFINITY),
        1,
    );
    let compensated =
        run_f64_affine_comparison_pair_sum::<_, true>(vec![1e16, 1.0, -1e16], &finite)
            .unwrap()
            .unwrap();
    assert_eq!(compensated, (3, Some(1.0)));
    assert_eq!(
        run_f64_terminal(vec![1e16, 1.0, -1e16], finite.clone(), 8).unwrap(),
        Some(0.0),
    );

    let empty = program(comparison(true, 10, 0.0), comparison(true, 12, 0.0), 1);
    let empty_total = run_f64_affine_comparison_pair_sum::<_, true>(values.clone(), &empty)
        .unwrap()
        .unwrap();
    assert_eq!(empty_total.0, 0);
    assert_eq!(empty_total.1.unwrap().to_bits(), 0.0_f64.to_bits());

    let rejected_interval = program(comparison(true, 12, -1.0), comparison(true, 10, 2.0), 2);
    let rejected = run_f64_affine_comparison_pair_sum::<_, true>(values, &rejected_interval)
        .unwrap()
        .unwrap();
    assert_eq!(rejected.0, 4);
    assert!(rejected.1.unwrap().is_nan());

    let scaled_map = vec![(0, 0.0), (1, -2.0), (4, 0.0), (1, 0.5), (2, 0.0)];
    let mut scaled_predicate = comparison(true, 12, -1.0);
    scaled_predicate.extend(comparison(true, 10, 5.0));
    scaled_predicate.push((14, 0.0));
    let scaled_program = vec![(0, scaled_map), (1, scaled_predicate)];
    let scaled = run_f64_affine_comparison_pair_sum::<_, true>(
        vec![-2.0, -1.0, 0.0, 1.0, 2.0],
        &scaled_program,
    )
    .unwrap()
    .unwrap();
    assert_eq!(scaled.0, 3);
    assert_same_optional_f64(scaled.1, Some(7.5));

    let unsupported_program = vec![(0, vec![(0, 0.0)])];
    let untouched = vec![1.0, 2.0, 3.0].into_iter();
    let untouched =
        match run_f64_affine_comparison_pair_sum::<_, true>(untouched, &unsupported_program) {
            Err(untouched) => untouched,
            Ok(_) => panic!("an unsupported program must retain its untouched iterator"),
        };
    assert_eq!(untouched.collect::<Vec<_>>(), vec![1.0, 2.0, 3.0]);
}

#[test]
fn direct_f64_identity_reducers_preserve_empty_cancellation_nan_and_signed_zero() {
    assert_eq!(run_f64_identity_terminal(Vec::new(), 0).unwrap(), (0, None));
    assert!(run_f64_identity_terminal(Vec::new(), 9).is_err());
    assert!(run_f64_identity_aggregate_masked(Vec::new(), 0).is_err());
    assert_eq!(
        run_f64_identity_terminal(vec![1e16, 1.0, -1e16], 1).unwrap(),
        (3, Some(1.0))
    );
    assert_eq!(
        run_f64_identity_terminal(vec![1e16, 1.0, -1e16], 8).unwrap(),
        (3, Some(0.0))
    );
    assert_eq!(
        run_f64_identity_terminal(Vec::new(), 1).unwrap(),
        (0, Some(0.0))
    );
    assert_eq!(
        run_f64_identity_mean(vec![1e16, 1.0, -1e16]).unwrap(),
        Some(1.0 / 3.0)
    );
    assert_eq!(run_f64_identity_mean(Vec::new()).unwrap(), None);

    let zeros =
        run_f64_identity_aggregate_masked(vec![-0.0, 0.0], AGGREGATE_MINIMUM | AGGREGATE_MAXIMUM)
            .unwrap();
    assert_eq!(zeros.2.unwrap().to_bits(), (-0.0_f64).to_bits());
    assert_eq!(zeros.3.unwrap().to_bits(), (-0.0_f64).to_bits());

    let nan = run_f64_identity_aggregate_masked(
        vec![f64::NAN, 1.0, 2.0],
        AGGREGATE_MINIMUM | AGGREGATE_MAXIMUM,
    )
    .unwrap();
    assert!(nan.2.unwrap().is_nan());
    assert!(nan.3.unwrap().is_nan());
}

#[test]
fn known_length_i64_mean_matches_streaming_numeric_semantics() {
    let integer_cases = [
        Vec::new(),
        vec![1, 2, 3],
        vec![2_i64.pow(53) + 1, -(2_i64.pow(53))],
        vec![2_i64.pow(53), 1, -(2_i64.pow(53))],
        vec![-(2_i64.pow(53)), -1, 2_i64.pow(53)],
        vec![7, -3, 11, 2_i64.pow(53), 1, -(2_i64.pow(53))],
        vec![i64::MIN, i64::MAX],
        vec![i64::MAX, i64::MAX, i64::MIN, i64::MIN],
    ];
    for values in integer_cases {
        assert_same_optional_f64(
            run_i64_identity_mean_by(&values, |value| *value).unwrap(),
            run_i64_identity_mean(values).unwrap(),
        );
    }
}

#[test]
fn f64_buffer_identity_specialization_does_not_bypass_expression_stages() {
    Python::initialize();
    Python::attach(|py| {
        let source = f64_array(py, vec![1.0, 2.0, 3.0]);
        let doubled = vec![(0, vec![(0, 0.0), (1, 2.0), (4, 0.0)])];

        assert_eq!(
            terminal_f64_buffer_v1(py, &source, doubled.clone(), 1).unwrap(),
            (3, Some(12.0))
        );
        assert_eq!(
            mean_f64_buffer_v1(py, &source, doubled.clone()).unwrap(),
            Some(4.0)
        );
        assert_eq!(
            aggregate_f64_buffer_masked_v1(py, &source, doubled, u8::MAX).unwrap(),
            (
                3,
                12.0,
                Some(2.0),
                Some(6.0),
                Some(2.0),
                Some(6.0),
                4.0,
                8.0,
            )
        );
    });
}

#[test]
fn f64_buffer_execute_and_materialize_run_the_fused_program() {
    Python::initialize();
    Python::attach(|py| {
        let source = f64_array(py, vec![1.0, 2.0, 3.0, 4.0]);
        let program = vec![
            (0, vec![(0, 0.0), (1, 1.5), (4, 0.0)]),
            (1, vec![(0, 0.0), (1, 3.0), (12, 0.0)]),
        ];

        assert_eq!(
            execute_f64_buffer_v1(py, &source, program.clone()).unwrap(),
            vec![4.5, 6.0]
        );
        let materialized = materialize_f64_buffer_v1(py, &source, program, 1).unwrap();
        assert_eq!(
            materialized.bind(py).extract::<(f64, f64)>().unwrap(),
            (4.5, 6.0)
        );
    });
}

#[test]
fn f64_buffer_aggregate_keeps_sequential_total_and_compensated_statistics() {
    Python::initialize();
    Python::attach(|py| {
        let source = f64_array(py, vec![1e16, 1.0, -1e16]);

        assert_eq!(
            mean_f64_buffer_v1(py, &source, Vec::new()).unwrap(),
            Some(1.0 / 3.0)
        );
        let mask = AGGREGATE_COUNT | AGGREGATE_TOTAL | AGGREGATE_MEAN | AGGREGATE_M2;
        let snapshot = aggregate_f64_buffer_masked_v1(py, &source, Vec::new(), mask).unwrap();
        assert_eq!(snapshot.0, 3);
        assert_eq!(snapshot.1, 0.0);
        assert_eq!(snapshot.6, 1.0 / 3.0);
        assert_eq!(snapshot.7, 1.9999999999999997e32);

        let empty = f64_array(py, Vec::new());
        assert_eq!(mean_f64_buffer_v1(py, &empty, Vec::new()).unwrap(), None);
        assert_eq!(
            aggregate_f64_buffer_masked_v1(py, &empty, Vec::new(), mask).unwrap(),
            (0, 0.0, None, None, None, None, 0.0, 0.0)
        );
    });
}

#[test]
fn f64_buffer_terminal_reports_empty_output_and_preserves_both_sum_modes() {
    Python::initialize();
    Python::attach(|py| {
        let cancellation = f64_array(py, vec![1e16, 1.0, -1e16]);
        assert_eq!(
            terminal_f64_buffer_v1(py, &cancellation, Vec::new(), 1).unwrap(),
            (3, Some(1.0))
        );
        assert_eq!(
            terminal_f64_buffer_v1(py, &cancellation, Vec::new(), 8).unwrap(),
            (3, Some(0.0))
        );

        let positive = f64_array(py, vec![1.0, 2.0, 3.0]);
        let reject_all = vec![(1, vec![(0, 0.0), (1, 0.0), (10, 0.0)])];
        assert_eq!(
            terminal_f64_buffer_v1(py, &positive, reject_all, 1).unwrap(),
            (0, Some(0.0))
        );
    });
}

#[test]
fn f64_buffer_rejects_non_1d_and_noncontiguous_views() {
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"import array\nvalues = array.array('d', [1.0, 2.0, 3.0, 4.0])\nmultidimensional = memoryview(values).cast('B').cast('d', shape=(2, 2))\nstrided = memoryview(values)[::2]\n",
            c"f64_buffer_layouts.py",
            c"f64_buffer_layouts",
        )
        .unwrap();

        for name in ["multidimensional", "strided"] {
            let error =
                execute_f64_buffer_v1(py, &fixture.getattr(name).unwrap(), Vec::new()).unwrap_err();
            assert!(error.to_string().contains("one C-contiguous dimension"));
        }
    });
}

#[test]
fn f64_buffer_rejects_wrong_width_and_opposite_endian_values() {
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"import array\nimport ctypes\nimport sys\nwrong_width = array.array('f', [1.0, 2.0])\nopposite_type = ctypes.c_double.__ctype_be__ if sys.byteorder == 'little' else ctypes.c_double.__ctype_le__\nopposite = (opposite_type * 2)(1.0, 2.0)\n",
            c"f64_buffer_formats.py",
            c"f64_buffer_formats",
        )
        .unwrap();

        let wrong_width =
            execute_f64_buffer_v1(py, &fixture.getattr("wrong_width").unwrap(), Vec::new())
                .unwrap_err();
        assert!(wrong_width.to_string().contains("64-bit"));

        let opposite = execute_f64_buffer_v1(py, &fixture.getattr("opposite").unwrap(), Vec::new())
            .unwrap_err();
        assert!(opposite.to_string().contains("native-endian"));
    });
}
