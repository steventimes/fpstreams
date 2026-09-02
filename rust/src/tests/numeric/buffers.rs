//! Buffer lifetime, staged execution, mean, and aggregate coverage.

use super::*;

pub(super) fn assert_same_f64(actual: f64, expected: f64) {
    if expected.is_nan() {
        assert!(actual.is_nan());
    } else {
        assert_eq!(actual.to_bits(), expected.to_bits());
    }
}

#[test]
fn i64_mean_exact_prefix_and_compensated_boundary_preserve_sequential_state() {
    use crate::numeric_mean::{CompensatedMean, update_i64_mean_state};

    const LIMIT: i64 = 1_i64 << 53;

    let exact = update_i64_mean_state(0, 0.0, 0.0, [LIMIT - 2, 1, -(LIMIT - 3)]).unwrap();
    assert_eq!(exact, (3, 2.0, 0.0));
    assert_eq!(
        (exact.1 / exact.0 as f64).to_bits(),
        (2.0_f64 / 3.0).to_bits()
    );

    let positive_boundary = update_i64_mean_state(0, 0.0, 0.0, [LIMIT, 1, -LIMIT]).unwrap();
    assert_eq!(positive_boundary, (3, 0.0, 1.0));
    assert_eq!(
        ((positive_boundary.1 + positive_boundary.2) / positive_boundary.0 as f64).to_bits(),
        (1.0_f64 / 3.0).to_bits()
    );

    let negative_boundary = update_i64_mean_state(0, 0.0, 0.0, [-LIMIT, -1, LIMIT]).unwrap();
    assert_eq!(negative_boundary, (3, 0.0, -1.0));
    assert_eq!(
        ((negative_boundary.1 + negative_boundary.2) / negative_boundary.0 as f64).to_bits(),
        (-1.0_f64 / 3.0).to_bits()
    );

    let untouched = update_i64_mean_state(2, -0.0, -0.0, []).unwrap();
    assert_eq!(untouched.0, 2);
    assert_eq!(untouched.1.to_bits(), (-0.0_f64).to_bits());
    assert_eq!(untouched.2.to_bits(), (-0.0_f64).to_bits());

    let mut small_random = Vec::with_capacity(20_000);
    let mut random_state = 0x4d59_5df4_d0f3_3173_u64;
    for _ in 0..20_000 {
        random_state ^= random_state << 13;
        random_state ^= random_state >> 7;
        random_state ^= random_state << 17;
        small_random.push((random_state % 2_001) as i64 - 1_000);
    }

    let mut full_range = Vec::with_capacity(2_000);
    for _ in 0..2_000 {
        random_state ^= random_state << 13;
        random_state ^= random_state >> 7;
        random_state ^= random_state << 17;
        full_range.push(random_state as i64);
    }

    for values in [
        vec![0, -1, 1, LIMIT, 1, -LIMIT, i64::MIN, i64::MAX],
        vec![-1, LIMIT + 1],
        vec![1, -(LIMIT + 1)],
        vec![-1, (1_i64 << 54) + 1, -((1_i64 << 54) + 1)],
        small_random,
        full_range,
    ] {
        let mut expected = CompensatedMean::default();
        let mut actual = (0, 0.0, 0.0);
        for value in values {
            expected.accept(value as f64).unwrap();
            actual = update_i64_mean_state(actual.0, actual.1, actual.2, [value]).unwrap();
            let expected_state = expected.state();
            assert_eq!(actual.0, expected_state.0);
            assert_eq!(actual.1.to_bits(), expected_state.1.to_bits());
            assert_eq!(actual.2.to_bits(), expected_state.2.to_bits());
        }
        let actual_value = (actual.0 != 0).then(|| (actual.1 + actual.2) / actual.0 as f64);
        assert_eq!(
            actual_value.map(f64::to_bits),
            expected.value().map(f64::to_bits)
        );
    }
}

pub(super) fn assert_same_optional_f64(actual: Option<f64>, expected: Option<f64>) {
    match (actual, expected) {
        (Some(actual), Some(expected)) => assert_same_f64(actual, expected),
        (None, None) => {}
        pair => panic!("optional f64 mismatch: {pair:?}"),
    }
}

pub(super) fn assert_same_f64_snapshot(
    actual: F64AggregateSnapshot,
    expected: F64AggregateSnapshot,
) {
    assert_eq!(actual.0, expected.0);
    assert_same_f64(actual.1, expected.1);
    assert_same_optional_f64(actual.2, expected.2);
    assert_same_optional_f64(actual.3, expected.3);
    assert_same_optional_f64(actual.4, expected.4);
    assert_same_optional_f64(actual.5, expected.5);
    assert_same_f64(actual.6, expected.6);
    assert_same_f64(actual.7, expected.7);
}

#[test]
fn empty_buffer_materialization_program_reuses_the_owned_snapshot() {
    let integers = vec![1_i64, 2, 3, 4];
    let integer_storage = integers.as_ptr();
    let integer_output = run_i64_buffer_materialization(integers, Vec::new()).unwrap();
    assert_eq!(integer_output.as_ptr(), integer_storage);

    let floats = vec![1.0_f64, -0.0, f64::NAN, f64::INFINITY];
    let float_storage = floats.as_ptr();
    let float_output = run_f64_buffer_materialization(floats, Vec::new()).unwrap();
    assert_eq!(float_output.as_ptr(), float_storage);
}

#[test]
fn i64_identity_aggregate_accepts_a_borrowed_value_iterator() {
    let values = [-3_i64, 4, 9];

    assert_eq!(
        run_i64_identity_aggregate_masked(values.iter().copied(), AGGREGATE_TOTAL).unwrap(),
        (0, 10, None, None, None, None, 0.0, 0.0),
    );
}

#[test]
#[cfg(not(Py_GIL_DISABLED))]
fn i64_slice_extrema_preserve_empty_singleton_order_remainders_and_boundaries() {
    let cases: &[(&[i64], Option<i64>, Option<i64>)] = &[
        (&[], None, None),
        (&[17], Some(17), Some(17)),
        (&[8, -4], Some(-4), Some(8)),
        (&[8, -4, 12], Some(-4), Some(12)),
        (&[8, -4, 12, 3], Some(-4), Some(12)),
        (&[-9, -3, 0, 4, 17], Some(-9), Some(17)),
        (&[17, 4, 0, -3, -9], Some(-9), Some(17)),
        (&[5, -11, 9, 0, 3, -7, 8, 2, 1], Some(-11), Some(9)),
        (
            &[i64::MAX, 0, i64::MIN, i64::MAX - 1, i64::MIN + 1],
            Some(i64::MIN),
            Some(i64::MAX),
        ),
    ];

    for &(values, expected_minimum, expected_maximum) in cases {
        assert_eq!(
            crate::integer::reduce_i64_min_by(values, |value| *value),
            expected_minimum,
        );
        assert_eq!(
            crate::integer::reduce_i64_max_by(values, |value| *value),
            expected_maximum,
        );
    }
}

#[test]
fn i64_buffer_v2_matches_the_snapshot_endpoints() {
    Python::initialize();
    Python::attach(|py| {
        let source = i64_array(py, vec![i64::MIN, -1, 0, 7, i64::MAX]);
        for mask in 1..=u8::MAX {
            assert_eq!(
                aggregate_i64_buffer_masked_v2(py, &source, Vec::new(), mask).unwrap(),
                aggregate_i64_buffer_masked_v1(py, &source, Vec::new(), mask).unwrap(),
            );
        }
        assert_eq!(
            mean_i64_buffer_v2(py, &source, Vec::new()).unwrap(),
            mean_i64_buffer_v1(py, &source, Vec::new()).unwrap(),
        );

        let empty_fixture = PyModule::from_code(
            py,
            c"import array\nstorage = array.array('q', [1])\nempty = memoryview(storage)[:0]\n",
            c"empty_i64_buffer.py",
            c"empty_i64_buffer",
        )
        .unwrap();
        let empty = empty_fixture.getattr("empty").unwrap();
        assert_eq!(
            aggregate_i64_buffer_masked_v2(py, &empty, Vec::new(), u8::MAX).unwrap(),
            aggregate_i64_buffer_masked_v1(py, &empty, Vec::new(), u8::MAX).unwrap(),
        );
        assert_eq!(
            mean_i64_buffer_v2(py, &empty, Vec::new()).unwrap(),
            mean_i64_buffer_v1(py, &empty, Vec::new()).unwrap(),
        );

        let staged_source = i64_array(py, vec![1, 2, 3]);
        let doubled = vec![(0, vec![(0, 0), (1, 2), (4, 0)])];
        assert_eq!(
            aggregate_i64_buffer_masked_v2(py, &staged_source, doubled.clone(), u8::MAX).unwrap(),
            aggregate_i64_buffer_masked_v1(py, &staged_source, doubled.clone(), u8::MAX).unwrap(),
        );
        assert_eq!(
            mean_i64_buffer_v2(py, &staged_source, doubled.clone()).unwrap(),
            mean_i64_buffer_v1(py, &staged_source, doubled).unwrap(),
        );
    });
}

#[test]
#[cfg(not(Py_GIL_DISABLED))]
fn staged_i64_buffer_v2_keeps_the_live_export_until_the_scan_finishes() {
    let _guard = BUFFER_GIL_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"import array\nimport sys\nimport threading\naggregate_values = array.array('q', [1]) * 2_000_000\nmean_values = array.array('q', [1]) * 2_000_000\naggregate_go = threading.Event()\naggregate_finished = threading.Event()\nmean_go = threading.Event()\nmean_finished = threading.Event()\nprevious_switch_interval = sys.getswitchinterval()\nsys.setswitchinterval(1000.0)\ndef mutate_aggregate_after_start():\n    aggregate_go.wait()\n    aggregate_values.append(9)\n    aggregate_finished.set()\ndef mutate_mean_after_start():\n    mean_go.wait()\n    mean_values.append(9)\n    mean_finished.set()\naggregate_worker = threading.Thread(target=mutate_aggregate_after_start)\nmean_worker = threading.Thread(target=mutate_mean_after_start)\naggregate_worker.start()\nmean_worker.start()\n",
            c"staged_i64_buffer_v2_export.py",
            c"staged_i64_buffer_v2_export",
        )
        .unwrap();
        let aggregate_values = fixture.getattr("aggregate_values").unwrap();
        let mean_values = fixture.getattr("mean_values").unwrap();
        let aggregate_go = fixture.getattr("aggregate_go").unwrap();
        let aggregate_finished = fixture.getattr("aggregate_finished").unwrap();
        let mean_go = fixture.getattr("mean_go").unwrap();
        let mean_finished = fixture.getattr("mean_finished").unwrap();
        let identity = vec![(0, vec![(0, 0)])];

        aggregate_go.call_method0("set").unwrap();
        assert_eq!(
            aggregate_i64_buffer_masked_v2(
                py,
                &aggregate_values,
                identity.clone(),
                AGGREGATE_TOTAL,
            )
            .unwrap(),
            (0, 2_000_000, None, None, None, None, 0.0, 0.0),
        );
        let aggregate_finished_before_return = aggregate_finished
            .call_method0("is_set")
            .unwrap()
            .is_truthy()
            .unwrap();

        mean_go.call_method0("set").unwrap();
        assert_eq!(
            mean_i64_buffer_v2(py, &mean_values, identity).unwrap(),
            Some(1.0),
        );
        let mean_finished_before_return = mean_finished
            .call_method0("is_set")
            .unwrap()
            .is_truthy()
            .unwrap();

        fixture
            .getattr("sys")
            .unwrap()
            .call_method1(
                "setswitchinterval",
                (fixture.getattr("previous_switch_interval").unwrap(),),
            )
            .unwrap();
        fixture
            .getattr("aggregate_worker")
            .unwrap()
            .call_method0("join")
            .unwrap();
        fixture
            .getattr("mean_worker")
            .unwrap()
            .call_method0("join")
            .unwrap();

        assert!(
            !aggregate_finished_before_return,
            "a staged aggregate v2 scan released the GIL and live export before it completed",
        );
        assert!(
            !mean_finished_before_return,
            "a staged mean v2 scan released the GIL and live export before it completed",
        );
        assert_eq!(aggregate_values.len().unwrap(), 2_000_001);
        assert_eq!(mean_values.len().unwrap(), 2_000_001);
    });
}

#[test]
#[cfg(not(Py_GIL_DISABLED))]
fn staged_i64_buffer_v2_releases_the_export_before_returning_an_error() {
    Python::initialize();
    Python::attach(|py| {
        let doubled = vec![(0, vec![(0, 0), (1, 2), (4, 0)])];

        let aggregate_values = i64_array(py, vec![i64::MAX]);
        let aggregate_error =
            aggregate_i64_buffer_masked_v2(py, &aggregate_values, doubled.clone(), AGGREGATE_TOTAL)
                .unwrap_err();
        assert!(aggregate_error.is_instance_of::<pyo3::exceptions::PyOverflowError>(py));
        aggregate_values.call_method1("append", (4,)).unwrap();

        let mean_values = i64_array(py, vec![i64::MAX]);
        let mean_error = mean_i64_buffer_v2(py, &mean_values, doubled).unwrap_err();
        assert!(mean_error.is_instance_of::<pyo3::exceptions::PyOverflowError>(py));
        mean_values.call_method1("append", (4,)).unwrap();

        assert_eq!(aggregate_values.len().unwrap(), 2);
        assert_eq!(mean_values.len().unwrap(), 2);
    });
}

#[test]
fn mean_buffer_state_updates_preserve_cross_buffer_order() {
    Python::initialize();
    Python::attach(|py| {
        let first = f64_array(py, vec![1e16]);
        let middle = f64_array(py, vec![1.0]);
        let last = f64_array(py, vec![-1e16]);

        let state = update_mean_f64_buffer_v1(py, &first, 0, 0.0, 0.0).unwrap();
        let state = update_mean_f64_buffer_v1(py, &middle, state.0, state.1, state.2).unwrap();
        let state = update_mean_f64_buffer_v1(py, &last, state.0, state.1, state.2).unwrap();

        assert_eq!(state.0, 3);
        assert_eq!(state.1.to_bits(), 0.0_f64.to_bits());
        assert_eq!(state.2.to_bits(), 1.0_f64.to_bits());
        assert_eq!((state.1 + state.2) / (state.0 as f64), 1.0 / 3.0);

        let integer_first = i64_array(py, vec![2_i64.pow(53) + 1]);
        let integer_last = i64_array(py, vec![-(2_i64.pow(53))]);
        let state = update_mean_i64_buffer_v1(py, &integer_first, 0, 0.0, 0.0).unwrap();
        let state =
            update_mean_i64_buffer_v1(py, &integer_last, state.0, state.1, state.2).unwrap();

        assert_eq!(state, (2, 0.0, 0.0));
    });
}

#[test]
fn mean_buffer_state_updates_preserve_empty_and_existing_state() {
    Python::initialize();
    Python::attach(|py| {
        let empty_i64_fixture = PyModule::from_code(
            py,
            c"import array\nstorage = array.array('q', [1])\nempty = memoryview(storage)[:0]\n",
            c"empty_mean_state_i64_buffer.py",
            c"empty_mean_state_i64_buffer",
        )
        .unwrap();
        let empty_i64 = empty_i64_fixture.getattr("empty").unwrap();
        let empty_f64 = f64_array(py, Vec::new());
        let initial = (7, -0.0_f64, 0.25_f64);

        let integer =
            update_mean_i64_buffer_v1(py, &empty_i64, initial.0, initial.1, initial.2).unwrap();
        let floating =
            update_mean_f64_buffer_v1(py, &empty_f64, initial.0, initial.1, initial.2).unwrap();

        assert_eq!(integer.0, initial.0);
        assert_eq!(integer.1.to_bits(), initial.1.to_bits());
        assert_eq!(integer.2.to_bits(), initial.2.to_bits());
        assert_eq!(floating.0, initial.0);
        assert_eq!(floating.1.to_bits(), initial.1.to_bits());
        assert_eq!(floating.2.to_bits(), initial.2.to_bits());

        let values = f64_array(py, vec![4.0, -2.0, 3.0]);
        let state = update_mean_f64_buffer_v1(py, &values, 0, 0.0, 0.0).unwrap();
        assert_eq!(state.0, 3);
        assert_eq!(
            (state.1 + state.2) / (state.0 as f64),
            mean_f64_buffer_v1(py, &values, Vec::new())
                .unwrap()
                .unwrap(),
        );
    });
}

#[test]
fn sequential_f64_sum_buffer_updates_preserve_order_edges_and_existing_state() {
    Python::initialize();
    Python::attach(|py| {
        let cases = [
            Vec::new(),
            vec![1e16, 1.0, -1e16],
            vec![f64::NAN, 1.0, 2.0],
            vec![1.0, f64::NAN, 2.0],
            vec![f64::INFINITY],
            vec![f64::NEG_INFINITY],
            vec![f64::INFINITY, f64::NEG_INFINITY],
            vec![f64::MAX, f64::MAX],
            vec![0.0, -0.0],
            vec![-0.0, 0.0],
            vec![f64::from_bits(1), -f64::from_bits(1)],
        ];

        for values in cases {
            let expected = legacy_compensated_total(&values, false);
            for split in 0..=values.len() {
                let first = f64_array(py, values[..split].to_vec());
                let second = f64_array(py, values[split..].to_vec());
                let total = crate::numeric_mean::update_sum_f64_buffer_v1(py, &first, 0.0).unwrap();
                let total =
                    crate::numeric_mean::update_sum_f64_buffer_v1(py, &second, total).unwrap();
                assert_same_f64(total, expected);
            }
        }

        let empty = f64_array(py, Vec::new());
        let unchanged = crate::numeric_mean::update_sum_f64_buffer_v1(py, &empty, -0.0).unwrap();
        assert_eq!(unchanged.to_bits(), (-0.0_f64).to_bits());

        let incompatible = i64_array(py, vec![1, 2, 3]);
        let error =
            crate::numeric_mean::update_sum_f64_buffer_v1(py, &incompatible, 0.0).unwrap_err();
        assert!(error.is_instance_of::<pyo3::exceptions::PyTypeError>(py));
        incompatible.call_method1("append", (4,)).unwrap();
        assert_eq!(incompatible.len().unwrap(), 4);
    });
}

#[test]
#[cfg(not(Py_GIL_DISABLED))]
fn sequential_f64_sum_buffer_update_keeps_the_live_export_until_the_scan_finishes() {
    let _guard = BUFFER_GIL_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"import array\nimport sys\nimport threading\nvalues = array.array('d', [1.0]) * 2_000_000\ngo = threading.Event()\nfinished = threading.Event()\nprevious_switch_interval = sys.getswitchinterval()\nsys.setswitchinterval(1000.0)\ndef mutate_after_start():\n    go.wait()\n    values.append(9.0)\n    finished.set()\nworker = threading.Thread(target=mutate_after_start)\nworker.start()\n",
            c"sequential_f64_sum_buffer_export.py",
            c"sequential_f64_sum_buffer_export",
        )
        .unwrap();
        let values = fixture.getattr("values").unwrap();
        let go = fixture.getattr("go").unwrap();
        let finished = fixture.getattr("finished").unwrap();

        go.call_method0("set").unwrap();
        let result = crate::numeric_mean::update_sum_f64_buffer_v1(py, &values, 0.0).unwrap();
        let finished_before_return = finished
            .call_method0("is_set")
            .unwrap()
            .is_truthy()
            .unwrap();

        fixture
            .getattr("sys")
            .unwrap()
            .call_method1(
                "setswitchinterval",
                (fixture.getattr("previous_switch_interval").unwrap(),),
            )
            .unwrap();
        fixture
            .getattr("worker")
            .unwrap()
            .call_method0("join")
            .unwrap();

        assert_eq!(result, 2_000_000.0);
        assert!(
            !finished_before_return,
            "the sequential sum scan released the GIL before it completed",
        );
        assert_eq!(values.len().unwrap(), 2_000_001);
    });
}

#[test]
fn f64_buffer_v2_preserves_identity_and_fallback_results() {
    Python::initialize();
    Python::attach(|py| {
        let cancellation = f64_array(py, vec![1e16, 1.0, -1e16]);
        assert_eq!(
            terminal_f64_buffer_v2(py, &cancellation, Vec::new(), 1).unwrap(),
            (3, Some(1.0)),
        );
        assert_eq!(
            terminal_f64_buffer_v2(py, &cancellation, Vec::new(), 8).unwrap(),
            (3, Some(0.0)),
        );
        assert_eq!(
            mean_f64_buffer_v2(py, &cancellation, Vec::new()).unwrap(),
            Some(1.0 / 3.0),
        );

        let mask = AGGREGATE_COUNT | AGGREGATE_TOTAL | AGGREGATE_MEAN | AGGREGATE_M2;
        assert_same_f64_snapshot(
            aggregate_f64_buffer_masked_v2(py, &cancellation, Vec::new(), mask).unwrap(),
            (
                3,
                0.0,
                None,
                None,
                None,
                None,
                1.0 / 3.0,
                1.9999999999999997e32,
            ),
        );

        let empty = f64_array(py, Vec::new());
        assert_eq!(
            terminal_f64_buffer_v2(py, &empty, Vec::new(), 1).unwrap(),
            (0, Some(0.0)),
        );
        assert_eq!(mean_f64_buffer_v2(py, &empty, Vec::new()).unwrap(), None);
        assert_same_f64_snapshot(
            aggregate_f64_buffer_masked_v2(py, &empty, Vec::new(), u8::MAX).unwrap(),
            (0, 0.0, None, None, None, None, 0.0, 0.0),
        );

        let staged_source = f64_array(py, vec![1.0, 2.0, 3.0]);
        let doubled = vec![(0, vec![(0, 0.0), (1, 2.0), (4, 0.0)])];
        assert_eq!(
            terminal_f64_buffer_v2(py, &staged_source, doubled.clone(), 1).unwrap(),
            terminal_f64_buffer_v1(py, &staged_source, doubled.clone(), 1).unwrap(),
        );
        assert_same_optional_f64(
            mean_f64_buffer_v2(py, &staged_source, doubled.clone()).unwrap(),
            mean_f64_buffer_v1(py, &staged_source, doubled.clone()).unwrap(),
        );
        assert_same_f64_snapshot(
            aggregate_f64_buffer_masked_v2(py, &staged_source, doubled.clone(), u8::MAX).unwrap(),
            aggregate_f64_buffer_masked_v1(py, &staged_source, doubled, u8::MAX).unwrap(),
        );
    });
}

#[test]
#[cfg(not(Py_GIL_DISABLED))]
fn staged_f64_buffer_v2_terminal_and_mean_keep_the_live_export_until_each_scan_finishes() {
    let _guard = BUFFER_GIL_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"import array\nimport sys\nimport threading\nvalues = array.array('d', [1.0]) * 2_000_000\nprevious_switch_interval = sys.getswitchinterval()\nsys.setswitchinterval(1000.0)\ndef start_mutation():\n    go = threading.Event()\n    finished = threading.Event()\n    def mutate_after_start():\n        go.wait()\n        values.append(9.0)\n        finished.set()\n    worker = threading.Thread(target=mutate_after_start)\n    worker.start()\n    return go, finished, worker\n",
            c"staged_f64_buffer_v2_export.py",
            c"staged_f64_buffer_v2_export",
        )
        .unwrap();
        let values = fixture.getattr("values").unwrap();
        let accept_all = vec![
            (0, vec![(0, 0.0), (1, 1.0), (4, 0.0), (1, 0.0), (2, 0.0)]),
            (
                1,
                vec![
                    (0, 0.0),
                    (1, 0.0),
                    (12, 0.0),
                    (0, 0.0),
                    (1, 2.0),
                    (10, 0.0),
                    (14, 0.0),
                ],
            ),
        ];

        let terminal_worker = fixture.call_method0("start_mutation").unwrap();
        let terminal_go = terminal_worker.get_item(0).unwrap();
        let terminal_finished = terminal_worker.get_item(1).unwrap();
        let terminal_thread = terminal_worker.get_item(2).unwrap();
        terminal_go.call_method0("set").unwrap();
        let terminal_result = terminal_f64_buffer_v2(py, &values, accept_all.clone(), 1);
        let terminal_finished_before_return = terminal_finished
            .call_method0("is_set")
            .unwrap()
            .is_truthy()
            .unwrap();
        terminal_thread.call_method0("join").unwrap();

        let mean_worker = fixture.call_method0("start_mutation").unwrap();
        let mean_go = mean_worker.get_item(0).unwrap();
        let mean_finished = mean_worker.get_item(1).unwrap();
        let mean_thread = mean_worker.get_item(2).unwrap();
        mean_go.call_method0("set").unwrap();
        let mean_result = mean_f64_buffer_v2(py, &values, accept_all.clone());
        let mean_finished_before_return = mean_finished
            .call_method0("is_set")
            .unwrap()
            .is_truthy()
            .unwrap();
        mean_thread.call_method0("join").unwrap();

        fixture
            .getattr("sys")
            .unwrap()
            .call_method1(
                "setswitchinterval",
                (fixture.getattr("previous_switch_interval").unwrap(),),
            )
            .unwrap();

        assert_eq!(terminal_result.unwrap(), (2_000_000, Some(2_000_000.0)),);
        assert!(
            !terminal_finished_before_return,
            "the staged terminal v2 scan released the GIL before it completed",
        );
        assert_eq!(mean_result.unwrap(), Some(1.0));
        assert!(
            !mean_finished_before_return,
            "the staged mean v2 scan released the GIL before it completed",
        );
        assert_eq!(values.len().unwrap(), 2_000_002);
    });
}

#[test]
#[cfg(not(Py_GIL_DISABLED))]
fn staged_f64_buffer_v2_releases_the_export_before_returning_an_error() {
    Python::initialize();
    Python::attach(|py| {
        let values = f64_array(py, vec![1.0, 2.0, 3.0]);
        let division_by_zero = vec![(0, vec![(0, 0.0), (1, 0.0), (18, 0.0)])];

        let error = terminal_f64_buffer_v2(py, &values, division_by_zero.clone(), 1).unwrap_err();
        assert!(error.is_instance_of::<pyo3::exceptions::PyZeroDivisionError>(py));
        values.call_method1("append", (4.0,)).unwrap();

        let error = mean_f64_buffer_v2(py, &values, division_by_zero.clone()).unwrap_err();
        assert!(error.is_instance_of::<pyo3::exceptions::PyZeroDivisionError>(py));
        values.call_method1("append", (5.0,)).unwrap();

        assert_eq!(values.len().unwrap(), 5);
    });
}

#[test]
fn f64_buffer_identity_reducers_match_the_generic_identity_stage_on_numeric_edges() {
    Python::initialize();
    Python::attach(|py| {
        let identity_stage = vec![(0, vec![(0, 0.0)])];
        let cases = [
            Vec::new(),
            vec![1e16, 1.0, -1e16],
            vec![f64::NAN, 1.0, 2.0],
            vec![1.0, f64::NAN, 2.0],
            vec![f64::INFINITY],
            vec![f64::NEG_INFINITY],
            vec![-0.0, 0.0],
            vec![0.0, -0.0],
            vec![f64::from_bits(1), -f64::from_bits(1)],
            vec![f64::MAX, f64::MAX],
            vec![f64::INFINITY, f64::NEG_INFINITY],
        ];

        for values in cases {
            let source = f64_array(py, values);
            for terminal in 0..=8 {
                let identity = terminal_f64_buffer_v1(py, &source, Vec::new(), terminal).unwrap();
                let staged =
                    terminal_f64_buffer_v1(py, &source, identity_stage.clone(), terminal).unwrap();
                assert_eq!(identity.0, staged.0);
                assert_same_optional_f64(identity.1, staged.1);
            }

            assert_same_optional_f64(
                mean_f64_buffer_v1(py, &source, Vec::new()).unwrap(),
                mean_f64_buffer_v1(py, &source, identity_stage.clone()).unwrap(),
            );
            for mask in 1..=u8::MAX {
                assert_same_f64_snapshot(
                    aggregate_f64_buffer_masked_v1(py, &source, Vec::new(), mask).unwrap(),
                    aggregate_f64_buffer_masked_v1(py, &source, identity_stage.clone(), mask)
                        .unwrap(),
                );
            }
        }

        let signed_zeros = f64_array(py, vec![-0.0, 0.0]);
        let extrema = aggregate_f64_buffer_masked_v1(
            py,
            &signed_zeros,
            Vec::new(),
            AGGREGATE_MINIMUM | AGGREGATE_MAXIMUM,
        )
        .unwrap();
        assert_eq!(extrema.2.unwrap().to_bits(), (-0.0_f64).to_bits());
        assert_eq!(extrema.3.unwrap().to_bits(), (-0.0_f64).to_bits());
    });
}
