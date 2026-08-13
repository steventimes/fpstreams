//! Native kernel parity and short-circuit tests.

use crate::float::{run_f64, run_f64_count, run_f64_statistics, run_f64_terminal};
use crate::integer::{
    evaluate, floor_div, modulo, run_i64_aggregate, run_i64_statistics, run_terminal, run_values,
};

#[test]
fn division_and_modulo_match_python_for_negative_operands() {
    assert_eq!(floor_div(-7, 3).unwrap(), -3);
    assert_eq!(floor_div(7, -3).unwrap(), -3);
    assert_eq!(modulo(-7, 3).unwrap(), 2);
    assert_eq!(modulo(7, -3).unwrap(), -2);
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
fn aggregate_snapshot_computes_every_terminal_in_one_pipeline_pass() {
    let snapshot = run_i64_aggregate(1..=4, Vec::new()).unwrap();

    assert_eq!(
        snapshot,
        (4, 10, Some(1), Some(4), Some(1), Some(4), 2.5, 5.0)
    );
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
