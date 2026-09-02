//! Callable join ordering, fallback, hashing, and error-boundary coverage.

use super::*;

#[test]
fn callable_unique_join_preserves_live_callback_order_snapshots_and_suffixes() {
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"events = []\n\nclass Record(dict):\n    pass\n\nright = [\n    {'id': 1, 'right': 'r1'},\n    {'id': 2, 'right': 'r2', 'tail': 't2'},\n]\nleft = [\n    {'id': 1, 'left': 'l1'},\n    {'id': 2, 'left': 'l2'},\n    {'id': 3, 'left': 'l3'},\n]\n\ndef right_key(row):\n    events.append(f\"right:{row['id']}\")\n    key = row['id']\n    if key == 1:\n        row['right'] = 'mutated'\n        right[1] = Record(right[1])\n    return key\n\ndef left_key(row):\n    events.append(f\"left:{row['id']}\")\n    key = row['id']\n    row['left'] = 'mutated'\n    return key\n\ndef as_record(row):\n    events.append(f\"adapt:{row['id']}\")\n    return dict(row)\n",
            c"callable_join_fixture.py",
            c"callable_join_fixture",
        )
        .unwrap();
        let shared = PyFrozenSet::empty(py).unwrap();

        let joined = join_hashable_unique_records_v1(
            &fixture.getattr("left").unwrap(),
            &fixture.getattr("right").unwrap(),
            &fixture.getattr("left_key").unwrap(),
            &fixture.getattr("right_key").unwrap(),
            &fixture.getattr("as_record").unwrap(),
            true,
            PyString::new(py, "_right").as_any(),
            shared.as_any(),
        )
        .unwrap()
        .unwrap();

        assert_eq!(joined.len(), 3);
        assert_eq!(
            fixture
                .getattr("events")
                .unwrap()
                .extract::<Vec<String>>()
                .unwrap(),
            vec![
                "right:1", "adapt:2", "right:2", "left:1", "left:2", "left:3",
            ]
        );
        assert_eq!(
            joined[0]
                .bind(py)
                .get_item("left")
                .unwrap()
                .unwrap()
                .extract::<&str>()
                .unwrap(),
            "l1"
        );
        assert_eq!(
            joined[0]
                .bind(py)
                .get_item("right")
                .unwrap()
                .unwrap()
                .extract::<&str>()
                .unwrap(),
            "r1"
        );
        assert_eq!(
            joined[0]
                .bind(py)
                .get_item("id_right")
                .unwrap()
                .unwrap()
                .extract::<i64>()
                .unwrap(),
            1
        );
        assert_eq!(
            joined[1]
                .bind(py)
                .get_item("left")
                .unwrap()
                .unwrap()
                .extract::<&str>()
                .unwrap(),
            "l2"
        );
        assert_eq!(
            joined[1]
                .bind(py)
                .get_item("right")
                .unwrap()
                .unwrap()
                .extract::<&str>()
                .unwrap(),
            "r2"
        );
        assert!(
            joined[2]
                .bind(py)
                .get_item("id_right")
                .unwrap()
                .unwrap()
                .is_none()
        );
        assert!(
            joined[2]
                .bind(py)
                .get_item("right")
                .unwrap()
                .unwrap()
                .is_none()
        );
        assert!(
            joined[2]
                .bind(py)
                .get_item("tail")
                .unwrap()
                .unwrap()
                .is_none()
        );
    });
}

#[test]
fn callable_many_join_preserves_order_snapshots_independence_and_suffix_identity() {
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"events = []\n\nright = [\n    {'right_id': 1, '': 'r1'},\n    {'right_id': 1, '': 'r2'},\n    {'right_id': 2, 'tail': 't2'},\n]\nleft = [\n    {'left_id': 1, '': 'l1'},\n    {'left_id': 1, '': 'l2'},\n    {'left_id': 3, '': 'l3'},\n]\n\ndef right_key(row):\n    events.append(f\"right:{row['right_id']}\")\n    key = row['right_id']\n    if '' in row:\n        row[''] = 'mutated-right'\n    return key\n\ndef left_key(row):\n    events.append(f\"left:{row['left_id']}\")\n    key = row['left_id']\n    row[''] = 'mutated-left'\n    return key\n\ndef as_record(row):\n    return dict(row)\n",
            c"callable_many_fixture.py",
            c"callable_many_fixture",
        )
        .unwrap();
        let shared = PyFrozenSet::empty(py).unwrap();
        let suffix = PyString::new(py, "__noninterned_right_suffix__");

        let joined = join_hashable_many_records_v1(
            &fixture.getattr("left").unwrap(),
            &fixture.getattr("right").unwrap(),
            &fixture.getattr("left_key").unwrap(),
            &fixture.getattr("right_key").unwrap(),
            &fixture.getattr("as_record").unwrap(),
            true,
            suffix.as_any(),
            shared.as_any(),
        )
        .unwrap()
        .unwrap();

        assert_eq!(joined.len(), 5);
        assert_eq!(
            fixture
                .getattr("events")
                .unwrap()
                .extract::<Vec<String>>()
                .unwrap(),
            vec![
                "right:1", "right:1", "right:2", "left:1", "left:1", "left:3"
            ]
        );
        for (position, (left_value, right_value)) in
            [("l1", "r1"), ("l1", "r2"), ("l2", "r1"), ("l2", "r2")]
                .into_iter()
                .enumerate()
        {
            assert_eq!(
                joined[position]
                    .bind(py)
                    .get_item("")
                    .unwrap()
                    .unwrap()
                    .extract::<&str>()
                    .unwrap(),
                left_value
            );
            let target = joined[position].bind(py).keys().get_item(3).unwrap();
            assert_eq!(
                target.extract::<&str>().unwrap(),
                "__noninterned_right_suffix__"
            );
            assert_eq!(
                joined[position]
                    .bind(py)
                    .get_item(&target)
                    .unwrap()
                    .unwrap()
                    .extract::<&str>()
                    .unwrap(),
                right_value
            );
        }
        let first_target = joined[0].bind(py).keys().get_item(3).unwrap();
        let second_target = joined[1].bind(py).keys().get_item(3).unwrap();
        let third_target = joined[2].bind(py).keys().get_item(3).unwrap();
        let fourth_target = joined[3].bind(py).keys().get_item(3).unwrap();
        assert!(first_target.is(&second_target));
        assert!(third_target.is(&fourth_target));
        assert!(!first_target.is(&third_target));

        joined[0].bind(py).set_item("left_id", 99_i64).unwrap();
        assert_eq!(
            joined[1]
                .bind(py)
                .get_item("left_id")
                .unwrap()
                .unwrap()
                .extract::<i64>()
                .unwrap(),
            1
        );
        assert!(
            joined[4]
                .bind(py)
                .get_item("right_id")
                .unwrap()
                .unwrap()
                .is_none()
        );
        assert!(
            joined[4]
                .bind(py)
                .get_item("tail")
                .unwrap()
                .unwrap()
                .is_none()
        );
    });
}

#[test]
fn callable_joins_restore_canonical_record_protocol_after_live_replacement() {
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"events = []\nright = []\nleft = [{'id': 1, 'left': 'L'}]\n\nclass AttributeRecord:\n    def __init__(self):\n        self.id = 1\n        self.right = 'R'\n\ndef reset(kind):\n    events.clear()\n    right[:] = [\n        {'id': 0, 'skip': 'S'},\n        {'id': 1, 'right': 'original'},\n    ]\n    global replacement\n    replacement = ([('id', 1), ('right', 'R')]\n                   if kind == 'pairs' else AttributeRecord())\n\ndef identifier(row):\n    if isinstance(row, list):\n        return dict(row)['id']\n    if isinstance(row, dict):\n        return row['id']\n    return row.id\n\ndef right_key(row):\n    key = identifier(row)\n    events.append(f'right:{key}')\n    if key == 0:\n        right[1] = replacement\n    return key\n\ndef left_key(row):\n    key = identifier(row)\n    events.append(f'left:{key}')\n    return key\n\ndef canonical_record(row):\n    events.append(f'fallback:{type(row).__name__}')\n    if isinstance(row, list):\n        raise RuntimeError('canonical list rejection')\n    return dict(vars(row))\n",
            c"callable_live_replacement_fixture.py",
            c"callable_live_replacement_fixture",
        )
        .unwrap();
        let adapters = PyTuple::new(
            py,
            [
                py.get_type::<PyDict>().as_any(),
                &fixture.getattr("canonical_record").unwrap(),
            ],
        )
        .unwrap();
        let shared = PyFrozenSet::empty(py).unwrap();
        let suffix = PyString::new(py, "_right");

        for many in [false, true] {
            fixture.call_method1("reset", ("pairs",)).unwrap();
            let error = if many {
                join_hashable_many_records_v1(
                    &fixture.getattr("left").unwrap(),
                    &fixture.getattr("right").unwrap(),
                    &fixture.getattr("left_key").unwrap(),
                    &fixture.getattr("right_key").unwrap(),
                    adapters.as_any(),
                    false,
                    suffix.as_any(),
                    shared.as_any(),
                )
                .unwrap_err()
            } else {
                join_hashable_unique_records_v1(
                    &fixture.getattr("left").unwrap(),
                    &fixture.getattr("right").unwrap(),
                    &fixture.getattr("left_key").unwrap(),
                    &fixture.getattr("right_key").unwrap(),
                    adapters.as_any(),
                    false,
                    suffix.as_any(),
                    shared.as_any(),
                )
                .unwrap_err()
            };
            assert!(error.is_instance_of::<pyo3::exceptions::PyRuntimeError>(py));
            assert_eq!(error.to_string(), "RuntimeError: canonical list rejection");
            assert_eq!(
                fixture
                    .getattr("events")
                    .unwrap()
                    .extract::<Vec<String>>()
                    .unwrap(),
                vec!["right:0", "fallback:list"]
            );

            fixture.call_method1("reset", ("attributes",)).unwrap();
            let joined = if many {
                join_hashable_many_records_v1(
                    &fixture.getattr("left").unwrap(),
                    &fixture.getattr("right").unwrap(),
                    &fixture.getattr("left_key").unwrap(),
                    &fixture.getattr("right_key").unwrap(),
                    adapters.as_any(),
                    false,
                    suffix.as_any(),
                    shared.as_any(),
                )
                .unwrap()
                .unwrap()
            } else {
                join_hashable_unique_records_v1(
                    &fixture.getattr("left").unwrap(),
                    &fixture.getattr("right").unwrap(),
                    &fixture.getattr("left_key").unwrap(),
                    &fixture.getattr("right_key").unwrap(),
                    adapters.as_any(),
                    false,
                    suffix.as_any(),
                    shared.as_any(),
                )
                .unwrap()
                .unwrap()
            };
            assert_eq!(joined.len(), 1);
            assert_eq!(
                joined[0]
                    .bind(py)
                    .get_item("right")
                    .unwrap()
                    .unwrap()
                    .extract::<&str>()
                    .unwrap(),
                "R"
            );
            assert_eq!(
                fixture
                    .getattr("events")
                    .unwrap()
                    .extract::<Vec<String>>()
                    .unwrap(),
                vec!["right:0", "fallback:AttributeRecord", "right:1", "left:1",]
            );
        }
    });
}

#[test]
fn direct_joins_restore_canonical_record_and_selector_protocols_after_live_replacement() {
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"from collections.abc import Mapping\n\nevents = []\nright = []\n\nclass Record(Mapping):\n    def __init__(self, label, **values):\n        self.label = label\n        self.values = values\n    def __iter__(self):\n        return iter(self.values)\n    def __len__(self):\n        return len(self.values)\n    def __getitem__(self, name):\n        events.append(f'get:{self.label}:{name}')\n        if self.label == 'right:0' and name == 'id' and right[1] is original:\n            right[1] = replacement\n            events.append('replace:right:1')\n        return self.values[name]\n\nclass AttributeRecord:\n    def __init__(self):\n        self.id = 1\n        self.right = 'R'\n\ndef reset(kind):\n    events.clear()\n    global original, replacement\n    original = Record('right:1', id=1, right='original')\n    replacement = ([('id', 1), ('right', 'R')]\n                   if kind == 'pairs' else AttributeRecord())\n    right[:] = [Record('right:0', id=0, skip='S'), original]\n\ndef canonical_record(row):\n    events.append(f'fallback:{type(row).__name__}')\n    if isinstance(row, list):\n        raise RuntimeError('canonical list rejection')\n    return dict(vars(row))\n\ndef left_selector(row):\n    events.append('select:left')\n    return row['id']\n\ndef right_selector(row):\n    events.append(f'select:right:{type(row).__name__}')\n    return row.id\n\nleft = [Record('left:1', id=1, left='L')]\n",
            c"direct_live_replacement_fixture.py",
            c"direct_live_replacement_fixture",
        )
        .unwrap();
        let allowed_types = PyTuple::new(py, [fixture.getattr("Record").unwrap()]).unwrap();
        let capabilities = PyTuple::new(
            py,
            [
                allowed_types.as_any(),
                &fixture.getattr("canonical_record").unwrap(),
                &fixture.getattr("left_selector").unwrap(),
                &fixture.getattr("right_selector").unwrap(),
            ],
        )
        .unwrap();
        let field = PyString::new(py, "id");
        let shared = PyFrozenSet::new(py, [field.as_any()]).unwrap();
        let suffix = PyString::new(py, "_right");

        for many in [false, true] {
            fixture.call_method1("reset", ("pairs",)).unwrap();
            let error = if many {
                join_hashable_many_direct_records_v1(
                    &fixture.getattr("left").unwrap(),
                    &fixture.getattr("right").unwrap(),
                    field.as_any(),
                    field.as_any(),
                    false,
                    suffix.as_any(),
                    shared.as_any(),
                    capabilities.as_any(),
                )
                .unwrap_err()
            } else {
                join_hashable_unique_direct_records_v1(
                    &fixture.getattr("left").unwrap(),
                    &fixture.getattr("right").unwrap(),
                    field.as_any(),
                    field.as_any(),
                    false,
                    suffix.as_any(),
                    shared.as_any(),
                    capabilities.as_any(),
                )
                .unwrap_err()
            };
            assert!(error.is_instance_of::<pyo3::exceptions::PyRuntimeError>(py));
            assert_eq!(error.to_string(), "RuntimeError: canonical list rejection");
            let pair_events = fixture
                .getattr("events")
                .unwrap()
                .extract::<Vec<String>>()
                .unwrap();
            assert_eq!(pair_events.last().unwrap(), "fallback:list");
            assert!(
                !pair_events
                    .iter()
                    .any(|event| event.starts_with("select:right:list"))
            );

            fixture.call_method1("reset", ("attributes",)).unwrap();
            let joined = if many {
                join_hashable_many_direct_records_v1(
                    &fixture.getattr("left").unwrap(),
                    &fixture.getattr("right").unwrap(),
                    field.as_any(),
                    field.as_any(),
                    false,
                    suffix.as_any(),
                    shared.as_any(),
                    capabilities.as_any(),
                )
                .unwrap()
                .unwrap()
            } else {
                join_hashable_unique_direct_records_v1(
                    &fixture.getattr("left").unwrap(),
                    &fixture.getattr("right").unwrap(),
                    field.as_any(),
                    field.as_any(),
                    false,
                    suffix.as_any(),
                    shared.as_any(),
                    capabilities.as_any(),
                )
                .unwrap()
                .unwrap()
            };
            assert_eq!(joined.len(), 1);
            assert_eq!(
                joined[0]
                    .bind(py)
                    .get_item("right")
                    .unwrap()
                    .unwrap()
                    .extract::<&str>()
                    .unwrap(),
                "R"
            );
            assert_eq!(
                fixture
                    .getattr("events")
                    .unwrap()
                    .extract::<Vec<String>>()
                    .unwrap()
                    .iter()
                    .filter(|event| event.as_str() == "select:right:AttributeRecord")
                    .count(),
                1
            );
        }
    });
}

#[test]
fn mapping_joins_recheck_same_type_mro_before_each_snapshot_and_selector() {
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"from collections.abc import Mapping\n\nevents = []\n\nclass Root:\n    def __init__(self, label, **attributes):\n        self.label = label\n        self.attributes = attributes\n        vars(self).update(attributes)\n    def _asdict(self):\n        return dict(self.attributes)\n\nclass MappingBase(Root, Mapping):\n    def __iter__(self):\n        events.append(f'iter:{self.label}')\n        return iter(tuple(reversed(self.attributes)))\n    def __len__(self):\n        return len(self.attributes)\n    def __getitem__(self, name):\n        events.append(f'get:{self.label}:{name}')\n        value = self.attributes[name]\n        if mode == 'direct' and self.label == mutation_label and name == 'id' and MappingBase in Record.__mro__:\n            Record.__bases__ = (AttributeBase,)\n            Mapping._abc_caches_clear()\n            events.append(f'mutate:{self.label}:getitem')\n        return value\n\nclass AttributeBase(Root):\n    pass\n\nclass Record(MappingBase):\n    pass\n\ndef reset(next_mode, next_mutation_label, many):\n    global mode, mutation_label, left, right, adapted, selected\n    Record.__bases__ = (MappingBase,)\n    Mapping._abc_caches_clear()\n    mode = next_mode\n    mutation_label = next_mutation_label\n    events.clear()\n    adapted = set()\n    selected = set()\n    right = [\n        Record('right:0', id='skip', payload='S'),\n        Record('right:1', id='match', payload='R1'),\n        Record('right:2', id='match' if many else 'other', payload='R2'),\n    ]\n    left = [Record('left:0', id='match', left='L')]\n\ndef canonical_record(row):\n    if row.label in adapted:\n        raise AssertionError(f'snapshot replayed for {row.label}')\n    adapted.add(row.label)\n    events.append(f'adapt:{row.label}')\n    return row._asdict()\n\ndef select(row):\n    if row.label in selected:\n        raise AssertionError(f'selector replayed for {row.label}')\n    selected.add(row.label)\n    value = row['id'] if isinstance(row, Mapping) else row.id\n    events.append(f'select:{row.label}:{value}')\n    if mode == 'callable' and row.label == mutation_label and MappingBase in Record.__mro__:\n        Record.__bases__ = (AttributeBase,)\n        Mapping._abc_caches_clear()\n        events.append(f'mutate:{row.label}:selector')\n    return value\n",
            c"mapping_live_mro_fixture.py",
            c"mapping_live_mro_fixture",
        )
        .unwrap();
        let field = PyString::new(py, "id");
        let suffix = PyString::new(py, "_right");
        let shared = PyFrozenSet::empty(py).unwrap();

        for direct in [false, true] {
            for many in [false, true] {
                for mutation_label in ["right:0", "right:1"] {
                    fixture
                        .call_method1(
                            "reset",
                            (
                                if direct { "direct" } else { "callable" },
                                mutation_label,
                                many,
                            ),
                        )
                        .unwrap();
                    let row_type = fixture.getattr("Record").unwrap();
                    let initial_mro = row_type.getattr("__mro__").unwrap();
                    let type_capability =
                        PyTuple::new(py, [row_type.as_any(), initial_mro.as_any()]).unwrap();
                    let capabilities = PyTuple::new(py, [type_capability.as_any()]).unwrap();
                    let adapter_pair = PyTuple::new(
                        py,
                        [
                            py.get_type::<PyDict>().as_any(),
                            &fixture.getattr("canonical_record").unwrap(),
                        ],
                    )
                    .unwrap();

                    let joined = if direct {
                        let direct_capabilities = PyTuple::new(
                            py,
                            [
                                capabilities.as_any(),
                                &fixture.getattr("canonical_record").unwrap(),
                                &fixture.getattr("select").unwrap(),
                                &fixture.getattr("select").unwrap(),
                            ],
                        )
                        .unwrap();
                        if many {
                            join_hashable_many_direct_records_v1(
                                &fixture.getattr("left").unwrap(),
                                &fixture.getattr("right").unwrap(),
                                field.as_any(),
                                field.as_any(),
                                false,
                                suffix.as_any(),
                                shared.as_any(),
                                direct_capabilities.as_any(),
                            )
                            .unwrap()
                            .unwrap()
                        } else {
                            join_hashable_unique_direct_records_v1(
                                &fixture.getattr("left").unwrap(),
                                &fixture.getattr("right").unwrap(),
                                field.as_any(),
                                field.as_any(),
                                false,
                                suffix.as_any(),
                                shared.as_any(),
                                direct_capabilities.as_any(),
                            )
                            .unwrap()
                            .unwrap()
                        }
                    } else if many {
                        crate::relational::join_hashable_many_records_v2(
                            &fixture.getattr("left").unwrap(),
                            &fixture.getattr("right").unwrap(),
                            &fixture.getattr("select").unwrap(),
                            &fixture.getattr("select").unwrap(),
                            adapter_pair.as_any(),
                            false,
                            suffix.as_any(),
                            shared.as_any(),
                            capabilities.as_any(),
                        )
                        .unwrap()
                        .unwrap()
                    } else {
                        join_hashable_unique_records_v2(
                            &fixture.getattr("left").unwrap(),
                            &fixture.getattr("right").unwrap(),
                            &fixture.getattr("select").unwrap(),
                            &fixture.getattr("select").unwrap(),
                            adapter_pair.as_any(),
                            false,
                            suffix.as_any(),
                            shared.as_any(),
                            capabilities.as_any(),
                        )
                        .unwrap()
                        .unwrap()
                    };

                    assert_eq!(joined.len(), if many { 2 } else { 1 });
                    assert_eq!(
                        joined[0]
                            .bind(py)
                            .get_item("payload")
                            .unwrap()
                            .unwrap()
                            .extract::<&str>()
                            .unwrap(),
                        "R1"
                    );
                    let events = fixture
                        .getattr("events")
                        .unwrap()
                        .extract::<Vec<String>>()
                        .unwrap();
                    assert_eq!(
                        events
                            .iter()
                            .filter(|event| event.starts_with("mutate:"))
                            .count(),
                        1
                    );
                    assert!(!row_type.getattr("__mro__").unwrap().is(&initial_mro));
                }
            }
        }
    });
}

#[test]
fn callable_many_join_preserves_python_hash_and_equality_trace() {
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"events = []\n\nclass Key:\n    def __init__(self, label):\n        self.label = label\n    def __hash__(self):\n        events.append(f\"hash:{self.label}\")\n        return 0\n    def __eq__(self, other):\n        events.append(f\"eq:{self.label}:{other.label}\")\n        return True\n\nright = [\n    {'key': Key('right-1'), 'right': 1},\n    {'key': Key('right-2'), 'right': 2},\n]\nleft = [{'key': Key('left'), 'left': True}]\n\ndef select(row):\n    events.append(f\"select:{row['key'].label}\")\n    return row['key']\n\ndef as_record(row):\n    return dict(row)\n",
            c"callable_many_hash_fixture.py",
            c"callable_many_hash_fixture",
        )
        .unwrap();
        let shared = PyFrozenSet::empty(py).unwrap();

        let joined = join_hashable_many_records_v1(
            &fixture.getattr("left").unwrap(),
            &fixture.getattr("right").unwrap(),
            &fixture.getattr("select").unwrap(),
            &fixture.getattr("select").unwrap(),
            &fixture.getattr("as_record").unwrap(),
            false,
            PyString::new(py, "_right").as_any(),
            shared.as_any(),
        )
        .unwrap()
        .unwrap();

        assert_eq!(joined.len(), 2);
        assert_eq!(
            fixture
                .getattr("events")
                .unwrap()
                .extract::<Vec<String>>()
                .unwrap(),
            vec![
                "select:right-1",
                "hash:right-1",
                "hash:right-1",
                "select:right-2",
                "hash:right-2",
                "eq:right-1:right-2",
                "select:left",
                "hash:left",
                "eq:right-1:left",
            ]
        );
    });
}

#[test]
fn callable_unique_join_preserves_python_hash_and_equality_trace() {
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"events = []\n\nclass Key:\n    def __init__(self, label):\n        self.label = label\n    def __hash__(self):\n        events.append(f\"hash:{self.label}\")\n        return 0\n    def __eq__(self, other):\n        events.append(f\"eq:{self.label}:{other.label}\")\n        return True\n\nright = [{'key': Key('right'), 'right': True}]\nleft = [{'key': Key('left'), 'left': True}]\n\ndef select(row):\n    events.append(f\"select:{row['key'].label}\")\n    return row['key']\n\ndef as_record(row):\n    return dict(row)\n",
            c"callable_hash_fixture.py",
            c"callable_hash_fixture",
        )
        .unwrap();
        let shared = PyFrozenSet::empty(py).unwrap();

        let joined = join_hashable_unique_records_v1(
            &fixture.getattr("left").unwrap(),
            &fixture.getattr("right").unwrap(),
            &fixture.getattr("select").unwrap(),
            &fixture.getattr("select").unwrap(),
            &fixture.getattr("as_record").unwrap(),
            false,
            PyString::new(py, "_right").as_any(),
            shared.as_any(),
        )
        .unwrap()
        .unwrap();

        assert_eq!(joined.len(), 1);
        assert_eq!(
            fixture
                .getattr("events")
                .unwrap()
                .extract::<Vec<String>>()
                .unwrap(),
            vec![
                "select:right",
                "hash:right",
                "hash:right",
                "select:left",
                "hash:left",
                "eq:right:left",
            ]
        );
    });
}

#[test]
fn callable_unique_join_mints_empty_name_suffix_keys_per_cached_output() {
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"left = [\n    {'left_id': 1, '': 'l1'},\n    {'left_id': 1, '': 'l2'},\n]\nright = [{'right_id': 1, '': 'r'}]\n\ndef left_key(row):\n    return row['left_id']\n\ndef right_key(row):\n    return row['right_id']\n\ndef as_record(row):\n    return dict(row)\n",
            c"callable_empty_name_fixture.py",
            c"callable_empty_name_fixture",
        )
        .unwrap();
        let shared = PyFrozenSet::empty(py).unwrap();
        let suffix = PyString::new(py, "__noninterned_right_suffix__");

        let joined = join_hashable_unique_records_v1(
            &fixture.getattr("left").unwrap(),
            &fixture.getattr("right").unwrap(),
            &fixture.getattr("left_key").unwrap(),
            &fixture.getattr("right_key").unwrap(),
            &fixture.getattr("as_record").unwrap(),
            false,
            suffix.as_any(),
            shared.as_any(),
        )
        .unwrap()
        .unwrap();

        let first_target = joined[0].bind(py).keys().get_item(3).unwrap();
        let second_target = joined[1].bind(py).keys().get_item(3).unwrap();
        assert!(first_target.eq(&suffix).unwrap());
        assert!(second_target.eq(&suffix).unwrap());
        assert!(!first_target.is(&suffix));
        assert!(!second_target.is(&suffix));
        assert!(!first_target.is(&second_target));
    });
}

#[test]
fn callable_unique_join_preserves_surrogates_in_duplicate_key_repr() {
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"class Key:\n    def __hash__(self):\n        return 0\n    def __eq__(self, other):\n        return True\n    def __repr__(self):\n        return '\\ud800'\n\nkey = Key()\nright = [{}, {}]\nexpected = \"join validate='m:1' requires unique right keys; found duplicate \" + repr(key)\n\ndef select(row):\n    return key\n\ndef as_record(row):\n    return dict(row)\n",
            c"callable_surrogate_repr_fixture.py",
            c"callable_surrogate_repr_fixture",
        )
        .unwrap();
        let shared = PyFrozenSet::empty(py).unwrap();
        let left = PyTuple::empty(py);

        let error = join_hashable_unique_records_v1(
            left.as_any(),
            &fixture.getattr("right").unwrap(),
            &fixture.getattr("select").unwrap(),
            &fixture.getattr("select").unwrap(),
            &fixture.getattr("as_record").unwrap(),
            false,
            PyString::new(py, "_right").as_any(),
            shared.as_any(),
        )
        .unwrap_err();

        let message = error
            .value(py)
            .getattr("args")
            .unwrap()
            .get_item(0)
            .unwrap();
        assert!(message.eq(fixture.getattr("expected").unwrap()).unwrap());
    });
}

#[test]
fn callable_unique_join_declines_only_before_callbacks_and_raises_duplicates_directly() {
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"events = []\n\ndef select(row):\n    events.append(row['id'])\n    return row['id']\n\ndef as_record(row):\n    return dict(row)\n",
            c"callable_decline_fixture.py",
            c"callable_decline_fixture",
        )
        .unwrap();
        let shared = PyFrozenSet::empty(py).unwrap();
        let invalid_left = PyList::new(py, [1_i64]).unwrap();
        let empty = PyTuple::empty(py);

        assert!(
            join_hashable_unique_records_v1(
                invalid_left.as_any(),
                empty.as_any(),
                &fixture.getattr("select").unwrap(),
                &fixture.getattr("select").unwrap(),
                &fixture.getattr("as_record").unwrap(),
                false,
                PyString::new(py, "_right").as_any(),
                shared.as_any(),
            )
            .unwrap()
            .is_none()
        );
        assert!(
            fixture
                .getattr("events")
                .unwrap()
                .extract::<Vec<i64>>()
                .unwrap()
                .is_empty()
        );

        let left = PyList::new(py, [PyDict::new(py)]).unwrap();
        let first = PyDict::new(py);
        first.set_item("id", 1_i64).unwrap();
        let second = PyDict::new(py);
        second.set_item("id", 1_i64).unwrap();
        let later = PyDict::new(py);
        later.set_item("id", 2_i64).unwrap();
        let right = PyList::new(py, [&first, &second, &later]).unwrap();
        let error = join_hashable_unique_records_v1(
            left.as_any(),
            right.as_any(),
            &fixture.getattr("select").unwrap(),
            &fixture.getattr("select").unwrap(),
            &fixture.getattr("as_record").unwrap(),
            false,
            PyString::new(py, "_right").as_any(),
            shared.as_any(),
        )
        .unwrap_err();

        assert!(error.is_instance_of::<pyo3::exceptions::PyValueError>(py));
        assert_eq!(
            error.to_string(),
            "ValueError: join validate='m:1' requires unique right keys; found duplicate 1"
        );
        assert_eq!(
            fixture
                .getattr("events")
                .unwrap()
                .extract::<Vec<i64>>()
                .unwrap(),
            vec![1, 1]
        );
    });
}
