//! Callable join behavior, caching, and snapshot tests.

use super::*;

#[test]
fn callable_joins_reuse_a_large_homogeneous_right_schema_by_identity() {
    const ROW_COUNT: usize = 20_000;
    const FIELD_COUNT: usize = 8;

    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"def select(row):\n    return row['key']\n\ndef as_record(row):\n    return dict(row)\n",
            c"callable_right_schema_cache_fixture.py",
            c"callable_right_schema_cache_fixture",
        )
        .unwrap();
        let fields = (0..FIELD_COUNT)
            .map(|index| {
                PyString::new(
                    py,
                    if index == 0 {
                        "key"
                    } else {
                        ["r1", "r2", "r3", "r4", "r5", "r6", "r7"][index - 1]
                    },
                )
                .unbind()
            })
            .collect::<Vec<_>>();
        let right = PyList::empty(py);
        for row_index in 0..ROW_COUNT {
            let row = PyDict::new(py);
            for (field_index, field) in fields.iter().enumerate() {
                row.set_item(field.bind(py), row_index + field_index)
                    .unwrap();
            }
            right.append(row).unwrap();
        }
        let empty_left = PyTuple::empty(py);
        let shared = PyFrozenSet::empty(py).unwrap();

        begin_callable_right_schema_probe_count();
        let joined = join_hashable_unique_records_v1(
            empty_left.as_any(),
            right.as_any(),
            &fixture.getattr("select").unwrap(),
            &fixture.getattr("select").unwrap(),
            &fixture.getattr("as_record").unwrap(),
            false,
            PyString::new(py, "_right").as_any(),
            shared.as_any(),
        )
        .unwrap()
        .unwrap();
        let probes = end_callable_right_schema_probe_count();

        assert!(joined.is_empty());
        assert_eq!(probes.full_field_probes, FIELD_COUNT);
        assert_eq!(probes.identity_cache_hits, ROW_COUNT - 1);
        assert_eq!(probes.value_cache_hits, 0);

        begin_callable_right_schema_probe_count();
        let joined = join_hashable_many_records_v1(
            empty_left.as_any(),
            right.as_any(),
            &fixture.getattr("select").unwrap(),
            &fixture.getattr("select").unwrap(),
            &fixture.getattr("as_record").unwrap(),
            false,
            PyString::new(py, "_right").as_any(),
            shared.as_any(),
        )
        .unwrap()
        .unwrap();
        let probes = end_callable_right_schema_probe_count();

        assert!(joined.is_empty());
        assert_eq!(probes.full_field_probes, FIELD_COUNT);
        assert_eq!(probes.identity_cache_hits, ROW_COUNT - 1);
        assert_eq!(probes.value_cache_hits, 0);
    });
}

#[test]
fn callable_many_join_bulk_merges_a_wide_identity_homogeneous_schema() {
    const ROW_COUNT: usize = 20_000;
    const FIELD_COUNT: usize = 24;

    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"from types import MappingProxyType\n\ndef select(row):\n    return row['key']\n\ndef as_record(row):\n    return dict(row)\n",
            c"callable_many_bulk_merge_fixture.py",
            c"callable_many_bulk_merge_fixture",
        )
        .unwrap();
        let field_names = (0..FIELD_COUNT)
            .map(|index| {
                if index == 0 {
                    "key".to_owned()
                } else {
                    format!("__fpstreams_bulk_r{index:02}__")
                }
            })
            .collect::<Vec<_>>();
        let fields = field_names
            .iter()
            .map(|name| PyString::new(py, name).unbind())
            .collect::<Vec<_>>();
        let right = PyList::empty(py);
        for row_index in 0..ROW_COUNT {
            let row = PyDict::new(py);
            for (field_index, field) in fields.iter().enumerate() {
                let value = if field_index == 0 {
                    row_index / 2
                } else {
                    row_index + field_index
                };
                row.set_item(field.bind(py), value).unwrap();
            }
            right.append(row).unwrap();
        }
        let left_field = PyString::new(py, "__fpstreams_bulk_left__").unbind();
        let left = PyList::empty(py);
        for left_index in 0..2 {
            let row = PyDict::new(py);
            row.set_item(fields[0].bind(py), 0).unwrap();
            row.set_item(left_field.bind(py), left_index).unwrap();
            left.append(row).unwrap();
        }
        let shared = PyFrozenSet::empty(py).unwrap();

        begin_callable_right_schema_probe_count();
        let joined = join_hashable_many_records_v1(
            left.as_any(),
            right.as_any(),
            &fixture.getattr("select").unwrap(),
            &fixture.getattr("select").unwrap(),
            &fixture.getattr("as_record").unwrap(),
            false,
            PyString::new(py, "_right").as_any(),
            shared.as_any(),
        )
        .unwrap()
        .unwrap();
        let probes = end_callable_right_schema_probe_count();

        assert_eq!(joined.len(), 4);
        assert_eq!(probes.bulk_merge_hits, 2);
        let first_bulk_keys = joined[2].bind(py).keys();
        let second_bulk_keys = joined[3].bind(py).keys();
        assert_eq!(first_bulk_keys.len(), FIELD_COUNT + 2);
        assert!(first_bulk_keys.get_item(0).unwrap().is(fields[0].bind(py)));
        assert!(first_bulk_keys.get_item(1).unwrap().is(left_field.bind(py)));
        assert_eq!(
            first_bulk_keys
                .get_item(2)
                .unwrap()
                .extract::<&str>()
                .unwrap(),
            "key_right"
        );
        assert!(
            first_bulk_keys
                .get_item(2)
                .unwrap()
                .is(second_bulk_keys.get_item(2).unwrap())
        );
        assert!(first_bulk_keys.get_item(3).unwrap().is(fields[1].bind(py)));
        assert_eq!(
            joined[2]
                .bind(py)
                .get_item(fields[1].bind(py))
                .unwrap()
                .unwrap()
                .extract::<usize>()
                .unwrap(),
            1
        );
        assert_eq!(
            joined[3]
                .bind(py)
                .get_item(fields[1].bind(py))
                .unwrap()
                .unwrap()
                .extract::<usize>()
                .unwrap(),
            2
        );

        let mapping_proxy_type = fixture.getattr("MappingProxyType").unwrap();
        let allowed_types = PyTuple::new(py, [mapping_proxy_type.clone()]).unwrap();
        let proxy_right = PyList::empty(py);
        for row_index in 0..right.len() {
            let row = right.get_item(row_index).unwrap();
            proxy_right
                .append(mapping_proxy_type.call1((row,)).unwrap())
                .unwrap();
        }
        let proxy_left = PyList::empty(py);
        for row_index in 0..left.len() {
            let row = left.get_item(row_index).unwrap();
            proxy_left
                .append(mapping_proxy_type.call1((row,)).unwrap())
                .unwrap();
        }
        begin_callable_right_schema_probe_count();
        let direct_joined = join_hashable_many_direct_records_v1(
            proxy_left.as_any(),
            proxy_right.as_any(),
            fields[0].bind(py).as_any(),
            fields[0].bind(py).as_any(),
            false,
            PyString::new(py, "_right").as_any(),
            shared.as_any(),
            allowed_types.as_any(),
        )
        .unwrap()
        .unwrap();
        let direct_probes = end_callable_right_schema_probe_count();

        assert_eq!(direct_joined.len(), joined.len());
        assert_eq!(direct_probes.bulk_merge_hits, 2);
        assert_eq!(direct_probes.mapping_proxy_snapshot_hits, ROW_COUNT + 2);
        assert!(direct_joined[3].bind(py).eq(joined[3].bind(py)).unwrap());

        let nonprefix_left = PyList::empty(py);
        for left_index in 0..2 {
            let row = PyDict::new(py);
            row.set_item(fields[0].bind(py), 0).unwrap();
            row.set_item(fields[2].bind(py), left_index).unwrap();
            nonprefix_left.append(row).unwrap();
        }
        begin_callable_right_schema_probe_count();
        let nonprefix_joined = join_hashable_many_records_v1(
            nonprefix_left.as_any(),
            right.as_any(),
            &fixture.getattr("select").unwrap(),
            &fixture.getattr("select").unwrap(),
            &fixture.getattr("as_record").unwrap(),
            false,
            PyString::new(py, "_right").as_any(),
            shared.as_any(),
        )
        .unwrap()
        .unwrap();
        let nonprefix_probes = end_callable_right_schema_probe_count();

        assert_eq!(nonprefix_probes.bulk_merge_hits, 0);
        let nonprefix_keys = nonprefix_joined[2].bind(py).keys();
        assert!(nonprefix_keys.get_item(0).unwrap().is(fields[0].bind(py)));
        assert!(nonprefix_keys.get_item(1).unwrap().is(fields[2].bind(py)));
        assert_eq!(
            nonprefix_keys
                .get_item(2)
                .unwrap()
                .extract::<&str>()
                .unwrap(),
            "key_right"
        );
        assert!(nonprefix_keys.get_item(3).unwrap().is(fields[1].bind(py)));
        assert_eq!(
            nonprefix_keys
                .get_item(4)
                .unwrap()
                .extract::<&str>()
                .unwrap(),
            format!("{}_right", field_names[2])
        );
    });
}

#[derive(Clone, Copy)]
enum CallableSchemaCardinality {
    Unique,
    Many,
}

#[derive(Clone, Copy)]
enum CallableSchemaVariation {
    AlternatingEqualValues,
    SecondRowDifferentValue,
}

fn probe_callable_schema_layout(
    row_count: usize,
    field_count: usize,
    cardinality: CallableSchemaCardinality,
    variation: CallableSchemaVariation,
) -> (CallableRightSchemaProbeCounts, bool) {
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"def select(row):\n    return row['key']\n\ndef as_record(row):\n    return dict(row)\n",
            c"callable_right_schema_value_fixture.py",
            c"callable_right_schema_value_fixture",
        )
        .unwrap();
        let field_names = (0..field_count)
            .map(|index| {
                if index == 0 {
                    "key".to_owned()
                } else {
                    format!("__fpstreams_schema_value_r{index:02}__")
                }
            })
            .collect::<Vec<_>>();
        let fields = field_names
            .iter()
            .map(|name| PyString::new(py, name).unbind())
            .collect::<Vec<_>>();
        let mut alternate_fields = field_names
            .iter()
            .map(|name| PyString::new(py, name).unbind())
            .collect::<Vec<_>>();
        if matches!(variation, CallableSchemaVariation::SecondRowDifferentValue) {
            alternate_fields[1] =
                PyString::new(py, "__fpstreams_schema_genuinely_different__").unbind();
        }
        for (field, alternate) in fields.iter().zip(&alternate_fields) {
            if matches!(variation, CallableSchemaVariation::AlternatingEqualValues) {
                assert!(
                    field
                        .bind(py)
                        .as_any()
                        .eq(alternate.bind(py).as_any())
                        .unwrap()
                );
            }
            assert!(!field.bind(py).is(alternate.bind(py)));
        }

        let right = PyList::empty(py);
        for row_index in 0..row_count {
            let row = PyDict::new(py);
            let alternate = match variation {
                CallableSchemaVariation::AlternatingEqualValues => row_index % 2 == 1,
                CallableSchemaVariation::SecondRowDifferentValue => row_index == 1,
            };
            let row_fields = if alternate {
                &alternate_fields
            } else {
                &fields
            };
            for (field_index, field) in row_fields.iter().enumerate() {
                let value = if field_index == 0 {
                    match cardinality {
                        CallableSchemaCardinality::Unique => row_index,
                        CallableSchemaCardinality::Many => row_index / 2,
                    }
                } else {
                    row_index + field_index
                };
                row.set_item(field.bind(py), value).unwrap();
            }
            right.append(row).unwrap();
        }
        let left = PyList::empty(py);
        for _ in 0..2 {
            let left_row = PyDict::new(py);
            left_row.set_item("key", 0).unwrap();
            left.append(left_row).unwrap();
        }
        let shared = PyFrozenSet::empty(py).unwrap();

        begin_callable_right_schema_probe_count();
        let joined = match cardinality {
            CallableSchemaCardinality::Unique => join_hashable_unique_records_v1(
                left.as_any(),
                right.as_any(),
                &fixture.getattr("select").unwrap(),
                &fixture.getattr("select").unwrap(),
                &fixture.getattr("as_record").unwrap(),
                false,
                PyString::new(py, "_right").as_any(),
                shared.as_any(),
            ),
            CallableSchemaCardinality::Many => join_hashable_many_records_v1(
                left.as_any(),
                right.as_any(),
                &fixture.getattr("select").unwrap(),
                &fixture.getattr("select").unwrap(),
                &fixture.getattr("as_record").unwrap(),
                false,
                PyString::new(py, "_right").as_any(),
                shared.as_any(),
            ),
        }
        .unwrap()
        .unwrap();
        let probes = end_callable_right_schema_probe_count();
        let first_right_output_field = joined[0].bind(py).keys().get_item(2).unwrap();
        (probes, first_right_output_field.is(fields[1].bind(py)))
    })
}

#[test]
fn callable_unique_schema_matches_equal_string_values_at_the_row_threshold() {
    let (probes, preserved_first_columns) = probe_callable_schema_layout(
        40_000,
        8,
        CallableSchemaCardinality::Unique,
        CallableSchemaVariation::AlternatingEqualValues,
    );

    assert_eq!(probes.full_field_probes, 8);
    assert_eq!(probes.identity_cache_hits, 19_999);
    assert_eq!(probes.value_cache_hits, 20_000);
    assert!(preserved_first_columns);
}

#[test]
fn callable_unique_schema_matches_equal_string_values_at_the_field_threshold() {
    let (probes, preserved_first_columns) = probe_callable_schema_layout(
        20_000,
        16,
        CallableSchemaCardinality::Unique,
        CallableSchemaVariation::AlternatingEqualValues,
    );

    assert_eq!(probes.full_field_probes, 16);
    assert_eq!(probes.identity_cache_hits, 9_999);
    assert_eq!(probes.value_cache_hits, 10_000);
    assert!(preserved_first_columns);
}

#[test]
fn callable_many_schema_matches_equal_string_values_at_the_existing_threshold() {
    let (probes, preserved_first_columns) = probe_callable_schema_layout(
        20_000,
        8,
        CallableSchemaCardinality::Many,
        CallableSchemaVariation::AlternatingEqualValues,
    );

    assert_eq!(probes.full_field_probes, 8);
    assert_eq!(probes.identity_cache_hits, 9_999);
    assert_eq!(probes.value_cache_hits, 10_000);
    assert!(preserved_first_columns);
}

#[test]
fn callable_many_wide_equal_value_schema_never_bulk_merges() {
    let (probes, preserved_first_columns) = probe_callable_schema_layout(
        20_000,
        24,
        CallableSchemaCardinality::Many,
        CallableSchemaVariation::AlternatingEqualValues,
    );

    assert_eq!(probes.full_field_probes, 24);
    assert_eq!(probes.identity_cache_hits, 9_999);
    assert_eq!(probes.value_cache_hits, 10_000);
    assert_eq!(probes.bulk_merge_hits, 0);
    assert!(preserved_first_columns);
}

#[test]
fn callable_unique_schema_rejects_value_matching_below_both_thresholds() {
    let (probes, preserved_first_columns) = probe_callable_schema_layout(
        20_000,
        8,
        CallableSchemaCardinality::Unique,
        CallableSchemaVariation::AlternatingEqualValues,
    );

    assert_eq!(probes.full_field_probes, 20_000 * 8);
    assert_eq!(probes.identity_cache_hits, 0);
    assert_eq!(probes.value_cache_hits, 0);
    assert!(preserved_first_columns);
}

#[test]
fn callable_right_schema_value_mismatch_permanently_disables_the_cache() {
    const ROW_COUNT: usize = 20_000;
    const FIELD_COUNT: usize = 16;

    let (probes, preserved_first_columns) = probe_callable_schema_layout(
        ROW_COUNT,
        FIELD_COUNT,
        CallableSchemaCardinality::Unique,
        CallableSchemaVariation::SecondRowDifferentValue,
    );

    assert_eq!(probes.full_field_probes, ROW_COUNT * FIELD_COUNT);
    assert_eq!(probes.identity_cache_hits, 0);
    assert_eq!(probes.value_cache_hits, 0);
    assert!(preserved_first_columns);
}

#[test]
fn callable_unique_join_v2_accepts_exact_mappingproxy_and_nominal_mapping_tokens() {
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"from collections.abc import Mapping\nfrom types import MappingProxyType\n\nevents = []\n\nclass Record(Mapping):\n    def __init__(self, values):\n        self.values = values\n    def __getitem__(self, name):\n        return self.values[name]\n    def __iter__(self):\n        return iter(self.values)\n    def __len__(self):\n        return len(self.values)\n\nleft = [MappingProxyType({'id': 1, 'left': 'L'})]\nright = (Record({'id': 1, 'right': 'R'}),)\n\ndef select(row):\n    events.append(f\"select:{row['id']}\")\n    return row['id']\n\ndef as_record(row):\n    events.append(f\"adapt:{row['id']}\")\n    return dict(row)\n",
            c"callable_mapping_v2_fixture.py",
            c"callable_mapping_v2_fixture",
        )
        .unwrap();
        let shared = PyFrozenSet::empty(py).unwrap();
        let allowed_types = PyTuple::new(
            py,
            [
                fixture.getattr("MappingProxyType").unwrap(),
                fixture.getattr("Record").unwrap(),
            ],
        )
        .unwrap();

        let joined = join_hashable_unique_records_v2(
            &fixture.getattr("left").unwrap(),
            &fixture.getattr("right").unwrap(),
            &fixture.getattr("select").unwrap(),
            &fixture.getattr("select").unwrap(),
            &fixture.getattr("as_record").unwrap(),
            false,
            PyString::new(py, "_right").as_any(),
            shared.as_any(),
            allowed_types.as_any(),
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
            vec!["adapt:1", "select:1", "adapt:1", "select:1"]
        );
        assert_eq!(
            joined[0]
                .bind(py)
                .get_item("left")
                .unwrap()
                .unwrap()
                .extract::<&str>()
                .unwrap(),
            "L"
        );
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
    });
}

#[test]
fn callable_unique_mappingproxy_snapshot_requires_the_builtin_dict_adapter() {
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"from types import MappingProxyType\n\nevents = []\nleft = [MappingProxyType({'id': 1, 'left': 'L'})]\nright = (MappingProxyType({'id': 1, 'right': 'R'}),)\n\ndef select(row):\n    events.append(f\"select:{row['id']}\")\n    return row['id']\n\ndef adapt(row):\n    events.append(f\"adapt:{row['id']}\")\n    return dict(row)\n",
            c"callable_mappingproxy_snapshot_fixture.py",
            c"callable_mappingproxy_snapshot_fixture",
        )
        .unwrap();
        let shared = PyFrozenSet::empty(py).unwrap();
        let allowed_types =
            PyTuple::new(py, [fixture.getattr("MappingProxyType").unwrap()]).unwrap();

        begin_callable_right_schema_probe_count();
        let canonical = join_hashable_unique_records_v2(
            &fixture.getattr("left").unwrap(),
            &fixture.getattr("right").unwrap(),
            &fixture.getattr("select").unwrap(),
            &fixture.getattr("select").unwrap(),
            &fixture.getattr("adapt").unwrap(),
            false,
            PyString::new(py, "_right").as_any(),
            shared.as_any(),
            allowed_types.as_any(),
        )
        .unwrap()
        .unwrap();
        let canonical_probes = end_callable_right_schema_probe_count();

        assert_eq!(canonical_probes.mapping_proxy_snapshot_hits, 0);
        assert_eq!(
            fixture
                .getattr("events")
                .unwrap()
                .extract::<Vec<String>>()
                .unwrap(),
            vec!["adapt:1", "select:1", "adapt:1", "select:1"]
        );
        fixture
            .getattr("events")
            .unwrap()
            .call_method0("clear")
            .unwrap();

        begin_callable_right_schema_probe_count();
        let optimized = join_hashable_unique_records_v2(
            &fixture.getattr("left").unwrap(),
            &fixture.getattr("right").unwrap(),
            &fixture.getattr("select").unwrap(),
            &fixture.getattr("select").unwrap(),
            py.get_type::<PyDict>().as_any(),
            false,
            PyString::new(py, "_right").as_any(),
            shared.as_any(),
            allowed_types.as_any(),
        )
        .unwrap()
        .unwrap();
        let optimized_probes = end_callable_right_schema_probe_count();

        assert_eq!(optimized_probes.mapping_proxy_snapshot_hits, 2);
        assert!(optimized[0].bind(py).eq(canonical[0].bind(py)).unwrap());
        assert_eq!(
            fixture
                .getattr("events")
                .unwrap()
                .extract::<Vec<String>>()
                .unwrap(),
            vec!["select:1", "select:1"]
        );
    });
}

#[test]
fn callable_unique_join_v2_declines_unlisted_exact_types_before_callbacks() {
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"from collections.abc import Mapping\n\nevents = []\n\nclass Allowed(Mapping):\n    def __init__(self, values):\n        self.values = values\n    def __getitem__(self, name):\n        return self.values[name]\n    def __iter__(self):\n        return iter(self.values)\n    def __len__(self):\n        return len(self.values)\n\nclass Unlisted(Allowed):\n    pass\n\nleft = [Allowed({'id': 1, 'left': True})]\nright = [Allowed({'id': 1, 'right': True}), Unlisted({'id': 2, 'right': False})]\nright_allowed = (Allowed({'id': 1, 'right': True}),)\n\ndef select(row):\n    events.append(f\"select:{row['id']}\")\n    return row['id']\n\ndef as_record(row):\n    events.append(f\"adapt:{row['id']}\")\n    return dict(row)\n",
            c"callable_mapping_decline_fixture.py",
            c"callable_mapping_decline_fixture",
        )
        .unwrap();
        let shared = PyFrozenSet::empty(py).unwrap();
        let allowed_types = PyTuple::new(py, [fixture.getattr("Allowed").unwrap()]).unwrap();

        assert!(
            join_hashable_unique_records_v2(
                &fixture.getattr("left").unwrap(),
                &fixture.getattr("right").unwrap(),
                &fixture.getattr("select").unwrap(),
                &fixture.getattr("select").unwrap(),
                &fixture.getattr("as_record").unwrap(),
                false,
                PyString::new(py, "_right").as_any(),
                shared.as_any(),
                allowed_types.as_any(),
            )
            .unwrap()
            .is_none()
        );
        assert!(
            join_hashable_unique_records_v1(
                &fixture.getattr("left").unwrap(),
                &fixture.getattr("right_allowed").unwrap(),
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
                .extract::<Vec<String>>()
                .unwrap()
                .is_empty()
        );
    });
}

#[test]
fn callable_unique_join_v2_validates_exact_type_tuple_before_callbacks() {
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"events = []\nleft = [{'id': 1}]\nright = ({'id': 1},)\n\ndef select(row):\n    events.append('select')\n    return row['id']\n\ndef as_record(row):\n    events.append('adapt')\n    return dict(row)\n",
            c"callable_token_validation_fixture.py",
            c"callable_token_validation_fixture",
        )
        .unwrap();
        let shared = PyFrozenSet::empty(py).unwrap();
        let invalid_container = PyList::new(py, [py.get_type::<PyDict>()]).unwrap();
        let invalid_item = PyTuple::new(py, [1_i64]).unwrap();

        for tokens in [invalid_container.as_any(), invalid_item.as_any()] {
            assert!(
                join_hashable_unique_records_v2(
                    &fixture.getattr("left").unwrap(),
                    &fixture.getattr("right").unwrap(),
                    &fixture.getattr("select").unwrap(),
                    &fixture.getattr("select").unwrap(),
                    &fixture.getattr("as_record").unwrap(),
                    false,
                    PyString::new(py, "_right").as_any(),
                    shared.as_any(),
                    tokens,
                )
                .unwrap()
                .is_none()
            );
        }
        assert!(
            fixture
                .getattr("events")
                .unwrap()
                .extract::<Vec<String>>()
                .unwrap()
                .is_empty()
        );
    });
}

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
