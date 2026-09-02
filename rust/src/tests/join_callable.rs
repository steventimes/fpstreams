//! Callable join behavior, caching, and snapshot tests.

use super::*;

mod behavior;
mod direct;

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

        for (collision_count, expected_bulk_merges) in [(6_usize, 2_usize), (7, 0)] {
            let collision_left = PyList::empty(py);
            for left_index in 0..2 {
                let row = PyDict::new(py);
                for (field_index, field) in fields[..collision_count].iter().enumerate() {
                    let value = if field_index == 0 { 0 } else { left_index };
                    row.set_item(field.bind(py), value).unwrap();
                }
                collision_left.append(row).unwrap();
            }
            begin_callable_right_schema_probe_count();
            let collision_joined = join_hashable_many_records_v1(
                collision_left.as_any(),
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
            let collision_probes = end_callable_right_schema_probe_count();

            assert_eq!(collision_joined.len(), 4);
            assert_eq!(collision_probes.bulk_merge_hits, expected_bulk_merges);
        }
    });
}

#[test]
fn callable_many_join_keeps_narrow_schemas_out_of_bulk_merge_across_versions() {
    const ROW_COUNT: usize = 20_000;
    const FIELD_COUNT: usize = 8;

    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"def select(row):\n    return row['key']\n\ndef as_record(row):\n    return dict(row)\n",
            c"callable_many_minimum_bulk_width_fixture.py",
            c"callable_many_minimum_bulk_width_fixture",
        )
        .unwrap();
        let field_names = (0..FIELD_COUNT)
            .map(|index| {
                if index == 0 {
                    "key".to_owned()
                } else {
                    format!("__fpstreams_bulk_min_r{index:02}__")
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
        let left = PyList::empty(py);
        for _ in 0..2 {
            let row = PyDict::new(py);
            row.set_item(fields[0].bind(py), 0).unwrap();
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
        assert_eq!(probes.bulk_merge_hits, 0);
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
fn callable_unique_join_v2_uses_canonical_fallback_for_unlisted_types() {
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"from collections.abc import Mapping\n\nevents = []\n\nclass Allowed(Mapping):\n    def __init__(self, values):\n        self.values = values\n    def __getitem__(self, name):\n        return self.values[name]\n    def __iter__(self):\n        return iter(self.values)\n    def __len__(self):\n        return len(self.values)\n\nclass Unlisted(Allowed):\n    pass\n\nleft = [Allowed({'id': 1, 'left': True})]\nright = [Allowed({'id': 1, 'right': True}), Unlisted({'id': 2, 'right': False})]\nexpected = [{'id': 1, 'left': True, 'id_right': 1, 'right': True}]\n\ndef select(row):\n    events.append(f\"select:{row['id']}\")\n    return row['id']\n\ndef as_record(row):\n    events.append(f\"adapt:{row['id']}\")\n    return dict(row)\n",
            c"callable_mapping_fallback_fixture.py",
            c"callable_mapping_fallback_fixture",
        )
        .unwrap();
        let shared = PyFrozenSet::empty(py).unwrap();
        let allowed_types = PyTuple::new(py, [fixture.getattr("Allowed").unwrap()]).unwrap();
        let record_adapter = PyTuple::new(
            py,
            [
                py.get_type::<PyDict>().as_any(),
                fixture.getattr("as_record").unwrap().as_any(),
            ],
        )
        .unwrap();

        let joined = join_hashable_unique_records_v2(
            &fixture.getattr("left").unwrap(),
            &fixture.getattr("right").unwrap(),
            &fixture.getattr("select").unwrap(),
            &fixture.getattr("select").unwrap(),
            record_adapter.as_any(),
            false,
            PyString::new(py, "_right").as_any(),
            shared.as_any(),
            allowed_types.as_any(),
        )
        .unwrap()
        .expect("the canonical fallback can consume an unlisted later row");

        let expected = fixture
            .getattr("expected")
            .unwrap()
            .cast_into::<PyList>()
            .unwrap();
        assert_eq!(joined.len(), expected.len());
        assert!(
            joined[0]
                .bind(py)
                .eq(expected.get_item(0).unwrap())
                .unwrap()
        );
        assert_eq!(
            fixture
                .getattr("events")
                .unwrap()
                .extract::<Vec<String>>()
                .unwrap(),
            vec!["select:1", "adapt:2", "select:2", "select:1"]
        );
    });
}

#[test]
fn callable_unique_join_v2_validates_exact_type_tuple_before_callbacks() {
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"events = []\nleft = [{'id': 1}]\nright = ({'id': 1},)\n\nclass ObservedMeta(type):\n    def __getattribute__(self, name):\n        if name == '__mro__':\n            events.append('custom-mro')\n        return super().__getattribute__(name)\n\nclass UnsafeType(metaclass=ObservedMeta):\n    pass\n\ndef select(row):\n    events.append('select')\n    return row['id']\n\ndef as_record(row):\n    events.append('adapt')\n    return dict(row)\n",
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
        let unsafe_type = fixture.getattr("UnsafeType").unwrap();
        let synthetic_mro = PyTuple::new(py, [unsafe_type.as_any()]).unwrap();
        let unsafe_pair = PyTuple::new(py, [unsafe_type.as_any(), synthetic_mro.as_any()]).unwrap();
        let unsafe_tokens = PyTuple::new(py, [unsafe_pair.as_any()]).unwrap();
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
                unsafe_tokens.as_any(),
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
fn callable_unique_join_v2_reads_mro_without_metaclass_descriptor_dispatch() {
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"events = []\nclass Descriptor:\n    def __get__(self, instance, owner):\n        events.append('mro-descriptor')\n        raise AssertionError('native preflight dispatched __mro__')\nclass Meta(type):\n    __mro__ = Descriptor()\nclass Row(dict, metaclass=Meta):\n    pass\nleft = [Row(id=1, left=True)]\nright = [Row(id=1, right=True)]\nactual_mro = type.__dict__['__mro__'].__get__(Row, Meta)\nfake_mro = (Row,)\ndef select(row):\n    events.append('select')\n    return row['id']\ndef as_record(row):\n    events.append('adapt')\n    return dict(row)\n",
            c"callable_metaclass_mro_descriptor.py",
            c"callable_metaclass_mro_descriptor",
        )
        .unwrap();
        let row_type = fixture.getattr("Row").unwrap();
        let pair = PyTuple::new(
            py,
            [row_type.clone(), fixture.getattr("actual_mro").unwrap()],
        )
        .unwrap();
        let tokens = PyTuple::new(py, [pair.as_any()]).unwrap();
        let shared = PyFrozenSet::empty(py).unwrap();

        let joined = join_hashable_unique_records_v2(
            &fixture.getattr("left").unwrap(),
            &fixture.getattr("right").unwrap(),
            &fixture.getattr("select").unwrap(),
            &fixture.getattr("select").unwrap(),
            &fixture.getattr("as_record").unwrap(),
            false,
            PyString::new(py, "_right").as_any(),
            shared.as_any(),
            tokens.as_any(),
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
            vec!["adapt", "select", "adapt", "select"]
        );

        fixture
            .getattr("events")
            .unwrap()
            .call_method0("clear")
            .unwrap();
        let fake_pair = PyTuple::new(py, [row_type, fixture.getattr("fake_mro").unwrap()]).unwrap();
        let fake_tokens = PyTuple::new(py, [fake_pair.as_any()]).unwrap();
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
                fake_tokens.as_any(),
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
