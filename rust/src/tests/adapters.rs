//! Record-adapter and native materializer tests.

use super::*;

#[test]
fn native_materializers_validate_targets_and_build_requested_containers() {
    assert!(materialize_target(3).is_err());

    Python::initialize();
    Python::attach(|py| {
        let list =
            materialize_values(py, vec![2_i64, 1, 2], materialize_target(0).unwrap()).unwrap();
        let tuple =
            materialize_values(py, vec![2_i64, 1, 2], materialize_target(1).unwrap()).unwrap();
        let set =
            materialize_values(py, vec![2_i64, 1, 2], materialize_target(2).unwrap()).unwrap();

        assert_eq!(list.bind(py).extract::<Vec<i64>>().unwrap(), vec![2, 1, 2]);
        assert_eq!(tuple.bind(py).extract::<Vec<i64>>().unwrap(), vec![2, 1, 2]);
        assert_eq!(
            set.bind(py)
                .extract::<std::collections::HashSet<i64>>()
                .unwrap()
                .len(),
            2
        );
    });
}

#[test]
fn exact_dict_sort_guard_accepts_only_exact_list_and_dict_types_without_protocol_calls() {
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"class Record(dict):\n    calls = 0\n    def __getitem__(self, key):\n        type(self).calls += 1\n        raise AssertionError('record protocol called')\nclass RecordList(list):\n    calls = 0\n    def __iter__(self):\n        type(self).calls += 1\n        raise AssertionError('list protocol called')\nrecord = Record(key=1)\nrecord_list = RecordList([{'key': 1}])\n",
            c"exact_dict_sort_guard.py",
            c"exact_dict_sort_guard",
        )
        .unwrap();
        let first = PyDict::new(py);
        first.set_item("key", 1_i64).unwrap();
        first.set_item(2_i64, 3_i64).unwrap();
        let exact = PyList::new(py, [&first]).unwrap();
        let tuple = PyTuple::new(py, [&first]).unwrap();
        let dict_subclass = PyList::new(py, [fixture.getattr("record").unwrap()]).unwrap();

        assert!(all_exact_dict_rows_v1(exact.as_any()).unwrap());
        assert!(!all_exact_dict_rows_v1(tuple.as_any()).unwrap());
        assert!(!all_exact_dict_rows_v1(dict_subclass.as_any()).unwrap());
        assert!(!all_exact_dict_rows_v1(&fixture.getattr("record_list").unwrap()).unwrap());
        assert_eq!(
            fixture
                .getattr("Record")
                .unwrap()
                .getattr("calls")
                .unwrap()
                .extract::<usize>()
                .unwrap(),
            0
        );
        assert_eq!(
            fixture
                .getattr("RecordList")
                .unwrap()
                .getattr("calls")
                .unwrap()
                .extract::<usize>()
                .unwrap(),
            0
        );
    });
}

#[cfg(not(Py_GIL_DISABLED))]
#[test]
fn standard_namedtuple_adapter_snapshots_two_types_and_preserves_zip_truncation() {
    let _guard = STANDARD_NAMEDTUPLE_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"from abc import get_cache_token\nfrom collections import namedtuple\n\nA = namedtuple('A', 'id value')\nB = namedtuple('B', 'id left tail')\nevents = []\ntoken = get_cache_token()\ncanonical_code = A.__dict__['_asdict'].__code__\nshort = tuple.__new__(A, (7,))\nlong = tuple.__new__(A, (8, 9, 10))\n\ndef fallback(row):\n    events.append(type(row).__name__)\n    return {'fallback': True}\n",
            c"standard_namedtuple_fast_fixture.py",
            c"standard_namedtuple_fast_fixture",
        )
        .unwrap();
        let record_types = PyTuple::new(
            py,
            [fixture.getattr("A").unwrap(), fixture.getattr("B").unwrap()],
        )
        .unwrap();
        let fallback = fixture.getattr("fallback").unwrap();
        let record_continuations =
            PyTuple::new(py, [py.get_type::<PyDict>().as_any(), &fallback]).unwrap();
        fixture
            .setattr("_RECORD_CONTINUATIONS", &record_continuations)
            .unwrap();
        let record_globals = fixture.getattr("__dict__").unwrap();
        let adapter = standard_namedtuple_record_adapter_v1(
            record_types.as_any(),
            &fallback,
            &fixture.getattr("get_cache_token").unwrap(),
            &fixture.getattr("token").unwrap(),
            &fixture.getattr("namedtuple").unwrap(),
            &py.import("types").unwrap().getattr("CodeType").unwrap(),
            &py.import("collections.abc")
                .unwrap()
                .getattr("Mapping")
                .unwrap(),
            record_continuations.as_any(),
            &record_globals,
        )
        .unwrap()
        .expect("two canonical NamedTuple layouts should be admitted");

        let first = adapter
            .bind(py)
            .call1((fixture.getattr("A").unwrap().call1((1, 2)).unwrap(),))
            .unwrap()
            .cast_into_exact::<PyDict>()
            .unwrap();
        let second = adapter
            .bind(py)
            .call1((fixture.getattr("A").unwrap().call1((1, 2)).unwrap(),))
            .unwrap()
            .cast_into_exact::<PyDict>()
            .unwrap();
        assert!(!first.is(&second));
        assert_eq!(
            first
                .iter()
                .map(|(name, value)| (
                    name.extract::<String>().unwrap(),
                    value.extract::<i64>().unwrap()
                ))
                .collect::<Vec<_>>(),
            vec![("id".to_owned(), 1), ("value".to_owned(), 2)]
        );

        let short = adapter
            .bind(py)
            .call1((fixture.getattr("short").unwrap(),))
            .unwrap()
            .cast_into_exact::<PyDict>()
            .unwrap();
        assert_eq!(short.len(), 1);
        assert_eq!(
            short
                .get_item("id")
                .unwrap()
                .unwrap()
                .extract::<i64>()
                .unwrap(),
            7
        );
        let long = adapter
            .bind(py)
            .call1((fixture.getattr("long").unwrap(),))
            .unwrap()
            .cast_into_exact::<PyDict>()
            .unwrap();
        assert_eq!(long.len(), 2);
        assert_eq!(
            long.iter()
                .map(|(name, value)| (
                    name.extract::<String>().unwrap(),
                    value.extract::<i64>().unwrap()
                ))
                .collect::<Vec<_>>(),
            vec![("id".to_owned(), 8), ("value".to_owned(), 9)]
        );

        let b = adapter
            .bind(py)
            .call1((fixture
                .getattr("B")
                .unwrap()
                .call1((3, "left", "tail"))
                .unwrap(),))
            .unwrap()
            .cast_into_exact::<PyDict>()
            .unwrap();
        assert_eq!(
            b.iter()
                .map(|(name, _value)| name.extract::<String>().unwrap())
                .collect::<Vec<_>>(),
            vec!["id", "left", "tail"]
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

#[cfg(not(Py_GIL_DISABLED))]
#[test]
fn standard_namedtuple_adapter_declines_noncanonical_factory_inputs() {
    let _guard = STANDARD_NAMEDTUPLE_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"from abc import get_cache_token\nfrom collections import namedtuple\nfrom collections.abc import Mapping\n\nA = namedtuple('A', 'id value')\nB = namedtuple('B', 'id value')\nC = namedtuple('C', 'id value')\ntoken = get_cache_token()\ncanonical_code = A.__dict__['_asdict'].__code__\nwrong_code = (lambda row: row).__code__\n\ndef fallback(row):\n    return dict(row._asdict())\n",
            c"standard_namedtuple_admission_fixture.py",
            c"standard_namedtuple_admission_fixture",
        )
        .unwrap();
        let fallback = fixture.getattr("fallback").unwrap();
        let get_cache_token = fixture.getattr("get_cache_token").unwrap();
        let token = fixture.getattr("token").unwrap();
        let canonical_factory = fixture.getattr("namedtuple").unwrap();
        let a = fixture.getattr("A").unwrap();
        let b = fixture.getattr("B").unwrap();
        let c = fixture.getattr("C").unwrap();
        let record_continuations =
            PyTuple::new(py, [py.get_type::<PyDict>().as_any(), &fallback]).unwrap();
        fixture
            .setattr("_RECORD_CONTINUATIONS", &record_continuations)
            .unwrap();
        let record_globals = fixture.getattr("__dict__").unwrap();

        let empty = PyTuple::empty(py);
        let three = PyTuple::new(py, [&a, &b, &c]).unwrap();
        let list = PyList::new(py, [&a]).unwrap();
        for invalid in [empty.as_any(), three.as_any(), list.as_any()] {
            assert!(
                standard_namedtuple_record_adapter_v1(
                    invalid,
                    &fallback,
                    &get_cache_token,
                    &token,
                    &canonical_factory,
                    &py.import("types").unwrap().getattr("CodeType").unwrap(),
                    &fixture.getattr("Mapping").unwrap(),
                    record_continuations.as_any(),
                    &record_globals,
                )
                .unwrap()
                .is_none()
            );
        }
        let one = PyTuple::new(py, [&a]).unwrap();
        assert!(
            standard_namedtuple_record_adapter_v1(
                one.as_any(),
                &fallback,
                &get_cache_token,
                &token,
                &fixture.getattr("wrong_code").unwrap(),
                &py.import("types").unwrap().getattr("CodeType").unwrap(),
                &fixture.getattr("Mapping").unwrap(),
                record_continuations.as_any(),
                &record_globals,
            )
            .unwrap()
            .is_none()
        );

        fixture
            .getattr("Mapping")
            .unwrap()
            .call_method1("register", (&b,))
            .unwrap();
        let registered_token = get_cache_token.call0().unwrap();
        let registered = PyTuple::new(py, [&b]).unwrap();
        assert!(
            standard_namedtuple_record_adapter_v1(
                registered.as_any(),
                &fallback,
                &get_cache_token,
                &registered_token,
                &canonical_factory,
                &py.import("types").unwrap().getattr("CodeType").unwrap(),
                &fixture.getattr("Mapping").unwrap(),
                record_continuations.as_any(),
                &record_globals,
            )
            .unwrap()
            .is_none()
        );

        a.setattr("__iter__", fixture.getattr("fallback").unwrap())
            .unwrap();
        assert!(
            standard_namedtuple_record_adapter_v1(
                one.as_any(),
                &fallback,
                &get_cache_token,
                &registered_token,
                &canonical_factory,
                &py.import("types").unwrap().getattr("CodeType").unwrap(),
                &fixture.getattr("Mapping").unwrap(),
                record_continuations.as_any(),
                &record_globals,
            )
            .unwrap()
            .is_none()
        );
    });
}

#[cfg(not(Py_GIL_DISABLED))]
#[test]
fn standard_namedtuple_adapter_guard_drift_falls_back_once_on_the_same_row() {
    let _guard = STANDARD_NAMEDTUPLE_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"from abc import get_cache_token\nfrom collections import namedtuple\nfrom collections.abc import Mapping\n\ndef make_case(kind):\n    T = namedtuple('T_' + kind, 'id value')\n    row = T(1, 2)\n    events = []\n    token = get_cache_token()\n    code = T.__dict__['_asdict'].__code__\n\n    def fallback(current):\n        events.append(kind)\n        return {'fallback': kind}\n\n    def replacement_factory():\n        _dict = dict\n        _zip = zip\n        def replacement(self):\n            return _dict(_zip(('id',), self))\n        return replacement\n\n    replacement = replacement_factory()\n    def mutate():\n        if kind == 'abc':\n            Mapping.register(T)\n        elif kind == 'mro':\n            class Base(tuple):\n                __slots__ = ()\n            T.__bases__ = (Base,)\n        elif kind == 'dataclass':\n            T.__dataclass_fields__ = {}\n        elif kind == 'fields':\n            T._fields = tuple(['id', 'value'])\n        elif kind == 'asdict_identity':\n            T._asdict = replacement\n        elif kind == 'asdict_code':\n            T.__dict__['_asdict'].__code__ = replacement.__code__\n        elif kind == 'closure':\n            names = T.__dict__['_asdict'].__code__.co_freevars\n            index = names.index('_dict')\n            T.__dict__['_asdict'].__closure__[index].cell_contents = list\n        elif kind == 'iter':\n            T.__iter__ = lambda self: iter(())\n        elif kind == 'getattribute':\n            T.__getattribute__ = lambda self, name: tuple.__getattribute__(self, name)\n        else:\n            raise AssertionError(kind)\n\n    return T, row, fallback, token, code, events, mutate\n",
            c"standard_namedtuple_guard_fixture.py",
            c"standard_namedtuple_guard_fixture",
        )
        .unwrap();
        for kind in [
            "abc",
            "mro",
            "dataclass",
            "fields",
            "asdict_identity",
            "asdict_code",
            "closure",
            "iter",
            "getattribute",
        ] {
            let case = fixture
                .getattr("make_case")
                .unwrap()
                .call1((kind,))
                .unwrap()
                .cast_into_exact::<PyTuple>()
                .unwrap();
            let record_type = case.get_item(0).unwrap();
            let row = case.get_item(1).unwrap();
            let fallback = case.get_item(2).unwrap();
            let token = case.get_item(3).unwrap();
            let _code = case.get_item(4).unwrap();
            let events = case.get_item(5).unwrap();
            let mutate = case.get_item(6).unwrap();
            let record_types = PyTuple::new(py, [&record_type]).unwrap();
            let record_continuations =
                PyTuple::new(py, [py.get_type::<PyDict>().as_any(), &fallback]).unwrap();
            fixture
                .setattr("_RECORD_CONTINUATIONS", &record_continuations)
                .unwrap();
            let record_globals = fixture.getattr("__dict__").unwrap();
            let adapter = standard_namedtuple_record_adapter_v1(
                record_types.as_any(),
                &fallback,
                &fixture.getattr("get_cache_token").unwrap(),
                &token,
                &fixture.getattr("namedtuple").unwrap(),
                &py.import("types").unwrap().getattr("CodeType").unwrap(),
                &fixture.getattr("Mapping").unwrap(),
                record_continuations.as_any(),
                &record_globals,
            )
            .unwrap()
            .unwrap_or_else(|| panic!("canonical {kind} fixture should be admitted"));

            mutate.call0().unwrap();
            let outcome = adapter.bind(py).call1((&row,));
            if kind == "abc" {
                assert!(
                    outcome
                        .unwrap_err()
                        .is_instance_of::<pyo3::exceptions::PyTypeError>(py)
                );
                assert!(events.extract::<Vec<String>>().unwrap().is_empty());
                continue;
            }
            let result = outcome.unwrap().cast_into_exact::<PyDict>().unwrap();
            assert_eq!(
                result
                    .get_item("fallback")
                    .unwrap()
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                kind
            );
            assert_eq!(events.extract::<Vec<String>>().unwrap(), vec![kind]);
        }
    });
}

#[cfg(not(Py_GIL_DISABLED))]
#[test]
fn callable_join_namedtuple_snapshot_falls_back_once_after_live_list_replacement() {
    let _guard = STANDARD_NAMEDTUPLE_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    Python::initialize();
    Python::attach(|py| {
        let fixture = PyModule::from_code(
            py,
            c"from abc import get_cache_token\nfrom collections import namedtuple\n\nRow = namedtuple('Row', 'id value')\ntoken = get_cache_token()\ncanonical_code = Row.__dict__['_asdict'].__code__\nevents = []\nright = [Row(1, 'r1'), Row(2, 'r2')]\nleft = [Row(2, 'l2')]\n\ndef stable_cache_token():\n    return token\n\nclass Replacement:\n    def __init__(self, id, value):\n        self.id = id\n        self.value = value\n    def _asdict(self):\n        return {'id': self.id, 'value': self.value}\n\ndef fallback(row):\n    events.append(f'fallback:{row.id}')\n    return dict(row._asdict())\n\ndef right_key(row):\n    events.append(f'right:{row.id}')\n    if row.id == 1:\n        right[1] = Replacement(2, 'r2')\n    return row.id\n\ndef left_key(row):\n    events.append(f'left:{row.id}')\n    return row.id\n",
            c"standard_namedtuple_live_join_fixture.py",
            c"standard_namedtuple_live_join_fixture",
        )
        .unwrap();
        let row_type = fixture.getattr("Row").unwrap();
        let record_types = PyTuple::new(py, [&row_type]).unwrap();
        let fallback = fixture.getattr("fallback").unwrap();
        let record_continuations =
            PyTuple::new(py, [py.get_type::<PyDict>().as_any(), &fallback]).unwrap();
        fixture
            .setattr("_RECORD_CONTINUATIONS", &record_continuations)
            .unwrap();
        let record_globals = fixture.getattr("__dict__").unwrap();
        let adapter = standard_namedtuple_record_adapter_v1(
            record_types.as_any(),
            &fallback,
            &fixture.getattr("stable_cache_token").unwrap(),
            &fixture.getattr("token").unwrap(),
            &fixture.getattr("namedtuple").unwrap(),
            &py.import("types").unwrap().getattr("CodeType").unwrap(),
            &py.import("collections.abc")
                .unwrap()
                .getattr("Mapping")
                .unwrap(),
            record_continuations.as_any(),
            &record_globals,
        )
        .unwrap()
        .expect("the live-list replacement fixture should admit its canonical NamedTuple");
        let shared = PyFrozenSet::empty(py).unwrap();
        let joined = join_hashable_unique_records_v2(
            &fixture.getattr("left").unwrap(),
            &fixture.getattr("right").unwrap(),
            &fixture.getattr("left_key").unwrap(),
            &fixture.getattr("right_key").unwrap(),
            adapter.bind(py),
            false,
            PyString::new(py, "_right").as_any(),
            shared.as_any(),
            record_types.as_any(),
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
            vec!["right:1", "fallback:2", "right:2", "left:2"]
        );
        assert_eq!(
            joined[0]
                .bind(py)
                .get_item("value")
                .unwrap()
                .unwrap()
                .extract::<String>()
                .unwrap(),
            "l2"
        );
        assert_eq!(
            joined[0]
                .bind(py)
                .get_item("value_right")
                .unwrap()
                .unwrap()
                .extract::<String>()
                .unwrap(),
            "r2"
        );
    });
}
