//! Direct `serde_json::Value` <-> Python object conversion.
//!
//! Why this exists: the in-process task path used to move arguments and
//! results across the boundary as JSON TEXT. Rust parsed the envelope into a
//! `Value`, re-serialized `args` into a `String`, handed the string to Python,
//! and Python parsed it back into Python objects; on the way out Python
//! validated the result tree, encoded it to a `String`, and Rust parsed that
//! string into a `Value` again. Arguments were parsed twice and serialized
//! once; results were transformed four times.
//!
//! Three of those passes ran **in Python, holding the GIL** -- the single
//! resource every in-process task contends for, so that work was subtracted
//! directly from task throughput. Converting between `Value` and Python
//! objects here deletes the intermediate text entirely: one traversal in, one
//! traversal out, no parser, no serializer, no `String`.
//!
//! Semantics are deliberately identical to what the Python side enforced
//! (PROTOCOL §8), so what is accepted or rejected does not change:
//! - non-finite floats (NaN/Infinity) are rejected -- they are not JSON;
//! - dict keys must be `str` (see `py/cauli/_codec.py` for why both codec
//!   backends require this rather than replicating the stdlib's silent
//!   key coercion);
//! - anything outside the JSON type set is rejected as unserializable;
//! - `bool` is checked before `int`, since Python's `bool` subclasses `int`
//!   and would otherwise serialize as 0/1.
//!
//! Depth is capped explicitly. Python's own recursion limit used to turn a
//! pathologically nested payload into a catchable `RecursionError`; in Rust
//! an unbounded recursive walk would overflow the stack and abort the whole
//! worker, so a cyclic or very deep structure must be rejected as data, not
//! discovered as a crash.

use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple};
use serde_json::{Map, Value};

/// Maximum nesting depth accepted in either direction: comfortably deeper
/// than any real task payload, far shallower than the Rust stack. On the
/// only path that ever feeds `json_to_py` a `Value`, namely text parsed from
/// redis, serde_json's own deserializer already bounds nesting one step
/// earlier and always cleanly, so this check is not what rejects on that
/// path today. It remains defence in depth for any caller that reaches
/// `json_to_py` or `py_to_json` without a parser in front of it, such as the
/// self referential Python container caught below.
pub const MAX_DEPTH: usize = 128;

/// Why a Python value could not be represented as JSON. Carries a message
/// shaped like the one the Python codec used to produce, so the resulting
/// `SerializationError` reads the same to task authors.
pub struct ConvError(pub String);

impl ConvError {
    fn new(msg: impl Into<String>) -> Self {
        ConvError(msg.into())
    }
}

/// `Value` -> Python object. Infallible for any `Value` within the depth cap:
/// every JSON type has a Python counterpart.
pub fn json_to_py<'py>(py: Python<'py>, v: &Value, depth: usize) -> PyResult<Bound<'py, PyAny>> {
    if depth > MAX_DEPTH {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "JSON nesting deeper than {MAX_DEPTH} levels"
        )));
    }
    Ok(match v {
        Value::Null => py.None().into_bound(py),
        Value::Bool(b) => PyBool::new(py, *b).to_owned().into_any(),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i.into_pyobject(py)?.into_any()
            } else if let Some(u) = n.as_u64() {
                u.into_pyobject(py)?.into_any()
            } else {
                let text = n.to_string();
                if text.contains('.') || text.contains('e') || text.contains('E') {
                    // Genuinely written with a decimal point or exponent, so
                    // this is a float on the wire, not an integer literal.
                    // serde_json only produces finite f64 here (it rejects
                    // NaN/Infinity at parse time), so this cannot make a
                    // non-JSON float.
                    n.as_f64().unwrap_or(0.0).into_pyobject(py)?.into_any()
                } else {
                    // A plain integer literal outside i64/u64 range, e.g.
                    // uuid.uuid4().int. With the arbitrary_precision feature
                    // (Cargo.toml), serde_json kept the exact source digits
                    // instead of collapsing them into an approximate f64, so
                    // build the equivalent Python int straight from that
                    // text: Python ints are unbounded, so this is exact.
                    py.get_type::<PyInt>().call1((text,))?
                }
            }
        }
        Value::String(s) => PyString::new(py, s).into_any(),
        Value::Array(items) => {
            let list = PyList::empty(py);
            for item in items {
                list.append(json_to_py(py, item, depth + 1)?)?;
            }
            list.into_any()
        }
        Value::Object(map) => {
            let dict = PyDict::new(py);
            for (k, val) in map {
                // Interned: object keys are overwhelmingly kwarg names and
                // record field names, i.e. the SAME handful of short strings
                // on every invocation of a task. Interning lets CPython reuse
                // one object per distinct key instead of allocating a fresh
                // PyString per key per task, and makes the dict insert hit the
                // fast pointer-equality path.
                dict.set_item(PyString::intern(py, k), json_to_py(py, val, depth + 1)?)?;
            }
            dict.into_any()
        }
    })
}

/// Python object -> `Value`, enforcing the JSON type set.
pub fn py_to_json(obj: &Bound<'_, PyAny>, depth: usize) -> Result<Value, ConvError> {
    if depth > MAX_DEPTH {
        return Err(ConvError::new(format!(
            "value nested deeper than {MAX_DEPTH} levels (circular reference?)"
        )));
    }
    if obj.is_none() {
        return Ok(Value::Null);
    }
    // bool BEFORE int: Python's bool is a subclass of int, and checking int
    // first would turn True into 1.
    if let Ok(b) = obj.downcast::<PyBool>() {
        return Ok(Value::Bool(b.is_true()));
    }
    if obj.downcast::<PyInt>().is_ok() {
        if let Ok(i) = obj.extract::<i64>() {
            return Ok(Value::Number(i.into()));
        }
        if let Ok(u) = obj.extract::<u64>() {
            return Ok(Value::Number(u.into()));
        }
        return Err(ConvError::new(
            "integer is outside the range JSON can represent",
        ));
    }
    if obj.downcast::<PyFloat>().is_ok() {
        let f: f64 = obj
            .extract()
            .map_err(|_| ConvError::new("could not read float"))?;
        return match serde_json::Number::from_f64(f) {
            Some(n) => Ok(Value::Number(n)),
            // from_f64 returns None exactly for NaN/Infinity.
            None => Err(ConvError::new(
                "Out of range float values are not JSON compliant",
            )),
        };
    }
    if let Ok(s) = obj.downcast::<PyString>() {
        let text: String = s
            .extract()
            .map_err(|_| ConvError::new("string is not valid UTF-8 (lone surrogate?)"))?;
        return Ok(Value::String(text));
    }
    if let Ok(d) = obj.downcast::<PyDict>() {
        let mut map = Map::with_capacity(d.len());
        for (k, v) in d.iter() {
            let key = match k.downcast::<PyString>() {
                Ok(s) => s
                    .extract::<String>()
                    .map_err(|_| ConvError::new("dict key is not valid UTF-8"))?,
                Err(_) => {
                    return Err(ConvError::new(format!(
                        "dict keys must be str, got {}",
                        type_name(&k)
                    )))
                }
            };
            map.insert(key, py_to_json(&v, depth + 1)?);
        }
        return Ok(Value::Object(map));
    }
    if let Ok(l) = obj.downcast::<PyList>() {
        let mut out = Vec::with_capacity(l.len());
        for item in l.iter() {
            out.push(py_to_json(&item, depth + 1)?);
        }
        return Ok(Value::Array(out));
    }
    if let Ok(t) = obj.downcast::<PyTuple>() {
        let mut out = Vec::with_capacity(t.len());
        for item in t.iter() {
            out.push(py_to_json(&item, depth + 1)?);
        }
        return Ok(Value::Array(out));
    }
    Err(ConvError::new(format!(
        "Object of type {} is not JSON serializable",
        type_name(obj)
    )))
}

fn type_name(obj: &Bound<'_, PyAny>) -> String {
    obj.get_type()
        .name()
        .map(|n| n.to_string())
        .unwrap_or_else(|_| "?".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Every JSON shape must survive Value -> Python -> Value unchanged. This
    /// is the property the whole optimization rests on: if the round trip is
    /// not lossless, tasks silently receive or return different data than
    /// they did when the boundary was JSON text.
    #[test]
    fn round_trip_is_lossless() {
        Python::initialize();
        Python::attach(|py| {
            let cases = vec![
                json!(null),
                json!(true),
                json!(false),
                json!(0),
                json!(-1),
                json!(9_007_199_254_740_993i64),
                json!(1.5),
                json!(""),
                json!("café — unicode"),
                json!([]),
                json!({}),
                json!([1, "a", null, true, 2.5]),
                json!({"a": 1, "b": [1, 2, {"c": null}]}),
                json!({"nested": {"deep": {"deeper": [1, [2, [3]]]}}}),
            ];
            for v in cases {
                let obj = json_to_py(py, &v, 0).expect("to python");
                let back = py_to_json(&obj, 0).map_err(|e| e.0).expect("back to json");
                assert_eq!(back, v, "round trip changed the value");
            }
        });
    }

    /// bool must not collapse into int in either direction (Python's bool is
    /// an int subclass, so a naive check order silently turns true into 1).
    #[test]
    fn bool_is_not_int() {
        Python::initialize();
        Python::attach(|py| {
            let obj = json_to_py(py, &json!({"t": true, "n": 1}), 0).unwrap();
            let back = py_to_json(&obj, 0).map_err(|e| e.0).unwrap();
            assert_eq!(back["t"], Value::Bool(true));
            assert_eq!(back["n"], json!(1));
        });
    }

    /// The same rejections the Python codec enforced (PROTOCOL §8), so moving
    /// the conversion into Rust does not quietly widen what is accepted.
    #[test]
    fn rejects_non_json_values() {
        Python::initialize();
        Python::attach(|py| {
            // NaN / Infinity are not JSON.
            for bad in ["float('nan')", "float('inf')", "float('-inf')"] {
                let obj = py
                    .eval(&std::ffi::CString::new(bad).unwrap(), None, None)
                    .unwrap();
                assert!(py_to_json(&obj, 0).is_err(), "{bad} must be rejected");
            }
            // Types outside the JSON set.
            for bad in ["{1, 2}", "object()", "b'bytes'", "(i for i in [1])"] {
                let obj = py
                    .eval(&std::ffi::CString::new(bad).unwrap(), None, None)
                    .unwrap();
                assert!(py_to_json(&obj, 0).is_err(), "{bad} must be rejected");
            }
            // Non-str dict keys.
            for bad in ["{1: 'a'}", "{True: 'a'}", "{None: 'a'}", "{2.5: 'a'}"] {
                let obj = py
                    .eval(&std::ffi::CString::new(bad).unwrap(), None, None)
                    .unwrap();
                assert!(py_to_json(&obj, 0).is_err(), "{bad} must be rejected");
            }
        });
    }

    /// A self referential container must come back as a normal error, not a
    /// stack overflow that aborts the process (Python's own recursion limit
    /// used to make this a catchable RecursionError).
    #[test]
    fn cyclic_structure_is_an_error_not_a_crash() {
        Python::initialize();
        Python::attach(|py| {
            let obj = py
                .eval(c"[]", None, None)
                .and_then(|l| {
                    l.call_method1("append", (&l,))?;
                    Ok(l)
                })
                .unwrap();
            match py_to_json(&obj, 0) {
                Ok(_) => panic!("a self referential list must be rejected"),
                Err(e) => assert!(e.0.contains("nested deeper"), "unexpected message: {}", e.0),
            }
        });
    }

    /// A tuple is JSON-encoded as an array, matching json.dumps.
    #[test]
    fn tuple_becomes_array() {
        Python::initialize();
        Python::attach(|py| {
            let obj = py.eval(c"(1, 'a', None)", None, None).unwrap();
            let v = py_to_json(&obj, 0).map_err(|e| e.0).unwrap();
            assert_eq!(v, json!([1, "a", null]));
        });
    }

    /// Audit finding, CRITICAL: `args`/`kwargs` are a bare `Value` with no
    /// bound on integer size, and serde_json's default `Number` silently
    /// falls back to f64 for any integer literal outside i64/u64 range, at
    /// parse time, before this function ever runs. A realistic value this
    /// size is simply `uuid.uuid4().int`. It must reach Python as the exact
    /// int the caller sent, never a corrupted float.
    #[test]
    fn huge_integer_survives_as_exact_python_int_not_float() {
        Python::initialize();
        Python::attach(|py| {
            let raw = "338958331192819208857724424333372550912"; // uuid4().int shaped
            let v: Value = serde_json::from_str(raw).unwrap();
            let obj = json_to_py(py, &v, 0).expect("convert");
            assert!(
                obj.is_instance_of::<PyInt>(),
                "a JSON integer outside i64/u64 range must still become a Python int, not a float"
            );
            assert_eq!(
                obj.str().unwrap().extract::<String>().unwrap(),
                raw,
                "value must round trip exactly, not lose precision to f64"
            );
        });
    }

    /// End to end version of the reproduction above: the exact value from
    /// the audit, arriving as a real envelope's kwargs, must reach the task
    /// as an int with the exact value the caller sent.
    #[test]
    fn envelope_kwargs_huge_integer_reaches_python_as_exact_int() {
        Python::initialize();
        Python::attach(|py| {
            let raw = r#"{"id":"a","task":"t","kwargs":{"uid": 338958331192819208857724424333372550912}}"#;
            let env: crate::envelope::Envelope = serde_json::from_str(raw).unwrap();
            let obj = json_to_py(py, env.kwargs_ref(), 0).expect("convert");
            let uid = obj.get_item("uid").unwrap();
            assert!(uid.is_instance_of::<PyInt>(), "must be an int, not a float");
            assert_eq!(
                uid.str().unwrap().extract::<String>().unwrap(),
                "338958331192819208857724424333372550912"
            );
        });
    }

    /// Boundary sweep around i64::MAX and u64::MAX, in both directions, plus
    /// the regression a careless fix causes: a normal small integer must
    /// stay an int, and a genuine large float must stay a float.
    #[test]
    fn integer_boundaries_are_exact_and_typed_correctly() {
        Python::initialize();
        Python::attach(|py| {
            let int_cases = [
                "9223372036854775807",              // 2^63 - 1 (i64::MAX)
                "9223372036854775808",              // 2^63
                "18446744073709551615",             // u64::MAX
                "18446744073709551616",             // u64::MAX + 1 (2^64): first corrupted value
                "1267650600228229401496703205376",  // 2^100
                "-1267650600228229401496703205376", // large negative, below i64::MIN
                "5",                                // normal small integer: must not regress
                "-5",
            ];
            for raw in int_cases {
                let v: Value = serde_json::from_str(raw).unwrap();
                let obj = json_to_py(py, &v, 0).expect("convert");
                assert!(obj.is_instance_of::<PyInt>(), "{raw}: must be a Python int");
                assert_eq!(
                    obj.str().unwrap().extract::<String>().unwrap(),
                    raw,
                    "{raw}: must round trip exactly"
                );
            }

            // A genuine float that happens to be large must stay a float,
            // never get swept into the large integer literal path above.
            let v: Value = serde_json::from_str("1.7976931348623157e308").unwrap();
            let obj = json_to_py(py, &v, 0).expect("convert");
            assert!(
                obj.is_instance_of::<PyFloat>(),
                "a real float literal must stay a float"
            );
        });
    }

    /// The corrected `MAX_DEPTH` doc comment rests on this: on the only path
    /// that ever feeds `json_to_py` a `Value` (text parsed from redis),
    /// serde_json's own deserializer bounds nesting at this same threshold
    /// before `json_to_py` runs at all. Confirms that holds regardless of
    /// the `arbitrary_precision` feature.
    #[test]
    fn serde_json_parser_bounds_nesting_at_max_depth_independent_of_the_check_here() {
        let nested = |depth: usize| {
            let mut s = String::with_capacity(depth * 2 + 1);
            for _ in 0..depth {
                s.push('[');
            }
            s.push('1');
            for _ in 0..depth {
                s.push(']');
            }
            s
        };
        assert!(serde_json::from_str::<Value>(&nested(MAX_DEPTH - 1)).is_ok());
        assert!(serde_json::from_str::<Value>(&nested(MAX_DEPTH)).is_err());
    }
}
