//! The JSON type gate for task results on the io lanes.
//!
//! Results are encoded by msgspec (the shim's `_encode`), the codec the
//! client and the cpu child use, so every lane writes a value the same way.
//! msgspec is permissive where the protocol is strict, though: it writes
//! NaN/Infinity as `null`, a `set` as an array, `bytes` as base64, a
//! `datetime` as a string, and coerces non-str dict keys. This walk rejects
//! all of that before the encoder runs, with the same rules the client's
//! `_validate_json_types` (`py/cauli/_codec.py`) applies on the cpu lane:
//! - non-finite floats (NaN/Infinity) are rejected -- they are not JSON;
//! - dict keys must be `str`;
//! - anything outside the JSON type set is rejected as unserializable.
//!
//! It is a check only: it allocates nothing and builds nothing, so the one
//! traversal that produces bytes is msgspec's.
//!
//! Depth is capped explicitly, so a cyclic or very deep structure comes back
//! as a normal error rather than a stack overflow that aborts the worker.

use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple};

/// Maximum nesting depth accepted: comfortably deeper than any real task
/// result, far shallower than the Rust stack.
pub const MAX_DEPTH: usize = 128;

/// Why a Python value cannot be represented as JSON. Carries a message
/// shaped like the one the Python codec produces, so the resulting
/// `SerializationError` reads the same to task authors.
pub struct ConvError(pub String);

impl ConvError {
    fn new(msg: impl Into<String>) -> Self {
        ConvError(msg.into())
    }
}

/// Ok when `obj` is made only of JSON types: None, bool, int, finite float,
/// str, and list, tuple or str keyed dict of those.
pub fn check_json(obj: &Bound<'_, PyAny>, depth: usize) -> Result<(), ConvError> {
    if depth > MAX_DEPTH {
        return Err(ConvError::new(format!(
            "value nested deeper than {MAX_DEPTH} levels (circular reference?)"
        )));
    }
    if obj.is_none()
        || obj.downcast::<PyBool>().is_ok()
        || obj.downcast::<PyInt>().is_ok()
        || obj.downcast::<PyString>().is_ok()
    {
        return Ok(());
    }
    if obj.downcast::<PyFloat>().is_ok() {
        let f: f64 = obj
            .extract()
            .map_err(|_| ConvError::new("could not read float"))?;
        return if f.is_finite() {
            Ok(())
        } else {
            Err(ConvError::new(
                "Out of range float values are not JSON compliant",
            ))
        };
    }
    if let Ok(d) = obj.downcast::<PyDict>() {
        for (k, v) in d.iter() {
            if k.downcast::<PyString>().is_err() {
                return Err(ConvError::new(format!(
                    "dict keys must be str, got {}",
                    type_name(&k)
                )));
            }
            check_json(&v, depth + 1)?;
        }
        return Ok(());
    }
    if let Ok(l) = obj.downcast::<PyList>() {
        return l.iter().try_for_each(|item| check_json(&item, depth + 1));
    }
    if let Ok(t) = obj.downcast::<PyTuple>() {
        return t.iter().try_for_each(|item| check_json(&item, depth + 1));
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

    fn eval<'py>(py: Python<'py>, src: &str) -> Bound<'py, PyAny> {
        py.eval(&std::ffi::CString::new(src).unwrap(), None, None)
            .unwrap()
    }

    /// Every JSON shape passes, including the ones msgspec writes exactly
    /// and the old tree conversion special cased: integers past 64 bits,
    /// `-0.0`, a tuple (an array, as json.dumps writes it), a bool next to
    /// an int.
    #[test]
    fn accepts_every_json_shape() {
        Python::initialize();
        Python::attach(|py| {
            for ok in [
                "None",
                "[True, 1, -0.0, 2**100, -2**70, 'café']",
                "{'a': 1, 'b': [1, 2, {'c': None}]}",
                "(1, 'a', None)",
                "{'nested': {'deep': {'deeper': [1, [2, [3]]]}}}",
            ] {
                assert!(check_json(&eval(py, ok), 0).is_ok(), "{ok} must pass");
            }
        });
    }

    /// The same rejections the client's codec enforces (PROTOCOL §8), each
    /// one a value msgspec would otherwise write without complaint.
    #[test]
    fn rejects_non_json_values() {
        Python::initialize();
        Python::attach(|py| {
            for bad in [
                "float('nan')",
                "float('inf')",
                "[float('-inf')]",
                "{1, 2}",
                "object()",
                "b'bytes'",
                "(i for i in [1])",
                "__import__('datetime').date(2026, 1, 1)",
                "__import__('uuid').UUID(int=1)",
                "{1: 'a'}",
                "{True: 'a'}",
                "{None: 'a'}",
                "{2.5: 'a'}",
            ] {
                assert!(
                    check_json(&eval(py, bad), 0).is_err(),
                    "{bad} must be rejected"
                );
            }
        });
    }

    /// A self referential container must come back as a normal error, not a
    /// stack overflow that aborts the process.
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
            match check_json(&obj, 0) {
                Ok(()) => panic!("a self referential list must be rejected"),
                Err(e) => assert!(e.0.contains("nested deeper"), "unexpected message: {}", e.0),
            }
        });
    }
}
