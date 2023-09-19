use std::sync::Arc;

use arrow::datatypes::{Field, Schema};
use pyo3::exceptions::{PyConnectionError, PyIOError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyString};
mod arrow_convert;
pub mod bulk_insert;
pub mod connect;

fn field_into_dict<'a>(py: Python<'a>, field: &'a Field) -> &'a PyDict {
    let d = PyDict::new(py);
    d.set_item("name", field.name().clone()).unwrap();
    d.set_item("arrow_type", field.data_type().to_string())
        .unwrap();

    d
}
fn into_dict<'a>(py: Python<'a>, schema: Arc<Schema>) -> &PyDict {
    let d = PyDict::new(py);
    let fields: Vec<&PyDict> = schema
        .fields
        .iter()
        .map(|f| field_into_dict(py, f))
        .collect();

    d.set_item("fields", fields).unwrap();
    let seq: Vec<(&PyString, &PyString)> = schema
        .metadata
        .iter()
        .map(|(key, value)| (PyString::new(py, key), PyString::new(py, value)))
        .collect();
    let metadata = PyDict::from_sequence(py, seq.into_py(py));
    d.set_item("metadata", metadata.unwrap()).unwrap();
    d
}

async fn insert_arrow_stream_to_sql_rs(
    connection_string: String,
    table_name: String,
    url: String,
    user: String,
    password: String,
    aad_token: Option<String>,
) -> Result<Arc<Schema>, PyErr> {
    let db_client = connect::connect_sql(&connection_string, aad_token).await;
    if let Err(er) = db_client {
        return Err(PyErr::new::<PyConnectionError, _>(format!(
            "Error connecting: {er}"
        )));
    }
    let mut db_client = db_client.unwrap();
    let bres = bulk_insert::bulk_insert(&mut db_client, &table_name, &url, &user, &password).await;
    if let Err(er) = bres {
        return Err(PyErr::new::<PyIOError, _>(format!(
            "Error connecting: {er}"
        )));
    }
    Ok(bres.unwrap())
}

#[pyfunction]
fn insert_arrow_stream_to_sql(
    py: Python,
    connection_string: String,
    table_name: String,
    url: String,
    user: String,
    password: String,
    aad_token: Option<String>,
) -> PyResult<&PyAny> {
    pyo3_asyncio::tokio::future_into_py(py, async move {
        let res = insert_arrow_stream_to_sql_rs(
            connection_string,
            table_name,
            url,
            user,
            password,
            aad_token,
        )
        .await?;
        Ok(Python::with_gil(|py| {
            let d: Py<PyDict> = into_dict(py, res).into();
            d
        }))
    })
}

/// A Python module implemented in Rust.
#[pymodule]
fn _lowlevel(_py: Python, m: &PyModule) -> PyResult<()> {
    pyo3_log::init();
    m.add_function(wrap_pyfunction!(insert_arrow_stream_to_sql, m)?)?;
    Ok(())
}
