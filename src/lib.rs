use pyo3::exceptions::{PyConnectionError, PyIOError};
use pyo3::prelude::*;
mod arrow_convert;
pub mod bulk_insert;
pub mod connect;

async fn insert_arrow_stream_to_sql_rs(
    connection_string: String,
    table_name: String,
    url: String,
    user: String,
    password: String,
    aad_token: Option<String>,
) -> Result<(), PyErr> {
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
    Ok(())
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
    pyo3_asyncio::tokio::future_into_py(
        py,
        insert_arrow_stream_to_sql_rs(
            connection_string,
            table_name,
            url,
            user,
            password,
            aad_token,
        ),
    )
}

/// A Python module implemented in Rust.
#[pymodule]
fn _lowlevel(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(insert_arrow_stream_to_sql, m)?)?;
    Ok(())
}
