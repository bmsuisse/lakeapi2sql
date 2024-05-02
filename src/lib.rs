use std::borrow::{Borrow, BorrowMut};
use std::sync::Arc;

use arrow::datatypes::{Field, Schema};
use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::pyarrow::FromPyArrow;
use error::LakeApi2SqlError;
use futures::{StreamExt, TryStreamExt};
use pyo3::exceptions::{PyConnectionError, PyIOError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyInt, PyList, PyString};
mod arrow_convert;
pub mod bulk_insert;
pub mod connect;
pub mod error;
use tiberius::{FromSql, QueryItem, QueryStream, ResultMetadata, Row, ToSql};
use tokio::net::TcpStream;

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
fn into_dict_result<'a>(py: Python<'a>, meta: Option<ResultMetadata>, rows: Vec<Row>) -> &PyDict {
    let d = PyDict::new(py);
    if let Some(meta) = meta {
        let fields: Vec<&PyDict> = meta
            .columns()
            .iter()
            .map(|f| {
                let mut d = PyDict::new(py);
                d.set_item("name", f.name().clone()).unwrap();
                d.set_item("column_type", format!("{0:?}", f.column_type()))
                    .unwrap();

                d
            })
            .collect();

        d.set_item("columns", fields).unwrap();
    }
    let mut py_rows = PyList::new(
        py,
        rows.iter().map(|row| {
            PyList::new(
                py,
                row.cells()
                    .map(|(c, val)| match val {
                        tiberius::ColumnData::U8(o) => o.into_py(py),
                        tiberius::ColumnData::I16(o) => o.into_py(py),
                        tiberius::ColumnData::I32(o) => o.into_py(py),
                        tiberius::ColumnData::I64(o) => o.into_py(py),
                        tiberius::ColumnData::F32(o) => o.into_py(py),
                        tiberius::ColumnData::F64(o) => o.into_py(py),
                        tiberius::ColumnData::Bit(o) => o.into_py(py),
                        tiberius::ColumnData::String(o) => {
                            o.as_ref().map(|x| x.clone().into_owned()).into_py(py)
                        }
                        tiberius::ColumnData::Guid(o) => o.map(|x| x.to_string()).into_py(py),
                        tiberius::ColumnData::Binary(o) => {
                            o.as_ref().map(|x| x.clone().into_owned()).into_py(py)
                        }
                        tiberius::ColumnData::Numeric(o) => o.map(|x| x.to_string()).into_py(py),
                        tiberius::ColumnData::Xml(o) => {
                            o.as_ref().map(|x| x.clone().to_string()).into_py(py)
                        }
                        tiberius::ColumnData::DateTime(o) => o
                            .map(|x| {
                                tiberius::time::time::PrimitiveDateTime::from_sql(&val)
                                    .unwrap()
                                    .unwrap()
                                    .to_string()
                            })
                            .into_py(py),
                        tiberius::ColumnData::SmallDateTime(o) => o
                            .map(|x| {
                                tiberius::time::time::PrimitiveDateTime::from_sql(&val)
                                    .unwrap()
                                    .unwrap()
                                    .to_string()
                            })
                            .into_py(py),
                        tiberius::ColumnData::Time(o) => o
                            .map(|x| {
                                tiberius::time::time::Time::from_sql(&val)
                                    .unwrap()
                                    .unwrap()
                                    .to_string()
                            })
                            .into_py(py),
                        tiberius::ColumnData::Date(o) => o
                            .map(|x| {
                                tiberius::time::time::Date::from_sql(&val)
                                    .unwrap()
                                    .unwrap()
                                    .to_string()
                            })
                            .into_py(py),
                        tiberius::ColumnData::DateTime2(o) => o
                            .map(|x| {
                                tiberius::time::time::PrimitiveDateTime::from_sql(&val)
                                    .unwrap()
                                    .unwrap()
                                    .to_string()
                            })
                            .into_py(py),
                        tiberius::ColumnData::DateTimeOffset(o) => o
                            .map(|x| {
                                tiberius::time::time::PrimitiveDateTime::from_sql(&val)
                                    .unwrap()
                                    .unwrap()
                                    .to_string()
                            })
                            .into_py(py),
                    })
                    .collect::<Vec<PyObject>>(),
            )
        }),
    );
    d.set_item("rows", py_rows);
    d
}

async fn insert_arrow_stream_to_sql_rs(
    connection_string: String,
    table_name: String,
    column_names: Vec<String>,
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
    let bres = bulk_insert::bulk_insert(
        &mut db_client,
        &table_name,
        &column_names
            .iter()
            .map(|x| x.as_str())
            .collect::<Vec<&str>>(),
        &url,
        &user,
        &password,
    )
    .await;
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
    column_names: Vec<String>,
    url: String,
    user: String,
    password: String,
    aad_token: Option<String>,
) -> PyResult<&PyAny> {
    pyo3_asyncio::tokio::future_into_py(py, async move {
        let res = insert_arrow_stream_to_sql_rs(
            connection_string,
            table_name,
            column_names,
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

/// Opaque type to transport connection to an MsSqlConnection over language boundry
#[pyclass]
pub struct MsSqlConnection(
    Arc<tokio::sync::Mutex<tiberius::Client<tokio_util::compat::Compat<TcpStream>>>>,
);

#[pyfunction]
fn connect_sql<'a>(
    py: Python<'a>,
    connection_string: String,
    aad_token: Option<String>,
) -> PyResult<&'a PyAny> {
    pyo3_asyncio::tokio::future_into_py(py, async move {
        let res = connect::connect_sql(&connection_string, aad_token).await;

        match res {
            Ok(re) => Python::with_gil(|py| {
                let cell = Py::new(py, MsSqlConnection(Arc::new(tokio::sync::Mutex::new(re))));
                return cell;
            }),
            Err(er) => Err(PyErr::new::<PyConnectionError, _>(format!(
                "Error connecting: {er}"
            ))),
        }
    })
}

struct ValueWrap(Box<dyn ToSql>);

impl ToSql for ValueWrap {
    fn to_sql(&self) -> tiberius::ColumnData<'_> {
        self.0.to_sql()
    }
}

fn to_exec_args(args: Vec<&PyAny>) -> Result<Vec<ValueWrap>, PyErr> {
    let mut res: Vec<ValueWrap> = Vec::new();
    for i in 0..args.len() - 1 {
        let x = args[i];
        res.push(ValueWrap(if x.is_none() {
            Box::new(Option::<i64>::None) as Box<dyn ToSql>
        } else if let Ok(v) = x.extract::<i64>() {
            Box::new(v) as Box<dyn ToSql>
        } else if let Ok(v) = x.extract::<f64>() {
            Box::new(v) as Box<dyn ToSql>
        } else if let Ok(v) = x.extract::<String>() {
            Box::new(v) as Box<dyn ToSql>
        } else if let Ok(v) = x.extract::<bool>() {
            Box::new(v) as Box<dyn ToSql>
        } else {
            return Err(PyErr::new::<PyTypeError, _>("Unsupported type"));
        }))
    }
    Ok(res)
}

#[pyfunction]
fn execute_sql<'a>(
    py: Python<'a>,
    conn: &MsSqlConnection,
    query: String,
    args: Vec<&PyAny>,
) -> PyResult<&'a PyAny> {
    fn into_list(els: &[u64]) -> Py<PyList> {
        return Python::with_gil(|py2| {
            let list = PyList::new::<u64, _>(py2, vec![]);
            for row in els {
                list.append(row).unwrap();
            }
            let list2: Py<PyList> = list.into();
            list2
        });
    }
    let tds_args = to_exec_args(args)?;

    let mutex = conn.0.clone();
    pyo3_asyncio::tokio::future_into_py(py, async move {
        let res = mutex
            .clone()
            .lock()
            .await
            .execute(
                query,
                tds_args
                    .iter()
                    .map(|x| x.0.borrow() as &dyn ToSql)
                    .collect::<Vec<&dyn ToSql>>()
                    .as_slice(),
            )
            .await;

        match res {
            Ok(re) => {
                return Ok(into_list(re.rows_affected()));
            }
            Err(er) => Err(PyErr::new::<PyIOError, _>(format!("Error executing: {er}"))),
        }
    })
}

#[pyfunction]
fn execute_sql_with_result<'a>(
    py: Python<'a>,
    conn: &MsSqlConnection,
    query: String,
    args: Vec<&PyAny>,
) -> PyResult<&'a PyAny> {
    let tds_args = to_exec_args(args)?;

    let mutex = conn.0.clone();
    pyo3_asyncio::tokio::future_into_py(py, async move {
        let arc = mutex.clone();
        let mut conn = arc.lock().await;
        let res = conn
            .query(
                query,
                tds_args
                    .iter()
                    .map(|x| x.0.borrow() as &dyn ToSql)
                    .collect::<Vec<&dyn ToSql>>()
                    .as_slice(),
            )
            .await;

        match res {
            Ok(mut stream) => {
                let mut meta = None;
                let mut rows = vec![];
                while let Some(item) = stream
                    .try_next()
                    .await
                    .map_err(|er| PyErr::new::<PyIOError, _>(format!("Error executing: {er}")))?
                {
                    match item {
                        // our first item is the column data always
                        QueryItem::Metadata(m) if m.result_index() == 0 => {
                            meta = Some(m);
                            // the first result column info can be handled here
                        }
                        // ... and from there on from 0..N rows
                        QueryItem::Row(row) if row.result_index() == 0 => rows.push(row),
                        // the second result set returns first another metadata item
                        QueryItem::Metadata(meta) => {
                            break;
                        }
                        // ...and, again, we get rows from the second resultset
                        QueryItem::Row(row) => {
                            break;
                        }
                    }
                }
                Ok(Python::with_gil(|py| {
                    let d: Py<PyDict> = into_dict_result(py, meta, rows).into();
                    d
                }))
            }
            Err(er) => Err(PyErr::new::<PyIOError, _>(format!("Error executing: {er}"))),
        }
    })
}

#[pyfunction]
fn insert_arrow_reader_to_sql<'a>(
    py: Python<'a>,
    connection_string: String,
    record_batch_reader: &PyAny,
    table_name: String,
    column_names: Vec<String>,
    aad_token: Option<String>,
) -> PyResult<&'a PyAny> {
    let mut reader: ArrowArrayStreamReader =
        ArrowArrayStreamReader::from_pyarrow(record_batch_reader)?;

    pyo3_asyncio::tokio::future_into_py(py, async move {
        let mut db_client = connect::connect_sql(&connection_string, aad_token).await?;
        let bres = bulk_insert::bulk_insert_reader(
            &mut db_client,
            &table_name,
            &column_names
                .iter()
                .map(|x| x.as_str())
                .collect::<Vec<&str>>(),
            &mut reader,
        )
        .await?;

        Ok(Python::with_gil(|py| {
            let d: Py<PyDict> = into_dict(py, bres).into();
            d
        }))
    })
}

/// A Python module implemented in Rust.
#[pymodule]
fn _lowlevel(_py: Python, m: &PyModule) -> PyResult<()> {
    pyo3_log::init();
    m.add_function(wrap_pyfunction!(insert_arrow_stream_to_sql, m)?)?;
    m.add_function(wrap_pyfunction!(connect_sql, m)?)?;
    m.add_function(wrap_pyfunction!(execute_sql, m)?)?;
    m.add_function(wrap_pyfunction!(execute_sql_with_result, m)?)?;
    m.add_function(wrap_pyfunction!(insert_arrow_reader_to_sql, m)?)?;

    Ok(())
}
