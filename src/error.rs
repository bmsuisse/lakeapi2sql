use pyo3::{exceptions::*, PyErr};
use thiserror::Error;

#[derive(Error, Debug)]

pub enum LakeApi2SqlError {
    #[error("Error connecting: {dtype} to {column_type:?}")]
    NotSupported {
        dtype: arrow::datatypes::DataType,
        column_type: tiberius::ColumnType,
    },

    #[error("Error joining: {0}")]
    JoinError(#[from] tokio::task::JoinError),

    #[error("Arrow Error: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),

    #[error("IO Error: {0}")]
    IOError(#[from] std::io::Error),

    #[error("HTTP Error: {0}")]
    HttpError(#[from] reqwest::Error),

    #[error("Send Error: {0}")]
    SendError(#[from] tokio::sync::mpsc::error::SendError<arrow::array::RecordBatch>),

    #[error(transparent)]
    TiberiusError(#[from] tiberius::error::Error),
}

impl From<LakeApi2SqlError> for PyErr {
    fn from(val: LakeApi2SqlError) -> Self {
        match val {
            v @ LakeApi2SqlError::NotSupported {
                dtype: _,
                column_type: _,
            } => PyErr::new::<PyTypeError, _>(format!("{:?}", v)),
            LakeApi2SqlError::JoinError(e) => PyErr::new::<PyIOError, _>(format!("{:?}", e)),
            LakeApi2SqlError::ArrowError(e) => PyErr::new::<PyValueError, _>(format!("{:?}", e)),
            LakeApi2SqlError::IOError(e) => PyErr::new::<PyIOError, _>(format!("{:?}", e)),
            LakeApi2SqlError::HttpError(e) => PyErr::new::<PyIOError, _>(format!("{:?}", e)),
            LakeApi2SqlError::SendError(e) => PyErr::new::<PyIOError, _>(format!("{:?}", e)),
            LakeApi2SqlError::TiberiusError(e) => PyErr::new::<PyIOError, _>(format!("{:?}", e)),
        }
    }
}
