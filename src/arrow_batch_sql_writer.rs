use std::fmt::Display;

use arrow::{datatypes::Schema, record_batch::RecordBatch};

pub(crate) struct ArrowBatchSqlWriter<'a> {
    schema: Schema,
    table: &'a str,
    column_names_csv: &'a str,
    chunk_size: usize,
}

#[derive(Debug, PartialEq, Eq, Ord, PartialOrd, Hash, Clone)]
pub enum WriterError {
    WellSee,
}

impl Display for WriterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("arrow error"))
    }
}

impl<'a> ArrowBatchSqlWriter<'a> {
    pub(crate) fn new(
        schema: Schema,
        table: &'a str,
        column_names_csv: &'a str,
        chunk_size: usize,
    ) -> Self {
        Self {
            schema,
            table,
            column_names_csv,
            chunk_size,
        }
    }

    pub(crate) fn write_batch(&self, batch: &RecordBatch) -> Result<(), WriterError> {
        todo!("write_batch")
    }

    pub(crate) fn flush(&self) -> Result<(), WriterError> {
        todo!("write_batch")
    }
}
