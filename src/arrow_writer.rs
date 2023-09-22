use std::{
    ffi::c_void,
    ptr::{self, null_mut, NonNull},
    slice, str,
    sync::Arc,
};

use crate::{arrow_batch_sql_writer::ArrowBatchSqlWriter, try_, RustPythonError};
use arrow::ffi::ArrowArrayRef;
use arrow::{array::ArrayData, datatypes::Schema, ffi::ArrowArray};
use arrow::{
    array::StructArray,
    ffi::{FFI_ArrowArray, FFI_ArrowSchema},
};
use arrow::{error::ArrowError, record_batch::RecordBatch};

/// Opaque type holding all the state associated with an ODBC writer implementation in Rust. This
/// type also has ownership of the ODBC Connection handle.
pub struct ArrowBulkWriter(ArrowBatchSqlWriter<'static>);

/// Frees the resources associated with an ArrowBulkWriter
///
/// # Safety
///
/// `writer` must point to a valid ArrowOdbcReader.
#[no_mangle]
pub unsafe extern "C" fn arrow_odbc_writer_free(writer: NonNull<ArrowBulkWriter>) {
    drop(Box::from_raw(writer.as_ptr()));
}

/// Creates an Arrow ODBC writer instance.
///
/// Takes ownership of connection even in case of an error.
///
/// # Safety
///
/// * `connection` must point to a valid OdbcConnection. This function takes ownership of the
///   connection, even in case of an error. So The connection must not be freed explicitly
///   afterwards.
/// * `table_buf` must point to a valid utf-8 string
/// * `table_len` describes the len of `table_buf` in bytes.
/// * `schema` pointer to an arrow schema.
/// * `writer_out` in case of success this will point to an instance of `ArrowBulkWriter`. Ownership
///   is transferred to the caller.
#[no_mangle]
pub unsafe extern "C" fn arrow_odbc_writer_make(
    table_buf: *const u8,
    table_len: usize,
    column_names_csv_buf: *const u8,
    column_names_csv_len: usize,
    chunk_size: usize,
    schema: *const c_void,
    writer_out: *mut *mut ArrowBulkWriter,
) -> *mut RustPythonError {
    let table = slice::from_raw_parts(table_buf, table_len);
    let table = str::from_utf8(table).unwrap();

    let column_names_csv = slice::from_raw_parts(column_names_csv_buf, column_names_csv_len);
    let column_names_csv = str::from_utf8(column_names_csv).unwrap();

    let schema = schema as *const FFI_ArrowSchema;
    let schema: Schema = try_!((&*schema).try_into());

    let writer = ArrowBatchSqlWriter::new(schema, table, column_names_csv, chunk_size);
    *writer_out = Box::into_raw(Box::new(ArrowBulkWriter(writer)));

    null_mut() // Ok(())
}
/// # Safety
///
/// * `writer` must be valid non-null writer, allocated by [`arrow_odbc_writer_make`].
/// * `batch` must be a valid pointer to an arrow batch
#[no_mangle]
pub unsafe extern "C" fn arrow_odbc_writer_write_batch(
    mut writer: NonNull<ArrowBulkWriter>,
    array_ptr: *mut c_void,
    schema_ptr: *mut c_void,
) -> *mut RustPythonError {
    let array_ptr = array_ptr as *mut FFI_ArrowArray;
    let schema_ptr = schema_ptr as *mut FFI_ArrowSchema;
    let array = ptr::replace(array_ptr, FFI_ArrowArray::empty());
    let schema = ptr::replace(schema_ptr, FFI_ArrowSchema::empty());

    // Dereference batch
    let array_data = try_!(ArrowArray::new(array, schema).to_data());
    let struct_array = StructArray::from(array_data);
    let record_batch = RecordBatch::from(&struct_array);

    // Dereference writer
    let writer = &mut writer.as_mut().0;

    try_!(writer.write_batch(&record_batch));
    null_mut() // Ok(())
}

/// # Safety
///
/// * `writer` must be valid non-null writer, allocated by [`arrow_odbc_writer_make`].
#[no_mangle]
pub unsafe extern "C" fn arrow_odbc_writer_flush(
    mut writer: NonNull<ArrowBulkWriter>,
) -> *mut RustPythonError {
    // Dereference writer
    let writer = &mut writer.as_mut().0;

    try_!(writer.flush());
    null_mut()
}
