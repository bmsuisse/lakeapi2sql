use std::sync::Arc;

use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::record_batch::RecordBatchReader;
use arrow::{datatypes::Schema, ipc::reader::StreamReader, record_batch::RecordBatch};
use futures::stream::TryStreamExt;
use log::info;
use tiberius::Client;
use tiberius::ColumnType;
use tiberius::SqlBulkCopyOptions;
use tokio::net::TcpStream;
use tokio_util::compat::Compat;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tokio_util::io::SyncIoBridge;

use tokio::sync::mpsc;
use tokio::task;

use crate::arrow_convert::get_token_rows;
use crate::error::LakeApi2SqlError;

async fn get_cols_from_table(
    db_client: &mut Client<Compat<TcpStream>>,
    table_name: &str,
    column_names: &[&str],
) -> Result<Vec<(String, ColumnType)>, LakeApi2SqlError> {
    let cols_sql = match column_names.len() {
        0 => "*".to_owned(),
        _ => column_names
            .iter()
            .map(|c| format!("[{}]", c))
            .collect::<Vec<String>>()
            .join(", "),
    };
    let query = format!("SELECT TOP 0 {} FROM {}", cols_sql, table_name);
    let mut colres = db_client.simple_query(query).await?;
    Ok(colres
        .columns()
        .await?
        .unwrap()
        .iter()
        .map(|x| (x.name().to_string(), x.column_type()))
        .collect::<Vec<(String, ColumnType)>>())
}

pub async fn bulk_insert_batch<'a>(
    table_name: &str,
    blk: &mut tiberius::BulkLoadRequest<'a, Compat<TcpStream>>,
    batch: &'a RecordBatch,
    collist: &'a Vec<(String, ColumnType)>,
) -> Result<(), LakeApi2SqlError> {
    let nrows = batch.num_rows();
    info!("{table_name}: received {nrows}");
    let rows = task::block_in_place(|| get_token_rows(batch, collist))?;
    info!("{table_name}: converted {nrows}");
    for rowdt in rows {
        blk.send(rowdt).await?;
    }
    info!("{table_name}: Written {nrows}");
    Ok(())
}

pub async fn bulk_insert<'a>(
    db_client: &'a mut Client<Compat<TcpStream>>,
    table_name: &str,
    column_names: &'a [&'a str],
    url: &str,
    user: &str,
    password: &str,
) -> Result<Arc<Schema>, LakeApi2SqlError> {
    //let mut row = TokenRow::new();
    //row.push(1.into_sql());
    //blk.send(row).await?;
    //blk.finalize().await?;

    let collist = get_cols_from_table(db_client, table_name, column_names).await?;
    log::debug!("{:?}", collist);
    let cclient = reqwest::Client::new();

    // a bit too complex if you ask me: https://github.com/benkay86/async-applied/tree/master/reqwest-tokio-compat

    let res = cclient
        .get(url)
        .basic_auth(user, Some(password))
        .send()
        .await?
        .error_for_status()?;

    info!("received http response");
    let res = res
        .bytes_stream()
        .map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e))
        .into_async_read()
        .compat();
    let (tx, mut rx) = mpsc::channel::<RecordBatch>(2);
    let syncstr = SyncIoBridge::new(res);
    let worker = tokio::task::spawn_blocking(move || -> Result<Arc<Schema>, LakeApi2SqlError> {
        let mut reader = StreamReader::try_new(syncstr, None)?;
        let schema = reader.schema();
        loop {
            match reader.next() {
                Some(x) => match x {
                    Ok(b) => {
                        tx.blocking_send(b)?;
                    }
                    Err(l) => println!("{:?}", l),
                },
                None => break,
            };
        }
        Ok(schema)
    });
    while let Some(v) = rx.recv().await {
        let mut blk = db_client
            .bulk_insert_with_options(table_name, column_names, SqlBulkCopyOptions::TableLock, &[])
            .await?;
        bulk_insert_batch(table_name, &mut blk, &v, &collist).await?;
        blk.finalize().await?;
    }

    worker.await?
}

pub async fn bulk_insert_reader(
    db_client: &mut Client<Compat<TcpStream>>,
    table_name: &str,
    column_names: &[&str],
    reader: &mut ArrowArrayStreamReader,
) -> Result<Arc<Schema>, LakeApi2SqlError> {
    //let mut row = TokenRow::new();
    //row.push(1.into_sql());
    //blk.send(row).await?;
    //blk.finalize().await?;

    let collist = get_cols_from_table(db_client, table_name, column_names).await?;
    log::debug!("{:?}", collist);
    let schema = reader.schema();
    loop {
        match reader.next() {
            Some(x) => match x {
                Ok(b) => {
                    let mut blk = db_client
                        .bulk_insert_with_options(
                            table_name,
                            column_names,
                            SqlBulkCopyOptions::TableLock,
                            &[],
                        )
                        .await?;
                    bulk_insert_batch(table_name, &mut blk, &b, &collist).await?;
                    blk.finalize().await?;
                }
                Err(l) => println!("{:?}", l),
            },
            None => break,
        };
    }
    Ok(schema)
}
