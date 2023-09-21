use std::{fmt::Display, sync::Arc};

use arrow::{
    datatypes::Schema, error::ArrowError, ipc::reader::StreamReader, record_batch::RecordBatch,
};
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

use crate::arrow_convert::get_token_rows;

#[derive(Debug)]
pub(crate) struct ArrowErrorWrap {
    error: ArrowError,
}
impl Display for ArrowErrorWrap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("arrow error {}", self.error))
    }
}
impl std::error::Error for ArrowErrorWrap {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }

    fn description(&self) -> &str {
        "description() is deprecated; use Display"
    }

    fn cause(&self) -> Option<&dyn std::error::Error> {
        self.source()
    }
}

#[derive(Debug)]
pub(crate) struct SendErrorWrap {
    error: String,
}
impl Display for SendErrorWrap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("send error {}", self.error))
    }
}
impl std::error::Error for SendErrorWrap {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }

    fn description(&self) -> &str {
        "description() is deprecated; use Display"
    }

    fn cause(&self) -> Option<&dyn std::error::Error> {
        self.source()
    }
}

async fn get_cols_from_table(
    db_client: &mut Client<Compat<TcpStream>>,
    table_name: &str,
) -> Result<Vec<(String, ColumnType)>, Box<dyn std::error::Error + Send + Sync>> {
    let query = format!("SELECT TOP 0 * FROM {}", table_name);
    let mut colres = db_client.simple_query(query).await?;
    Ok(colres
        .columns()
        .await?
        .unwrap()
        .iter()
        .map(|x| (x.name().to_string(), x.column_type()))
        .collect::<Vec<(String, ColumnType)>>())
}

pub async fn bulk_insert<'a>(
    db_client: &'a mut Client<Compat<TcpStream>>,
    table_name: &str,
    column_names: &'a [&'a str],
    url: &str,
    user: &str,
    password: &str,
) -> Result<Arc<Schema>, Box<dyn std::error::Error + Send + Sync>> {
    //let mut row = TokenRow::new();
    //row.push(1.into_sql());
    //blk.send(row).await?;
    //blk.finalize().await?;

    let collist = get_cols_from_table(db_client, table_name).await?;
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
    let worker = tokio::task::spawn_blocking(
        move || -> Result<Arc<Schema>, Box<dyn std::error::Error + Send + Sync>> {
            let reader = StreamReader::try_new(syncstr, None);
            if let Err(err) = reader {
                return Err(Box::new(ArrowErrorWrap { error: err }));
            }
            let mut reader = reader.unwrap();
            let schema = reader.schema();
            loop {
                match reader.next() {
                    Some(x) => match x {
                        Ok(b) => {
                            tx.blocking_send(b).map_err(|e| {
                                Box::new(SendErrorWrap {
                                    error: e.to_string(),
                                })
                            })?;
                        }
                        Err(l) => println!("{:?}", l),
                    },
                    None => break,
                };
            }
            Ok(schema)
        },
    );

    while let Some(v) = rx.recv().await {
        let nrows = v.num_rows();
        info!("received {nrows}");
        let rows = get_token_rows(&v, &collist)?;
        let mut blk: tiberius::BulkLoadRequest<'_, Compat<TcpStream>> = db_client
            .bulk_insert_with_options(table_name, column_names, SqlBulkCopyOptions::TableLock, &[])
            .await?;
        for rowdt in rows {
            blk.send(rowdt).await?;
        }
        blk.finalize().await?;
        info!("Written {nrows}");
    }
    let schema = worker.await?;
    schema
}
