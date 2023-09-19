use std::{fmt::Display, sync::Arc};

use arrow::{
    datatypes::Schema, error::ArrowError, ipc::reader::StreamReader, record_batch::RecordBatch,
};
use futures::stream::TryStreamExt;
use log::info;
use tiberius::Client;
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

pub async fn bulk_insert<'a>(
    db_client: &'a mut Client<Compat<TcpStream>>,
    table_name: &str,
    url: &str,
    user: &str,
    password: &str,
) -> Result<Arc<Schema>, Box<dyn std::error::Error + Send + Sync>> {
    //let mut row = TokenRow::new();
    //row.push(1.into_sql());
    //blk.send(row).await?;
    //blk.finalize().await?;
    let colres = db_client
        .query(
            "
    select sc.name from sys.columns sc
        inner join sys.objects o on o.object_id=sc.object_id
        inner join sys.schemas sch on sch.schema_id=o.schema_id  
        where is_computed=0 and is_identity=0 and system_type_id<>189
            and o.name=parsename(@P1,1)
            and sch.name=coalesce(parsename(@P1,2), SCHEMA_NAME())
        order by sc.column_id
    ",
            &[&table_name],
        )
        .await?;
    let res = colres.into_first_result().await?;
    let collist = res
        .iter()
        .map(|row| {
            let val: &str = row.get(0).unwrap();
            return val.to_string();
        })
        .collect::<Vec<String>>();

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
        let mut blk: tiberius::BulkLoadRequest<'_, Compat<TcpStream>> =
            db_client.bulk_insert(table_name).await?;
        for row in rows {
            blk.send(row).await?;
        }
        blk.finalize().await?;
        info!("Written {nrows}");
    }
    let schema = worker.await?;
    schema
}
