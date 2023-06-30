use std::{thread, time::Duration};

use arrow2::{
    array::Array,
    chunk::Chunk,
    io::ipc::read::{self, read_stream_metadata, StreamReader}, datatypes::Schema,
};
use futures::stream::TryStreamExt;
use tiberius::Client;
use tokio::net::TcpStream;
use tokio_util::compat::Compat;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tokio_util::io::SyncIoBridge;
use log::info;

use tokio::sync::mpsc;

use crate::arrow_convert::get_token_rows;

pub async fn bulk_insert<'a>(
    db_client: &'a mut Client<Compat<TcpStream>>,
    table_name: &str,
    url: &str,
    user: &str,
    password: &str,
) -> Result<Schema, Box<dyn std::error::Error + Send + Sync>> {
    //let mut row = TokenRow::new();
    //row.push(1.into_sql());
    //blk.send(row).await?;
    //blk.finalize().await?;
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
    let (tx, mut rx) = mpsc::channel::<Chunk<Box<dyn Array>>>(2);
    let mut syncstr = SyncIoBridge::new(res);
    let worker = tokio::task::spawn_blocking(move || {
        let metadata = read_stream_metadata(&mut syncstr).unwrap();
        let schema = metadata.schema.clone();
        let mut reader = StreamReader::new(syncstr, metadata, None);
        loop {
            match reader.next() {
                Some(x) => match x {
                    Ok(read::StreamState::Some(b)) => {
                        tx.blocking_send(b).unwrap();
                    }
                    Ok(read::StreamState::Waiting) => thread::sleep(Duration::from_millis(2000)),
                    Err(l) => println!("{:?}", l),
                },
                None => break,
            };
        }
        schema
    });

    while let Some(v) = rx.recv().await {
        let nrows = v.len();
        info!("received {nrows}");
        let rows = get_token_rows(&v);        
        let mut blk: tiberius::BulkLoadRequest<'_, Compat<TcpStream>> =
            db_client.bulk_insert(table_name).await?;
        for row in rows {
            blk.send(row).await?;
        }
        blk.finalize().await?;
        info!("Written {nrows}");
    }
    let schema = worker.await?;
    Ok(schema)
}
