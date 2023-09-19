use tiberius::error::Error;
use tiberius::AuthMethod;
use tiberius::Client;
use tiberius::Config;
use tiberius::SqlBrowser;
use tokio::net::TcpStream;
use tokio_util::compat::Compat;
use tokio_util::compat::TokioAsyncWriteCompatExt;

pub async fn connect_sql(
    con_str: &str,
    aad_token: Option<String>,
) -> Result<Client<Compat<TcpStream>>, Box<dyn std::error::Error + Send + Sync>> {
    let mut config = Config::from_ado_string(con_str)?;
    if let Some(tv) = aad_token.clone() {
        config.authentication(AuthMethod::AADToken(tv));
    }

    config.encryption(tiberius::EncryptionLevel::Required);

    let tcp = TcpStream::connect_named(&config).await?;
    tcp.set_nodelay(true)?;

    let client = match Client::connect(config, tcp.compat_write()).await {
        // Connection successful.
        Ok(client) => client,
        // The server wants us to redirect to a different address
        Err(Error::Routing { host, port }) => {
            let mut config = Config::from_ado_string(con_str)?;
            if let Some(tv) = aad_token {
                config.authentication(AuthMethod::AADToken(tv));
            }
            config.host(&host);
            config.port(port);

            let tcp = TcpStream::connect(config.get_addr()).await?;
            tcp.set_nodelay(true)?;

            // we should not have more than one redirect, so we'll short-circuit here.
            Client::connect(config, tcp.compat_write()).await?
        }
        Err(e) => Err(e)?,
    };
    Ok(client)
}
