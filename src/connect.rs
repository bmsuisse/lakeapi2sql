use tiberius::client::AdoNetConfig;
use tiberius::client::ConfigString;
use tiberius::error::Error;
use tiberius::AuthMethod;
use tiberius::Client;
use tiberius::Config;
use tiberius::SqlBrowser;
use tokio::net::TcpStream;
use tokio_util::compat::Compat;
use tokio_util::compat::TokioAsyncWriteCompatExt;

use crate::error::LakeApi2SqlError;

pub async fn connect_sql(
    con_str: &str,
    aad_token: Option<String>,
) -> Result<Client<Compat<TcpStream>>, LakeApi2SqlError> {
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
            let ado_cfg: AdoNetConfig = con_str.parse()?;
            let ado_cfg2: AdoNetConfig = con_str.parse()?;

            let mut config = Config::from_config_string(ado_cfg)?;
            if let Some(tv) = aad_token {
                config.authentication(AuthMethod::AADToken(tv));
            }
            config.host(&host);
            config.port(port);
            let instance = match ado_cfg2.server() {
                Ok(v) => v.instance,
                Err(_) => None,
            };
            if instance.is_some() {
                let tcp = TcpStream::connect_named(&config).await?;
                tcp.set_nodelay(true)?;
                Client::connect(config, tcp.compat_write()).await?
            } else {
                let tcp = TcpStream::connect(config.get_addr()).await?;
                tcp.set_nodelay(true)?;

                // we should not have more than one redirect, so we'll short-circuit here.
                Client::connect(config, tcp.compat_write()).await?
            }
        }
        Err(e) => Err(e)?,
    };
    Ok(client)
}
