use crate::utils::cmd::cmd_to_resp_first_line;
use redis::{Client, aio::Connection};

use std::error::Error;
use std::ops::Add;

use async_std::net::TcpStream;


pub async fn open_tcp_conn(url: &str, pass: &str) ->  Result<TcpStream, Box<dyn Error>>{
    let mut source = TcpStream::connect(url).await?;
    if !pass.is_empty() {
        let auth_resp = cmd_to_resp_first_line(&mut source, vec!["auth", pass]).await?;
        if auth_resp.contains("ERR") {
            return Err(Box::from(auth_resp));
        } else {
            println!("auth success")
        }
    };
    Ok(source)
}

pub async fn open_redis_sync_conn(
    url: &str,
    pass: &str,
    mut index: &str,
) -> Result<Connection, Box<dyn Error>> {
    if index == "" {
        index = "0"
    }
    let mut path = format!("redis://{}/{}", url, index);
    if pass != "" {
        path = path.add(":");
        path = path.add(pass);
    }
    Ok(Client::open(path.as_str())?.get_async_connection().await?)
}