use crate::rdb::cmd::cmd_to_resp_first_line;
use net2::TcpStreamExt;
use std::convert::TryFrom;
use std::error::Error;
use std::net::TcpStream;
use std::time::Duration;

pub fn open_tcp_conn(url: &str, pass: &str) -> Result<TcpStream, Box<dyn Error>> {
    let mut source = std::net::TcpStream::connect(url)?;
    if !pass.is_empty() {
        let auth_resp = cmd_to_resp_first_line(&mut source, vec!["auth", pass])?;
        if auth_resp.contains("ERR") {
            return Err(Box::try_from(auth_resp)?);
        } else {
            println!("auth success")
        }
    };
    source.set_write_timeout(Some(Duration::from_secs(30)))?;
    source.set_read_timeout(Some(Duration::from_secs(30)))?;
    source.set_recv_buffer_size(10 * 1024 * 1024)?;
    source.set_send_buffer_size(10 * 1024 * 1024)?;
    Ok(source)
}
