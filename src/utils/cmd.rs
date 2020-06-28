use std::convert::AsRef;
use std::error::Error;
use async_std::net::TcpStream;
use std::ops::Add;

use futures_util::{AsyncWriteExt,AsyncReadExt};

pub fn cmd_to_string(cmd: Vec<&str>) -> String {
    let mut rsl = String::new();
    rsl = rsl.add("*");
    rsl = rsl.add(format!("{}\r\n", cmd.len()).as_ref());
    for cmd_ in cmd {
        rsl = rsl.add(format!("${}", cmd_.len()).as_ref());
        rsl = rsl.add("\r\n");
        rsl = rsl.add(cmd_.as_ref());
        rsl = rsl.add("\r\n");
    }
    return rsl;
}

pub async fn cmd_to_resp_first_line(
    conn: &mut TcpStream,
    cmd: Vec<&str>,
) ->  Result<String, Box<dyn Error>> {
    conn.write(cmd_to_string(cmd).as_bytes()).await?;
    let mut resp = String::new();
    let mut resp_char = [0;1];
    loop {
        match conn.read_exact(&mut resp_char).await{
            Ok(())=>{
                if resp_char[0] == '\r' as u8 {
                    break;
                }
                if resp_char[0] == '\n' as u8{
                    continue;
                }
                resp.push(char::from(resp_char[0]));
            },
            Err(_e)=>{
                break;
            }
        };
    }
    Ok(resp)
}

pub async fn read_line(conn: &mut TcpStream) -> Result<String, Box<dyn Error>> {
    let mut resp = String::new();
    let mut resp_char = [0;1];
    loop {
        match conn.read_exact(&mut resp_char).await{
            Ok(())=>{
                if resp_char[0] == '\r' as u8 {
                    break;
                }
                if resp_char[0] == '\n' as u8{
                    continue;
                }
                resp.push(char::from(resp_char[0]));
            },
            Err(_e)=>{
                break;
            }
        };
    }
    Ok(resp)
}
