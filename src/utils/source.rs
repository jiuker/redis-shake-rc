
use std::convert::TryFrom;
use std::error;

use async_std::net::TcpStream;

use crate::utils::cmd::{cmd_to_resp_first_line, cmd_to_string, read_line};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use async_std::task::sleep;
use std::time::Duration;
use futures_util::{AsyncWriteExt, AsyncReadExt};

pub async fn pre_to_rdb(source: &mut TcpStream) -> Result<(i64, i64, String), Box<dyn error::Error>> {
    // 设置监听端口
    let set_port_resp = cmd_to_resp_first_line(source, vec!["replconf", "listening-port", "8083"]).await?;
    if !set_port_resp.eq(&String::from("+OK")) {
        return Err(Box::from("设置监听端口失败"));
    }
    println!("set listening-port is {}", set_port_resp);

    // psync ? -1
    let header = cmd_to_resp_first_line(source, vec!["psync", "?", "-1"]).await?;
    let mut resp = String::new();
    let mut uuid = String::new();
    let mut offset = 0;
    let mut index = 0;
    for s_str in header.split(" ") {
        if index == 0 {
            resp = String::from(s_str);
        }
        if index == 1 {
            uuid = String::from(s_str);
        }
        if index == 2 {
            offset = String::from(s_str).parse::<i64>().unwrap();
        }
        index = index + 1;
    }
    if index != 3 {
        return Err(Box::from("未知的响应头!"));
    }
    println!("uuid   is {} \r\noffset is {}", uuid, offset);
    // rdb size
    let rdb_size = read_line(source).await?.replace("$", "").parse::<i64>().unwrap();
    println!("rdb_size  {:?}", rdb_size);
    // ignore \n
    let mut resp_char = [0;1];
    match source.read_exact(&mut resp_char).await{
        Ok(())=>{
            if resp_char[0] == '\n' as u8{

            }
        },
        Err(e)=>{

        }
    };
    Ok((offset, rdb_size, uuid))
}

pub async fn pre_to_inc(
    source: &mut TcpStream,
    uuid: &str,
    offset: &str,
) -> Result<(), Box<dyn error::Error>> {
    // 设置监听端口
    let set_port_resp = cmd_to_resp_first_line(source, vec!["replconf", "listening-port", "8083"]).await?;
    if !set_port_resp.eq(&String::from("+OK")) {
        return Err(Box::from("设置监听端口失败"));
    }
    println!("set listening-port is {}", set_port_resp);
    // psync ? -1
    let header = cmd_to_resp_first_line(source, vec!["psync", uuid, offset]).await?;
    if header.to_uppercase() == "+CONTINUE" {
        println!("源端重连成功!");
    } else {
        return Err(Box::from("重连失败!"));
    }
    // ignore \n
    let mut resp_char = [0;1];
    match source.read_exact(&mut resp_char).await{
        Ok(())=>{
            if resp_char[0] == '\n' as u8{

            }
        },
        Err(e)=>{

        }
    };
    Ok(())
}

pub async fn report_offset(
    source: &mut TcpStream,
    offset: &Arc<AtomicU64>,
) -> Result<(), Box<dyn error::Error>> {
    // 上报发送的offset
    loop {
        let send_offset = offset.load(Ordering::SeqCst);
        source.write(
            cmd_to_string(vec!["replconf", "ack", format!("{}", send_offset).as_str()]).as_bytes(),
        ).await?;
        sleep(Duration::from_secs(1)).await;
    }
    Ok(())
}
#[macro_export(source_report_offset)]
macro_rules! source_report_offset {
    ($conn:ident,$offset:ident) => {
        async_std::task::spawn(async move {
            // 上报头部
            loop {
                if let Err(e) = report_offset(&mut $conn, &$offset).await {
                    println!("write err is {}", e.to_string());
                    break;
                };
            }
        });
    };
}
