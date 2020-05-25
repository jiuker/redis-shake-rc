use crate::rdb::cmd::{cmd_to_resp_first_line, cmd_to_string, read_line};

use byteorder::ReadBytesExt;


use std::convert::TryFrom;
use std::error;

use std::io::{Write};
use std::net::{TcpStream};


use std::sync::atomic::{Ordering,AtomicU64};
use std::sync::{Arc};
use std::thread::sleep;
use std::time::Duration;

pub fn pre_to_rdb(source: &mut TcpStream) -> Result<(i64,i64), Box<dyn error::Error>> {
    // 设置监听端口
    let set_port_resp = cmd_to_resp_first_line(source, vec!["replconf", "listening-port", "8083"])?;
    if !set_port_resp.eq(&String::from("+OK")) {
        return Err(Box::try_from("设置监听端口失败").unwrap());
    }
    println!("set listening-port is {}", set_port_resp);

    source.write(cmd_to_string(vec!["psync", "?", "-1"]).as_bytes())?;
    // psync ? -1
    let header = cmd_to_resp_first_line(source, vec!["psync", "?", "-1"])?;
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
    println!("uuid   is {} \r\noffset is {}", uuid, offset);
    // rdb size
    let rdb_size = read_line(source)?.replace("$", "").parse::<i64>().unwrap();
    println!("rdb_size  {:?}", rdb_size);
    // ignore \n
    let ch = source.read_u8().unwrap() as char;
    if ch == '\n' {
    } else {
    }
    Ok((offset,rdb_size))
}
pub fn report_offset(
    source: &mut TcpStream,
    offset: &Arc<AtomicU64>,
) -> Result<(), Box<dyn error::Error>> {
    // 上报发送的offset
    loop {
        let send_offset = offset.load(Ordering::SeqCst);
        source.write(
            cmd_to_string(vec!["replconf", "ack", format!("{}", send_offset).as_str()]).as_bytes(),
        ).unwrap();
        sleep(Duration::from_secs(1));
    }
    Ok(())
}
