use crate::rdb::cmd::cmd_to_string;
use crate::rdb::loader::Loader;
use redis::{Client, ConnectionLike};
use std::convert::TryFrom;
use std::error::Error;
use std::fs::read;
use std::io::{BufReader, Read, Write};
use std::net::TcpStream;
use std::ops::DerefMut;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::thread::{sleep, spawn};
use std::time::Duration;

pub fn incr(
    loader: &mut TcpStream,
    target_url: &str,
    target_pass: &str,
    offset: Arc<AtomicUsize>,
) -> Result<(), Box<dyn Error>> {
    let (sender, receiver) = channel::<cmd_pack>();
    let mut path = format!("redis://{}/0", target_url);
    if target_pass != "" {
        path.push_str(":");
        path.push_str(target_pass)
    }
    spawn(move || {
        let time_out = Duration::from_millis(10);
        let mut conn = Client::open(path.as_str())
            .unwrap()
            .get_connection()
            .unwrap();
        let mut req_packed: Vec<u8> = vec![];
        let mut batch_count = 0;
        loop {
            match receiver.recv_timeout(time_out) {
                Ok(mut cmd) => {
                    req_packed.append(&mut cmd.full_pack);
                    batch_count = batch_count + 1;
                    if batch_count >= 300 {
                        match conn.req_packed_commands(req_packed.as_slice(), 0, batch_count) {
                            Ok(d) => {}
                            Err(e) => println!("增量阶段出现错误 {}", e),
                        };
                        batch_count = 0;
                        req_packed.clear();
                    }
                }
                Err(e) => {
                    if batch_count > 0 {
                        match conn.req_packed_commands(req_packed.as_slice(), 0, batch_count) {
                            Ok(d) => {}
                            Err(e) => println!("增量阶段出现错误 {}", e),
                        };
                        batch_count = 0;
                        req_packed.clear();
                    }
                }
            }
        }
    });
    let (mut read_, mut write) = os_pipe::pipe().unwrap();
    let mut read = BufReader::with_capacity(10 * 1024 * 1024, read_);
    spawn(move || {
        loop {
            let mut p = [0; 1];
            let r_len = read.read(&mut p).unwrap();
            if r_len != 0 {
                // 这里就是一个完整的包体
                let mut pack = cmd_pack {
                    cmd: vec![],
                    full_pack: vec![],
                };
                if p[0] == '*' as u8 {
                    pack.full_pack.push(p[0]);
                    let mut args_num_vec = Vec::new();
                    loop {
                        let mut p_ = [0; 1];
                        let r_len = read.read(&mut p_).unwrap();
                        if r_len != 0 {
                            pack.full_pack.push(p_[0]);
                            if p_[0] == '\r' as u8 {
                            } else if p_[0] == '\n' as u8 {
                                break;
                            } else {
                                args_num_vec.push(p_[0])
                            }
                        }
                    }
                    let args_num = String::from_utf8(args_num_vec)
                        .unwrap()
                        .parse::<i32>()
                        .unwrap();
                    for i in 0..args_num {
                        // 先读$
                        let mut args_num_vec = Vec::new();
                        loop {
                            let mut p_ = [0; 1];
                            let r_len = read.read(&mut p_).unwrap();
                            if r_len != 0 {
                                pack.full_pack.push(p_[0]);
                                if p_[0] == '\r' as u8 {
                                } else if p_[0] == '$' as u8 {
                                    args_num_vec.clear();
                                } else if p_[0] == '\n' as u8 {
                                    break;
                                } else {
                                    args_num_vec.push(p_[0])
                                }
                            }
                        }
                        // 再读数据
                        let args_num = String::from_utf8(args_num_vec)
                            .unwrap()
                            .parse::<i32>()
                            .unwrap();
                        let mut p_: Vec<u8> = vec![0; (args_num + 2) as usize];
                        read.read_exact(&mut p_).unwrap();
                        pack.full_pack.append(p_.clone().as_mut());
                        if i == 0 {
                            p_.pop();
                            p_.pop();
                            pack.cmd = p_
                        }
                    }
                    sender.send(pack);
                } else {
                    println!("unchar is {}", p[0] as char);
                }
            }
        }
    });
    let mut p = [0; 512 * 1024];
    loop {
        let r_len = loader.read(&mut p).unwrap();
        if r_len != 0 {
            offset_incr(&offset, r_len);
            write.write_all((p[0..r_len]).as_ref());
        }
        sleep(Duration::from_millis(5));
    }
}
/*
*4
$4
hset
$4
a123
$1
b
$1
c
*/
#[derive(Debug)]
pub struct cmd_pack {
    cmd: Vec<u8>,
    full_pack: Vec<u8>, // 储存完整的命令包
}
fn offset_incr(offset: &Arc<AtomicUsize>, incr_num: usize) {
    let send_offset = offset.load(Ordering::Relaxed);
    offset.store(send_offset + incr_num, Ordering::Relaxed);
}
