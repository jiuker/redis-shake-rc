

use redis::{Client, ConnectionLike, Connection, ErrorKind};

use std::error::Error;

use std::io::{BufReader, Read, Write};


use std::sync::atomic::{AtomicUsize, Ordering, AtomicPtr,AtomicU64};
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::thread::{sleep, spawn};
use std::time::Duration;
use crate::utils::conn::open_redis_conn;

pub fn incr(
    source_reader: &mut dyn Read,
    target_url: &'static str,
    target_pass: &'static str,
) -> Result<(), Box<dyn Error>> {
    let (sender, receiver) = channel::<cmd_pack>();
    let send_count = Arc::new(AtomicU64::new(0));
    let send_count_c = send_count.clone();
    let parse_count = Arc::new(AtomicU64::new(0));;
    let parse_count_c = parse_count.clone();
    let count_all_bytes = Arc::new(AtomicU64::new(0));
    let count_all_bytes_c = count_all_bytes.clone();
    let count_ten_bytes = Arc::new(AtomicU64::new(0));
    let count_ten_bytes_c = count_ten_bytes.clone();
    spawn(move||{
        let mut print_count = 0;
        loop{
            let cabc = count_all_bytes.load(Ordering::SeqCst);
            let pcc = parse_count_c.load(Ordering::SeqCst);
            let scc = send_count_c.load(Ordering::SeqCst);
            println!("[INC] parse_cmd_number:{},send_cmd_number:{},left:{} all bytes:{}",pcc,scc,pcc - scc,cabc);
            print_count=(print_count + 1)% 10 ;
            // 清零
            parse_count_c.store(0,Ordering::SeqCst);
            send_count_c.store(0,Ordering::SeqCst);
            if print_count ==0{
                let ctb = count_ten_bytes.load(Ordering::Acquire);
                println!("[INC] 10s bytes: {} byte",ctb);
                count_ten_bytes.store(0,Ordering::SeqCst);
            }
            sleep(Duration::from_secs(1));
        }
    });
    // 发送
    spawn(move || {
        let time_out = Duration::from_millis(10);
        let mut req_packed: Vec<u8> = vec![];
        let mut batch_count = 0;
        let mut conn:Connection;
        let mut last_select_full_pack = vec![];
        loop {
            loop{
                println!("连接目的端redis中...");
                conn = match open_redis_conn(target_url,target_pass,"0"){
                    Ok(mut d)=>{
                        // 选择redis的db
                        if last_select_full_pack.len()!=0{
                            match d.send_packed_command(last_select_full_pack.as_ref()){
                                Ok(d1)=>{
                                    println!("重新连接成功!");
                                },
                                Err(e)=>{
                                    continue
                                }
                            }
                        }
                        d
                    },
                    Err(e)=>{
                        sleep(Duration::from_secs(1));
                        continue
                    }
                };
                break;
            }
            loop{
                match receiver.recv_timeout(time_out) {
                    Ok(mut cmd) => {
                        // 先查看是不是select
                        if String::from_utf8_lossy(cmd.cmd.as_ref()).to_lowercase().eq("select"){
                            match String::from_utf8(cmd.full_pack.clone()){
                                Ok(d)=>{
                                    last_select_full_pack = cmd.full_pack.clone()
                                },
                                Err(e)=>{
                                    println!("select error?")
                                }
                            }
                        };
                        req_packed.append(&mut cmd.full_pack);
                        batch_count = batch_count + 1;
                        if batch_count >= 300 {
                            match conn.req_packed_commands(req_packed.as_slice(), 0, batch_count) {
                                Ok(_d) => {}
                                Err(e) => {
                                    match e.kind() {
                                        ErrorKind::IoError=>{
                                            break;
                                        },
                                        _ =>{
                                          println!("增量阶段读取响应错误:{}",e.to_string());
                                        }
                                    }
                                },
                            };
                            batch_count = 0;
                            req_packed.clear();
                        }
                        send_count.fetch_add(1,Ordering::SeqCst);
                    }
                    Err(_e) => {
                        if batch_count > 0 {
                            match conn.req_packed_commands(req_packed.as_slice(), 0, batch_count) {
                                Ok(_d) => {}
                                Err(e) => {
                                    match e.kind() {
                                        ErrorKind::IoError=>{
                                            break;
                                        },
                                        _ =>{
                                            println!("增量阶段读取响应错误:{}",e.to_string());
                                        }
                                    }
                                },
                            };
                            batch_count = 0;
                            req_packed.clear();
                        }
                    }
                }
            }
        }
    });
    // 解包
    loop {
        let mut p = [0; 1];
        let r_len = source_reader.read(&mut p).unwrap();
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
                    let r_len = source_reader.read(&mut p_).unwrap();
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
                        let r_len = source_reader.read(&mut p_).unwrap();
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
                    source_reader.read_exact(&mut p_).unwrap();
                    pack.full_pack.append(p_.clone().as_mut());
                    if i == 0 {
                        p_.pop();
                        p_.pop();
                        pack.cmd = p_
                    }
                }

                // 解析加1
                parse_count.fetch_add(1,Ordering::SeqCst);
                // 统计全部
                count_all_bytes_c.fetch_add(pack.full_pack.len() as u64,Ordering::SeqCst);
                // 统计10s
                count_ten_bytes_c.fetch_add(pack.full_pack.len() as u64,Ordering::SeqCst);
                // 发送
                sender.send(pack);
            } else {
                print!("{}", p[0] as char);
            }
        }
    };
    Ok(())
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
