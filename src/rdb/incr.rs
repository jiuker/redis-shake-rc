use redis::{Connection, ConnectionLike, ErrorKind};

use std::error::Error;

use std::io::{BufReader, Read, Write};

use crate::utils::conn::open_redis_conn;
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};
use tokio::sync::mpsc::channel;
use std::sync::Arc;
use async_std::task::{spawn,sleep};
use std::time::Duration;
use crate::rdb::loader::Loader;
use tokio::io::AsyncReadExt;
#[macro_export(atomic_u64_fetch_add)]
macro_rules! atomic_u64_fetch_add {
    ($data:ident,$inr:expr) => {
        $data.fetch_add($inr, Ordering::Relaxed)
    };
}
#[macro_export(atomic_u64_load)]
macro_rules! atomic_u64_load {
    ($data:ident) => {
        $data.load(Ordering::Relaxed);
    };
}
macro_rules! send_cmd {
    // 连接，发送的包,发送统计，单次发送的count统计，超过多少就发送的值
    ($conn:ident,$req_packed:ident,$send_count:ident,$batch_count:ident,$over_max_to_send:expr) => {
        if $batch_count > $over_max_to_send {
            match $conn.req_packed_commands($req_packed.as_slice(), 0, $batch_count) {
                Ok(_d) => {}
                Err(e) => match e.kind() {
                    ErrorKind::IoError => {
                        break;
                    }
                    _ => {
                        println!("增量阶段读取响应错误:{}", e.to_string());
                    }
                },
            };
            atomic_u64_fetch_add!($send_count, $batch_count as u64);
            $batch_count = 0;
            $req_packed.clear();
        }
    };
}

pub async fn incr(
    loader: &mut Loader,
    target_url: &'static str,
    target_pass: &'static str,
) -> Result<(), Box<dyn Error>> {
    let (mut sender, mut receiver) = channel::<cmd_pack>(20000);
    let send_count = Arc::new(AtomicU64::new(0));
    let send_count_c = send_count.clone();
    let parse_count = Arc::new(AtomicU64::new(0));
    let parse_count_c = parse_count.clone();
    let count_all_bytes = Arc::new(AtomicU64::new(0));
    let count_all_bytes_c = count_all_bytes.clone();
    spawn(async move {
        loop {
            let cabc = atomic_u64_load!(count_all_bytes);
            let pcc = atomic_u64_load!(parse_count_c);
            let scc = atomic_u64_load!(send_count_c);
            println!(
                "[INC] parse_cmd_number:{},send_cmd_number:{},left:{} all bytes:{}",
                pcc,
                scc,
                pcc - scc,
                cabc
            );
            // 清零
            sleep(Duration::from_secs(1)).await;
        }
    });
    // 发送
    spawn(async move  {
        let mut req_packed: Vec<u8> = vec![];
        let mut batch_count = 0;
        let mut conn: Connection;
        let mut last_select_full_pack = vec![];
        loop {
            loop {
                println!("连接目的端redis中...");
                conn = match open_redis_conn(target_url, target_pass, "0") {
                    Ok(mut d) => {
                        // 选择redis的db
                        if last_select_full_pack.len() != 0 {
                            match d.send_packed_command(last_select_full_pack.as_ref()) {
                                Ok(_d1) => {
                                    print!("重新目的端redis");
                                }
                                Err(_e) => continue,
                            }
                        }
                        d
                    }
                    Err(_e) => {
                        sleep(Duration::from_secs(1));
                        continue;
                    }
                };
                println!("连接成功!");
                break;
            }
            loop {
                match receiver.recv().await {
                    Some(mut cmd) => {
                        // 先查看是不是select
                        if String::from_utf8_lossy(cmd.cmd.as_ref())
                            .to_lowercase()
                            .eq("select")
                        {
                            match String::from_utf8(cmd.full_pack.clone()) {
                                Ok(_d) => last_select_full_pack = cmd.full_pack.clone(),
                                Err(_e) => println!("select error?"),
                            }
                        };
                        req_packed.append(&mut cmd.full_pack);
                        batch_count = batch_count + 1;
                        send_cmd!(conn, req_packed, send_count, batch_count, 300);
                    }
                    None => {
                        send_cmd!(conn, req_packed, send_count, batch_count, 0);
                    }
                }
            }
        }
    });
    // 解包
    loop {
        let mut p = [0; 1];
        let r_len = loader.rdbReader.raw.borrow_mut().read(&mut p).await.unwrap();
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
                    let r_len = loader.rdbReader.raw.borrow_mut().read(&mut p_).await.unwrap();
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
                        let r_len = loader.rdbReader.raw.borrow_mut().read(&mut p_).await.unwrap();
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
                    loader.rdbReader.raw.borrow_mut().read_exact(&mut p_).await.unwrap();
                    pack.full_pack.append(p_.clone().as_mut());
                    if i == 0 {
                        p_.pop();
                        p_.pop();
                        pack.cmd = p_
                    }
                }

                // 解析加1
                atomic_u64_fetch_add!(parse_count, 1);
                // 统计全部
                atomic_u64_fetch_add!(count_all_bytes_c, pack.full_pack.len() as u64);
                // 发送
                sender.send(pack).await;
            } else {
                print!("{}", p[0] as char);
            }
        }
    }
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
