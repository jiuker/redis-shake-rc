pub mod Runner {
    use crate::rdb::full::full;
    use crate::rdb::incr::incr;
    use crate::rdb::loader::Loader;
    use crate::utils::conn::{open_tcp_conn, open_redis_sync_conn};
    use crate::utils::source::{pre_to_inc, pre_to_rdb, report_offset};
    use crate::{atomic_u64_fetch_add, atomic_u64_load, source_report_offset};
    use redis::{Cmd, Value, RedisResult};
    use std::cell::RefCell;
    use std::io::{Write};
    
    
    use std::process::exit;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use async_std::task::sleep;
    use async_std::task::spawn;
    use std::time::Duration;
    
    
    
    use futures_util::AsyncReadExt;
    
    use tokio::io::{AsyncWriteExt, BufReader};
    use tokio::sync::mpsc::channel;
    use tokio::sync::mpsc::error::TryRecvError;

    pub async fn mod_full(
        source_url: &'static str,
        source_pass: &'static str,
        target_url: &'static str,
        target_pass: &'static str,
    ) {
        let mut source = open_tcp_conn(source_url, source_pass).await.unwrap();
        let (offset, rdb_size, uuid) = pre_to_rdb(&mut source).await.unwrap();

        // 带缓存的管道
        let (mut pipe_writer, pipe_reader) = async_pipe::pipe();
        let pipe_reader_buf = BufReader::with_capacity(10*1024*1024,pipe_reader);
        let mut loader = Loader::new(Rc::new(RefCell::new(pipe_reader_buf)));

        let rdb_read_count = Arc::new(AtomicU64::new(0));
        let rdb_read_count_c = rdb_read_count.clone();
        // rdb_status 0 reading, 1 read done,2 send done
        let rdb_status = Arc::new(AtomicU64::new(0));
        let rdb_status_c = rdb_status.clone();
        let rdb_status_c1 = rdb_status_c.clone();
        // offset
        let offset_count = Arc::new(AtomicU64::new(offset as u64));
        let offset_count_c = offset_count.clone();
        // 读取源端数据
        spawn(async move {
            sleep(Duration::from_secs(1)).await;
            let mut source_c = source.clone();
            source_report_offset!(source_c, offset_count);
            let mut p = [0; 256*1024];
            // 全量的数据
            loop {
                let r_len = match source.read(&mut p).await {
                    Ok(d) => d,
                    Err(e) => {
                        println!("source tcp error {}", e);
                        0
                    }
                };
                if r_len != 0 {
                    atomic_u64_fetch_add!(rdb_read_count, r_len as u64);
                    let rrc = atomic_u64_load!(rdb_read_count);
                    pipe_writer.write_all(&p[0..r_len]).await.unwrap();
                    if rrc >= rdb_size as u64 {
                        // 现在是增量阶段，不需要写入了
                        break;
                    }
                }
            }
            // 如果读取多了需要上报offset
            let rrc = atomic_u64_load!(rdb_read_count);
            atomic_u64_fetch_add!(offset_count_c, rrc - rdb_size as u64);
            println!("停止读取RDB!");
            atomic_u64_fetch_add!(rdb_status,1);
            loop {
                let ird = atomic_u64_load!(rdb_status);
                if ird!=2 {
                    sleep(Duration::from_millis(100)).await
                } else {
                    break;
                }
            }
            println!("开始读取增量!");
            loop {
                let r_len = match source.read(&mut p).await {
                    Ok(d) => d,
                    Err(e) => {
                        println!("source tcp error {}", e);
                        0
                    }
                };
                if r_len != 0 {
                    atomic_u64_fetch_add!(offset_count_c, r_len as u64);
                    pipe_writer.write_all(&p[0..r_len]).await.unwrap();
                } else {
                    // todo
                    // 没有读取到,只有错误的时候没有读取到?
                    let re_connect_conn = match open_tcp_conn(source_url, source_pass).await {
                        Ok(d) => d,
                        Err(_e) => {
                            continue
                        },
                    };
                    source = re_connect_conn;
                    match pre_to_inc(
                        &mut source,
                        uuid.as_ref(),
                        format!("{}", offset_count_c.load(Ordering::SeqCst) + 1).as_ref(),
                    ).await {
                        Ok(()) => {
                            let offset_count_c_1 = offset_count_c.clone();
                            let mut source_c = source.clone();
                            source_report_offset!(source_c, offset_count_c_1);
                        }
                        Err(_e) => {
                            // 增量已经无法满足了
                            exit(1);
                        }
                    };
                }
            }
        });
        // 全量阶段输出读取进度
        spawn(async move {
            loop{
                let rrcc = atomic_u64_load!(rdb_read_count_c);
                println!("[RDB] total bytes:{} byte, read: {} ", rdb_size, rrcc);
                if rrcc >= rdb_size as u64 {
                    break;
                }
                sleep(Duration::from_secs(1)).await
            }
        });
        //读取rdb文件的header
        println!("读取RDB文件头部!");
        println!("rdb头部为 {:?}", loader.Header().await);
        // 全量rdb的命令
        let (mut full_cmd_sender, mut full_cmd_receiver) = channel::<Cmd>(20000);
        spawn(async move {
            let mut pipe = redis::pipe();
            let mut full_cmd_count = 0;
            let mut target_conn = open_redis_sync_conn(target_url, target_pass, "").await.unwrap();
            loop {
                match full_cmd_receiver.try_recv() {
                    Ok(cmd) => {
                        full_cmd_count = full_cmd_count + 1;
                        pipe.add_command(cmd);
                        if full_cmd_count >= 300 {
                            let _:RedisResult<Value> = pipe.query_async(&mut target_conn).await;
                            pipe.clear();
                            full_cmd_count = 0;
                        }
                    }
                    Err(e) => {
                        match e {
                            TryRecvError::Empty=>{
                                if atomic_u64_load!(rdb_status_c)==1{
                                    if full_cmd_count==0{
                                        // 认为rdb完成了
                                        atomic_u64_fetch_add!(rdb_status_c,1);
                                        break;
                                    }
                                }
                                if full_cmd_count > 0 {
                                    let _:RedisResult<Value> = pipe.query_async(&mut target_conn).await;
                                    pipe.clear();
                                    full_cmd_count = 0;
                                };
                                sleep(Duration::from_millis(100)).await;
                            },
                            TryRecvError::Closed=>{
                                unimplemented!("RDB channel close!")
                            }
                        }
                    }
                };
            }
        });
        full(&mut loader, &mut full_cmd_sender).await.unwrap();
        // 等待RDB完成命令发送
        loop {
            let ird = atomic_u64_load!(rdb_status_c1);
            if ird!=2 {
                sleep(Duration::from_millis(100)).await
            } else {
                break;
            }
        }
        incr(&mut loader, target_url, target_pass).await.unwrap();
    }
}
