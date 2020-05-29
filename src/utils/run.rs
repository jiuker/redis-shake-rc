pub mod Runner {
    use crate::rdb::loader::Loader;
    use crate::utils::conn::{open_tcp_conn, open_redis_conn};
    use std::cell::RefCell;
    use std::rc::Rc;
    use crate::utils::source::{pre_to_rdb, pre_to_inc, report_offset};
    use std::io::{BufReader, Read, Write};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering,AtomicU64};
    use std::thread::{spawn, sleep};
    use std::time::Duration;
    use crate::rdb::incr::incr;
    use crate::rdb::full::full;
    use std::sync::mpsc::{channel, RecvTimeoutError};
    use redis::{Cmd, Value};
    use std::error::Error;
    use std::convert::TryInto;

    pub fn mod_full(source_url:&'static str, source_pass:&'static str, target_url:&'static str, target_pass:&'static str){
        let mut loader = Loader::new(Rc::new(RefCell::new("".as_bytes())));

        let mut source = open_tcp_conn(source_url, source_pass).unwrap();
        let (offset,rdb_size,uuid) = pre_to_rdb(&mut source).unwrap();

        // 带缓存的管道
        let (pipe_reader,mut pipe_writer) = os_pipe::pipe().unwrap();

        let mut pipe_reader_buf = BufReader::with_capacity(10*1024*1024, pipe_reader);

        let rdb_read_count = Arc::new(AtomicU64::new(0));
        let rdb_read_count_c = rdb_read_count.clone();

        let is_rdb_done = Arc::new(AtomicBool::new(false));
        let is_rdb_done_c = is_rdb_done.clone();
        let is_rdb_done_c1 = is_rdb_done_c.clone();
        // offset
        let offset_count = Arc::new(AtomicU64::new(offset as u64));
        let offset_count_c = offset_count.clone();
        // 读取源端数据
        spawn(move||{
            let mut source_c = source.try_clone().unwrap();
            spawn(move || {
                // 上报头部
                loop{
                    if let Err(e) = report_offset(&mut source_c, &offset_count) {
                        println!("write err is {}",e.to_string());
                        break;
                    };
                };
            });
            let mut p = [0; 512 * 1024];
            loop {
                let r_len =match source.read(&mut p){
                    Ok(d)=>d,
                    Err(e) => {
                        println!("source tcp error {}",e);
                        0
                    },
                };
                if r_len != 0 {
                    rdb_read_count.fetch_add(r_len as u64,Ordering::SeqCst);
                    let rrc = rdb_read_count.load(Ordering::SeqCst);
                    pipe_writer.write_all((p[0..r_len]).as_ref()).unwrap();
                    if rrc >= rdb_size as u64{
                        // 现在是增量阶段，不需要写入了
                        break
                    }
                }
            }
            // 读取多余的也要包含进去
            let rrc = rdb_read_count.load(Ordering::SeqCst);
            offset_count_c.fetch_add(rrc - rdb_size as u64,Ordering::SeqCst);
            println!("停止读取RDB!");
            loop{
                let ird = is_rdb_done.load(Ordering::Relaxed);
                if !ird{
                    sleep(Duration::from_millis(100))
                }else{
                    break;
                }
            }
            println!("开始读取增量!");
            loop {
                let r_len =match source.read(&mut p){
                    Ok(d)=>d,
                    Err(e) =>{
                        println!("source tcp error {}",e);
                        0
                    },
                };
                if r_len != 0 {
                    offset_count_c.fetch_add(r_len as u64,Ordering::SeqCst);
                    pipe_writer.write_all((p[0..r_len]).as_ref()).unwrap();
                }else{
                    // todo
                    // 没有读取到,只有错误的时候没有读取到?
                    source = open_tcp_conn(source_url, source_pass).unwrap();
                    pre_to_inc(&mut source, uuid.as_ref(), format!("{}", offset_count_c.load(Ordering::SeqCst)+1).as_ref());
                    let offset_count_c_1 = offset_count_c.clone();
                    let mut source_c = source.try_clone().unwrap();
                    spawn(move || {
                        // 上报头部
                        loop{
                            if let Err(e) = report_offset(&mut source_c, &offset_count_c_1) {
                                println!("write err is {}",e.to_string());
                                break;
                            };
                        };
                    });
                }
                // 防止空转
                sleep(Duration::from_millis(5));
            }
        });
        // 全量阶段输出读取进度
        spawn(move||{
            loop {
                let rrcc = rdb_read_count_c.load(Ordering::SeqCst);
                if rrcc < rdb_size as u64{
                    println!("[RDB] total bytes:{} byte, read: {} ", rdb_size, rrcc);
                    sleep(Duration::from_secs(1))
                }else{
                    break;
                }
            };
        });
        loader.rdbReader.raw = Rc::new(RefCell::new(pipe_reader_buf.get_mut().try_clone().unwrap()));
        println!("rdb头部为 {:?}", loader.Header());
        // 全量rdb的命令
        let (full_cmd_sender,full_cmd_receiver) = channel::<Cmd>();
        spawn(move || {
            let mut pipe = redis::pipe();
            let mut full_cmd_count = 0;
            let mut target_conn = open_redis_conn(target_url, target_pass, "").unwrap();
            let dur = Duration::from_secs(1);
            loop{
                 match full_cmd_receiver.recv_timeout(dur){
                     Ok(cmd)=>{
                         full_cmd_count = full_cmd_count + 1;
                         pipe.add_command(cmd);
                         if full_cmd_count>=300{
                            pipe.query::<Value>(&mut target_conn).unwrap();
                            pipe.clear();
                            full_cmd_count = 0;
                         }
                     }
                     Err(e)=>{
                         match e {
                             RecvTimeoutError::Timeout => {
                                if full_cmd_count >0 {
                                    pipe.query::<Value>(&mut target_conn).unwrap();
                                    pipe.clear();
                                    full_cmd_count = 0;
                                };
                                 // 认为rdb完成了
                                is_rdb_done_c.store(true,Ordering::Release);
                                break;
                             }
                             RecvTimeoutError::Disconnected=>{
                                 println!("dis is {}",e);
                             }
                         }
                     }
                 };

            };
        });
        full(&mut loader,&full_cmd_sender);
        // 等待RDB完成命令发送
        loop{
            let ird = is_rdb_done_c1.load(Ordering::Relaxed);
            if !ird{
                sleep(Duration::from_millis(100))
            }else{
                break;
            }
        }
        incr(&mut pipe_reader_buf, target_url, target_pass);
    }
}