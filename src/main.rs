

use redis_shake_rs::rdb::conn::open_tcp_conn;
use redis_shake_rs::rdb::full::full;
use redis_shake_rs::rdb::incr::incr;
use redis_shake_rs::rdb::loader::{
    Loader
};

use redis_shake_rs::rdb::source::{pre_to_rdb, report_offset, pre_to_inc};
use std::cell::{RefCell};


use std::io::{Write, BufReader, Read};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering,AtomicU64};

use std::sync::{Arc};
use std::thread::{sleep, spawn};
use std::time::{Duration};

fn main() {
    let source_url = "127.0.0.1:6379";
    let source_pass  = "";
    let target_url = "127.0.0.1:6400";
    let target_pass  = "";
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
    // offset
    let offset_count = Arc::new(AtomicU64::new(offset as u64));
    let offset_count_c = offset_count.clone();
    // 读取源端数据
    spawn(move||{
        let mut source_c = source.try_clone().unwrap();
        spawn(move || {
            // 上报头部
            loop{
                report_offset(&mut source_c, &offset_count);
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
                Err(e) =>0,
            };
            if r_len != 0 {
                offset_count_c.fetch_add(r_len as u64,Ordering::SeqCst);
                pipe_writer.write_all((p[0..r_len]).as_ref()).unwrap();
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
    full(&mut loader, target_url, target_pass);
    is_rdb_done_c.store(true,Ordering::Release);
    incr(&mut pipe_reader_buf, target_url, target_pass);
}
