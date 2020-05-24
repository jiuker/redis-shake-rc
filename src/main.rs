use redis::{Client, Commands, Connection, Value};
use redis_shake_rs::rdb::cmd::cmd_to_string;
use redis_shake_rs::rdb::conn::open_tcp_conn;
use redis_shake_rs::rdb::full::full;
use redis_shake_rs::rdb::incr::incr;
use redis_shake_rs::rdb::loader::{
    rdbReader, BinEntry, Loader, RDBTypeStreamListPacks, RdbFlagAUX, RdbTypeQuicklist,
};
use redis_shake_rs::rdb::slice_buffer::sliceBuffer;
use redis_shake_rs::rdb::source::{pre_to_rdb, report_offset};
use std::cell::{Cell, RefCell};
use std::error;
use std::fs::{File, OpenOptions};
use std::io::{Write, BufReader, Read};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};
use std::thread::{sleep, spawn};
use std::time::{Duration, SystemTime};

fn main() {
    let source_url = "127.0.0.1:6379";
    let source_pass  = "";
    let target_url = "127.0.0.1:6400";
    let target_pass  = "";
    let mut loader = Loader::new(Rc::new(RefCell::new("".as_bytes())));
    let mut source = open_tcp_conn(source_url, source_pass).unwrap();
    let mut source_c = source.try_clone().unwrap();
    let (offset,rdb_size) = pre_to_rdb(&mut source).unwrap();

    let mut source_read = BufReader::with_capacity(10*1024*1024, source);
    let (mut pipe_reader,mut pipe_writer) = os_pipe::pipe().unwrap();

    let mut rdb_read_count = Arc::new(AtomicUsize::new(0));
    let mut rdb_read_count_c = rdb_read_count.clone();

    let is_rdb_done = Arc::new(AtomicBool::new(false));
    let is_rdb_done_c = is_rdb_done.clone();
    // offset
    let offset_count = Arc::new(AtomicUsize::new(offset as usize));
    let offset_count_c = offset_count.clone();
    spawn(move||{
        let mut p = [0; 512 * 1024];
        loop {
            let r_len =match source_read.read(&mut p){
                Ok(d)=>d,
                Err(_) => 0,
            };
            if r_len != 0 {
                let mut rrc = rdb_read_count.load(Ordering::Acquire);
                rrc = rrc + r_len;
                rdb_read_count.store(rrc,Ordering::Release);
                pipe_writer.write_all((p[0..r_len]).as_ref());
                if rrc >= rdb_size as usize{
                    // 现在是增量阶段，不需要写入了
                    break
                }
            }
        }
        // 读取多余的也要包含进去
        let mut rrc = rdb_read_count.load(Ordering::Acquire);
        let mut occ = offset_count_c.load(Ordering::Acquire);
        occ = occ + (rrc - rdb_size as usize);
        offset_count_c.store(occ,Ordering::Release);
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
            let r_len =match source_read.read(&mut p){
                Ok(d)=>d,
                Err(_) => 0,
            };
            if r_len != 0 {
                let mut occ = offset_count_c.load(Ordering::Acquire);
                occ = occ + r_len;
                offset_count_c.store(occ,Ordering::Release);
                pipe_writer.write_all((p[0..r_len]).as_ref());
            }
            // 防止空转
            sleep(Duration::from_millis(5));
        }
    });

    spawn(move||{
        // 全量阶段输出读取进度
        loop {
            let rrcc = rdb_read_count_c.load(Ordering::Relaxed);
            if rrcc < rdb_size as usize{
                println!("[RDB] total bytes:{} byte, read: {} ", rdb_size, rrcc);
                sleep(Duration::from_secs(1))
            }else{
                break;
            }
        };
    });
    loader.rdbReader.raw = Rc::new(RefCell::new(pipe_reader.try_clone().unwrap()));
    println!("rdb头部为 {:?}", loader.Header());
    spawn(move || {
        // 上报头部
        report_offset(&mut source_c, &offset_count);
    });
    full(&mut loader, target_url, target_pass);
    is_rdb_done_c.store(true,Ordering::Release);
    incr(&mut pipe_reader, target_url, target_pass);
}
