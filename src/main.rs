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
    let mut source_c1 = source.try_clone().unwrap();
    let (offset,rdb_size) = pre_to_rdb(&mut source).unwrap();

    let mut source_read = BufReader::with_capacity(10*1024*1024, source);
    let (pipe_reader,mut pipe_writer) = os_pipe::pipe().unwrap();

    let mut rdb_read_count = Arc::new(AtomicUsize::new(0));
    let mut rdb_read_count_c = rdb_read_count.clone();

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
            }
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
    loader.rdbReader.raw = Rc::new(RefCell::new(pipe_reader));
    let offset_count = Arc::new(AtomicUsize::new(offset as usize));
    let offset_count_c = offset_count.clone();
    println!("rdb头部为 {:?}", loader.Header());
    spawn(move || {
        // 上报头部
        report_offset(&mut source_c, &offset_count);
    });
    full(&mut loader, target_url, target_pass);
    println!("全量阶段已经结束!");
    incr(&mut source_c1, target_url, target_pass, offset_count_c);
}
