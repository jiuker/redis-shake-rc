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
use std::io::{Write, BufReader};
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
    let offset = pre_to_rdb(&mut source).unwrap();
    let source_read = BufReader::with_capacity(10*1024*1024,source);
    loader.rdbReader.raw = Rc::new(RefCell::new(source_read));
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
    sleep(Duration::from_secs(1000000000000));
}
