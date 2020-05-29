use crate::rdb::loader;
use crate::rdb::loader::{
    rdbReader, BinEntry, Loader, RDBTypeStreamListPacks, RdbFlagAUX, RdbTypeQuicklist,
};
use crate::rdb::slice_buffer::sliceBuffer;
use redis::{Cmd};

use std::cell::RefCell;
use std::error;
use std::error::Error;

use std::io::Write;

use std::rc::Rc;

use std::sync::mpsc::{SyncSender};

use std::thread::{spawn};

use crc64::Crc64;
use time::{Time};


pub fn full(
    loader: &mut Loader,
    full_cmd_sender:&SyncSender<Cmd>
) -> Result<(), Box<dyn Error>> {
    let mut now_db_index = 0;
    loop {
        let mut e = BinEntry {
            DB: 0,
            Key: vec![],
            Type: 0,
            Value: vec![],
            ExpireAt: 0,
            RealMemberCount: 0,
            NeedReadLen: 0,
            IdleTime: 0,
            Freq: 0,
        };
        match loader.NextBinEntry(&mut e) {
            Ok(()) => {
                // 切换DB
                if now_db_index != e.DB {
                    now_db_index = e.DB;
                    full_cmd_sender.send(redis::cmd("SELECT").arg(e.DB).to_owned());
                };
                if e.Type == RdbTypeQuicklist {
                    full_cmd_sender.send(redis::cmd("DEL").arg(e.Key.clone()).to_owned());
                    OverRestoreQuicklistEntry(&e,&full_cmd_sender);
                    if e.ExpireAt != 0 {
                        full_cmd_sender.send( redis::cmd("EXPIREAT").arg(e.Key.clone()).arg(e.ExpireAt).to_owned());
                    }
                } else if e.Type == RdbFlagAUX
                    && String::from_utf8_lossy(e.Key.clone().as_slice()).eq("lua")
                {
                    full_cmd_sender.send( redis::cmd("SCRIPT").arg("load").arg(e.Value).to_owned());
                } else if e.Type != RDBTypeStreamListPacks
                    && (e.Value.len() >= 10*1024*1024 || e.RealMemberCount != 0)
                {
                    OverRestoreBigRdbEntry(&e,&full_cmd_sender);
                } else {
                    let mut ttlms = 0;
                    if e.ExpireAt != 0{
                        let now = Time::now().millisecond();
                        if now>= e.ExpireAt as u16 {
                            ttlms = 1
                        }else{
                            ttlms = e.ExpireAt - now as u64
                        }
                    }
                    full_cmd_sender.send(redis::cmd("DEL").arg(e.Key.clone()).to_owned());
                    full_cmd_sender.send(redis::cmd("RESTORE").arg(e.Key).arg(ttlms).arg(e.Value).to_owned());
                }
            }
            Err(e) => {
                if e.to_string().eq("RDB END") {
                    println!("RDB END!");
                    loader.Footer().unwrap();
                    break;
                } else {
                    println!("err is {}", e);
                }
            }
        }
    };
    Ok(())
}
pub fn OverRestoreQuicklistEntry(
    e: &BinEntry,
    full_cmd_sender:&SyncSender<Cmd>
) -> Result<(), Box<dyn error::Error>> {
    let (read, mut write) = os_pipe::pipe().unwrap();
    let value = e.Value.clone();
    spawn(move||{
        write.write_all(value.as_slice());
    });
    let mut r = rdbReader {
        raw: Rc::new(RefCell::new(read)),
        crc64:Crc64::new(),
        is_cache_buf: false,
        buf: vec![],
        nread: 0,
        remainMember: 0,
        lastReadCount: 0,
        totMemberCount: 0,
    };
    r.ReadByte()?;
    let n = r.ReadLength()?;
    for _ in 0..n {
        let ziplist = r.ReadString()?;
        let mut buf = sliceBuffer::new(ziplist);
        let zln = r.ReadZiplistLength(&mut buf)?;
        for _ in 0..zln {
            let entry = r.ReadZiplistEntry(&mut buf)?;
            full_cmd_sender.send(redis::cmd("RPUSH").arg(e.Key.clone()).arg(0).arg(entry).to_owned());
        }
    }
    Ok(())
}
pub fn OverRestoreBigRdbEntry(
    e: &BinEntry,
    full_cmd_sender:&SyncSender<Cmd>
) -> Result<(), Box<dyn error::Error>> {
    let (read, mut write) = os_pipe::pipe().unwrap();
    let value = e.Value.clone();
    spawn(move||{
        write.write_all(value.as_slice());
    });
    let mut r = rdbReader {
        raw: Rc::new(RefCell::new(read)),
        is_cache_buf: false,
        buf: vec![],
        crc64:Crc64::new(),
        nread: 0,
        remainMember: 0,
        lastReadCount: 0,
        totMemberCount: 0,
    };
    let t = r.ReadByte()?;
    match t {
        loader::RdbTypeHashZiplist => {
            let ziplist = r.ReadString()?;
            let mut buf = sliceBuffer::new(ziplist);
            let mut length = r.ReadZiplistLength(&mut buf)?;
            length = length / 2;
            println!(
                "restore big hash key {} field count {}",
                String::from_utf8(e.Key.clone()).unwrap().as_str(),
                length
            );
            for _ in 0..length {
                let filed = r.ReadZiplistEntry(&mut buf)?;
                let value = r.ReadZiplistEntry(&mut buf)?;
                full_cmd_sender.send(redis::cmd("HSET").arg(e.Key.clone()).arg(filed).arg(value).to_owned());
            }
        }
        loader::RdbTypeZSetZiplist => {
            let ziplist = r.ReadString()?;
            let mut buf = sliceBuffer::new(ziplist);
            let mut cardinality = r.ReadZiplistLength(&mut buf)?;
            cardinality = cardinality / 2;
            println!(
                "restore big zset key {} field count {}",
                String::from_utf8(e.Key.clone()).unwrap().as_str(),
                cardinality
            );
            for _ in 0..cardinality {
                let member = r.ReadZiplistEntry(&mut buf)?;
                let scoreBytes = r.ReadZiplistEntry(&mut buf)?;
                String::from_utf8_lossy(scoreBytes.clone().as_ref())
                    .parse::<f64>()?;
                full_cmd_sender.send(redis::cmd("ZADD").arg(e.Key.clone()).arg(scoreBytes).arg(member).to_owned());
            }
        }
        loader::RdbTypeSetIntset => {
            let intset = r.ReadString()?;
            let mut buf = sliceBuffer::new(intset);
            let intSizeBytes = buf.Slice(4)?;
            let intSize = r.u32(intSizeBytes.as_slice());
            println!("intSizeBytes {:?}",intSizeBytes);
            if intSize != 2 && intSize != 4 && intSize != 8 {
                panic!("rdb: unknown intset encoding ");
            }
            let lenBytes = buf.Slice(4)?;
            let cardinality = r.u32(lenBytes.as_slice());
            println!(
                "restore big set key {} field count {}",
                String::from_utf8(e.Key.clone()).unwrap().as_str(),
                cardinality
            );
            for _ in 0..cardinality {
                let intBytes = buf.Slice(intSize as i32)?;
                let mut intString = vec![];
                match intSize {
                    2 => {
                        intString = format!("{}", r.u16(intBytes.as_slice())).into_bytes();
                    }
                    4 => {
                        intString = format!("{}", r.u32(intBytes.as_slice())).into_bytes();
                    }
                    8 => {
                        intString = format!("{}", r.u64(intBytes.as_slice())).into_bytes();
                    }
                    _ => {}
                }
                full_cmd_sender.send(redis::cmd("SADD").arg(e.Key.clone()).arg(intString).to_owned());
            }
        }
        loader::RdbTypeListZiplist => {
            let ziplist = r.ReadString()?;
            let mut buf = sliceBuffer::new(ziplist);
            let length = r.ReadZiplistLength(&mut buf)?;
            println!(
                "restore big list key {} field count {}",
                String::from_utf8(e.Key.clone()).unwrap().as_str(),
                length
            );
            for _ in 0..length {
                let entry = r.ReadZiplistEntry(&mut buf)?;
                full_cmd_sender.send(redis::cmd("RPUSH").arg(e.Key.clone()).arg(entry).to_owned());
            }
        }
        loader::RdbTypeHashZipmap => {
            let mut length = 0;
            let ziplist = r.ReadString()?;
            let mut buf = sliceBuffer::new(ziplist);
            let lenByte = r.ReadByte()?;
            if lenByte >= 254 {
                length = r.CountZipmapItems(&mut buf)?;
                length = length / 2;
            } else {
                length = lenByte as i32;
            }
            println!(
                "restore big hash key {} field count {}",
                String::from_utf8(e.Key.clone()).unwrap().as_str(),
                length
            );
            for _ in 0..length {
                let field = r.ReadZipmapItem(&mut buf, false)?;
                let value = r.ReadZipmapItem(&mut buf, true)?;
                full_cmd_sender.send(redis::cmd("HSET").arg(e.Key.clone()).arg(field).arg(value).to_owned());
            }
        }
        loader::RdbTypeString => {
            let value = r.ReadString()?;
            full_cmd_sender.send(redis::cmd("SET").arg(e.Key.clone()).arg(value).to_owned());
        }
        loader::RdbTypeList => {
            let n = r.ReadLength()?;
            println!(
                "restore big list key {} field count {}",
                String::from_utf8(e.Key.clone()).unwrap().as_str(),
                n
            );
            for _ in 0..n {
                let field = r.ReadString()?;
                full_cmd_sender.send(redis::cmd("RPUSH").arg(e.Key.clone()).arg(field).to_owned());
            }
        }
        loader::RdbTypeSet => {
            let n = r.ReadLength()?;
            println!(
                "restore big set key {} field count {}",
                String::from_utf8(e.Key.clone()).unwrap().as_str(),
                n
            );
            for _ in 0..n {
                let member = r.ReadString()?;
                full_cmd_sender.send(redis::cmd("SADD").arg(e.Key.clone()).arg(member).to_owned());
            }
        }
        loader::RdbTypeZSet | loader::RdbTypeZSet2 => {
            let n = r.ReadLength()?;
            println!(
                "restore big zset key {} field count {}",
                String::from_utf8(e.Key.clone()).unwrap().as_str(),
                n
            );
            for _ in 0..n {
                let member = r.ReadString()?;
                let score;
                if t == loader::RdbTypeZSet2 {
                    score = r.ReadDouble()?;
                } else {
                    score = r.ReadFloat()?;
                }
                println!(
                    "restore zset key {} field count {} member {}",
                    String::from_utf8(e.Key.clone()).unwrap().as_str(),
                    score,
                    String::from_utf8(member.clone()).unwrap().as_str()
                );
                full_cmd_sender.send( redis::cmd("ZADD").arg(e.Key.clone()).arg(score).arg(member).to_owned());
            }
        }
        loader::RdbTypeHash => {
            let n;
            if e.NeedReadLen == 1 {
                let rlen = r.ReadLength()?;
                if e.RealMemberCount != 0 {
                    n = e.RealMemberCount
                } else {
                    n = rlen
                }
            } else {
                n = e.RealMemberCount
            }
            println!(
                "restore big hash key {} field count {}",
                String::from_utf8(e.Key.clone()).unwrap().as_str(),
                n
            );
            for _ in 0..n {
                let field = r.ReadString()?;
                let value = r.ReadString()?;
                full_cmd_sender.send( redis::cmd("HSET").arg(e.Key.clone()).arg(field).arg(value).to_owned());
            }
        }
        loader::RdbTypeQuicklist => {
            let n = r.ReadLength()?;
            for _ in 0..n {
                let ziplist = r.ReadString()?;
                let mut buf = sliceBuffer::new(ziplist);
                let zln = r.ReadLength()?;
                for _ in 0..zln {
                    let entry = r.ReadZiplistEntry(&mut buf)?;
                    full_cmd_sender.send(redis::cmd("RPUSH").arg(e.Key.clone()).arg(entry).to_owned());
                }
            }
        }
        _ => panic!("restore big key error"),
    };
    Ok(())
}
