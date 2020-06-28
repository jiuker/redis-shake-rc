use crate::rdb::slice_buffer::sliceBuffer;
use byteorder::{LittleEndian, WriteBytesExt};
use crc64::Crc64;

use std::cell::{RefCell};

use std::error::Error;
use std::f32::INFINITY;
use std::f64::{NAN, NEG_INFINITY};
use std::io::{Read, Write};

use std::rc::Rc;
use tokio::io::AsyncReadExt;


pub struct Loader {
    pub rdbReader: rdbReader,
    db: u32,
    lastEntry: Box<BinEntry>,
}
pub const rdbFlagModuleAux: u8 = 0xf7;
pub const rdbFlagIdle: u8 = 0xf8;
pub const rdbFlagFreq: u8 = 0xf9;
pub const RdbFlagAUX: u8 = 0xfa;
pub const rdbFlagResizeDB: u8 = 0xfb;
pub const rdbFlagExpiryMS: u8 = 0xfc;
pub const rdbFlagExpiry: u8 = 0xfd;
pub const rdbFlagSelectDB: u8 = 0xfe;
pub const rdbFlagEOF: u8 = 0xff;

pub const RdbTypeString: u8 = 0;
pub const RdbTypeList: u8 = 1;
pub const RdbTypeSet: u8 = 2;
pub const RdbTypeZSet: u8 = 3;
pub const RdbTypeHash: u8 = 4;
pub const RdbTypeZSet2: u8 = 5;

pub const RdbTypeHashZipmap: u8 = 9;
pub const RdbTypeListZiplist: u8 = 10;
pub const RdbTypeSetIntset: u8 = 11;
pub const RdbTypeZSetZiplist: u8 = 12;
pub const RdbTypeHashZiplist: u8 = 13;
pub const RdbTypeQuicklist: u8 = 14;
pub const RDBTypeStreamListPacks: u8 = 15; // stream;

pub const rdbEncInt8: u8 = 0;
pub const rdbEncInt16: u8 = 1;
pub const rdbEncInt32: u8 = 2;
pub const rdbEncLZF: u8 = 3;

pub const rdb6bitLen: u8 = 0;
pub const rdb14bitLen: u8 = 1;
pub const rdb32bitLen: u8 = 0x80;
pub const rdb64bitLen: u8 = 0x81;
pub const rdbEncVal: u8 = 3;

// Module serialized values sub opcodes
pub const rdbModuleOpcodeEof: u32 = 0;
pub const rdbModuleOpcodeSint: u32 = 1;
pub const rdbModuleOpcodeUint: u32 = 2;
pub const rdbModuleOpcodeFloat: u32 = 3;
pub const rdbModuleOpcodeDouble: u32 = 4;
pub const rdbModuleOpcodeString: u32 = 5;

pub const rdbZiplist6bitlenString: u8 = 0;
pub const rdbZiplist14bitlenString: u8 = 1;
pub const rdbZiplist32bitlenString: u8 = 2;

pub const rdbZiplistInt16: u8 = 0xc0;
pub const rdbZiplistInt32: u8 = 0xd0;
pub const rdbZiplistInt64: u8 = 0xe0;
pub const rdbZiplistInt24: u8 = 0xf0;
pub const rdbZiplistInt8: u8 = 0xfe;
pub const rdbZiplistInt4: u8 = 15;
impl Loader {
    pub fn new(r: Rc<RefCell<async_pipe::PipeReader>>) -> Loader {
        Loader {
            rdbReader: rdbReader {
                raw: r,
                crc64: Crc64::new(),
                is_cache_buf: false,
                buf: vec![],
                nread: 0,
                remainMember: 0,
                lastReadCount: 0,
                totMemberCount: 0,
            },
            db: 0,
            lastEntry: Box::from(BinEntry {
                DB: 0,
                Key: vec![],
                Type: 0,
                Value: vec![],
                ExpireAt: 0,
                RealMemberCount: 0,
                NeedReadLen: 0,
                IdleTime: 0,
                Freq: 0,
            }),
        }
    }
    pub async fn Header(&mut self) -> Result<(), Box<dyn Error>> {
        let mut head_byt = [0 as u8; 9];
        self.readFull(&mut head_byt).await?;
        if head_byt[0..5].ne("REDIS".as_bytes()) {
            return Err(Box::from("不是rdb文件的header"));
        }
        let version = String::from_utf8(Vec::from(&head_byt[5..9]))?.parse::<i32>()?;
        println!("rdb version is {}", version);
        Ok(())
    }
    async fn readFull(&mut self, p: &mut [u8]) -> Result<(), Box<dyn Error>> {
        self.rdbReader.raw.borrow_mut().read_exact(p).await;
        self.rdbReader.crc64.write_all(p);
        if self.rdbReader.is_cache_buf{
            self.rdbReader.buf.append(&mut p.to_vec());
        }
        Ok(())
    }
    pub async fn Footer(&mut self) -> Result<(), Box<dyn Error>> {
        let crc = self.rdbReader.crc64.get();
        let rdb_file_u64 = self.rdbReader.readUint64().await?;
        if rdb_file_u64 != crc {
            println!("{},{}", rdb_file_u64, crc);
            return Err(Box::from("sum校验 不一致!{}{}"));
        };
        Ok(())
    }
    pub async fn NextBinEntry(&mut self, entry: &mut BinEntry) -> Result<(), Box<dyn Error>> {
        loop {
            let mut t = 0;
            if self.rdbReader.remainMember != 0 {
                t = self.lastEntry.Type
            } else {
                t = self.rdbReader.ReadByte().await?;
            }
            match t {
                RdbFlagAUX => {
                    let aux_key = self.rdbReader.ReadString().await?;
                    let aux_value = self.rdbReader.ReadString().await?;
                    println!(
                        "Aux information key:{:?} {:?}",
                        String::from_utf8(aux_key.clone()),
                        String::from_utf8(aux_value.clone())
                    );
                    if (&aux_key).eq(&Vec::from("lua".as_bytes())) {
                        entry.DB = self.db;
                        entry.Key = aux_key;
                        entry.Type = t;
                        entry.Value = aux_value;
                        return Ok(());
                    }
                }
                rdbFlagResizeDB => {
                    let db_size = self.rdbReader.ReadLength().await?;
                    let expire_size = self.rdbReader.ReadLength().await?;
                    println!("db_size:{} expire_size: {}", db_size, expire_size);
                }
                rdbFlagExpiryMS => {
                    let ttlms = self.rdbReader.readUint64().await?;
                    entry.ExpireAt = ttlms;
                }
                rdbFlagExpiry => {
                    let ttls = self.rdbReader.readUint32().await?;
                    entry.ExpireAt = (ttls as u64) * 1000
                }
                rdbFlagSelectDB => {
                    let dbnum = self.rdbReader.ReadLength().await?;
                    self.db = dbnum
                }
                rdbFlagEOF => {
                    return Err(Box::from("RDB END"));
                }
                rdbFlagModuleAux => {
                    let _ = self.rdbReader.ReadLength().await?;
                    rdbLoadCheckModuleValue(self).await?;
                }
                rdbFlagIdle => {
                    let idle = self.rdbReader.ReadLength().await?;
                    entry.IdleTime = idle;
                }
                rdbFlagFreq => {
                    let freq = self.rdbReader.readUint8().await?;
                    entry.Freq = freq
                }
                _ => {
                    let mut key = vec![];
                    if self.rdbReader.remainMember == 0 {
                        // first time visit this key.
                        key = self.rdbReader.ReadString().await?;
                        entry.NeedReadLen = 1; // read value length when it's the first time.
                    } else {
                        key = self.lastEntry.Key.clone()
                    }
                    //log.Debugf("l %p r %p", l, l.rdbReader)
                    //log.Debug("remainMember:", l.remainMember, " key:", string(key[:]), " type:", t)
                    //log.Debug("r.remainMember:", l.rdbReader.remainMember)
                    let val = self.rdbReader.readObjectValue(t).await?;
                    entry.DB = self.db;
                    entry.Key = key;
                    entry.Type = t;
                    entry.Value = createValueDump(t, val);
                    // entry.RealMemberCount = l.lastReadCount
                    if self.rdbReader.lastReadCount == self.rdbReader.totMemberCount {
                        entry.RealMemberCount = 0
                    } else {
                        // RealMemberCount > 0 means this is big entry which also is a split key.
                        entry.RealMemberCount = self.rdbReader.lastReadCount
                    }
                    self.lastEntry = Box::from(entry.clone());
                    // 每次循环把buf清空，因为没用,只有在 readObjectValue 才有用
                    if self.rdbReader.is_cache_buf{
                        self.rdbReader.buf.clear();
                        self.rdbReader.is_cache_buf = false;
                    }
                    // 原来的处理不是这样的，但是要实现原来的处理，很麻烦
                    return Ok(());
                }
            }

        }
        return Err(Box::from("RDB END"));
    }
}
#[derive(Clone, Debug)]
pub struct BinEntry {
    pub DB: u32,
    pub Key: Vec<u8>,
    pub Type: u8,
    pub Value: Vec<u8>,
    pub ExpireAt: u64,
    pub RealMemberCount: u32,
    pub NeedReadLen: u8,
    pub IdleTime: u32,
    pub Freq: u8,
}
pub struct rdbReader {
    pub raw: Rc<RefCell<async_pipe::PipeReader>>,
    pub crc64:Crc64,
    pub is_cache_buf:bool,
    pub buf: Vec<u8>,
    pub nread: i64,
    pub remainMember: u32,
    pub lastReadCount: u32,
    pub totMemberCount: u32,
}
macro_rules! read_uint {
    ($fun_name_uint:ident,$fun_name_int:ident,$n:expr,$reslut_fun:ident,$result_type:ty,$result_type_int:ty) => (
        pub async fn $fun_name_uint(&mut self) -> Result<$result_type, Box<dyn Error>> {
            let mut p: Vec<u8> = vec![0; $n];
            self.raw.borrow_mut().read_exact(p.as_mut()).await?;
            self.crc64.write_all(p.to_vec().as_slice());
            if self.is_cache_buf {
                self.buf.append(p.to_vec().as_mut());
            }
            Ok(self.$reslut_fun(&p))
        }
        pub async fn $fun_name_int(&mut self)->Result<$result_type_int, Box<dyn Error>> {
            Ok(self.$fun_name_uint().await? as $result_type_int)
        }
    );
}
macro_rules! read_uint_big {
    ($fun_name:ident,$n:expr,$reslut_fun:ident) => (
         pub async fn $fun_name(&mut self) -> Result<u32, Box<dyn Error>> {
            let mut p = [0 as u8; $n];
            self.raw.borrow_mut().read_exact(p.as_mut()).await?;
            self.crc64.write_all(p.to_vec().as_slice());
            if self.is_cache_buf {
                self.buf.append(p.to_vec().as_mut());
            }
            Ok(self.$reslut_fun(&p))
        }
    );
}
macro_rules! base_u {
    ($fun_name:ident,$fun_name_big:ident,$result_type:ty,$max:expr) => (
        pub fn $fun_name(&mut self, b: &[u8]) -> $result_type {
            let mut rsl:$result_type = 0;
            let mut index = 0;
            while index <= $max {
                rsl = (b[index] as $result_type) << 8*index | rsl;
                index = index + 1;
            }
            rsl
        }
        pub fn $fun_name_big(&mut self, b: &[u8]) -> $result_type {
            let mut rsl:$result_type = 0;
            let mut index = 0;
            while index <= $max {
                rsl = (b[($max-index)] as $result_type) << 8*index | rsl;
                index = index + 1;
            }
            rsl
        }
    );
}
impl rdbReader {
    pub async fn ReadZipmapItem(
        &mut self,
        buf: &mut sliceBuffer,
        readFree: bool,
    ) -> Result<Vec<u8>, Box<dyn Error>> {
        let (length, free) = self.readZipmapItemLength(buf, readFree).await?;
        if length == -1 {
            return Ok(vec![]);
        };
        let value = buf.Slice(length)?;
        buf.Seek(free as i64, 1);
        Ok(value)
    }
    pub async fn readZipmapItemLength(
        &mut self,
        buf: &mut sliceBuffer,
        readFree: bool,
    ) -> Result<(i32, i32), Box<dyn Error>> {
        let b = buf.ReadByte()?;
        match b {
            253 => {
                let s = buf.Slice(5)?;
                return Ok((
                    self.u32big(s.as_slice()) as i32,
                    (*s.get(4).unwrap()) as i32,
                ));
            }
            254 => {
                return Err(Box::from("rdb: invalid zipmap item length"));
            }
            255 => {
                return Ok((-1, 1));
            }
            _ => {}
        };
        let mut free = 0;
        if readFree {
            free = buf.ReadByte()?;
        };
        Ok((b as i32, free as i32))
    }
    pub async fn CountZipmapItems(&mut self, buf: &mut sliceBuffer) -> Result<i32, Box<dyn Error>> {
        let mut n = 0;
        loop {
            let (strLen, free) = self.readZipmapItemLength(buf, n % 2 != 0).await?;
            if strLen == -1 {
                break;
            };
            buf.Seek((strLen + free) as i64, 1)?;
            n = n + 1;
        }
        buf.Seek(0, 0)?;
        Ok(n)
    }
    pub async fn ReadZiplistEntry(&mut self, buf: &mut sliceBuffer) -> Result<Vec<u8>, Box<dyn Error>> {
        let prevLen = buf.ReadByte()?;
        if prevLen == 254 {
            buf.Seek(4, 1); // skip the 4-byte prevlen
        };
        let header = buf.ReadByte()?;
        if (header >> 6) as u8 == rdbZiplist6bitlenString {
            return Ok(buf.Slice((header & 0x3f) as i32)?);
        };
        if (header >> 6) as u8 == rdbZiplist14bitlenString {
            let b = buf.ReadByte()?;
            return Ok(buf.Slice((((header & 0x3f) as i32) << 8) as i32 | b as i32)?);
        }
        if (header >> 6) as u8 == rdbZiplist32bitlenString {
            let lenBytes = buf.Slice(4)?;
            return Ok(buf.Slice(self.u32big(lenBytes.as_ref()) as i32)?);
        }
        if header == rdbZiplistInt16 as u8 {
            let intBytes = buf.Slice(2)?;
            return Ok(format!("{}", self.u16(intBytes.as_slice())).into_bytes());
        }
        if header == rdbZiplistInt32 as u8 {
            let intBytes = buf.Slice(4)?;
            return Ok(format!("{}", self.u32(intBytes.as_slice())).into_bytes());
        }
        if header == rdbZiplistInt64 as u8 {
            let intBytes = buf.Slice(8)?;
            return Ok(format!("{}", self.u64(intBytes.as_slice())).into_bytes());
        }
        if header == rdbZiplistInt24 as u8 {
            let intBytes_ = [0 as u8; 3];
            buf.Read(&mut intBytes_.to_vec());
            let mut intBytes = [0 as u8;4];
            let mut index = 0;
            for _ in intBytes_.iter(){
                *intBytes.get_mut(index+1).unwrap() = *intBytes_.get(index).unwrap();
                index=index+1;
            }
            return Ok(format!("{}", self.u32(intBytes.as_ref()) >> 8).into_bytes());
        }
        if header == rdbZiplistInt8 as u8 {
            let b = buf.ReadByte()?;
            return Ok(format!("{}", b as i8).into_bytes());
        }
        if (header >> 4) as u8 == rdbZiplistInt4 {
            return Ok(format!("{}", (header & 0x0f) as i64 - 1).into_bytes());
        }
        Err(Box::from("rdb: unknown ziplist header byte"))
    }
    pub async fn ReadZiplistLength(&mut self, buf: &mut sliceBuffer) -> Result<i64, Box<dyn Error>> {
        buf.Seek(8, 0); // skip the zlbytes and zltail
        let lenBytes = buf.Slice(2)?;
        Ok(self.u16(lenBytes.as_slice()) as i64)
    }
    pub async fn ReadByte(&mut self) -> Result<u8, Box<dyn Error>> {
        let mut p = [0 as u8; 1];
        self.raw.borrow_mut().read_exact(p.as_mut()).await?;
        self.crc64.write_all(p.to_vec().as_slice());
        if self.is_cache_buf{
            self.buf.append(p.to_vec().as_mut());
        }
        Ok(p[0])
    }
    pub async fn ReadString(&mut self) -> Result<Vec<u8>, Box<dyn Error>> {
        let (length, encoded) = self.readEncodedLength().await?;
        if !encoded {
            return self.ReadBytes(length as usize).await;
        }
        match length as u8 {
            rdbEncInt8 => {
                let i = self.readInt8().await?;
                return Ok(Vec::from(format!("{}", i)));
            }
            rdbEncInt16 => {
                let i = self.readInt16().await?;
                return Ok(Vec::from(format!("{}", i)));
            }
            rdbEncInt32 => {
                let i = self.readInt32().await?;
                return Ok(Vec::from(format!("{}", i)));
            }
            rdbEncLZF => {
                let inlen = self.ReadLength().await?;
                let outlen = self.ReadLength().await?;
                let in_data = self.ReadBytes(inlen as usize).await?;
                return lzfDecompress(&in_data, outlen as usize);
            }
            _ => {
                return Err(Box::from("invalid encoded-string"));
            }
        }
        Ok(Vec::from("".as_bytes()))
    }
    pub async fn readEncodedLength(&mut self) -> Result<(u32, bool), Box<dyn Error>> {
        let u = self.readUint8().await?;
        let mut length = 0;
        let mut encoded = false;
        match u >> 6 {
            rdb6bitLen => {
                length = (u & 0x3f) as u32;
            }
            rdb14bitLen => {
                let u2 = self.readUint8().await?;
                length = (((u & 0x3f) as u32) << 8) + u2 as u32;
            }
            rdbEncVal => {
                encoded = true;
                length = (u & 0x3f) as u32;
            }
            _ => match u {
                rdb32bitLen => {
                    length = self.readUint32BigEndian().await?;
                }
                rdb64bitLen => {
                    length = self.readUint64BigEndian().await?;
                }
                _ => {
                    return Err(Box::from(format!("unknown encoding length {}",u)));
                }
            },
        };
        Ok((length, encoded))
    }
    read_uint!(readUint8,readInt8,1,u8,u8,i8);
    read_uint!(readUint16,readInt16,2,u16,u16,i16);
    read_uint!(readUint32,readInt32,4,u32,u32,i32);
    read_uint!(readUint64,readInt64,8,u64,u64,i64);
    read_uint_big!(readUint32BigEndian,4,u32big);
    read_uint_big!(readUint64BigEndian,8,u32big);
    base_u!(u8,u8big,u8,0);
    base_u!(u16,u16big,u16,1);
    base_u!(u32,u32big,u32,3);
    base_u!(u64,u64big,u64,7);
    pub async fn ReadBytes(&mut self, n: usize) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut p: Vec<u8> = vec![0; n];
        self.raw.borrow_mut().read_exact(&mut p).await?;
        self.crc64.write_all(p.to_vec().as_slice());
        if self.is_cache_buf{
            self.buf.append(p.clone().as_mut());
        }
        Ok(p)
    }
    pub async fn ReadLength(&mut self) -> Result<u32, Box<dyn Error>> {
        let (length, encoded) = self.readEncodedLength().await?;
        if encoded {
            return Err(Box::from("encoded-length"));
        };
        Ok(length)
    }
    pub async fn ReadFloat(&mut self) -> Result<f64, Box<dyn Error>> {
        let u = self.readUint8().await?;
        match u {
            253 => {
                return Ok(NAN);
            }
            254 => return Ok(INFINITY as f64),
            255 => return Ok(NEG_INFINITY),
            _ => {
                let b = self.ReadBytes(u as usize).await?;
                return Ok(String::from_utf8(b)?.parse::<f64>()?);
            }
        }
    }
    pub async fn ReadDouble(&mut self) -> Result<f64, Box<dyn Error>> {
        let mut p = [0 as u8; 8];
        self.raw.borrow_mut().read_exact(p.as_mut()).await?;
        self.crc64.write_all(p.to_vec().as_slice());
        if self.is_cache_buf {
            self.buf.append(p.to_vec().as_mut());
        }
        Ok(self.u64(&p) as f64)
    }

    pub async fn readObjectValue(&mut self, t: u8) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut lr = self;
        lr.is_cache_buf = true;
        match t {
            RdbFlagAUX | rdbFlagResizeDB | RdbTypeHashZipmap | RdbTypeListZiplist
            | RdbTypeSetIntset | RdbTypeZSetZiplist | RdbTypeHashZiplist | RdbTypeString => {
                lr.lastReadCount = 0;
                lr.remainMember = 0;
                lr.totMemberCount = 0;
                lr.ReadString().await?;
            }
            RdbTypeList | RdbTypeSet | RdbTypeQuicklist => {
                lr.lastReadCount = 0;
                lr.remainMember = 0;
                lr.totMemberCount = 0;
                let n = lr.ReadLength().await?;
                for _i in 0..n {
                    lr.ReadString().await?;
                }
            }
            RdbTypeZSet | RdbTypeZSet2 => {
                lr.lastReadCount = 0;
                lr.remainMember = 0;
                lr.totMemberCount = 0;
                let n = lr.ReadLength().await?;
                for _i in 0..n {
                    lr.ReadString().await?;
                    if t == RdbTypeZSet2 {
                        lr.ReadDouble().await?;
                    } else {
                        lr.ReadFloat().await?;
                    }
                }
            }
            RdbTypeHash => {
                let mut n = 0;
                if lr.remainMember != 0 {
                    n = lr.remainMember
                } else {
                    let rlen = lr.ReadLength().await?;
                    n = rlen;
                    lr.totMemberCount = rlen;
                }
                lr.lastReadCount = 0;
                for i in 0..n {
                    lr.ReadString().await?;
                    lr.ReadString().await?;
                    lr.lastReadCount = lr.lastReadCount + 1;
                    if lr.buf.len() > 16*1024*1024 && i != (n - 1) {
                        lr.remainMember = n - i - 1;
                        // log.Infof("r %p", lr)
                        // log.Info("r: ", lr, " set remainMember:", lr.remainMember)
                        // debug.FreeOSMemory()
                        break;
                    }
                }
                if lr.lastReadCount == n {
                    lr.remainMember = 0
                }
            }
            RDBTypeStreamListPacks => {
                lr.lastReadCount = 0;
                lr.remainMember = 0;
                lr.totMemberCount = 0;
                let nListPacks = lr.ReadLength().await?;
                // list pack length
                for _ in 0..nListPacks {
                    // read twice
                    lr.ReadString().await?;
                    lr.ReadString().await?;
                }
                // items
                lr.ReadLength().await?;
                // last_entry_id timestamp second
                lr.ReadLength().await?;
                // last_entry_id timestamp millisecond
                lr.ReadLength().await?;
                // cgroups length
                let nCgroups = lr.ReadLength().await?;
                for _ in 0..nCgroups {
                    // cname
                    lr.ReadString().await?;
                    // last_cg_entry_id timestamp second
                    lr.ReadLength().await?;
                    // last_cg_entry_id timestamp millisecond
                    lr.ReadLength().await?;
                    // pending number
                    let nPending = lr.ReadLength().await?;
                    for _ in 0..nPending {
                        // eid, read 16 bytes
                        lr.ReadBytes(16).await?;
                        // seen_time
                        lr.ReadBytes(8).await?;
                        // delivery_count
                        lr.ReadLength().await?;
                    }
                    // consumers
                    let nConsumers = lr.ReadLength().await?;
                    for _ in 0..nConsumers {
                        // cname
                        lr.ReadString().await?;
                        // seen_time
                        lr.ReadBytes(8).await?;
                        // pending
                        let nPending2 = lr.ReadLength().await?;
                        for _ in 0..nPending2 {
                            lr.ReadBytes(16);
                        }
                    }
                }
            }
            _ => {
                return Err(Box::from(format!("unknown object-type {}", t)));
            }
        };
        Ok(lr.buf.clone())
    }
}
macro_rules! must_get {
    ($data:ident,$i:ident) => (
        match $data.get($i) {
            Some(d) => *d,
            None => {
                return Err(Box::from("data not exist!"));
            }
        }
    );
}
macro_rules! must_get_mut {
    ($data:ident,$i:ident) => (
        match $data.get_mut($i) {
            Some(d) => d,
            None => {
                return Err(Box::from("data not exist!"));
            }
        }
    );
}
pub fn lzfDecompress(in_data: &Vec<u8>, outlen: usize) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut out: Vec<u8> = vec![0; outlen];
    let (mut i, mut o) = (0, 0);
    while i < in_data.len() {
        let ctrl: i32 = must_get!(in_data,i) as i32;
        i = i + 1;
        if ctrl < 32 {
            for _ in 0..=ctrl {
                * must_get_mut!(out,o) = must_get!(in_data,i);
                i = i + 1;
                o = o + 1;
            }
        } else {
            let mut length = ctrl >> 5;
            if length == 7 {
                length = length + must_get!(in_data,i) as i32;
                i = i + 1;
            }
            let mut ref_d = o
                - ((ctrl & 0x1f) << 8) as usize
                - must_get!(in_data,i) as usize
                - 1;
            i = i + 1;
            for _ in 0..=(length + 1) {
                * must_get_mut!(out,o) = must_get!(out,ref_d);
                ref_d = ref_d + 1;
                o = o + 1;
            }
        }
    }
    Ok(out)
}

async fn rdbLoadCheckModuleValue(l: &mut Loader) -> Result<(), Box<dyn Error>> {
    let _opcode: u32 = 0;
    loop {
        let opcode = l.rdbReader.ReadLength().await?;
        if opcode == rdbModuleOpcodeEof {
            break;
        }
        match opcode {
            rdbModuleOpcodeSint => {
                l.rdbReader.ReadLength().await?;
            }
            rdbModuleOpcodeUint => {
                l.rdbReader.ReadLength().await?;
            }
            rdbModuleOpcodeString => {
                l.rdbReader.ReadString().await?;
            }
            rdbModuleOpcodeFloat => {
                l.rdbReader.ReadFloat().await?;
            }
            rdbModuleOpcodeDouble => {
                l.rdbReader.ReadDouble().await?;
            }
            _ => {}
        }
    }
    Ok(())
}
// 把数据变成6版本的
pub fn createValueDump(t: u8, val: Vec<u8>) -> Vec<u8> {
    let mut wtr = vec![];
    let mut crc = Crc64::new();
    wtr.push(t);
    crc.write_u8(t);
    wtr.append(&mut val.clone());
    crc.write(val.as_slice());
    wtr.write_u16::<LittleEndian>(6 as u16).unwrap();
    crc.write_u16::<LittleEndian>(6 as u16).unwrap();
    wtr.write_u64::<LittleEndian>(crc.get()).unwrap();
    return wtr;
}
