use crate::rdb::slice_buffer::sliceBuffer;
use byteorder::{LittleEndian, WriteBytesExt};
use crc64::Crc64;
use std::borrow::Borrow;
use std::cell::{Cell, RefCell};
use std::convert::TryFrom;
use std::error::Error;
use std::f32::INFINITY;
use std::f64::{NAN, NEG_INFINITY};
use std::io::{BufReader, Read, Write};
use std::ops::Deref;
use std::rc::Rc;

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
    pub fn new(r: Rc<RefCell<dyn Read>>) -> Loader {
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
    pub fn Header(&mut self) -> Result<(), Box<dyn Error>> {
        let mut head_byt = [0 as u8; 9];
        self.readFull(&mut head_byt)?;
        if head_byt[0..5].ne("REDIS".as_bytes()) {
            return Err(Box::try_from("不是rdb文件的header")?);
        }
        let version = String::from_utf8(Vec::from(&head_byt[5..9]))?.parse::<i32>()?;
        println!("rdb version is {}", version);
        Ok(())
    }
    fn readFull(&mut self, p: &mut [u8]) -> Result<(), Box<dyn Error>> {
        self.rdbReader.raw.borrow_mut().read_exact(p);
        self.rdbReader.crc64.write_all(p);
        if self.rdbReader.is_cache_buf{
            self.rdbReader.buf.append(&mut p.to_vec());
        }
        Ok(())
    }
    pub fn Footer(&mut self) -> Result<(), Box<dyn Error>> {
        let crc = self.rdbReader.crc64.get();
        let rdb_file_u64 = self.rdbReader.readUint64()?;
        if rdb_file_u64 != crc {
            println!("{},{}", rdb_file_u64, crc);
            return Err(Box::try_from("sum校验 不一致!{}{}")?);
        };
        Ok(())
    }
    pub fn NextBinEntry(&mut self, entry: &mut BinEntry) -> Result<(), Box<dyn Error>> {
        loop {
            let mut t = 0;
            if self.rdbReader.remainMember != 0 {
                t = self.lastEntry.Type
            } else {
                t = self.rdbReader.ReadByte()?;
            }
            match t {
                RdbFlagAUX => {
                    let aux_key = self.rdbReader.ReadString()?;
                    let aux_value = self.rdbReader.ReadString()?;
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
                    let db_size = self.rdbReader.ReadLength()?;
                    let expire_size = self.rdbReader.ReadLength()?;
                    println!("db_size:{} expire_size: {}", db_size, expire_size);
                }
                rdbFlagExpiryMS => {
                    let ttlms = self.rdbReader.readUint64()?;
                    entry.ExpireAt = ttlms;
                }
                rdbFlagExpiry => {
                    let ttls = self.rdbReader.readUint32()?;
                    entry.ExpireAt = (ttls as u64) * 1000
                }
                rdbFlagSelectDB => {
                    let dbnum = self.rdbReader.ReadLength()?;
                    self.db = dbnum
                }
                rdbFlagEOF => {
                    return Err(Box::try_from("RDB END")?);
                }
                rdbFlagModuleAux => {
                    let _ = self.rdbReader.ReadLength()?;
                    rdbLoadCheckModuleValue(self)?;
                }
                rdbFlagIdle => {
                    let idle = self.rdbReader.ReadLength()?;
                    entry.IdleTime = idle;
                }
                rdbFlagFreq => {
                    let freq = self.rdbReader.readUint8()?;
                    entry.Freq = freq
                }
                _ => {
                    let mut key = vec![];
                    if self.rdbReader.remainMember == 0 {
                        // first time visit this key.
                        key = self.rdbReader.ReadString()?;
                        entry.NeedReadLen = 1; // read value length when it's the first time.
                    } else {
                        key = self.lastEntry.Key.clone()
                    }
                    //log.Debugf("l %p r %p", l, l.rdbReader)
                    //log.Debug("remainMember:", l.remainMember, " key:", string(key[:]), " type:", t)
                    //log.Debug("r.remainMember:", l.rdbReader.remainMember)
                    let val = self.rdbReader.readObjectValue(t)?;
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
        return Err(Box::try_from("RDB END")?);
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
    pub raw: Rc<RefCell<dyn Read>>,
    pub crc64:Crc64,
    pub is_cache_buf:bool,
    pub buf: Vec<u8>,
    pub nread: i64,
    pub remainMember: u32,
    pub lastReadCount: u32,
    pub totMemberCount: u32,
}

impl rdbReader {
    pub fn ReadZipmapItem(
        &mut self,
        buf: &mut sliceBuffer,
        readFree: bool,
    ) -> Result<Vec<u8>, Box<dyn Error>> {
        let (length, free) = self.readZipmapItemLength(buf, readFree)?;
        if length == -1 {
            return Ok(vec![]);
        };
        let value = buf.Slice(length)?;
        buf.Seek(free as i64, 1);
        Ok(value)
    }
    pub fn readZipmapItemLength(
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
                return Err(Box::try_from("rdb: invalid zipmap item length")?);
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
    pub fn CountZipmapItems(&mut self, buf: &mut sliceBuffer) -> Result<i32, Box<dyn Error>> {
        let mut n = 0;
        loop {
            let (strLen, free) = self.readZipmapItemLength(buf, n % 2 != 0)?;
            if strLen == -1 {
                break;
            };
            buf.Seek((strLen + free) as i64, 1)?;
            n = n + 1;
        }
        buf.Seek(0, 0)?;
        Ok(n)
    }
    pub fn ReadZiplistEntry(&mut self, buf: &mut sliceBuffer) -> Result<Vec<u8>, Box<dyn Error>> {
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
            return Ok((buf.Slice(self.u32big(lenBytes.as_ref()) as i32)?));
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
            let mut intBytes_ = [0 as u8; 3];
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
        Err(Box::try_from("rdb: unknown ziplist header byte")?)
    }
    pub fn ReadZiplistLength(&mut self, buf: &mut sliceBuffer) -> Result<i64, Box<dyn Error>> {
        buf.Seek(8, 0); // skip the zlbytes and zltail
        let lenBytes = buf.Slice(2)?;
        Ok(self.u16(lenBytes.as_slice()) as i64)
    }
    pub fn ReadByte(&mut self) -> Result<u8, Box<dyn Error>> {
        let mut p = [0 as u8; 1];
        self.raw.borrow_mut().read_exact(p.as_mut())?;
        self.crc64.write_all(p.to_vec().as_slice());
        if self.is_cache_buf{
            self.buf.append(p.to_vec().as_mut());
        }
        Ok(p[0])
    }
    pub fn ReadString(&mut self) -> Result<Vec<u8>, Box<dyn Error>> {
        let (length, encoded) = self.readEncodedLength()?;
        if !encoded {
            return self.ReadBytes(length as usize);
        }
        match length as u8 {
            rdbEncInt8 => {
                let i = self.readInt8()?;
                return Ok(Vec::from(format!("{}", i)));
            }
            rdbEncInt16 => {
                let i = self.readInt16()?;
                return Ok(Vec::from(format!("{}", i)));
            }
            rdbEncInt32 => {
                let i = self.readInt32()?;
                return Ok(Vec::from(format!("{}", i)));
            }
            rdbEncLZF => {
                let inlen = self.ReadLength()?;
                let outlen = self.ReadLength()?;
                let in_data = self.ReadBytes(inlen as usize)?;
                return lzfDecompress(&in_data, outlen as usize);
            }
            _ => {
                return Err(Box::try_from("invalid encoded-string")?);
            }
        }
        Ok(Vec::from("".as_bytes()))
    }
    pub fn readEncodedLength(&mut self) -> Result<(u32, bool), Box<dyn Error>> {
        let u = self.readUint8()?;
        let mut length = 0;
        let mut encoded = false;
        match u >> 6 {
            rdb6bitLen => {
                length = (u & 0x3f) as u32;
            }
            rdb14bitLen => {
                let u2 = self.readUint8()?;
                length = (((u & 0x3f) as u32) << 8) + u2 as u32;
            }
            rdbEncVal => {
                encoded = true;
                length = (u & 0x3f) as u32;
            }
            _ => match u {
                rdb32bitLen => {
                    length = self.readUint32BigEndian()?;
                }
                rdb64bitLen => {
                    length = self.readUint64BigEndian()?;
                }
                _ => {
                    return Err(Box::try_from(format!("unknown encoding length {}",u))?);
                }
            },
        };
        Ok((length, encoded))
    }
    pub fn readUint8(&mut self) -> Result<u8, Box<dyn Error>> {
        Ok(self.ReadByte()?)
    }
    pub fn readInt8(&mut self) -> Result<i8, Box<dyn Error>> {
        Ok(self.readUint8()? as i8)
    }
    pub fn readUint32BigEndian(&mut self) -> Result<u32, Box<dyn Error>> {
        let mut p = [0 as u8; 4];
        self.raw.borrow_mut().read_exact(p.as_mut())?;
        self.crc64.write_all(p.to_vec().as_slice());
        if self.is_cache_buf {
            self.buf.append(p.to_vec().as_mut());
        }
        Ok(self.u32big(&p))
    }
    pub fn readUint64BigEndian(&mut self) -> Result<u32, Box<dyn Error>> {
        let mut p = [0 as u8; 8];
        self.raw.borrow_mut().read_exact(p.as_mut())?;
        self.crc64.write_all(p.to_vec().as_slice());
        if self.is_cache_buf {
            self.buf.append(p.to_vec().as_mut());
        }
        Ok(self.u32big(&p) as u32)
    }
    pub fn u16big(&mut self, b: &[u8]) -> u16 {
        return (b[1]) as u16 | ((b[0] as u16) << 8);
    }
    pub fn u16(&mut self, b: &[u8]) -> u16 {
        return (b[0]) as u16 | ((b[1] as u16) << 8);
    }
    pub fn u32big(&mut self, b: &[u8]) -> u32 {
        return (b[3]) as u32
            | ((b[2] as u32) << 8)
            | ((b[1] as u32) << 16)
            | ((b[0] as u32) << 24);
    }
    pub fn u32(&mut self, b: &[u8]) -> u32 {
        return (b[0]) as u32
            | ((b[1] as u32) << 8)
            | ((b[2] as u32) << 16)
            | ((b[3] as u32) << 24);
    }
    pub fn u64big(&mut self, b: &[u8]) -> u64 {
        return (b[7]) as u64
            | ((b[6] as u64) << 8)
            | ((b[5] as u64) << 16)
            | ((b[4] as u64) << 24)
            | ((b[3]) as u64) << 32
            | ((b[2] as u64) << 40)
            | ((b[1] as u64) << 48)
            | ((b[0] as u64) << 56);
    }
    pub fn u64(&mut self, b: &[u8]) -> u64 {
        return (b[0]) as u64
            | ((b[1] as u64) << 8)
            | ((b[2] as u64) << 16)
            | ((b[3] as u64) << 24)
            | ((b[4]) as u64) << 32
            | ((b[5] as u64) << 40)
            | ((b[6] as u64) << 48)
            | ((b[7] as u64) << 56);
    }
    pub fn ReadBytes(&mut self, n: usize) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut p: Vec<u8> = vec![0; n];
        self.raw.borrow_mut().read_exact(&mut p)?;
        self.crc64.write_all(p.to_vec().as_slice());
        if self.is_cache_buf{
            self.buf.append(p.clone().as_mut());
        }
        Ok(p)
    }
    pub fn readUint16(&mut self) -> Result<u16, Box<dyn Error>> {
        let mut p: Vec<u8> = vec![0; 2];
        self.raw.borrow_mut().read_exact(p.as_mut())?;
        self.crc64.write_all(p.to_vec().as_slice());
        if self.is_cache_buf {
            self.buf.append(p.to_vec().as_mut());
        }
        Ok(self.u16(&p))
    }
    pub fn readInt16(&mut self)->Result<i16, Box<dyn Error>> {
        Ok(self.readUint16()? as i16)
    }
    pub fn readUint32(&mut self) -> Result<u32, Box<dyn Error>> {
        let mut p: Vec<u8> = vec![0; 4];
        self.raw.borrow_mut().read_exact(p.as_mut())?;
        self.crc64.write_all(p.to_vec().as_slice());
        if self.is_cache_buf {
            self.buf.append(p.to_vec().as_mut());
        }
        Ok(self.u32(&p))
    }
    pub fn readInt32(&mut self)->Result<i32, Box<dyn Error>> {
        Ok(self.readUint32()? as i32)
    }
    pub fn readUint64(&mut self) -> Result<u64, Box<dyn Error>> {
        let mut p: Vec<u8> = vec![0; 8];
        self.raw.borrow_mut().read_exact(p.as_mut())?;
        self.crc64.write_all(p.to_vec().as_slice());
        if self.is_cache_buf {
            self.buf.append(p.to_vec().as_mut());
        }
        Ok(self.u64(&p))
    }
    pub fn ReadLength(&mut self) -> Result<u32, Box<dyn Error>> {
        let (length, encoded) = self.readEncodedLength()?;
        if encoded {
            return Err(Box::try_from("encoded-length")?);
        };
        Ok(length)
    }
    pub fn ReadFloat(&mut self) -> Result<f64, Box<dyn Error>> {
        let u = self.readUint8()?;
        match u {
            253 => {
                return Ok(NAN);
            }
            254 => return Ok(INFINITY as f64),
            255 => return Ok(NEG_INFINITY),
            _ => {
                let b = self.ReadBytes(u as usize)?;
                return Ok(String::from_utf8(b)?.parse::<f64>()?);
            }
        }
    }
    pub fn ReadDouble(&mut self) -> Result<f64, Box<dyn Error>> {
        let mut p = [0 as u8; 8];
        self.raw.borrow_mut().read_exact(p.as_mut())?;
        self.crc64.write_all(p.to_vec().as_slice());
        if self.is_cache_buf {
            self.buf.append(p.to_vec().as_mut());
        }
        Ok(self.u64(&p) as f64)
    }

    pub fn readObjectValue(&mut self, t: u8) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut lr = self;
        lr.is_cache_buf = true;
        match t {
            RdbFlagAUX | rdbFlagResizeDB | RdbTypeHashZipmap | RdbTypeListZiplist
            | RdbTypeSetIntset | RdbTypeZSetZiplist | RdbTypeHashZiplist | RdbTypeString => {
                lr.lastReadCount = 0;
                lr.remainMember = 0;
                lr.totMemberCount = 0;
                lr.ReadString()?;
            }
            RdbTypeList | RdbTypeSet | RdbTypeQuicklist => {
                lr.lastReadCount = 0;
                lr.remainMember = 0;
                lr.totMemberCount = 0;
                let n = lr.ReadLength()?;
                for i in 0..n {
                    lr.ReadString()?;
                }
            }
            RdbTypeZSet | RdbTypeZSet2 => {
                lr.lastReadCount = 0;
                lr.remainMember = 0;
                lr.totMemberCount = 0;
                let n = lr.ReadLength()?;
                for i in 0..n {
                    lr.ReadString()?;
                    if t == RdbTypeZSet2 {
                        lr.ReadDouble()?;
                    } else {
                        lr.ReadFloat()?;
                    }
                }
            }
            RdbTypeHash => {
                let mut n = 0;
                if lr.remainMember != 0 {
                    n = lr.remainMember
                } else {
                    let rlen = lr.ReadLength()?;
                    n = rlen;
                    lr.totMemberCount = rlen;
                }
                lr.lastReadCount = 0;
                for i in 0..n {
                    lr.ReadString()?;
                    lr.ReadString()?;
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
                let nListPacks = lr.ReadLength()?;
                // list pack length
                for _ in 0..nListPacks {
                    // read twice
                    lr.ReadString()?;
                    lr.ReadString()?;
                }
                // items
                lr.ReadLength()?;
                // last_entry_id timestamp second
                lr.ReadLength()?;
                // last_entry_id timestamp millisecond
                lr.ReadLength()?;
                // cgroups length
                let nCgroups = lr.ReadLength()?;
                for _ in 0..nCgroups {
                    // cname
                    lr.ReadString()?;
                    // last_cg_entry_id timestamp second
                    lr.ReadLength()?;
                    // last_cg_entry_id timestamp millisecond
                    lr.ReadLength()?;
                    // pending number
                    let nPending = lr.ReadLength()?;
                    for _ in 0..nPending {
                        // eid, read 16 bytes
                        lr.ReadBytes(16)?;
                        // seen_time
                        lr.ReadBytes(8)?;
                        // delivery_count
                        lr.ReadLength()?;
                    }
                    // consumers
                    let nConsumers = lr.ReadLength()?;
                    for _ in 0..nConsumers {
                        // cname
                        lr.ReadString()?;
                        // seen_time
                        lr.ReadBytes(8)?;
                        // pending
                        let nPending2 = lr.ReadLength()?;
                        for _ in 0..nPending2 {
                            lr.ReadBytes(16);
                        }
                    }
                }
            }
            _ => {
                return Err(Box::try_from(format!("unknown object-type {}", t))?);
            }
        };
        Ok(lr.buf.clone())
    }
}
pub fn lzfDecompress(in_data: &Vec<u8>, outlen: usize) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut out: Vec<u8> = vec![0; outlen];
    let (mut i, mut o) = (0, 0);
    while i < in_data.len() {
        let ctrl: i32 = match in_data.get(i) {
            Some(d) => *d,
            None => {
                return Err(Box::try_from("in_data first not exits")?);
            }
        } as i32;
        i = i + 1;
        if ctrl < 32 {
            for _ in 0..=ctrl {
                *match out.get_mut(o) {
                    Some(d) => d,
                    None => {
                        return Err(Box::try_from("will set not exits")?);
                    }
                } = match in_data.get(i) {
                    Some(d) => *d,
                    None => {
                        return Err(Box::try_from("in_data handle not exits")?);
                    }
                };
                i = i + 1;
                o = o + 1;
            }
        } else {
            let mut length = ctrl >> 5;
            if length == 7 {
                length = length
                    + match in_data.get(i) {
                        Some(d) => *d,
                        None => {
                            return Err(Box::try_from("623 not exits")?);
                        }
                    } as i32;
                i = i + 1;
            }
            let mut ref_d = o
                - ((ctrl & 0x1f) << 8) as usize
                - match in_data.get(i) {
                    Some(d) => *d,
                    None => {
                        return Err(Box::try_from("633 not exits")?);
                    }
                } as usize
                - 1;
            i = i + 1;
            for _ in 0..=(length + 1) {
                *match out.get_mut(o) {
                    Some(d) => d,
                    None => {
                        return Err(Box::try_from("642 not exits")?);
                    }
                } = match out.get(ref_d) {
                    Some(d) => *d,
                    None => {
                        return Err(Box::try_from("647 not exits")?);
                    }
                };
                ref_d = ref_d + 1;
                o = o + 1;
            }
        }
    }
    Ok(out)
}

fn rdbLoadCheckModuleValue(l: &mut Loader) -> Result<(), Box<dyn Error>> {
    let opcode: u32 = 0;
    loop {
        let opcode = l.rdbReader.ReadLength()?;
        if opcode == rdbModuleOpcodeEof {
            break;
        }
        match opcode {
            rdbModuleOpcodeSint => {
                l.rdbReader.ReadLength()?;
            }
            rdbModuleOpcodeUint => {
                l.rdbReader.ReadLength()?;
            }
            rdbModuleOpcodeString => {
                l.rdbReader.ReadString()?;
            }
            rdbModuleOpcodeFloat => {
                l.rdbReader.ReadFloat()?;
            }
            rdbModuleOpcodeDouble => {
                l.rdbReader.ReadDouble()?;
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
