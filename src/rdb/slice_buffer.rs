
use std::error::Error;


pub struct sliceBuffer {
    pub(crate) s: Vec<u8>,
    pub(crate) i: i32,
}

impl sliceBuffer {
    pub fn new(s: Vec<u8>) -> Self {
        sliceBuffer { s, i: 0 }
    }
    pub fn Slice(&mut self, n: i32) -> Result<Vec<u8>, Box<dyn Error>> {
        if (self.i + n) > self.s.len() as i32 {
            return Err(Box::from("io error"));
        };
        let mut index = self.i as usize;
        let mut rsl = vec![];
        while index < (self.i + n) as usize {
            rsl.push(*self.s.get(index).unwrap());
            index = index + 1;
        }
        self.i = self.i + n;
        Ok(rsl)
    }
    pub fn ReadByte(&mut self) -> Result<u8, Box<dyn Error>> {
        if self.i >= self.s.len() as i32 {
            return Err(Box::from("io error"));
        };
        let rsl = *self.s.get(self.i as usize).unwrap();
        self.i = self.i + 1;
        Ok(rsl)
    }
    pub fn Read(&mut self, p: &mut Vec<u8>) -> Result<usize, Box<dyn Error>> {
        if p.len() == 0 {
            return Err(Box::from("nil read"));
        }
        if self.i >= self.s.len() as i32 {
            return Err(Box::from("io error"));
        };
        let mut index = 0usize;
        while index < p.len() {
            let p = p.get_mut(index).unwrap();
            *p = *self.s.get(index + self.i as usize).unwrap();
            index = index + 1;
        }
        Ok(index)
    }
    pub fn Seek(&mut self, offset: i64, whence: i32) -> Result<i64, Box<dyn Error>> {
        let mut abs = 0;
        match whence {
            0 => {
                abs = offset;
            }
            1 => {
                abs = (self.i + offset as i32) as i64;
            }
            2 => abs = (self.s.len() + offset as usize) as i64,
            _ => {
                return Err(Box::from("invalid whence"));
            }
        };
        if abs < 0 {
            return Err(Box::from("negative position"));
        }
        if abs >= 1 << 31 {
            return Err(Box::from("position out of range"));
        }
        self.i = abs as i32;
        Ok(abs)
    }
}
