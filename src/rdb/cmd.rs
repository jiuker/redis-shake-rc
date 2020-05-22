use byteorder::ReadBytesExt;
use std::convert::AsRef;
use std::error::Error;
use std::io::Write;
use std::net::TcpStream;
use std::ops::Add;

pub fn cmd_to_string(cmd: Vec<&str>) -> String {
    let mut rsl = String::new();
    rsl = rsl.add("*");
    rsl = rsl.add(format!("{}\r\n", cmd.len()).as_ref());
    for cmd_ in cmd {
        rsl = rsl.add(format!("${}", cmd_.len()).as_ref());
        rsl = rsl.add("\r\n");
        rsl = rsl.add(cmd_.as_ref());
        rsl = rsl.add("\r\n");
    }
    return rsl;
}

pub fn cmd_to_resp_first_line(
    conn: &mut TcpStream,
    cmd: Vec<&str>,
) -> Result<String, Box<dyn Error>> {
    conn.write(cmd_to_string(cmd).as_bytes())?;
    let mut resp = String::new();
    loop {
        let ch = conn.read_u8().unwrap() as char;
        if ch == '\r' {
            break;
        }
        if ch == '\n' {
            continue;
        }
        resp.push(ch);
    }
    Ok(resp)
}

pub fn read_line(conn: &mut TcpStream) -> Result<String, Box<dyn Error>> {
    let mut resp = String::new();
    loop {
        let ch = conn.read_u8().unwrap() as char;
        if ch == '\r' {
            break;
        }
        if ch == '\n' {
            continue;
        }
        resp.push(ch);
    }
    Ok(resp)
}
