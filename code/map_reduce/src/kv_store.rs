use std::net::TcpStream;

use std::io::{self, BufRead};

use std::fmt::Display;
pub fn read_reply(stream: &mut TcpStream) -> String {
    let mut reader = io::BufReader::new(stream);

    let rec: Vec<u8> = reader.fill_buf().unwrap().to_vec();
    reader.consume(rec.len());
    return String::from_utf8(rec).unwrap();
}

pub fn format_write<K: Display, V: Display>(key: K, value: V, add: bool) -> String {
    if add {
        return format!(
            "add {key} 0 0 {}\r\n{}",
            value.to_string().as_bytes().len(),
            value
        );
    } else {
        return format!(
            "append {key} 0 0 {}\r\n,{}",
            value.to_string().as_bytes().len() + 1,
            value
        );
    }
}
pub fn format_get(key: String) -> String {
    return format!("get {key}\r\n");
}

fn extract_value(value: String) -> String {
    let mut string = String::new();
    let mut lines = value.split("\r\n");
    lines.next();
    for line in lines {
        if line != "END" {
            string.push_str(line);
        } else {
            return string;
        }
    }
    return string;
}
pub fn map_kv(key: String, value: String) -> (String, Vec<String>) {
    let r_value = extract_value(value);
    let mut vs: Vec<String> = Vec::new();
    let s = r_value.split(",");
    for v in s {
        vs.push(v.to_string());
    }
    return (key, vs);
}
