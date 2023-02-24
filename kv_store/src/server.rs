use super::COMMANDS;
use crate::command_parser::{parse_cmd, Command, CommandType, Response};
use crate::data_store::Value;
use crate::DATA_STORE;
use rand::Rng;
use std::io::{self, BufRead, Write};
use std::net::{TcpListener, TcpStream};
use std::str::FromStr;
use std::thread;
use std::time::{Duration, SystemTime};

pub fn listen() {
    let listener = TcpListener::bind("127.0.0.1:6000").unwrap();

    for stream in listener.incoming() {
        // thread::spawn(|| handler(&mut stream.unwrap()));
        handler(&mut stream.unwrap());
    }
}

fn match_cmd(cmd: &Command) -> String {
    match cmd.name {
        CommandType::Set => set_res(cmd),
        CommandType::Get => get_res(cmd),
        CommandType::Add => add_res(cmd),
        CommandType::Prepend => prepend_res(cmd),
        CommandType::Append => append_res(cmd),
        CommandType::Delete => delete_res(cmd),
        CommandType::Replace => replace_res(cmd),
        CommandType::FlushAll => flush_all_res(),
    }
}

fn split_commands(st: &str) -> Vec<String> {
    let mut v: Vec<String> = Vec::new();
    let mut command = st.split("\r\n");
    let mut nxt = command.next();
    while nxt.is_some() {
        let mut proto = nxt.unwrap().trim().split(" ");
        match proto.next() {
            Some(val) => match COMMANDS.get(val).copied() {
                Some(t) => {
                    let mut s: String = nxt.unwrap().to_string() + "\r\n";
                    if t != CommandType::Get
                        && t != CommandType::Delete
                        && t != CommandType::FlushAll
                    {
                        let val = command.next().unwrap().to_string() + "\r\n";
                        s += &val;
                    }
                    v.push(s);
                }
                None => {
                    let string = nxt.unwrap();
                    if string.len() > 0 && string.as_bytes()[0] != 10 {
                        v.push(nxt.unwrap().to_string())
                    }
                }
            },
            None => (),
        }
        nxt = command.next();
    }
    return v;
}
fn handler(stream: &mut TcpStream) {
    let mut reader = io::BufReader::new(stream);
    loop {
        let rec: Vec<u8> = reader.fill_buf().unwrap().to_vec();
        reader.consume(rec.len());
        let st = String::from_utf8(rec.clone()).unwrap();
        let cmds = split_commands(&st);
        if rec.len() == 0 {
            break;
        }
        let ss = reader.into_inner();

        for i in cmds.iter() {
            let mut rng = rand::thread_rng();
            // thread::sleep(Duration::from_millis(rng.gen_range(0..999)));
            let typed = parse_cmd(&i);
            match typed {
                Ok(val) => {
                    let response = match_cmd(&val);
                    if val.reply {
                        ss.write(response.as_bytes()).expect("socket error");
                        ss.flush().expect("error sending message");
                    }
                }
                Err(_) => {
                    ss.write(Response::Error.to_string().as_bytes())
                        .expect("socket error");
                    ss.flush().expect("error sending message");
                }
            }
        }

        reader = io::BufReader::new(ss);
    }
}

fn valid_value(cmd: &Command) -> Result<Value, Response> {
    if cmd.values.len() < 5 {
        return Err(Response::Error);
    }
    let flags = u32::from_str(cmd.values[1].as_str());
    let exp = u64::from_str(cmd.values[2].as_str());
    let bytes = usize::from_str(cmd.values[3].as_str());
    match bytes {
        Ok(val) => {
            if cmd.values[4].as_bytes().len() != val || cmd.values[0].as_bytes().len() > 250 {
                return Err(Response::ClientError("Wrong Bytes".to_string()));
            }
            match (flags, exp) {
                (Ok(val1), Ok(val2)) => {
                    let mut sec = 0;
                    if val2 != 0 {
                        sec = SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .unwrap()
                            .as_secs()
                            + val2;
                    }
                    return Ok(Value {
                        value: cmd.values[4].to_string(),
                        exp: sec,
                        flags: val1,
                    });
                }
                _ => return Err(Response::ClientError("Unsupported Syntax".to_string())),
            }
        }
        Err(_) => return Err(Response::ClientError("Unsupported Syntax".to_string())),
    }
}
fn set_res(cmd: &Command) -> String {
    match valid_value(&cmd) {
        Ok(val) => {
            let mut ds = DATA_STORE.lock().unwrap();
            ds.set_key(&cmd.values[0], val);
            return Response::Stored.to_string();
        }
        Err(error) => return error.to_string(),
    }
}

fn get_res(cmd: &Command) -> String {
    if cmd.values.len() == 0 {
        return Response::Error.to_string();
    }

    let mut ds = DATA_STORE.lock().unwrap();
    match ds.get_key(cmd.values[0].to_string()) {
        Some(val) => {
            return format!(
                "VALUE {} {} {} \r\n{}\r\nEND\r\n",
                cmd.values[0],
                val.flags,
                val.value.as_bytes().len(),
                val.value
            );
        }
        None => Response::NotFound.to_string(),
    }
}
fn append_res(cmd: &Command) -> String {
    match valid_value(&cmd) {
        Ok(val) => {
            let mut ds = DATA_STORE.lock().unwrap();
            if ds.append_key(&cmd.values[0], val) {
                return Response::Stored.to_string();
            }
            return Response::NotStored.to_string();
        }
        Err(error) => return error.to_string(),
    }
}
fn prepend_res(cmd: &Command) -> String {
    match valid_value(&cmd) {
        Ok(val) => {
            let mut ds = DATA_STORE.lock().unwrap();
            if ds.prepend_key(&cmd.values[0], val) {
                return Response::Stored.to_string();
            }
            return Response::NotStored.to_string();
        }
        Err(error) => return error.to_string(),
    }
}

fn add_res(cmd: &Command) -> String {
    match valid_value(&cmd) {
        Ok(val) => {
            let mut ds = DATA_STORE.lock().unwrap();

            if !ds.has_key(&cmd.values[0]) {
                ds.set_key(&cmd.values[0], val);
                return Response::Stored.to_string();
            }
            return Response::NotStored.to_string();
        }
        Err(error) => return error.to_string(),
    }
}
fn delete_res(cmd: &Command) -> String {
    if cmd.values.len() == 0 {
        return Response::Error.to_string();
    }
    let mut ds = DATA_STORE.lock().unwrap();
    if ds.delete_key(&cmd.values[0]) {
        return Response::Deleted.to_string();
    }
    return Response::NotFound.to_string();
}
fn replace_res(cmd: &Command) -> String {
    match valid_value(&cmd) {
        Ok(val) => {
            let mut ds = DATA_STORE.lock().unwrap();
            if ds.has_key(&cmd.values[0]) {
                ds.set_key(&cmd.values[0], val);
                return Response::Stored.to_string();
            }
            return Response::NotStored.to_string();
        }
        Err(error) => return error.to_string(),
    }
}

fn flush_all_res() -> String {
    let mut ds = DATA_STORE.lock().unwrap();
    ds.delete_all();
    return Response::Ok.to_string();
}
