use crate::kv_store::{format_get, map_kv, read_reply};
use nix::sys::wait::{waitpid, WaitPidFlag, WaitStatus};
use nix::unistd::{fork, ForkResult, Pid};

use std::collections::HashMap;
use std::fmt::Display;
use std::io::{BufRead, Seek, SeekFrom};
use std::net::TcpStream;
use std::ops::Div;
use std::{fs::File, io::BufReader, io::Read};
use std::{panic, thread};
use zmq;

use serde_json::Value;

use crate::{mapper, reducer};
use std::io::Write;

fn load_config() -> Value {
    let mut buf: String = String::new();
    let mut file = File::options()
        .read(true)
        .open("config.json")
        .expect("error while opening config file");
    file.read_to_string(&mut buf)
        .expect("error while reading config file");

    let v: Value = serde_json::from_str(&buf).unwrap();
    return v;
}
fn getVar(config: &Value, key: &str) -> String {
    let value = config[key].to_string();
    return String::from(&value[1..value.len() - 1]);
}

pub struct Cluster {
    pub num_mappers: i32,
}

impl Cluster {
    pub fn new(num_mappers: i32) -> Self {
        return Cluster { num_mappers };
    }
    pub fn run_mapred<K: 'static + Display, V: 'static + Display>(
        self,
        input_file: String,
        map_fn: fn(i32, String) -> Vec<(K, V)>,
        reduce_fn: fn(String, Vec<String>) -> String,
        output_location: String,
    ) {
        // open file and split file between each mapper ? by Lines
        let mut file = File::open(&input_file).expect("error while opening input file");
        let ve = self.partition_data(&input_file);

        file.seek(SeekFrom::Start(0)).unwrap();

        let num_mappers = ve.len();
        thread::spawn(move || run_mappers(ve, map_fn));
        let s = map_barrier(num_mappers as i32);
        println!("ALL KEYS: {:?}", s);
        // let success = panic::catch_unwind(|| run_m.join().unwrap());

        let CONFIG: Value = load_config();

        let kv_addr = getVar(&CONFIG, "KV_STORE_ADDR");
        let mut stream = TcpStream::connect(kv_addr).expect("cannot bind socket");
        let mut red_input: Vec<(String, Vec<String>)> = Vec::new();
        for item in s.keys() {
            stream
                .write(format_get(item.to_string()).as_bytes())
                .unwrap();
            let reply = read_reply(&mut stream);
            let items = map_kv(item.to_string(), reply);
            if items.0 != "" {
                red_input.push(items);
            }
        }
        let first = red_input.get(0).unwrap().clone();

        let num_red = red_input.len();
        // println!("REDUCE INPUT: {:?}", red_input);
        thread::spawn(move || run_reducers(reduce_fn, red_input));
        self.cap_reducers(output_location, num_red as i32);
        // flush all values in kv_store
        stream.write("flush_all\r\n".as_bytes());
    }

    fn cap_reducers(self, output_file: String, num_red: i32) {
        let mut file = File::options()
            .read(true)
            .create(true)
            .write(true)
            .open(output_file)
            .expect("error while opening output file");

        let context = zmq::Context::new();
        let responder = context.socket(zmq::REP).unwrap();
        assert!(responder.bind("tcp://*:5555").is_ok());
        let mut msg = zmq::Message::new();
        let mut red_returned = 0;
        println!("waiting for {num_red} reducers to return");
        while red_returned < num_red {
            responder.recv(&mut msg, 0).unwrap();
            let message = msg.as_str().unwrap();
            file.write(format!("{}\n", message).as_bytes())
                .expect("error while writing to output file");

            println!("Received: {}", message);
            responder.send("", 0).unwrap();
            red_returned += 1;
        }
        println!("ALL REDUCERS RETURNED!");
    }
    fn partition_data(&self, file_name: &str) -> Vec<String> {
        let mut file = File::open(file_name).expect("error while opening config file");
        let buf = BufReader::new(&mut file);

        let mut line_count = 0;

        for _ in buf.lines() {
            line_count += 1;
        }

        let each_mappers = (line_count as f32).div(self.num_mappers as f32);
        let div_mappers = each_mappers.ceil() as i32;

        file.seek(SeekFrom::Start(0)).unwrap();
        let mut parts: Vec<String> = Vec::new();
        let mut string_values = String::new();

        let buf2 = BufReader::new(file);
        line_count = 0;
        for line in buf2.lines() {
            if line_count == div_mappers {
                parts.push(string_values);
                string_values = String::new();
                line_count = 0;
            }
            let val = line.unwrap();
            if val != "" {
                string_values.push_str(&val);
                string_values.push('\n');
                line_count += 1;
            }
        }
        if string_values.len() > 0 {
            parts.push(string_values);
        }
        return parts;
    }
}
fn map_barrier(num_mappers: i32) -> HashMap<String, bool> {
    let context = zmq::Context::new();
    let responder = context.socket(zmq::REP).unwrap();
    assert!(responder.bind("tcp://*:5555").is_ok());
    let mut msg = zmq::Message::new();
    let mut mappers_returned = 0;
    println!("waiting for {num_mappers} mappers to return");
    let mut all_keys: HashMap<String, bool> = HashMap::new();
    while mappers_returned < num_mappers {
        responder.recv(&mut msg, 0).unwrap();
        let message = msg.as_str().unwrap();
        for i in message.split(",") {
            if i != "" {
                all_keys.insert(i.to_string(), true);
            }
        }

        println!("Received: {}", message);
        responder.send("", 0).unwrap();
        mappers_returned += 1;
    }
    println!("ALL MAPPERS RETURNED!");
    return all_keys;
}

fn run_mappers<K: 'static + Display, V: 'static + Display>(
    values: Vec<String>,
    map_fn: fn(i32, String) -> Vec<(K, V)>,
) {
    let CONFIG: Value = load_config();
    let mut starting_port = CONFIG["WORKER_START_PORT"]
        .to_string()
        .parse::<i32>()
        .unwrap();

    let kv_addr = getVar(&CONFIG, "KV_STORE_ADDR");
    let return_addr = getVar(&CONFIG, "MAIN_THREAD_ADDR");
    let mut cpids: Vec<Pid> = Vec::new();

    let mut p_count = 0;
    for item in values.iter() {
        spawn_mapper(
            p_count,
            item.to_string(),
            kv_addr.clone(),
            return_addr.clone(),
            map_fn,
            &mut cpids,
        );
        p_count += 1;
    }
    // for child_pid in cpids {
    //     match waitpid(child_pid, Some(WaitPidFlag::WUNTRACED)) {
    //         Ok(WaitStatus::Exited(_, code)) => {
    //             println!("Child process exited with status {}", code);
    //         }
    //         Ok(WaitStatus::Signaled(_, signal, _)) => {
    //             println!("Child process terminated with signal {}", signal);
    //         }
    //         Ok(_) => {
    //             // Should not happen, as we only set WUNTRACED flag
    //             unreachable!();
    //         }
    //         Err(err) => {
    //             println!("Error waiting for child process: {}", err);
    //         }
    //     }
    // }
}
fn spawn_mapper<K: Display + 'static, V: Display + 'static>(
    key: i32,
    value: String,
    kv_addr: String,
    return_addr: String,
    map_fn: fn(i32, String) -> Vec<(K, V)>,
    cpids: &mut Vec<Pid>,
) {
    match fork() {
        Err(_) => {
            eprintln!("Failed to fork process!");
            std::process::exit(1);
        }
        Ok(ForkResult::Child) => {
            // child process
            //
            mapper::run_mapper(map_fn, key, value, kv_addr.clone(), return_addr.clone());
            std::process::exit(0);
        }
        Ok(ForkResult::Parent { child }) => cpids.push(child),
    }
}

fn run_reducers(reduce_fn: fn(String, Vec<String>) -> String, values: Vec<(String, Vec<String>)>) {
    let CONFIG: Value = load_config();
    let kv_addr = getVar(&CONFIG, "KV_STORE_ADDR");
    let return_addr = getVar(&CONFIG, "MAIN_THREAD_ADDR");
    for (key, value) in values.iter() {
        spawn_reducer(
            (key.to_string(), value.to_vec()),
            kv_addr.clone(),
            return_addr.clone(),
            reduce_fn,
        );
    }
}

fn spawn_reducer(
    values: (String, Vec<String>),
    kv_addr: String,
    return_addr: String,
    reduce_fn: fn(String, Vec<String>) -> String,
) {
    match unsafe { libc::fork() } {
        -1 => {
            eprintln!("Failed to fork process!");
            std::process::exit(1);
        }
        0 => {
            reducer::run_reducer(reduce_fn, values, kv_addr, return_addr);
            std::process::exit(0);
        }
        _ => (),
    }
}
