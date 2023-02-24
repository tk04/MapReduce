// mod kv_store;
use crate::kv_store::{format_write, read_reply};

use std::net::TcpStream;
use std::{fmt::Display, io::Write};

// key is num of bytes in text
pub fn run_mapper<K: Display, V: Display>(
    map_fn: fn(i32, String) -> Vec<(K, V)>,
    key: i32,
    input: String,
    kv_store_address: String,
    return_addr: String,
) {
    println!("mapper {key} RUNNING");
    // panic!("map panic!");
    let map_output = map_fn(key, input); // vec

    let mut stream = TcpStream::connect(kv_store_address).expect("cannot bind socket");

    let mut send = String::from("");
    for (k, v) in map_output.iter() {
        if k.to_string() != "" {
            send.push_str(&format!("{},", k));
            stream
                .write(format_write(k, v, true).as_bytes())
                .expect("cannot write to kv_store");
            let mut res = read_reply(&mut stream);
            let mut res_counter = 0;
            while res != String::from("STORED\r\n") && res_counter < 3 {
                stream
                    .write(format_write(k, v, false).as_bytes())
                    .expect("cannot write to kv_store");
                res = read_reply(&mut stream);
                res_counter += 1;
            }
        }
    }
    println!("mapper {key} finished");
    let context = zmq::Context::new();
    let requester = context.socket(zmq::REQ).unwrap();

    assert!(requester.connect(&return_addr).is_ok());
    requester.send(&send, 0).unwrap();
}
