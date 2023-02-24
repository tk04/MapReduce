use std::fmt::Display;

pub fn run_reducer(
    reduce_fn: fn(String, Vec<String>) -> String,
    input: (String, Vec<String>),
    kv_store_address: String,
    return_addr: String,
) {
    let key = input.0.clone();
    let output = reduce_fn(input.0, input.1);

    let context = zmq::Context::new();
    let requester = context.socket(zmq::REQ).unwrap();

    assert!(requester.connect(&return_addr).is_ok());
    println!("reducer returning");
    let response = format!("KEY: {}, VALUE: {}", key, output);
    requester.send(&response, 0).unwrap();
}
