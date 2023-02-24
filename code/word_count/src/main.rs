use map_reduce;

use std::{collections::HashMap, fs::File, io::BufReader, io::Read};

fn main() {
    let s = map_reduce::init(2);
    s.run_mapred(
        "test.txt".to_string(),
        map,
        reduce,
        "word_count.txt".to_string(),
    );
    // map reduce finished
    let mut file = File::open("word_count.txt").unwrap();
    let mut buf = BufReader::new(&mut file);
    let mut values = String::new();
    buf.read_to_string(&mut values).unwrap();
    let output = parse_output(values);

    // check for correct word count values
    assert_eq!(output.get("the").unwrap().to_string(), String::from("4"));
    assert_eq!(output.get("what").unwrap().to_string(), String::from("1"));
    assert_eq!(output.get("work").unwrap().to_string(), String::from("2"));
    assert_eq!(output.get("of").unwrap().to_string(), String::from("4"));
}

fn map(key: i32, value: String) -> Vec<(String, i32)> {
    let mut v = vec![];
    println!("map number: {key}");
    let values = value.split("\n");
    for i in values {
        for word in i.split(" ") {
            v.push((String::from(word), 1));
        }
    }
    return v;
}

fn reduce(key: String, value: Vec<String>) -> String {
    let mut total = 0;
    for i in value.iter() {
        total += i.parse::<i32>().unwrap();
    }
    return total.to_string();
}
fn parse_output(values: String) -> HashMap<String, String> {
    let mut res = HashMap::new();
    for line in values.lines() {
        let mut vv = line.split(" ");
        vv.next();
        let key = vv.next().unwrap().split(",").next().unwrap();
        vv.next();
        let value = vv.next().unwrap();
        res.insert(key.to_string(), value.to_string());
    }
    return res;
}
