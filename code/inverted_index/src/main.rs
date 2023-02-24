use map_reduce;

use std::{collections::HashMap, fs::File, io::BufReader, io::Read};

fn main() {
    let s = map_reduce::init(3);
    s.run_mapred(
        "test.txt".to_string(),
        map,
        reduce,
        "i_index.txt".to_string(),
    );

    let mut file = File::open("i_index.txt").unwrap();
    let mut buf = BufReader::new(&mut file);
    let mut values = String::new();
    buf.read_to_string(&mut values).unwrap();
    let output = parse_output(values);

    //check for correct word count values
    assert_eq!(
        output.get("of").unwrap().to_string(),
        String::from("0, 1, ")
    );

    assert_eq!(
        output.get("Cipher").unwrap().to_string(),
        String::from("0, ")
    );

    assert_eq!(output.get("his").unwrap().to_string(), String::from("1, "));
    assert_eq!(
        output.get("and").unwrap().to_string(),
        String::from("1, 2, ")
    );
}

fn map(key: i32, value: String) -> Vec<(String, i32)> {
    let mut v = vec![];
    let mut word_hash: HashMap<String, i32> = HashMap::new();
    let values = value.split("\n");
    for i in values {
        for word in i.split(" ") {
            if !word_hash.contains_key(word) {
                v.push((String::from(word), key));
                word_hash.insert(word.to_string(), key);
            }
        }
    }
    return v;
}

fn reduce(key: String, value: Vec<String>) -> String {
    let mut vals = String::new();
    for i in value.iter() {
        vals.push_str(&format!("{}, ", i));
    }
    return vals;
}

fn parse_output(values: String) -> HashMap<String, String> {
    let mut res = HashMap::new();
    for line in values.lines() {
        let mut values = line.clone().split("VALUE: ");
        let mut vv = line.split(" ");
        vv.next();
        let key = vv.next().unwrap().split(",").next().unwrap();
        vv.next();
        values.next();
        let value = values.next().unwrap();
        res.insert(key.to_string(), value.to_string());
    }
    return res;
}
