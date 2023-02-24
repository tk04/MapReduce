use std::{
    collections::HashMap,
    fs::File,
    io::{self, BufRead, Seek, SeekFrom, Write},
    str::FromStr,
    time::{Duration, SystemTime},
};

#[derive(Clone, Debug)]
pub struct Value {
    pub value: String,
    pub exp: u64,
    pub flags: u32,
}
pub struct DataStore {
    file: File,
    store: HashMap<String, Value>,
}

fn parse_value(val: &str) -> Option<Value> {
    // key [SPACE] flags [SPACE] exp [SPACE] value
    if let Some(idx) = val.find(" ") {
        match u32::from_str(&val[0..idx]) {
            Ok(num) => {
                let second_idx = val[idx + 1..].find(" ").expect("PARSING ERROR") + idx;
                match u64::from_str(&val[idx + 1..second_idx + 1]) {
                    Ok(int) => {
                        let mut exp: u64 = 0;
                        if int != 0 {
                            exp = SystemTime::now()
                                .duration_since(SystemTime::UNIX_EPOCH)
                                .unwrap()
                                .as_secs()
                                + Duration::from_secs(exp).as_secs()
                        }
                        return Some(Value {
                            value: val[second_idx + 1..].to_string(),
                            exp,
                            flags: num,
                        });
                    }
                    Err(_) => return None,
                }
            }
            Err(_) => None,
        }
    } else {
        None
    }
}
fn populate_hash(file: &mut File) -> HashMap<String, Value> {
    let mut hash: HashMap<String, Value> = HashMap::new();
    let lines = io::BufReader::new(file).lines();
    for line in lines {
        let kv = line.unwrap();
        if let Some(idx) = kv.find(" ") {
            let val = parse_value(&kv[idx + 1..]);
            match val {
                Some(item) => {
                    hash.insert(kv[0..idx].to_string(), item);
                }
                None => (),
            }
        } else {
            // delete key
            match kv.find("-") {
                Some(val) => {
                    hash.remove(&kv[val + 1..]);
                }
                None => (),
            }
        }
    }

    return hash;
}
impl DataStore {
    pub fn new() -> Self {
        let mut file = File::options()
            .create(true)
            .read(true)
            .write(true)
            .open("data_store.txt")
            .expect("error while opening data_store.txt");
        let store = populate_hash(&mut file);
        file.set_len(0).expect("error while handling file");

        file.seek(SeekFrom::End(0))
            .expect("error while handling file");

        for (key, value) in store.iter() {
            file.write(
                format!("{} {} {} {}\n", key, value.flags, value.exp, value.value).as_bytes(),
            )
            .expect("error while writing to file");
        }
        return Self {
            file: file.try_clone().unwrap(),
            store,
        };
    }
    pub fn has_key(&self, key: &str) -> bool {
        match self.store.get(key) {
            Some(_) => true,
            _ => false,
        }
    }
    pub fn get_key(&mut self, key: String) -> Option<Value> {
        match self.store.get(&key) {
            Some(val) => {
                if SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    > val.exp
                    && val.exp != 0
                {
                    //delete key
                    self.delete_key(&key);
                    return None;
                } else {
                    return Some(val.clone());
                }
            }
            None => None,
        }
    }

    pub fn set_key(&mut self, key: &str, value: Value) -> bool {
        self.store.insert(key.to_string(), value.clone());
        self.file
            .write(format!("{} {} {} {}\n", key, value.flags, value.exp, value.value).as_bytes())
            .expect("error while writing to file");
        return true;
    }
    pub fn append_key(&mut self, key: &str, value: Value) -> bool {
        match self.store.get(key) {
            Some(val) => {
                let new_val = val.value.to_string() + &value.value.to_string();
                self.store.insert(
                    key.to_string(),
                    Value {
                        value: new_val.clone(),
                        exp: value.exp,
                        flags: value.flags,
                    },
                );
                self.file
                    .write(
                        format!("{} {} {} {}\n", key, value.flags, value.exp, new_val).as_bytes(),
                    )
                    .expect("error while writing to file");
                return true;
            }
            None => false,
        }
    }
    pub fn prepend_key(&mut self, key: &str, value: Value) -> bool {
        match self.store.get(key) {
            Some(val) => {
                let new_val = value.value.to_string() + &val.value.to_string();
                self.store.insert(
                    key.to_string(),
                    Value {
                        value: new_val.clone(),
                        exp: value.exp,
                        flags: value.flags,
                    },
                );
                self.file
                    .write(
                        format!("{} {} {} {}\n", key, value.flags, value.exp, new_val).as_bytes(),
                    )
                    .expect("error while writing to file");
                return true;
            }
            None => false,
        }
    }
    pub fn delete_key(&mut self, key: &str) -> bool {
        match self.store.get(key) {
            Some(_) => {
                self.store.remove(key);
                self.file
                    .write(format!("delete-{}\n", key).as_bytes())
                    .expect("error while writing to file");
                return true;
            }
            None => false,
        }
    }
    pub fn delete_all(&mut self) -> bool {
        self.store.clear();
        self.file.set_len(0).expect("error while handling file");
        self.file
            .seek(SeekFrom::Start(0))
            .expect("error while handling file");

        return true;
    }
}
