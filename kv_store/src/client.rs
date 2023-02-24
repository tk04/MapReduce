use std::io::prelude::Write;
use std::io::{self, BufRead};
use std::net::TcpStream;
// test client
pub fn send() -> std::io::Result<()> {
    let mut stream = TcpStream::connect("localhost:6000")?;
    stream.write(b"set newV 0 0 5\r\nhello\r\n")?;
    let res = read_reply(stream);
    assert_eq!(res.0, String::from("STORED\r\n"));
    stream = res.1;

    stream.write(b"get newV\r\n")?;

    let res = read_reply(stream);
    assert_eq!(res.0, String::from("VALUE newV 0 5 \r\nhello\r\nEND\r\n"));

    Ok(())
}
pub fn client2() -> std::io::Result<()> {
    let mut stream = TcpStream::connect("localhost:6000")?;
    stream.write(b"set val2 10 0 5\r\nhello\r\n")?;
    let res = read_reply(stream);
    assert_eq!(res.0, String::from("STORED\r\n"));
    stream = res.1;

    stream.write(b"get val2\r\n")?;

    let res = read_reply(stream);
    assert_eq!(res.0, String::from("VALUE val2 10 5 \r\nhello\r\nEND\r\n"));

    stream = res.1;
    stream.write(b"get newV\r\n")?;

    let res = read_reply(stream);
    assert_eq!(res.0, String::from("VALUE newV 0 5 \r\nhello\r\nEND\r\n"));
    Ok(())
}

fn read_reply(stream: TcpStream) -> (String, TcpStream) {
    let mut reader = io::BufReader::new(stream);

    let rec: Vec<u8> = reader.fill_buf().unwrap().to_vec();
    reader.consume(rec.len());
    let n_stream = reader.into_inner();
    return (String::from_utf8(rec).unwrap(), n_stream);
}
