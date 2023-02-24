use super::COMMANDS;
use std::result::Result;
use std::str::Split;

#[derive(Debug, Clone)]
pub struct Command {
    pub name: CommandType,
    pub values: Vec<String>,
    pub reply: bool, // flags: HashMap<Flags, String>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandType {
    Set,
    Get,
    Add,
    Replace,
    Append,
    Prepend,
    Delete,
    FlushAll,
}
#[derive(Debug)]
pub enum Response {
    Stored,
    NotStored,
    Error,
    Deleted,
    NotFound,
    ClientError(String),
    Ok,
}
impl Response {
    pub fn to_string(&self) -> String {
        match self {
            Response::Stored => "STORED\r\n".to_string(),
            Response::NotStored => "NOT_STORED\r\n".to_string(),
            Response::Error => "ERROR\r\n".to_string(),
            Response::NotFound => "NOT_FOUND\r\n".to_string(),
            Response::Deleted => "DELETED\r\n".to_string(),
            Response::Ok => "OK\r\n".to_string(),
            Response::ClientError(val) => format!("CLIENT_ERROR {val}\r\n"),
        }
    }
}

fn try_parse(
    cmd_type: CommandType,
    cmd: &mut Split<&str>,
    val: &mut Split<&str>,
) -> Result<Command, Response> {
    let mut v: Vec<String> = Vec::new();

    for i in cmd {
        v.push(i.to_string());
    }
    let mut reply = true;
    if v.len() > 0 && v[v.len() - 1] == "noreply" {
        reply = false;
        v.pop();
    }
    for i in val {
        v.push(i.to_string());
    }
    match cmd_type {
        CommandType::FlushAll => Ok(Command {
            name: cmd_type,
            values: v,
            reply,
        }),
        _ => {
            if v.len() > 0 {
                return Ok(Command {
                    name: cmd_type,
                    values: v,
                    reply,
                });
            }

            return Err(Response::ClientError("Unsupported Syntax".to_string()));
        }
    }
}

pub fn parse_cmd(cmd: &str) -> Result<Command, Response> {
    let mut command = cmd.split("\r\n");
    let mut proto = command.next().unwrap().split(" ");

    match proto.next() {
        Some(val) => match COMMANDS.get(val) {
            Some(t) => try_parse(t.clone(), &mut proto, &mut command),
            None => Err(Response::Error),
        },
        None => Err(Response::ClientError("Syntax Error".to_string())),
    }
}
