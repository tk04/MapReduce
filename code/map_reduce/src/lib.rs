use std::collections::HashMap;

mod cluster;
mod kv_store;
mod mapper;
mod reducer;

pub fn init(num_mappers:i32) -> cluster::Cluster{
    return cluster::Cluster{num_mappers}
}
