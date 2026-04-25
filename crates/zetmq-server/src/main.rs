mod config;
mod error;
mod network;
mod runtime;
mod session;
mod shutdown;

use config::ServerConfig;

fn main() {
    let config = ServerConfig::default();
    println!("ZetMQ Server - binding to {}", config.addr());
}
