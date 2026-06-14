//! `edge:networking` — TCP/UDP/DNS.

pub struct Network;

impl Network {
    pub fn new() -> Self {
        Self
    }

    pub fn resolve(&self, hostname: &str) -> Vec<String> {
        let addr_format = format!("{}:443", hostname);
        match addr_format.parse::<std::net::SocketAddr>() {
            Ok(addr) => vec![addr.ip().to_string()],
            Err(_) => vec![],
        }
    }
}
