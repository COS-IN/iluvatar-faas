use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[allow(unused, non_snake_case)]
pub struct Namespace {
    pub name: String,
    pub namespace: ContdNamespace,
}

#[derive(Debug, Deserialize)]
#[allow(unused, non_snake_case)]
pub struct ContdNamespace {
    pub cniVersion: String,
    pub interfaces: Vec<Interface>,
    pub ips: Vec<IP>,
    pub routes: Vec<Route>,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct Interface {
    pub name: String,
    pub mac: String,
    pub sandbox: Option<String>,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct IP {
    pub version: String,
    pub interface: u32,
    pub address: String,
    pub gateway: String,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct Route {
    pub dst: String,
    pub gw: String,
}
