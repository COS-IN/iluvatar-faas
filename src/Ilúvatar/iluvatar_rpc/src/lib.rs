use tonic::Status;

pub mod rpc;

#[derive(Debug)]
pub struct RPCError {
    pub message: Status,
    pub source: String,
}
impl RPCError {
    pub fn new(message: Status, source: String) -> Self {
        RPCError { message, source }
    }
}
impl std::fmt::Display for RPCError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{} RPC connection failed because: {:?}", self.source, self.message)?;
        Ok(())
    }
}
impl std::error::Error for RPCError {}
