use guid_create::GUID;

pub type TransactionId = String;

pub fn gen_tid() -> TransactionId {
  GUID::rand().to_string().to_lowercase().replace("-", "to")
}
