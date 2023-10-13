use std::error::Error;

#[derive(Debug)]
pub struct MissingAsyncCookieError {}
impl std::fmt::Display for MissingAsyncCookieError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "Was unable to locate async cookie")?;
        Ok(())
    }
}
impl Error for MissingAsyncCookieError {}
