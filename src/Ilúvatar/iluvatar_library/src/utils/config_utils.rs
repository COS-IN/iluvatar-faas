/// Converts the vector into a json dictionary of arguments
/// Each item in the list should be an argument pair in the form key=value
pub fn args_to_json(args: &Vec<String>) -> anyhow::Result<String> {
    let mut ret = String::from("{");
    for arg in args {
        // Use splitn(2, '=') so we only split once
        let split: Vec<&str> = arg.splitn(2, '=').collect();

        // If there's no '=' at all, bail out
        if split.len() != 2 {
            anyhow::bail!(
                "Function argument '{}' does not contain an '=' or has empty key/value",
                arg
            );
        }

        let key = split[0];
        let val = split[1];

        // Format as "key":"value"
        let fmt = format!("\"{}\":\"{}\"", key, val);

        // If this isn’t the first key-value pair, add a comma
        if ret.len() > 1 {
            ret.push(',');
        }
        ret.push_str(fmt.as_str());
    }
    ret.push('}');
    Ok(ret)
}
