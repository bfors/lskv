use clap::{arg, Command};
use kvs::KvStore;

fn cli() -> Command {
    Command::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .bin_name("kvs")
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .subcommand(
            Command::new("set")
                .arg(arg!(key: [KEY]).required(true))
                .arg(arg!(value: [VALUE]).required(true))
                .arg_required_else_help(true),
        )
        .subcommand(Command::new("get").arg(arg!(key: [KEY])))
        .subcommand(Command::new("rm").arg(arg!(key: [KEY])))
        .arg_required_else_help(true)
}

fn main() -> Result<()> {
    let matches = cli().get_matches();
    let mut path = std::path::PathBuf::new();
    path.push("db.bfors");
    let mut kv = KvStore::new(path);

    match matches.subcommand() {
        Some(("set", sub_matches)) => {
            let key = sub_matches
                .get_one::<String>("key")
                .map(|s| s.as_str())
                .unwrap();
            let value = sub_matches
                .get_one::<String>("value")
                .map(|s| s.as_str())
                .unwrap();

            kv.set(key.to_string(), value.to_string())?;
        }

        Some(("get", sub_matches)) => {
            let key = sub_matches.get_one::<String>("key").map(|s| s.as_str());

            let result = kv.get(key.unwrap().to_string());
            if let Ok(value) = result {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        Some(("rm", sub_matches)) => {
            let key = sub_matches.get_one::<String>("key").map(|s| s.as_str());
            kv.remove(key.unwrap().to_string())?;
        }
        Some(("version", _sub_matches)) => {
            println!("{}", env!("CARGO_PKG_VERSION"));
        }
        _ => unreachable!(),
    }
    Ok(())
}
