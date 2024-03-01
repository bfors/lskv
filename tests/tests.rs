use kvs::*;
use tempfile::TempDir;

fn create_tmp() -> Kvs {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut path = temp_dir.path().to_path_buf();
    path.push("testdb".to_owned());
    Kvs::open(path, None)
}

#[test]
fn set_get() -> Result<()> {
    let mut kvs = create_tmp();
    let exp = "value1";
    kvs.set("key1".to_owned(), exp.to_owned())?;
    let actual = kvs.get(&"key1".to_owned()).unwrap();
    assert_eq!(exp, actual);

    Ok(())
}

#[test]
fn set_multiple() -> Result<()> {
    let mut kvs = create_tmp();
    let exp = "value1";
    kvs.set("key1".to_owned(), "somethingelse".to_owned())?;
    kvs.set("key2".to_owned(), exp.to_owned())?;
    let actual = kvs.get(&"key2".to_owned()).unwrap();
    assert_eq!(exp, actual);

    Ok(())
}

#[test]
fn open() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut path = temp_dir.path().to_path_buf();
    path.push("testdb".to_owned());

    let mut kvs = Kvs::open(path.clone(), None);
    let exp = "value1";
    kvs.set("key1".to_owned(), exp.to_owned())?;
    drop(kvs);

    let mut kvs = Kvs::open(path, None);
    let actual = kvs.get(&"key1".to_owned()).unwrap();
    assert_eq!(exp, actual);

    Ok(())
}

#[test]
fn overwrite() -> Result<()> {
    let mut kvs = create_tmp();
    let exp = "last";
    kvs.set("key1".to_owned(), "somethingelse".to_owned())?;
    kvs.set("key1".to_owned(), "somethingelse".to_owned())?;
    kvs.set("key1".to_owned(), "somethingelse".to_owned())?;
    kvs.set("key1".to_owned(), exp.to_owned())?;
    let actual = kvs.get(&"key1".to_owned()).unwrap();

    assert_eq!(exp, actual);
    Ok(())
}

#[test]
fn log_threshold() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut path = temp_dir.path().to_path_buf();
    path.push("testdb".to_owned());
    let mut kvs = Kvs::open(path.clone(), Some(1));

    kvs.set("key1".to_owned(), "somethingelse".to_owned())?;
    kvs.set("key1".to_owned(), "somethingelse".to_owned())?;
    kvs.set("key1".to_owned(), "somethingelse".to_owned())?;

    let entries = std::fs::read_dir(&path)?
        .map(|res| res.map(|e| e.path()))
        .filter_map(|p| p.ok())
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .count();

    assert_eq!(2, entries);
    Ok(())
}

#[test]
fn compaction() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut path = temp_dir.path().to_path_buf();
    path.push("testdb".to_owned());
    let mut kvs = Kvs::open(path.clone(), Some(1));

    kvs.set("key1".to_owned(), "val1".to_owned())?;
    kvs.set("key1".to_owned(), "val1".to_owned())?;
    kvs.set("key2".to_owned(), "val2".to_owned())?;
    kvs.set("key2".to_owned(), "val2".to_owned())?;
    kvs.set("key3".to_owned(), "val3".to_owned())?;
    kvs.set("key3".to_owned(), "val3".to_owned())?;

    let entries: Vec<_> = std::fs::read_dir(&path)?
        .map(|res| res.map(|e| e.path()))
        .filter_map(|p| p.ok())
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .collect();

    println!("entries: {:?}", entries);
    assert_eq!(2, entries.len());

    Ok(())
}

#[test]
fn reload_compaction() -> Result<()> {
    {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut path = temp_dir.path().to_path_buf();
        path.push("testdb".to_owned());
        let mut kvs = Kvs::open(path.clone(), Some(1));

        kvs.set("key1".to_owned(), "val1".to_owned())?;
        kvs.set("key1".to_owned(), "val1".to_owned())?;
        kvs.set("key2".to_owned(), "val2".to_owned())?;
        kvs.set("key2".to_owned(), "val2".to_owned())?;
        kvs.set("key3".to_owned(), "val3".to_owned())?;
        kvs.set("key3".to_owned(), "val3".to_owned())?;

        drop(kvs);
        let mut kvs = Kvs::open(path, None);

        assert_eq!("val1", kvs.get(&"key1".to_owned()).unwrap());
        assert_eq!("val2", kvs.get(&"key2".to_owned()).unwrap());
        assert_eq!("val3", kvs.get(&"key3".to_owned()).unwrap());

        Ok(())
    }
}
