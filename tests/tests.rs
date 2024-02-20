use kvs::*;
use tempfile::TempDir;

fn create_tmp() -> Kvs {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut path = temp_dir.path().to_path_buf();
    path.push("testdb".to_owned());
    Kvs::new(path)
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
    let mut kvs = Kvs::new(path);

    let exp = "value1";
    kvs.set("key1".to_owned(), "somethingelse".to_owned())?;
    kvs.set("key2".to_owned(), "somethingelse".to_owned())?;
    kvs.set("key2".to_owned(), exp.to_owned())?;
    drop(kvs);

    let mut path = temp_dir.path().to_path_buf();
    path.push("testdb".to_owned());
    let mut kvs = Kvs::open(path);
    let actual = kvs.get(&"key2".to_owned()).unwrap();
    assert_eq!(exp, actual);

    Ok(())
}

#[test]
fn open_rm() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut path = temp_dir.path().to_path_buf();
    path.push("testdb".to_owned());
    let mut kvs = Kvs::new(path);

    kvs.set("key2".to_owned(), "somethingelse".to_owned())?;
    kvs.rm("key2".to_owned())?;
    drop(kvs);

    let mut path = temp_dir.path().to_path_buf();
    path.push("testdb".to_owned());
    let mut kvs = Kvs::open(path);
    let actual = kvs.get(&"key2".to_owned());
    assert!(actual.is_err());
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
