use bytelines::*;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Error, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

#[macro_use]
extern crate serde_derive;
extern crate rmp_serde as rmps;
use rmps::Serializer;
use serde::{Deserialize, Serialize};

pub type Result<String> = std::result::Result<String, KvsError>;

pub struct Kvs {
    pub path: PathBuf,
    pub readers: HashMap<u64, BufReaderWithPos<File>>,
    pub writer: BufWriterWithPos<File>,
    pub index: HashMap<String, CommandPos>,
    pub uncompacted: u64,
    pub compaction_limit: u64,
}

#[derive(Debug)]
pub struct KvsError {
    pub msg: String,
}

impl From<Error> for KvsError {
    fn from(err: Error) -> KvsError {
        KvsError {
            msg: err.to_string(),
        }
    }
}

#[derive(Debug)]
pub struct CommandPos {
    pos: u64,
    len: u64,
    log: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum KvsCommand {
    Set { key: String, value: String },
    Rm { key: String },
}

#[derive(Debug)]
pub struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    pub fn new(file: R) -> BufReaderWithPos<R> {
        BufReaderWithPos {
            reader: BufReader::new(file),
            pos: 0,
        }
    }
}

impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let len = self.reader.read(buf).unwrap();
        self.pos += len as u64;
        Ok(len)
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let newpos = self.reader.seek(pos).unwrap();
        self.pos = newpos;
        Ok(newpos)
    }
}

#[derive(Debug)]
pub struct BufWriterWithPos<R: Write + Seek> {
    writer: BufWriter<R>,
    pos: u64,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    pub fn new(mut file: W) -> BufWriterWithPos<W> {
        let pos = file.seek(SeekFrom::Current(0)).unwrap();
        BufWriterWithPos {
            writer: BufWriter::new(file),
            pos,
        }
    }
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let len = self.writer.write(buf).unwrap();
        self.pos = len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

impl Kvs {
    pub fn open(path: PathBuf, compaction_limit: Option<u64>) -> Self {
        let compaction_limit = compaction_limit.unwrap_or(1024 * 1024 as u64);
        println!("Opening directory {:?}", path);
        std::fs::create_dir_all(&path).expect("Cannot create directory");

        // Get log file ids, sorted
        let logs = get_logs(&path).expect("Cannot load logs");
        let l = logs.last().unwrap_or(&0).clone();

        let mut index = HashMap::new();
        let mut readers = HashMap::new();

        if !logs.is_empty() {
            println!("Logs found");
        } else {
            println!("No logs found");
            let logpath = &path.join(&"0.log");
            println!("Creating reader for new file {}", logpath.display());
            let rfile = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&logpath)
                .unwrap();
            readers.insert(0, BufReaderWithPos::new(rfile));
        }

        // Create writer from last file
        let wpath = &path.join(format!("{}.log", l));
        println!("Opening file to write {}", wpath.display());

        let wfile = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(&wpath)
            .unwrap();
        let writer = BufWriterWithPos::new(wfile);

        // Populate index from files
        for log in logs {
            let logpath = &path.join(format!("{}.log", log));
            let rfile = OpenOptions::new().read(true).open(&logpath).unwrap();
            let reader = BufReaderWithPos::new(rfile);
            let _ = load(&logpath, &mut index);
            println!("Creating reader for log {}", log);
            readers.insert(log, reader);
        }

        let uncompacted = 0 as u64;

        Kvs {
            path,
            writer,
            readers,
            index,
            uncompacted,
            compaction_limit,
        }
    }

    pub fn create_new(&mut self) {
        let logs = get_logs(&self.path).unwrap();
        let last = logs.last().unwrap_or(&0) + 1;

        let wpath = self.path.join(format!("{}.log", last));
        println!("Opening file to write {}", wpath.display());

        let wfile = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(&wpath)
            .unwrap();

        self.writer = BufWriterWithPos::new(wfile);
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        println!("Writing entry {}:{}", &key, &value);
        let cmd = KvsCommand::Set { key, value };
        let mut buf = Vec::new();
        cmd.serialize(&mut Serializer::new(&mut buf)).unwrap();
        let mut ser = rmps::to_vec(&cmd).unwrap();
        ser.push(b'\n');

        let pos = self.writer.pos;
        let len = self.writer.write(&ser).unwrap();

        self.writer.pos = pos + len as u64;
        self.writer.flush()?;

        if let KvsCommand::Set { key, .. } = cmd {
            if let Some(old_cmd) = self.index.insert(
                key,
                CommandPos {
                    pos,
                    len: len as u64,
                    log: 0 as u64,
                },
            ) {
                self.uncompacted += old_cmd.len;
            }
        }

        if self.uncompacted > self.compaction_limit {
            println!("Creating new log");
            let _ = self.compact();
        }

        Ok(())
    }

    fn compact(&mut self) -> Result<()> {
        // Condense logs

        self.create_new();
        Ok(())
    }

    pub fn get(&mut self, key: &String) -> Result<String> {
        println!("Getting key: {}", &key);

        let pos = self.index.get(key).ok_or(KvsError {
            msg: "Key not found in index".to_owned(),
        })?;

        println!("Getting reader for {:?}", pos);

        let reader = self.readers.get_mut(&pos.log).expect("Can't get reader");

        reader.seek(SeekFrom::Start(pos.pos)).unwrap();

        let mut buf = vec![0; pos.len as usize];
        let _ = reader.read(&mut buf);
        if let KvsCommand::Set { value, .. } = rmps::decode::from_slice::<KvsCommand>(&buf).unwrap()
        {
            println!("{:?}", value);
            Ok(value)
        } else {
            panic!("ah!");
        }
    }

    pub fn rm(&mut self, key: String) -> Result<()> {
        let msg = KvsCommand::Rm { key: key.clone() };
        let mut buf = Vec::new();
        msg.serialize(&mut Serializer::new(&mut buf)).unwrap();
        let mut ser = rmps::to_vec(&msg).unwrap();
        ser.push(b'\n');
        let pos = self.writer.pos;
        let len = self.writer.write(&ser).unwrap();
        self.index.remove(&key);
        self.writer.pos = pos + len as u64;
        self.writer.flush()?;

        Ok(())
    }
}

fn load(logpath: &PathBuf, index: &mut HashMap<String, CommandPos>) -> Result<u64> {
    println!("Getting lines from log {:?}", logpath.display());

    let file = File::open(logpath).unwrap();
    let reader = BufReader::new(file);

    let mut lines = ByteLines::new(reader);
    let mut pos = 0;
    let mut uncompacted = 0;
    while let Some(line) = lines.next() {
        println!("Reading line {:?}", line);
        let l: &[u8] = line.unwrap();
        let len = l.len() as u64;
        if let KvsCommand::Set { key, .. } = rmps::decode::from_slice::<KvsCommand>(l).unwrap() {
            if let Some(_) = index.insert(
                key.to_owned(),
                CommandPos {
                    len,
                    pos,
                    log: 0 as u64,
                },
            ) {
                uncompacted += len;
            }
            // Account for newline byte so position stays accurate
            pos += len + 1;
        } else if let KvsCommand::Rm { key } = rmps::decode::from_slice::<KvsCommand>(l).unwrap() {
            index.remove(&key);
            // Account for newline byte so position stays accurate
            pos += len + 1;
        }
    }

    Ok(uncompacted)
}

fn get_logs(path: &PathBuf) -> Result<Vec<u64>> {
    let mut entries: Vec<_> = fs::read_dir(&path)?
        .map(|res| res.map(|e| e.path()))
        .filter_map(|p| p.ok())
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();
    entries.sort_unstable();

    Ok(entries)
}
