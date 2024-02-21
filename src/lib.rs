use bytelines::*;
use std::collections::HashMap;
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
    pub reader: BufReaderWithPos<File>,
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
    pub fn new(path: PathBuf, compaction_limit: Option<u64>) -> Self {
        let compaction_limit = compaction_limit.unwrap_or(1024 * 1024 as u64);
        println!("Creating file {:?}", path);
        let wfile = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();
        let writer = BufWriterWithPos::new(wfile);
        let rfile = OpenOptions::new().read(true).open(&path).unwrap();
        let reader = BufReaderWithPos::new(rfile);
        let index = HashMap::new();
        let kvs = Kvs {
            reader,
            writer,
            index,
            uncompacted: 0 as u64,
            compaction_limit,
        };
        kvs
    }

    fn new_file() -> Result<()> {
        Ok(())
    }

    pub fn open(path: PathBuf, compaction_limit: Option<u64>) -> Self {
        let compaction_limit = compaction_limit.unwrap_or(1024 * 1024 as u64);
        println!("Opening file {:?}", path);
        let mut index = HashMap::new();

        let bytefile = OpenOptions::new().read(true).open(&path).unwrap();
        let r = BufReader::new(bytefile);
        let mut lines = ByteLines::new(r);
        let mut uncompacted: u64 = 0;

        let mut pos = 0;
        while let Some(line) = lines.next() {
            let l: &[u8] = line.unwrap();
            let len = l.len() as u64;
            if let KvsCommand::Set { key, .. } = rmps::decode::from_slice::<KvsCommand>(l).unwrap()
            {
                if let Some(_) = index.insert(key.to_owned(), CommandPos { len, pos }) {
                    uncompacted += len;
                }
                // Account for newline byte so position stays accurate
                pos += len + 1;
            } else if let KvsCommand::Rm { key } =
                rmps::decode::from_slice::<KvsCommand>(l).unwrap()
            {
                index.remove(&key);
                // Account for newline byte so position stays accurate
                pos += len + 1;
            }
        }

        let wfile = OpenOptions::new().write(true).open(&path).unwrap();
        let writer = BufWriterWithPos::new(wfile);
        let rfile = OpenOptions::new().read(true).open(&path).unwrap();
        let reader = BufReaderWithPos::new(rfile);

        Kvs {
            writer,
            reader,
            index,
            uncompacted,
            compaction_limit,
        }
    }

    fn load() -> Result<()> {
        Ok(())
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = KvsCommand::Set { key, value };
        let mut buf = Vec::new();
        cmd.serialize(&mut Serializer::new(&mut buf)).unwrap();
        let mut ser = rmps::to_vec(&cmd).unwrap();
        ser.push(b'\n');
        let pos = self.writer.pos;
        let len = self.writer.write(&ser).unwrap();

        if let KvsCommand::Set { key, .. } = cmd {
            if let Some(old_cmd) = self.index.insert(
                key,
                CommandPos {
                    pos,
                    len: len as u64,
                },
            ) {
                self.uncompacted += old_cmd.len;
                if self.uncompacted > self.compaction_limit {
                    // Create new log
                    //

                    println!("Creating new log");
                }
            }
        }

        self.writer.pos = pos + len as u64;
        self.writer.flush()?;
        Ok(())
    }

    pub fn get(&mut self, key: &String) -> Result<String> {
        println!("Getting key: {}", &key);
        let pos = self.index.get(key).ok_or(KvsError {
            msg: "wasup".to_owned(),
        })?;
        self.reader.seek(SeekFrom::Start(pos.pos)).unwrap();

        let mut buf = vec![0; pos.len as usize];
        let _ = self.reader.read(&mut buf);
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

    fn compact(&self) -> Result<()> {
        Ok(())
    }
}
