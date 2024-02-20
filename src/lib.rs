use bytelines::*;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Error, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

extern crate rmp_serde as rmps;
extern crate serde;
extern crate serde_derive;
use rmps::Serializer;
use serde::{Deserialize, Serialize};

pub type Result<String> = std::result::Result<String, KvsError>;

pub struct Kvs {
    pub reader: BufReaderWithPos<File>,
    pub writer: BufWriterWithPos<File>,
    pub index: HashMap<String, CommandPos>,
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
    pub fn new(path: PathBuf) -> Self {
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
        };
        kvs
    }

    pub fn open(path: PathBuf) -> Self {
        println!("Opening file {:?}", path);
        let mut index = HashMap::new();

        let bytefile = OpenOptions::new().read(true).open(&path).unwrap();
        let r = BufReader::new(bytefile);
        let mut lines = ByteLines::new(r);

        let mut pos = 0;
        while let Some(line) = lines.next() {
            let l: &[u8] = line.unwrap();
            let len = l.len() as u64;
            if let KvsCommand::Set { key, .. } = rmps::decode::from_slice::<KvsCommand>(l).unwrap()
            {
                index.insert(key.to_owned(), CommandPos { len, pos });
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
        }
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let msg = KvsCommand::Set {
            key: key.clone(),
            value: value.clone(),
        };
        let mut buf = Vec::new();
        msg.serialize(&mut Serializer::new(&mut buf)).unwrap();
        let mut ser = rmps::to_vec(&msg).unwrap();
        ser.push(b'\n');
        let pos = self.writer.pos;
        let len = self.writer.write(&ser).unwrap();
        self.index.insert(
            key,
            CommandPos {
                pos,
                len: len as u64,
            },
        );
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
        let mut buf: Vec<u8> = Vec::new();

        for key in self.index.keys() {
            println!("{:?}", self.get(key));
        }

        // let msg = KvsCommand::Set {
        //     key: key.clone(),
        //     value: v?,
        // };
        // let mut b = Vec::new();
        // msg.serialize(&mut Serializer::new(&mut b)).unwrap();
        // let mut ser = rmps::to_vec(&msg).unwrap();

        self.writer.pos = buf.len() as u64;
        self.writer.flush()?;

        self.writer.write_all(b"");

        Ok(())
    }
}
