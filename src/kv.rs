// MIT License
//
// Copyright (c) 2023 Chunfung
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

use super::Result;
use crate::error::KvsError;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use std::{
    borrow::BorrowMut,
    collections::{BTreeMap, HashMap},
    ffi::OsStr,
    fs,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    ops::Range,
    path::{Path, PathBuf},
};

/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are stored in a `HashMap` in memory and not persisted to disk.
///
/// Example:
///
/// ```rust
/// # use kvs::KvStore;
/// let mut store = KvStore::new();
/// store.set("key".to_owned(), "value".to_owned());
/// let val = store.get("key".to_owned());
/// assert_eq!(val, Some("value".to_owned()));
/// ```
pub struct KvStore {
    log: u64,
    // reader of the current log.
    readers: HashMap<u64, BufReaderWithPos<File>>,
    // writer of the current log.
    writer: BufWriterWithPos<File>,
    // map log file to the record args
    records: BTreeMap<String, RecordArgs>,
}

impl KvStore {
    /// Open the KvStore at a given path
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        fs::create_dir_all(&path)?;

        let mut readers = HashMap::new();
        let mut records = BTreeMap::new();

        let log_list = sorted_log_list(&path)?;

        for &log in &log_list {
            let mut reader = BufReaderWithPos::new(File::open(log_path(&path, log))?)?;
            load(log, &mut reader, &mut records);
            readers.insert(log, reader);
        }

        let log = log_list.last().unwrap_or(&0) + 1;
        let writer = new_log_file(&path, log, &mut readers)?;

        Ok(KvStore {
            log,
            readers,
            writer,
            records,
        })
    }

    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = MultipleCmd::set(key.clone(), value);
        let pos = self.writer.pos;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;
        if let MultipleCmd::Set { key, .. } = cmd {
            self.records
                .insert(key, (self.log, pos..self.writer.pos).into());
        }
        Ok(())
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(record) = self.records.get(&key) {
            let reader = self.readers.get_mut(&record.log).unwrap();
            reader.seek(SeekFrom::Start(record.pos))?;
            let cmd = reader.borrow_mut().take(record.len);
            if let MultipleCmd::Set { value, .. } = serde_json::from_reader(cmd)? {
                return Ok(Some(value));
            } else {
                return Err(KvsError::UnexpectedCommandType);
            }
        }
        Ok(None)
    }

    /// Remove a given key.
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.records.contains_key(&key) {
            let cmd = MultipleCmd::rm(key);
            serde_json::to_writer(&mut self.writer, &cmd)?;
            self.writer.flush()?;
            if let MultipleCmd::Rm { key } = cmd {
                self.records.remove(&key);
            }
            return Ok(());
        }
        Err(KvsError::KeyNotFound)
    }
}

/// Returns sorted log files in the given directory.
fn sorted_log_list(path: &Path) -> Result<Vec<u64>> {
    let mut log_list: Vec<u64> = fs::read_dir(&path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();
    log_list.sort_unstable();
    Ok(log_list)
}

fn log_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("{}.log", gen))
}

fn load(
    log: u64,
    reader: &mut BufReaderWithPos<File>,
    records: &mut BTreeMap<String, RecordArgs>,
) -> Result<()> {
    // To make sure we read from the beginning of the file.
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<MultipleCmd>();
    while let Some(cmd) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        match cmd? {
            MultipleCmd::Set { key, .. } => {
                records.insert(key, (log, pos..new_pos).into());
            }
            MultipleCmd::Rm { key } => {
                records.remove(&key);
            }
        }
        pos = new_pos;
    }
    Ok(())
}

fn new_log_file(
    path: &Path,
    log: u64,
    readers: &mut HashMap<u64, BufReaderWithPos<File>>,
) -> Result<BufWriterWithPos<File>> {
    let path = log_path(&path, log);
    let writer = BufWriterWithPos::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&path)?,
    )?;
    readers.insert(log, BufReaderWithPos::new(File::open(&path)?)?);
    Ok(writer)
}

struct RecordArgs {
    log: u64,
    pos: u64,
    len: u64,
}

impl From<(u64, Range<u64>)> for RecordArgs {
    fn from((log, range): (u64, Range<u64>)) -> Self {
        RecordArgs {
            log,
            pos: range.start,
            len: range.end - range.start,
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
enum MultipleCmd {
    Set { key: String, value: String },
    Rm { key: String },
}

impl MultipleCmd {
    fn set(key: String, value: String) -> MultipleCmd {
        MultipleCmd::Set { key, value }
    }
    fn rm(key: String) -> MultipleCmd {
        MultipleCmd::Rm { key }
    }
}

struct BufWriterWithPos<W: Write> {
    writer: BufWriter<W>,
    pos: u64,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    fn new(mut inner: W) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufWriterWithPos {
            writer: BufWriter::new(inner),
            pos,
        })
    }
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }

    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let length = self.writer.write(buf)?;
        self.pos += length as u64;
        Ok(length)
    }
}

struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    fn new(mut inner: R) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufReaderWithPos {
            reader: BufReader::new(inner),
            pos,
        })
    }
}

impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let length = self.reader.read(buf)?;
        self.pos += length as u64;
        Ok(length)
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}
