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
    io,
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    ops::Range,
    path::{Path, PathBuf},
};

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are persisted to disk in log files. Log files are named after
/// monotonically increasing generation numbers with a `log` extension name.
/// A `BTreeMap` in memory stores the keys and the value locations for fast query.
///
/// ```rust
/// # use kvs::{KvStore, Result};
/// # fn try_main() -> Result<()> {
/// use std::env::current_dir;
/// let mut store = KvStore::open(current_dir()?)?;
/// store.set("key".to_owned(), "value".to_owned())?;
/// let val = store.get("key".to_owned())?;
/// assert_eq!(val, Some("value".to_owned()));
/// # Ok(())
/// # }
/// ```
pub struct KvStore {
    path: PathBuf,
    log: u64,
    // the number of bytes representing "stale" commands that could be
    // deleted during a compaction.
    uncompacted: u64,
    // reader of the current log.
    readers: HashMap<u64, BufReaderWithPos<File>>,
    // writer of the current log.
    writer: BufWriterWithPos<File>,
    // map log file to the record args
    records: BTreeMap<String, RecordArgs>,
}

impl KvStore {
    /// Opens a `KvStore` with the given path.
    ///
    /// This will create a new directory if the given one does not exist.
    ///
    /// # Errors
    ///
    /// It propagates I/O or deserialization errors during the log replay.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        fs::create_dir_all(&path)?;

        let mut readers = HashMap::new();
        let mut records = BTreeMap::new();
        let mut uncompacted = 0;

        let log_list = sorted_log_list(&path)?;

        for &log in &log_list {
            let mut reader = BufReaderWithPos::new(File::open(log_path(&path, log))?)?;
            uncompacted += load(log, &mut reader, &mut records)?;
            readers.insert(log, reader);
        }

        let log = log_list.last().unwrap_or(&0) + 1;
        let writer = new_log_file(&path, log, &mut readers)?;

        Ok(KvStore {
            path,
            log,
            uncompacted,
            readers,
            writer,
            records,
        })
    }

    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    ///
    /// # Errors
    ///
    /// It propagates I/O or serialization errors during writing the log.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = MultipleCmd::set(key.clone(), value);
        let pos = self.writer.pos;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;
        if let MultipleCmd::Set { key, .. } = cmd {
            if let Some(old_cmd) = self
                .records
                .insert(key, (self.log, pos..self.writer.pos).into())
            {
                self.uncompacted += old_cmd.len;
            }
        }
        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?;
        }
        Ok(())
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    ///
    /// # Errors
    ///
    /// It returns `KvsError::UnexpectedCommandType` if the given command type unexpected.
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

    /// Removes a given key.
    ///
    /// # Errors
    ///
    /// It returns `KvsError::KeyNotFound` if the given key is not found.
    ///
    /// It propagates I/O or serialization errors during writing the log.
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.records.contains_key(&key) {
            let cmd = MultipleCmd::rm(key);
            serde_json::to_writer(&mut self.writer, &cmd)?;
            self.writer.flush()?;
            if let MultipleCmd::Rm { key } = cmd {
                match self.records.remove(&key) {
                    Some(old_cmd) => self.uncompacted += old_cmd.len,
                    _ => return Err(KvsError::KeyNotFound),
                }
            }
            return Ok(());
        }
        Err(KvsError::KeyNotFound)
    }

    /// Clears stale entries in the log.
    pub fn compact(&mut self) -> Result<()> {
        // increase current gen by 2. current_gen + 1 is for the compaction file.
        let compaction_log = self.log + 1;
        self.log += 2;
        self.writer = self.new_log_file(self.log)?;

        let mut compaction_writer = self.new_log_file(compaction_log)?;

        let mut new_pos = 0; // pos in the new log file.
        for record in &mut self.records.values_mut() {
            let reader = self.readers.get_mut(&record.log).unwrap();
            if reader.pos != record.pos {
                reader.seek(SeekFrom::Start(record.pos))?;
            }

            let mut cmd = reader.take(record.len);
            let length = io::copy(&mut cmd, &mut compaction_writer)?;
            *record = (compaction_log, new_pos..new_pos + length).into();
            new_pos += length;
        }

        let stale_logs: Vec<_> = self
            .readers
            .keys()
            .filter(|&&log| log < compaction_log)
            .cloned()
            .collect();
        for stale_log in stale_logs {
            self.readers.remove(&stale_log);
            fs::remove_file(log_path(&self.path, stale_log))?;
        }

        self.uncompacted = 0;

        Ok(())
    }

    /// Create a new log file with given generation number and add the reader to the readers map.
    ///
    /// Returns the writer to the log.
    fn new_log_file(&mut self, gen: u64) -> Result<BufWriterWithPos<File>> {
        new_log_file(&self.path, gen, &mut self.readers)
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

/// Load the whole log file and store value locations in the index map.
///
/// Returns how many bytes can be saved after a compaction.
fn load(
    log: u64,
    reader: &mut BufReaderWithPos<File>,
    records: &mut BTreeMap<String, RecordArgs>,
) -> Result<u64> {
    let mut uncompacted = 0;
    // To make sure we read from the beginning of the file.
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<MultipleCmd>();
    while let Some(cmd) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        match cmd? {
            MultipleCmd::Set { key, .. } => {
                if let Some(old_cmd) = records.insert(key, (log, pos..new_pos).into()) {
                    uncompacted += old_cmd.len;
                }
            }
            MultipleCmd::Rm { key } => {
                if let Some(old_cmd) = records.remove(&key) {
                    uncompacted += old_cmd.len;
                }
                uncompacted += new_pos - pos;
            }
        }
        pos = new_pos;
    }
    Ok(uncompacted)
}

/// Create a new log file with given generation number and add the reader to the readers map.
///
/// Returns the writer to the log.
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

/// Represents the position and length of a json-serialized record in the log.
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

/// Struct representing a multiple command.
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

/// KvsEngine
pub trait KvsEngine {}
