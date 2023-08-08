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

use crate::error::KvsError;

use super::Result;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, path::PathBuf};

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
    map: HashMap<String, String>,
}

impl KvStore {
    /// Open the KvStore at a given path
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let mut map = HashMap::new();
        Ok(KvStore { map })
    }

    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let set_cmd = MultipleCmd::set(key.clone(), value.clone());
        if let Ok(log) = serde_json::to_string(&set_cmd) {
            self.map.insert(key, value);
            return Ok(());
        }
        Err(KvsError::KeyNotFoud)
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    pub fn get(&self, key: String) -> Result<Option<String>> {
        if let Some(log) = self.map.get(&key) {
            if let MultipleCmd::Set { value, .. } = serde_json::from_str(log)? {
                return Ok(Some(value));
            }
        }
        Err(KvsError::KeyNotFoud)
    }

    /// Remove a given key.
    pub fn remove(&mut self, key: String) -> Result<()> {
        if let Some(log) = self.map.get(&key) {
            if let MultipleCmd::Rm { key } = serde_json::from_str(log)? {
                let rm_cmd = MultipleCmd::rm(key);
                if let Ok(log) = serde_json::to_string(&rm_cmd) {}
                return Ok(());
            }
        }
        Err(KvsError::KeyNotFoud)
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
