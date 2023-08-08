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

use clap::{Parser, Subcommand};
use kvs::{KvStore, Result};
use std::env::current_dir;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Set the value of a string key to a string
    Set {
        /// A string key
        key: String,
        /// The string value of the key
        value: String,
    },

    /// Get the string value of a given string key
    Get {
        /// A string key
        key: String,
    },

    /// Remove a given key
    Rm {
        /// A string key
        key: String,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Set { key, value } => {
            let mut store = KvStore::open(current_dir()?)?;
            store.set(key, value)?
        }
        Command::Get { key } => {
            let store = KvStore::open(current_dir()?)?;
            if let Some(value) = store.get(key)? {
                println!("{value}")
            } else {
                println!("Key not found")
            }
        }
        Command::Rm { key } => {
            let mut store = KvStore::open(current_dir()?)?;
            store.remove(key)?
        }
    }
    Ok(())
}
