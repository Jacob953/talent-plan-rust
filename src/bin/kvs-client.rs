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
use kvs::{KvStore, KvsError, Result};
use std::env::current_dir;
use std::net::SocketAddr;
use std::process::exit;

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:4000";
const ADDRESS_FORMAT: &str = "IP:PORT";

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(name = "kvs-client",author, version, about, long_about = None)]
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
        /// Sets the server address
        #[arg(long, value_name = ADDRESS_FORMAT, default_value = DEFAULT_LISTENING_ADDRESS)]
        addr: SocketAddr,
    },

    /// Get the string value of a given string key
    Get {
        /// A string key
        key: String,
        /// Sets the server address
        #[arg(long, value_name = ADDRESS_FORMAT, default_value = DEFAULT_LISTENING_ADDRESS)]
        addr: SocketAddr,
    },

    /// Remove a given key
    Rm {
        /// A string key
        key: String,
        /// Sets the server address
        #[arg(long, value_name = ADDRESS_FORMAT, default_value = DEFAULT_LISTENING_ADDRESS)]
        addr: SocketAddr,
    },
}

fn main() {
    let cli = Cli::parse();

    if let Err(err) = run(cli) {
        eprintln!("{err}");
        exit(1);
    }
}

fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Command::Set { key, value, addr } => {
            let mut store = KvStore::open(current_dir()?)?;
            store.set(key, value)?
        }
        Command::Get { key, addr } => {
            let mut store = KvStore::open(current_dir()?)?;
            match store.get(key.to_string())? {
                Some(value) => println!("{value}"),
                _ => println!("Key not found"),
            }
        }
        Command::Rm { key, addr } => {
            let mut store = KvStore::open(current_dir()?)?;
            match store.remove(key.to_string()) {
                Ok(()) => {}
                Err(KvsError::KeyNotFound) => {
                    println!("Key not found");
                    exit(1);
                }
                Err(e) => return Err(e),
            }
        }
    }
    Ok(())
}
