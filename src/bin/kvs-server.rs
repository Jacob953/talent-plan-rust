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

use clap::{Parser, ValueEnum};
use kvs::{KvsError, Result};
use std::env::current_dir;
use std::fs;
use std::net::SocketAddr;
use std::process::exit;
use std::str::FromStr;

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:4000";
const DEFAULT_ENGINE: Engine = Engine::kvs;

#[derive(Debug, Parser)]
#[command(name = "kvs-server", version)]
struct Cli {
    /// Sets the listening address
    #[arg(long, value_name = "IP:PORT", default_value = DEFAULT_LISTENING_ADDRESS)]
    addr: SocketAddr,
    /// Sets the storage engine
    #[arg(value_enum, long, value_name = "ENGINE-NAME")]
    engine: Option<Engine>,
}

#[derive(Debug, ValueEnum, Clone, Copy, PartialEq, Eq)]
#[allow(non_camel_case_types)]
enum Engine {
    kvs,
    sled,
}

impl FromStr for Engine {
    type Err = KvsError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "kvs" => Ok(Engine::kvs),
            "sled" => Ok(Engine::sled),
            _ => Err(KvsError::UnexpectedEngineType),
        }
    }
}

fn main() {
    let mut cli = Cli::parse();
    let res = current_engine().and_then(move |curr_engine| {
        if cli.engine.is_none() {
            cli.engine = curr_engine;
        }
        if curr_engine.is_some() && cli.engine != curr_engine {
            eprintln!("Wrong engine!");
            exit(1);
        }
        run(cli)
    });

    if let Err(e) = res {
        eprintln!("{}", e);
        exit(1);
    }
}

fn current_engine() -> Result<Option<Engine>> {
    let engine_file = current_dir()?.join("engine");

    if !engine_file.exists() {
        return Ok(None);
    }

    match fs::read_to_string(engine_file)?.parse() {
        Ok(engine) => Ok(Some(engine)),
        Err(_) => Err(KvsError::UnexpectedEngineType),
    }
}

fn run(cli: Cli) -> Result<()> {
    let engine = cli.engine.unwrap_or(DEFAULT_ENGINE);

    fs::write(current_dir()?.join("engine"), format!("{:?}", engine))?;

    match engine {
        Engine::kvs => Ok(()),
        Engine::sled => Ok(()),
    }
}
