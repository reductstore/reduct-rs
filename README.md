# ReductStore Client SDK for Rust

[![Crates.io](https://img.shields.io/crates/v/reduct-rs)](https://crates.io/crates/reduct-rs)
[![Docs.rs](https://docs.rs/reduct-rs/badge.svg)](https://docs.rs/reduct-rs/latest/reduct_rs/)
[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/reductstore/reduct-rs/ci.yml?branch=main)](https://github.com/reductstore/reduct-rs/actions)

This package provides an HTTP client for interacting with the [ReductStore](https://www.reduct.store), time-series
database for unstructured data.

## Features

* Supports the [ReductStore HTTP API v1.8](https://reduct.store/docs/http-api)
* Built on top of [reqwest](https://github.com/seanmonstar/reqwest)
* Asynchronous API

## Example

```rust

use bytes::Bytes;
use reduct_rs::{ReductClient, ReductError};
use std::str::from_utf8;
use std::time::SystemTime;

use tokio;

#[tokio::main]
async fn main() -> Result<(), ReductError> {
    let client = ReductClient::builder().url("http://127.0.0.1:8383").build();

    let timestamp = SystemTime::now();

    let bucket = client.create_bucket("test").exist_ok(true).send().await?;
    bucket
        .write_record("entry-1")
        .timestamp(timestamp)
        .data(Bytes::from("Hello, World!"))
        .send()
        .await?;

    let record = bucket
        .read_record("entry-1")
        .timestamp(timestamp)
        .send()
        .await?;

    println!("Record: {:?}", record);
    println!(
        "Data: {}",
        from_utf8(&record.bytes().await?.to_vec()).unwrap()
    );

    Ok(())
}

```

## References

* [Documentation](https://docs.rs/reduct-rs/latest/reduct_rs/)
* [ReductStore HTTP API](https://reduct.store/docs/http-api)
