# ReductStore Client SDK for Rust

[![Crates.io](https://img.shields.io/crates/v/reduct-rs)](https://crates.io/crates/reduct-rs)
[![Docs.rs](https://docs.rs/reduct-rs/badge.svg)](https://docs.rs/reduct-rs/latest/reduct_rs/)
[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/reductstore/reduct-rs/ci.yml?branch=main)](https://github.com/reductstore/reduct-rs/actions)

This package provides an HTTP client for interacting with the [ReductStore](https://www.reduct.store), time-series
database for unstructured data.

## Features

* Supports the [ReductStore HTTP API v1.16](https://www.reduct.store/docs/http-api)
* Built on top of [reqwest](https://github.com/seanmonstar/reqwest)
* Asynchronous API

## Example

```rust
use futures_util::stream::StreamExt;
use reduct_rs::{QuotaType, ReductClient, ReductError};
use serde_json::json;
use std::pin::pin;
use std::time::{Duration, SystemTime};
use tokio;

#[tokio::main]
async fn main() -> Result<(), ReductError> {
    // 1. Create a ReductStore client
    let client = ReductClient::builder()
        .url("http://127.0.0.1:8383")
        .api_token("my-token")
        .build();

    // 2. Get or create a bucket with 1Gb quota
    let bucket = client
        .create_bucket("my-bucket")
        .quota_type(QuotaType::FIFO)
        .quota_size(1_000_000_000)
        .exist_ok(true)
        .send()
        .await?;

    // 3. Write some data with timestamps and labels to the 'entry-1' entry
    let start = SystemTime::now();
    bucket
        .write_record("sensor-1")
        .data("<Blob data>")
        .timestamp(start)
        .add_label("score", 10)
        .send()
        .await?;

    bucket
        .write_record("sensor-1")
        .data("<Blob data>")
        .timestamp(start + Duration::from_secs(1))
        .add_label("score", 20)
        .send()
        .await?;

    // 4. Query the data by time range and condition
    let query = bucket
        .query("sensor-1")
        .start(start)
        .stop(start + Duration::from_secs(2))
        .when(json!({"&score": {"$gt": 15}}))
        .send()
        .await?;

    let mut query = pin!(query);
    while let Some(record) = query.next().await {
        let record = record?;
        println!("Record timestamp: {:?}", record.timestamp());
        println!("Record size: {}", record.content_length());
        println!("{:?}", record.bytes().await?);
    }

    // 5. Exit
    Ok(())
}

```

For more examples, see the [Guides](https://reduct.store/docs/guides) section in the ReductStore documentation.


## Supported ReductStore Versions and  Backward Compatibility

The library is backward compatible with the previous versions. However, some methods have been deprecated and will be
removed in the future releases. Please refer to the [Changelog](CHANGELOG.md) for more details.
The SDK supports the following ReductStore API versions:

* v1.16
* v1.15
* v1.14

It can work with newer and older versions, but it is not guaranteed that all features will work as expected because
the API may change and some features may be deprecated or the SDK may not support them yet.
