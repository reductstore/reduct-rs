// Copyright 2023-2025 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::http_client::{map_error, HttpClient};
use crate::record::{from_system_time, Record};
use crate::RecordStream;
use async_channel::{unbounded, Receiver};
use async_stream::stream;
use bytes::Bytes;
use bytes::BytesMut;
use futures::Stream;
use futures_util::{pin_mut, StreamExt};
use reduct_base::batch::{parse_batched_header, sort_headers_by_time, RecordHeader};
use reduct_base::error::ErrorCode::Unknown;
use reduct_base::error::{ErrorCode, IntEnum, ReductError};
use reduct_base::msg::entry_api::{QueryEntry, QueryInfo, QueryType, RemoveQueryInfo};
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::Method;
use serde_json::Value;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

/// Builder for a query request.
pub struct QueryBuilder {
    query: QueryEntry,

    bucket: String,
    entry: String,
    client: Arc<HttpClient>,
}

impl QueryBuilder {
    pub(crate) fn new(bucket: String, entry: String, client: Arc<HttpClient>) -> Self {
        Self {
            query: QueryEntry::default(),
            bucket,
            entry,
            client,
        }
    }

    /// Set the start time of the query.
    pub fn start(mut self, time: SystemTime) -> Self {
        self.query.start = Some(from_system_time(time));
        self
    }

    /// Set the start time of the query as a unix timestamp in microseconds.
    pub fn start_us(mut self, time_us: u64) -> Self {
        self.query.start = Some(time_us);
        self
    }

    /// Set the end time of the query.
    pub fn stop(mut self, time: SystemTime) -> Self {
        self.query.stop = Some(from_system_time(time));
        self
    }

    /// Set the end time of the query as a unix timestamp in microseconds.
    pub fn stop_us(mut self, time_us: u64) -> Self {
        self.query.stop = Some(time_us);
        self
    }

    /// Set the condition for the query.
    pub fn when(mut self, condition: Value) -> Self {
        self.query.when = Some(condition);
        self
    }

    /// Set the query to be strict.
    /// If the query is strict, the query will return an error if any of the conditions are invalid.
    /// default: false
    pub fn strict(mut self, strict: bool) -> Self {
        self.query.strict = Some(strict);
        self
    }

    /// Set extension parameters for the query.
    /// This is a JSON object that will be passed to extensions on the server side.
    pub fn ext(mut self, ext: Value) -> Self {
        self.query.ext = Some(ext);
        self
    }

    /// Set S, to return a record every S seconds.
    /// default: return all records
    #[deprecated(
        since = "1.15.0",
        note = "Use `$each_t` operator in `when` condition. It will be removed in v1.18.0."
    )]
    pub fn each_s(mut self, each_s: f64) -> Self {
        self.query.each_s = Some(each_s);
        self
    }

    /// Set N, to return every N records.
    /// default: return all records
    #[deprecated(
        since = "1.15.0",
        note = "Use `$each_n` operator in `when` condition. It will be removed in v1.18.0."
    )]
    pub fn each_n(mut self, each_n: u64) -> Self {
        self.query.each_n = Some(each_n);
        self
    }

    /// Set a limit for the query.
    /// default: unlimited
    #[deprecated(
        since = "1.15.0",
        note = "Use `$limit` operator in `when` condition. It will be removed in v1.18.0."
    )]
    pub fn limit(mut self, limit: u64) -> Self {
        self.query.limit = Some(limit);
        self
    }

    /// Set TTL for the query.
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.query.ttl = Some(ttl.as_secs());
        self
    }

    /// Set the query to be continuous.
    pub fn continuous(mut self) -> Self {
        self.query.continuous = Some(true);
        self
    }

    /// Set the query to head only.
    /// default: false
    pub fn head_only(mut self, head_only: bool) -> Self {
        self.query.only_metadata = Some(head_only);
        self
    }

    /// Send the query request.
    pub async fn send(
        mut self,
    ) -> Result<impl Stream<Item = Result<Record, ReductError>>, ReductError> {
        // use new POST API for new features
        self.query.query_type = QueryType::Query;
        let response = self
            .client
            .send_and_receive_json::<QueryEntry, QueryInfo>(
                Method::POST,
                &format!("/b/{}/{}/q", self.bucket, self.entry),
                Some(self.query.clone()),
            )
            .await?;

        let head_only = self.query.only_metadata.as_ref().unwrap_or(&false).clone();

        Ok(stream! {
            let mut last = false;
            while !last {
                let method = if head_only { Method::HEAD } else { Method::GET };
                let request = self.client.request(method, &format!("/b/{}/{}/batch?q={}", self.bucket, self.entry, response.id));
                let response = self.client.send_request(request).await?;

                if response.status() == reqwest::StatusCode::NO_CONTENT {
                    break;
                }

                let headers = response.headers().clone();


                let (tx, rx) = unbounded();
                tokio::spawn(async move {
                    let mut stream = response.bytes_stream();
                    while let Some(bytes) = stream.next().await {
                        if let Err(_) = tx.send(bytes).await {
                            break;
                        }
                    }
                });

                let stream = parse_batched_records(headers, rx, head_only).await?;
                pin_mut!(stream);
                while let Some(record) = stream.next().await {
                    let record = record?;
                    last = record.1;
                    yield Ok(record.0);
                }
            }
        })
    }
}

/**
 * Builder for a remove query request.
 */
pub struct RemoveQueryBuilder {
    query: QueryEntry,

    bucket: String,
    entry: String,
    client: Arc<HttpClient>,
}

impl RemoveQueryBuilder {
    pub(crate) fn new(bucket: String, entry: String, client: Arc<HttpClient>) -> Self {
        Self {
            query: QueryEntry::default(),
            bucket,
            entry,
            client,
        }
    }

    /// Set the start time of the query.
    pub fn start(mut self, time: SystemTime) -> Self {
        self.query.start = Some(from_system_time(time));
        self
    }

    /// Set the start time of the query as a unix timestamp in microseconds.
    pub fn start_us(mut self, time_us: u64) -> Self {
        self.query.start = Some(time_us);
        self
    }

    /// Set the end time of the query.
    pub fn stop(mut self, time: SystemTime) -> Self {
        self.query.stop = Some(from_system_time(time));
        self
    }

    /// Set the end time of the query as a unix timestamp in microseconds.
    pub fn stop_us(mut self, time_us: u64) -> Self {
        self.query.stop = Some(time_us);
        self
    }

    /// Set the condition for the query.
    /// This will remove all records that match the condition.
    /// This is a destructive operation.
    pub fn when(mut self, condition: Value) -> Self {
        self.query.when = Some(condition);
        self
    }

    /// Set the query to be strict.
    /// If the query is strict, the query will return an error if any of the conditions are invalid.
    /// default: false
    pub fn strict(mut self, strict: bool) -> Self {
        self.query.strict = Some(strict);
        self
    }

    /// Set S, to return a record every S seconds.
    /// default: return all records
    #[deprecated(
        since = "1.15.0",
        note = "Use `$each_t` operator in `when` condition. It will be removed in v1.18.0."
    )]
    pub fn each_s(mut self, each_s: f64) -> Self {
        self.query.each_s = Some(each_s);
        self
    }

    /// Set N, to return every N records.
    /// default: return all records
    #[deprecated(
        since = "1.15.0",
        note = "Use `$each_n` operator in `when` condition. It will be removed in v1.18.0."
    )]
    pub fn each_n(mut self, each_n: u64) -> Self {
        self.query.each_n = Some(each_n);
        self
    }

    /// Send the remove query request.
    /// This will remove all records that match the query.
    /// This is a destructive operation.
    ///
    /// # Returns
    ///
    /// * `Result<u64, ReductError>` - The number of records removed.
    pub async fn send(mut self) -> Result<u64, ReductError> {
        self.query.query_type = QueryType::Remove;
        let response = self
            .client
            .send_and_receive_json::<QueryEntry, RemoveQueryInfo>(
                Method::POST,
                &format!("/b/{}/{}/q", self.bucket, self.entry),
                Some(self.query.clone()),
            )
            .await?;

        Ok(response.removed_records)
    }
}

async fn parse_batched_records(
    headers: HeaderMap,
    rx: Receiver<Result<Bytes, reqwest::Error>>,
    head_only: bool,
) -> Result<impl Stream<Item = Result<(Record, bool), ReductError>>, ReductError> {
    //sort headers by names
    let sorted_records = sort_headers_by_time(&headers)?;

    let records_total = sorted_records.iter().count();
    let mut records_count = 0;

    let unwrap_byte = |bytes: Result<Bytes, reqwest::Error>| match bytes {
        Ok(b) => Ok(b),
        Err(err) => {
            if let Some(status) = err.status() {
                Err(ReductError::new(
                    ErrorCode::from_int(status.as_u16() as i16).unwrap_or(Unknown),
                    &err.to_string(),
                ))
            } else {
                Err(map_error(err))
            }
        }
    };

    Ok(stream! {
        let mut rest_data = BytesMut::new();

        for (timestamp, value) in sorted_records {
                let RecordHeader{content_length, content_type, labels} = parse_batched_header(value.to_str().unwrap()).unwrap();
                let last =  headers.get("x-reduct-last") == Some(&HeaderValue::from_str("true").unwrap());

                records_count += 1;


                let data: Option<RecordStream> = if head_only {
                    None
                } else if records_count == records_total {
                    // last record in batched records read in client code
                    let first_chunk: Bytes = rest_data.clone().into();
                    let rx = rx.clone();

                    Some(Box::pin(stream! {
                        yield Ok(first_chunk);
                        while let Ok(bytes) = rx.recv().await {
                            yield unwrap_byte(bytes);
                        }

                    }))
                } else {
                    // batched records must be read in order, so it is safe to read them here
                    // instead of reading them in the use code with an async interator.
                    // The batched records are small if they are not the last.
                    // The last batched record is read in the async generator in chunks.
                    let mut data = rest_data.clone();
                    while let Ok(bytes) = rx.recv().await {
                        data.extend_from_slice(&unwrap_byte(bytes)?);
                        if data.len() >= content_length {
                            break;
                        }
                    }

                    rest_data = data.split_off(content_length);
                    data.truncate(content_length);

                    Some(Box::pin(stream! {
                        yield Ok(data.into());
                    }))
                };

                yield Ok((Record {
                    timestamp,
                    labels,
                    content_type,
                    content_length,
                    data
                }, last));

        }
    })
}
