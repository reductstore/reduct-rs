// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::http_client::HttpClient;
use crate::record::{from_system_time, Record};
use crate::RecordStream;
use async_channel::{unbounded, Receiver};
use async_stream::stream;
use bytes::Bytes;
use bytes::BytesMut;
use futures::Stream;
use futures_util::{pin_mut, StreamExt};
use reduct_base::batch::{parse_batched_header, sort_headers_by_time, RecordHeader};
use reduct_base::error::ReductError;
use reduct_base::msg::entry_api::{QueryInfo, RemoveQueryInfo};
use reduct_base::Labels;
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::Method;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

struct BaseQueryParameters {
    start: Option<u64>,
    stop: Option<u64>,
    include: Option<Labels>,
    exclude: Option<Labels>,
    each_s: Option<f64>,
    each_n: Option<u64>,
}

/// Builder for a query request.
pub struct QueryBuilder {
    base_params: BaseQueryParameters,

    limit: Option<u64>,
    ttl: Option<Duration>,
    continuous: bool,
    head_only: bool,

    bucket: String,
    entry: String,
    client: Arc<HttpClient>,
}

impl QueryBuilder {
    pub(crate) fn new(bucket: String, entry: String, client: Arc<HttpClient>) -> Self {
        Self {
            base_params: BaseQueryParameters {
                start: None,
                stop: None,
                include: None,
                exclude: None,
                each_s: None,
                each_n: None,
            },

            limit: None,

            ttl: None,
            continuous: false,
            head_only: false,

            bucket,
            entry,
            client,
        }
    }

    /// Set the start time of the query.
    pub fn start(mut self, time: SystemTime) -> Self {
        self.base_params.start = Some(from_system_time(time));
        self
    }

    /// Set the start time of the query as a unix timestamp in microseconds.
    pub fn start_us(mut self, time_us: u64) -> Self {
        self.base_params.start = Some(time_us);
        self
    }

    /// Set the end time of the query.
    pub fn stop(mut self, time: SystemTime) -> Self {
        self.base_params.stop = Some(from_system_time(time));
        self
    }

    /// Set the end time of the query as a unix timestamp in microseconds.
    pub fn stop_us(mut self, time_us: u64) -> Self {
        self.base_params.stop = Some(time_us);
        self
    }

    /// Set the labels to include in the query.
    pub fn include(mut self, labels: Labels) -> Self {
        self.base_params.include = Some(labels);
        self
    }

    /// Add a label to include in the query.
    pub fn add_include<Str>(mut self, key: Str, value: Str) -> Self
    where
        Str: Into<String>,
    {
        if let Some(mut labels) = self.base_params.include {
            labels.insert(key.into(), value.into());
            self.base_params.include = Some(labels);
        } else {
            let mut labels = Labels::new();
            labels.insert(key.into(), value.into());
            self.base_params.include = Some(labels);
        }
        self
    }

    /// Set the labels to exclude from the query.
    pub fn exclude(mut self, labels: Labels) -> Self {
        self.base_params.exclude = Some(labels);
        self
    }

    /// Add a label to exclude from the query.
    pub fn add_exclude<Str>(mut self, key: Str, value: Str) -> Self
    where
        Str: Into<String>,
    {
        if let Some(mut labels) = self.base_params.exclude {
            labels.insert(key.into(), value.into());
            self.base_params.exclude = Some(labels);
        } else {
            let mut labels = Labels::new();
            labels.insert(key.into(), value.into());
            self.base_params.exclude = Some(labels);
        }
        self
    }

    /// Set S, to return a record every S seconds.
    /// default: return all records
    pub fn each_s(mut self, each_s: f64) -> Self {
        self.base_params.each_s = Some(each_s);
        self
    }

    /// Set N, to return every N records.
    /// default: return all records
    pub fn each_n(mut self, each_n: u64) -> Self {
        self.base_params.each_n = Some(each_n);
        self
    }

    /// Set a limit for the query.
    /// default: unlimited
    pub fn limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set TTL for the query.
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Set the query to be continuous.
    pub fn continuous(mut self) -> Self {
        self.continuous = true;
        self
    }

    /// Set the query to head only.
    /// default: false
    pub fn head_only(mut self, head_only: bool) -> Self {
        self.head_only = head_only;
        self
    }

    /// Send the query request.
    pub async fn send(
        self,
    ) -> Result<impl Stream<Item = Result<Record, ReductError>>, ReductError> {
        let mut url = build_base_url(self.base_params, &self.bucket, &self.entry);

        if let Some(limit) = self.limit {
            url.push_str(&format!("&limit={}", limit));
        }

        // control parameters
        if self.continuous {
            url.push_str("&continuous=true");
        }

        if let Some(ttl) = self.ttl {
            url.push_str(&format!("&ttl={}", ttl.as_secs()));
        }

        let response = self
            .client
            .send_and_receive_json::<(), QueryInfo>(Method::GET, &url, None)
            .await?;

        let Self {
            head_only,
            bucket,
            entry,
            client,
            ..
        } = self;
        Ok(stream! {
            let mut last = false;
            while !last {
                let method = if head_only { Method::HEAD } else { Method::GET };
                let request = client.request(method, &format!("/b/{}/{}/batch?q={}", bucket, entry, response.id));
                let response = client.send_request(request).await?;

                if response.status() == reqwest::StatusCode::NO_CONTENT {
                    break;
                }

                let headers = response.headers().clone();


                let (tx, rx) = unbounded();
                tokio::spawn(async move {
                    let mut stream = response.bytes_stream();
                    while let Some(bytes) = stream.next().await {
                        if let Err(_) = tx.send(bytes.unwrap()).await {
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
    base_params: BaseQueryParameters,

    bucket: String,
    entry: String,
    client: Arc<HttpClient>,
}

impl RemoveQueryBuilder {
    pub(crate) fn new(bucket: String, entry: String, client: Arc<HttpClient>) -> Self {
        Self {
            base_params: BaseQueryParameters {
                start: None,
                stop: None,
                include: None,
                exclude: None,
                each_s: None,
                each_n: None,
            },
            bucket,
            entry,
            client,
        }
    }

    /// Set the start time of the query.
    pub fn start(mut self, time: SystemTime) -> Self {
        self.base_params.start = Some(from_system_time(time));
        self
    }

    /// Set the start time of the query as a unix timestamp in microseconds.
    pub fn start_us(mut self, time_us: u64) -> Self {
        self.base_params.start = Some(time_us);
        self
    }

    /// Set the end time of the query.
    pub fn stop(mut self, time: SystemTime) -> Self {
        self.base_params.stop = Some(from_system_time(time));
        self
    }

    /// Set the end time of the query as a unix timestamp in microseconds.
    pub fn stop_us(mut self, time_us: u64) -> Self {
        self.base_params.stop = Some(time_us);
        self
    }

    /// Set the labels to include in the query.
    pub fn include(mut self, labels: Labels) -> Self {
        self.base_params.include = Some(labels);
        self
    }

    /// Add a label to include in the query.
    pub fn add_include<Str>(mut self, key: Str, value: Str) -> Self
    where
        Str: Into<String>,
    {
        if let Some(mut labels) = self.base_params.include {
            labels.insert(key.into(), value.into());
            self.base_params.include = Some(labels);
        } else {
            let mut labels = Labels::new();
            labels.insert(key.into(), value.into());
            self.base_params.include = Some(labels);
        }
        self
    }

    /// Set the labels to exclude from the query.
    pub fn exclude(mut self, labels: Labels) -> Self {
        self.base_params.exclude = Some(labels);
        self
    }

    /// Add a label to exclude from the query.
    pub fn add_exclude<Str>(mut self, key: Str, value: Str) -> Self
    where
        Str: Into<String>,
    {
        if let Some(mut labels) = self.base_params.exclude {
            labels.insert(key.into(), value.into());
            self.base_params.exclude = Some(labels);
        } else {
            let mut labels = Labels::new();
            labels.insert(key.into(), value.into());
            self.base_params.exclude = Some(labels);
        }
        self
    }

    /// Set S, to return a record every S seconds.
    /// default: return all records
    pub fn each_s(mut self, each_s: f64) -> Self {
        self.base_params.each_s = Some(each_s);
        self
    }

    /// Set N, to return every N records.
    /// default: return all records
    pub fn each_n(mut self, each_n: u64) -> Self {
        self.base_params.each_n = Some(each_n);
        self
    }

    /// Send the remove query request.
    /// This will remove all records that match the query.
    /// This is a destructive operation.
    ///
    /// # Returns
    ///
    /// * `Result<u64, ReductError>` - The number of records removed.
    pub async fn send(self) -> Result<u64, ReductError> {
        let url = build_base_url(self.base_params, &self.bucket, &self.entry);
        let response = self
            .client
            .send_and_receive_json::<(), RemoveQueryInfo>(Method::DELETE, &url, None)
            .await?;
        Ok(response.removed_records)
    }
}

fn build_base_url(params: BaseQueryParameters, bucket: &str, entry: &str) -> String {
    let mut url = format!("/b/{}/{}/q?", bucket, entry);
    // filter parameters
    if let Some(start) = params.start {
        url.push_str(&format!("start={}", start));
    }

    if let Some(stop) = params.stop {
        url.push_str(&format!("&stop={}", stop));
    }

    if let Some(include) = &params.include {
        for (key, value) in include {
            url.push_str(&format!("&include-{}={}", key, value));
        }
    }
    if let Some(exclude) = &params.exclude {
        for (key, value) in exclude {
            url.push_str(&format!("&exclude-{}={}", key, value));
        }
    }

    if let Some(each_s) = params.each_s {
        url.push_str(&format!("&each_s={}", each_s));
    }

    if let Some(each_n) = params.each_n {
        url.push_str(&format!("&each_n={}", each_n));
    }
    url
}

async fn parse_batched_records(
    headers: HeaderMap,
    rx: Receiver<Bytes>,
    head_only: bool,
) -> Result<impl Stream<Item = Result<(Record, bool), ReductError>>, ReductError> {
    //sort headers by names
    let sorted_records = sort_headers_by_time(&headers)?;

    let records_total = sorted_records.iter().count();
    let mut records_count = 0;

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
                            yield Ok(bytes);
                        }
                    }))
                } else {
                    // batched records must be read in order, so it is safe to read them here
                    // instead of reading them in the use code with an async interator.
                    // The batched records are small if they are not the last.
                    // The last batched record is read in the async generator in chunks.
                    let mut data = rest_data.clone();
                    while let Ok(bytes) = rx.recv().await {
                        data.extend_from_slice(&bytes);

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
