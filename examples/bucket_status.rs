// Copyright 2025 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use reduct_rs::{ReductClient, ReductError, ResourceStatus};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), ReductError> {
    // Connect to ReductStore
    let client = ReductClient::builder()
        .url("http://127.0.0.1:8383")
        .api_token(&std::env::var("RS_API_TOKEN").unwrap_or_default())
        .build();

    println!("Server v{:?}", client.server_info().await?.version);

    // Create a bucket
    let bucket = client
        .create_bucket("status-example")
        .exist_ok(true)
        .send()
        .await?;

    // Write some data
    bucket
        .write_record("entry-1")
        .data("Sample data")
        .send()
        .await?;

    // Check bucket status - should be READY
    let info = bucket.info().await?;
    println!("Bucket status: {:?}", info.status);
    assert_eq!(info.status, ResourceStatus::Ready);

    // Check entry status - should be READY
    let entries = bucket.entries().await?;
    for entry in entries {
        println!("Entry '{}' status: {:?}", entry.name, entry.status);
        assert_eq!(entry.status, ResourceStatus::Ready);
    }

    // Demonstrate deletion behavior
    println!("\nDeleting bucket...");
    bucket.remove().await?;

    // After deletion, the bucket may be in DELETING state for large buckets
    // If you try to access it, you might get HTTP 409 (Conflict) error
    // This is expected behavior during non-blocking deletion

    // Note: For small buckets, deletion is usually instant and you'll get 404
    // For large buckets with many records, the status would be DELETING
    // and operations would return 409 until cleanup finishes

    match bucket.info().await {
        Ok(info) => {
            if info.status == ResourceStatus::Deleting {
                println!("Bucket is being deleted (DELETING state)");
                println!("Operations will return HTTP 409 until deletion completes");

                // Wait for deletion to complete
                loop {
                    sleep(Duration::from_millis(100)).await;
                    match bucket.info().await {
                        Ok(_) => {
                            // Still exists, check status again
                            continue;
                        }
                        Err(_) => {
                            // Bucket no longer exists
                            println!("Bucket deletion completed");
                            break;
                        }
                    }
                }
            } else {
                println!("Unexpected status: {:?}", info.status);
            }
        }
        Err(e) => {
            // For small buckets, deletion is instant and we get 404
            println!("Bucket deleted (instant): {}", e);
        }
    }

    Ok(())
}
