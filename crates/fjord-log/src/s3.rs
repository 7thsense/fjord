//! [`BlobStore`] over an S3-compatible object store (e.g. self-hosted Garage),
//! using aws-sdk-s3 with path-style addressing. This is fjord's production
//! durable-storage tier (ADR-008 differentiator: self-hosted object storage).
//!
//! Sync [`BlobStore`] bridged to the async SDK via `block_in_place` + the
//! current runtime handle; must run under a multi-thread tokio runtime.

use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use tokio::runtime::Handle;

use crate::BlobStore;

/// S3-compatible blob store. Construct with [`S3BlobStore::new`] pointing at any
/// S3 endpoint (AWS, MinIO, Garage, …).
pub struct S3BlobStore {
    client: Client,
    bucket: String,
}

impl S3BlobStore {
    /// Build a client for an S3-compatible endpoint with static credentials and
    /// path-style addressing (required by Garage/MinIO).
    pub fn new(
        endpoint_url: &str,
        region: &str,
        bucket: &str,
        access_key_id: &str,
        secret_access_key: &str,
    ) -> Self {
        let creds = Credentials::new(access_key_id, secret_access_key, None, None, "fjord-static");
        let conf = aws_sdk_s3::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .endpoint_url(endpoint_url)
            .region(Region::new(region.to_string()))
            .credentials_provider(creds)
            .force_path_style(true)
            .build();
        Self {
            client: Client::from_conf(conf),
            bucket: bucket.to_string(),
        }
    }
}

impl BlobStore for S3BlobStore {
    fn put(&self, key: &str, bytes: Vec<u8>) -> Result<(), String> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = key.to_string();
        tokio::task::block_in_place(|| {
            Handle::current().block_on(async move {
                client
                    .put_object()
                    .bucket(bucket)
                    .key(key)
                    .body(ByteStream::from(bytes))
                    .send()
                    .await
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            })
        })
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = key.to_string();
        tokio::task::block_in_place(|| {
            Handle::current().block_on(async move {
                match client.get_object().bucket(bucket).key(key).send().await {
                    Ok(resp) => {
                        let data = resp.body.collect().await.map_err(|e| e.to_string())?;
                        Ok(Some(data.into_bytes().to_vec()))
                    }
                    Err(err) => {
                        let svc = err.into_service_error();
                        if svc.is_no_such_key() {
                            Ok(None)
                        } else {
                            Err(svc.to_string())
                        }
                    }
                }
            })
        })
    }
}
