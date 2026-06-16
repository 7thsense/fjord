//! [`BlobStore`] over an S3-compatible object store (e.g. self-hosted Garage),
//! using aws-sdk-s3 with path-style addressing. This is fjord's production
//! durable-storage tier (ADR-008 differentiator: self-hosted object storage).
//!
//! Sync [`BlobStore`] bridged to the async SDK. When called from inside a tokio
//! runtime (the broker), it reuses that runtime via `block_in_place` +
//! `Handle::block_on`; when called from a plain OS thread with no ambient
//! runtime (e.g. the server-side flush thread), it falls back to a small owned
//! runtime. (Mirrors `PgCoordinator`'s bridge.)

use std::future::Future;

use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use tokio::runtime::{Handle, Runtime};

use crate::BlobStore;

/// S3-compatible blob store. Construct with [`S3BlobStore::new`] pointing at any
/// S3 endpoint (AWS, MinIO, Garage, …).
pub struct S3BlobStore {
    client: Client,
    bucket: String,
    /// Fallback runtime for callers with no ambient tokio runtime (the flush
    /// thread). Always present; unused when an ambient runtime is available.
    rt: Runtime,
}

impl S3BlobStore {
    /// Drive an async S3 op: reuse the ambient runtime if present, else the
    /// owned one. Never nests a runtime on a busy thread.
    fn block_on<F: Future>(&self, fut: F) -> F::Output {
        match Handle::try_current() {
            Ok(h) => tokio::task::block_in_place(move || h.block_on(fut)),
            Err(_) => self.rt.block_on(fut),
        }
    }
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
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("build S3 fallback runtime");
        Self {
            client: Client::from_conf(conf),
            bucket: bucket.to_string(),
            rt,
        }
    }
}

impl BlobStore for S3BlobStore {
    fn put(&self, key: &str, bytes: Vec<u8>) -> Result<(), String> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = key.to_string();
        self.block_on(async move {
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
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = key.to_string();
        self.block_on(async move {
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
    }
}
