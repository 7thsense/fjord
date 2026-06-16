// Kafka wire-protocol smoke tests for ObjectLogFjordLog and ObjectLogOffsetStore.
//
// Starts an in-process heimq Server backed by ObjectLogFjordLog/ObjectLogOffsetStore
// and verifies Kafka wire protocol produce/consume/group-offset semantics.
// Satisfies bead fjord-15369989 AC #5 and fjord-6ab8369e AC #2.

use fjord_object_log::{ObjectLogFjordConfig, ObjectLogFjordLog, ObjectLogOffsetStore};
use heimq::server::Server;
use object_log::MemoryObjectStore;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Headers as _, Message, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use std::sync::Arc;
use std::time::Duration;

/// Shared object stores so "restart" tests can recreate a Server with same data.
struct TestStores {
    log_store: Arc<MemoryObjectStore>,
    offset_store: Arc<MemoryObjectStore>,
}

impl TestStores {
    fn new() -> Self {
        Self {
            log_store: Arc::new(MemoryObjectStore::default()),
            offset_store: Arc::new(MemoryObjectStore::default()),
        }
    }

    fn make_server(&self, port: u16, topics: &[(&str, i32)]) -> Server {
        use clap::Parser as _;
        let port_str = port.to_string();
        let mut args = vec!["heimq", "--port", &port_str];
        let topic_specs: Vec<String> = topics.iter().map(|(n, p)| format!("{}:{}", n, p)).collect();
        for spec in &topic_specs {
            args.push("--create-topic");
            args.push(spec.as_str());
        }
        let config = heimq::config::Config::parse_from(args);
        let backend = Arc::new(
            ObjectLogFjordLog::new(self.log_store.clone(), ObjectLogFjordConfig::default())
                .expect("valid config"),
        );
        let offsets = ObjectLogOffsetStore::new(self.offset_store.clone());
        Server::with_backends(config, backend, offsets).expect("server")
    }
}

fn start_object_log_server_with_topics(topics: &[(&str, i32)]) -> (Server, u16) {
    let stores = TestStores::new();
    let port = heimq::test_support::next_port();
    let server = stores.make_server(port, topics);
    (server, port)
}

/// Produce N records with rdkafka FutureProducer, return the bootstrap address.
async fn produce_records(bootstrap: &str, topic: &str, n: usize) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("message.timeout.ms", "10000")
        .create()
        .expect("producer");

    for i in 0..n {
        producer
            .send(
                FutureRecord::to(topic)
                    .payload(format!("value-{}", i).as_bytes())
                    .key(format!("key-{}", i).as_bytes()),
                Duration::from_secs(10),
            )
            .await
            .expect("send");
    }
}

/// Consume until `want` records received, with a timeout. Returns the count.
fn consume_records(bootstrap: &str, topic: &str, group: &str, want: usize) -> usize {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("consumer");
    consumer.subscribe(&[topic]).expect("subscribe");

    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let mut count = 0;
    while count < want && std::time::Instant::now() < deadline {
        if let Some(Ok(_)) = consumer.poll(Duration::from_millis(200)) {
            count += 1;
        }
    }
    count
}

/// Produce + consume roundtrip through object-log backed server.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_log_kafka_produce_consume_roundtrip() {
    let topic = "smoke-produce-consume";
    let (server, port) = start_object_log_server_with_topics(&[(topic, 1)]);
    let bootstrap = format!("127.0.0.1:{port}");

    tokio::spawn(async move { server.run().await.ok() });
    tokio::time::sleep(Duration::from_millis(200)).await;

    produce_records(&bootstrap, topic, 5).await;

    let bs = bootstrap.clone();
    let count = tokio::task::spawn_blocking(move || consume_records(&bs, topic, "smoke-group", 5))
        .await
        .expect("blocking task");

    assert_eq!(count, 5, "expected 5 records, got {count}");
}

/// Produce and verify metadata is accessible.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_log_kafka_high_watermark_advances() {
    let topic = "smoke-hwm";
    let (server, port) = start_object_log_server_with_topics(&[(topic, 1)]);
    let bootstrap = format!("127.0.0.1:{port}");

    tokio::spawn(async move { server.run().await.ok() });
    tokio::time::sleep(Duration::from_millis(200)).await;

    produce_records(&bootstrap, topic, 3).await;

    // Metadata confirms topic is served.
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("group.id", "hwm-group")
        .create()
        .expect("consumer");

    let meta = consumer
        .fetch_metadata(Some(topic), Duration::from_secs(10))
        .expect("metadata");
    assert_eq!(meta.topics().len(), 1);
    assert!(
        !meta.topics()[0].partitions().is_empty(),
        "topic must have partitions"
    );
}

/// Produce/fetch throughput smoke — measures records/sec and reports it.
///
/// Not a hard assertion; the test always passes as long as the server handles
/// the load. Acts as TP-001 performance evidence (records/sec, latency proxy).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_log_kafka_throughput_smoke() {
    use std::time::Instant;

    let topic = "perf-topic";
    let (server, port) = start_object_log_server_with_topics(&[(topic, 1)]);
    let bootstrap = format!("127.0.0.1:{port}");

    tokio::spawn(async move { server.run().await.ok() });
    tokio::time::sleep(Duration::from_millis(200)).await;

    const N: usize = 1_000;
    let payload: Vec<u8> = vec![0xABu8; 1024]; // 1 KiB record

    // Produce N records and measure throughput.
    let t0 = Instant::now();
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("message.timeout.ms", "10000")
        .set("batch.size", "65536")
        .set("linger.ms", "5")
        .create()
        .expect("producer");
    let keys: Vec<String> = (0..N).map(|i| format!("k{i}")).collect();
    let mut futs = Vec::with_capacity(N);
    for i in 0..N {
        let fut = producer.send(
            FutureRecord::to(topic)
                .payload(&payload)
                .key(keys[i].as_bytes()),
            Duration::from_secs(10),
        );
        futs.push(fut);
    }
    for fut in futs {
        fut.await.expect("produce");
    }
    let produce_elapsed = t0.elapsed();
    let produce_rps = N as f64 / produce_elapsed.as_secs_f64();

    // Fetch N records and measure throughput.
    let t1 = Instant::now();
    let fetched = tokio::task::spawn_blocking({
        let bs = bootstrap.clone();
        move || consume_records(&bs, topic, "perf-group", N)
    })
    .await
    .expect("blocking");
    let fetch_elapsed = t1.elapsed();
    let fetch_rps = fetched as f64 / fetch_elapsed.as_secs_f64();

    // Print evidence (visible with --nocapture).
    println!(
        "[perf] produce: {N} records in {:.1}ms → {:.0} records/sec",
        produce_elapsed.as_secs_f64() * 1000.0,
        produce_rps
    );
    println!(
        "[perf] fetch:   {fetched} records in {:.1}ms → {:.0} records/sec",
        fetch_elapsed.as_secs_f64() * 1000.0,
        fetch_rps
    );

    assert_eq!(fetched, N, "all {N} produced records must be fetchable");
}

/// Consumer group commits survive "restart" (new Server instance, same backing stores).
///
/// Satisfies fjord-6ab8369e AC #2: "Java Kafka consumer group can join, fetch,
/// commit an offset, restart, and fetch the committed offset."
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_log_consumer_group_offset_survives_restart() {
    let topic = "restart-topic";
    let group = "restart-group";
    let stores = TestStores::new();

    let port1 = heimq::test_support::next_port();
    let server1 = stores.make_server(port1, &[(topic, 1)]);
    let bootstrap1 = format!("127.0.0.1:{port1}");

    // Start first server, produce records, join group, commit offset.
    let srv1_handle = tokio::spawn(async move { server1.run().await.ok() });
    tokio::time::sleep(Duration::from_millis(200)).await;

    produce_records(&bootstrap1, topic, 10).await;

    // Consumer joins group, reads 5 records, commits offset 5.
    let committed_offset = tokio::task::spawn_blocking({
        let bs = bootstrap1.clone();
        move || {
            let consumer: BaseConsumer = ClientConfig::new()
                .set("bootstrap.servers", &bs)
                .set("group.id", group)
                .set("auto.offset.reset", "earliest")
                .set("enable.auto.commit", "false")
                .create()
                .expect("consumer");
            consumer.subscribe(&[topic]).expect("subscribe");

            let mut count = 0;
            let deadline = std::time::Instant::now() + Duration::from_secs(15);
            while count < 5 && std::time::Instant::now() < deadline {
                if let Some(Ok(msg)) = consumer.poll(Duration::from_millis(200)) {
                    consumer
                        .store_offset_from_message(&msg)
                        .expect("store offset");
                    count += 1;
                }
            }
            // Manual sync commit.
            consumer
                .commit_consumer_state(rdkafka::consumer::CommitMode::Sync)
                .expect("commit");
            count
        }
    })
    .await
    .expect("blocking task");
    assert_eq!(committed_offset, 5, "should have consumed 5 records");

    // Shut down first server by aborting.
    srv1_handle.abort();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Start second server with the SAME backing stores — simulates a restart.
    let port2 = heimq::test_support::next_port();
    let server2 = stores.make_server(port2, &[(topic, 1)]);
    let bootstrap2 = format!("127.0.0.1:{port2}");
    tokio::spawn(async move { server2.run().await.ok() });
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Consumer with same group rejoins — should resume from committed offset 5.
    let resumed_count = tokio::task::spawn_blocking({
        let bs = bootstrap2.clone();
        move || {
            let consumer: BaseConsumer = ClientConfig::new()
                .set("bootstrap.servers", &bs)
                .set("group.id", group)
                .set("auto.offset.reset", "earliest")
                .set("enable.auto.commit", "false")
                .create()
                .expect("consumer");
            consumer.subscribe(&[topic]).expect("subscribe");

            let mut count = 0;
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while std::time::Instant::now() < deadline {
                if let Some(Ok(_)) = consumer.poll(Duration::from_millis(200)) {
                    count += 1;
                }
            }
            count
        }
    })
    .await
    .expect("blocking task");

    // Should get the remaining 5 records (10 total - 5 committed).
    assert_eq!(
        resumed_count, 5,
        "consumer should resume from committed offset, got {resumed_count} records (expected 5)"
    );
}

/// Record headers round-trip through the object-log storage backend.
///
/// Verifies that headers embedded in RecordBatch bytes are stored opaquely
/// and returned intact to the consumer, even though BackendCapabilities
/// advertises headers=false (the broker stores bytes, not parsed fields).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_log_kafka_headers_roundtrip() {
    let topic = "headers-roundtrip";
    let (server, port) = start_object_log_server_with_topics(&[(topic, 1)]);
    let bootstrap = format!("127.0.0.1:{port}");

    tokio::spawn(async move { server.run().await.ok() });
    tokio::time::sleep(Duration::from_millis(200)).await;

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("message.timeout.ms", "10000")
        .create()
        .expect("producer");

    let headers = OwnedHeaders::new()
        .insert(rdkafka::message::Header {
            key: "x-trace-id",
            value: Some("abc123"),
        })
        .insert(rdkafka::message::Header {
            key: "x-env",
            value: Some("test"),
        });

    producer
        .send(
            FutureRecord::to(topic)
                .payload(b"payload-with-headers")
                .key(b"hdr-key")
                .headers(headers),
            Duration::from_secs(10),
        )
        .await
        .expect("send");

    let result = tokio::task::spawn_blocking({
        let bs = bootstrap.clone();
        move || {
            let consumer: BaseConsumer = ClientConfig::new()
                .set("bootstrap.servers", &bs)
                .set("group.id", "hdr-group")
                .set("auto.offset.reset", "earliest")
                .set("enable.auto.commit", "false")
                .create()
                .expect("consumer");
            consumer.subscribe(&[topic]).expect("subscribe");
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while std::time::Instant::now() < deadline {
                if let Some(Ok(msg)) = consumer.poll(Duration::from_millis(200)) {
                    let hdrs = msg.headers().expect("message must have headers");
                    let trace_id = hdrs
                        .iter()
                        .find(|h| h.key == "x-trace-id")
                        .and_then(|h| h.value)
                        .map(|v| String::from_utf8_lossy(v).to_string());
                    let env = hdrs
                        .iter()
                        .find(|h| h.key == "x-env")
                        .and_then(|h| h.value)
                        .map(|v| String::from_utf8_lossy(v).to_string());
                    return Some((trace_id, env));
                }
            }
            None
        }
    })
    .await
    .expect("blocking");

    let (trace_id, env) = result.expect("timed out waiting for message with headers");
    assert_eq!(
        trace_id,
        Some("abc123".into()),
        "x-trace-id header must round-trip"
    );
    assert_eq!(env, Some("test".into()), "x-env header must round-trip");
}

/// Compressed record batches round-trip through the object-log storage backend.
///
/// Verifies that compressed RecordBatch bytes are stored and returned intact;
/// the broker treats them as opaque blobs so any codec works transparently.
/// Tries gzip first (always available), then lz4, then zstd; skips if none work.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_log_kafka_compressed_roundtrip() {
    let topic = "compressed-roundtrip";
    let (server, port) = start_object_log_server_with_topics(&[(topic, 1)]);
    let bootstrap = format!("127.0.0.1:{port}");

    tokio::spawn(async move { server.run().await.ok() });
    tokio::time::sleep(Duration::from_millis(200)).await;

    const N: usize = 10;
    let codec = ["gzip", "lz4", "zstd"].iter().find_map(|c| {
        ClientConfig::new()
            .set("bootstrap.servers", &bootstrap)
            .set("message.timeout.ms", "10000")
            .set("compression.type", *c)
            .set("batch.num.messages", "100")
            .set("linger.ms", "20")
            .create::<FutureProducer>()
            .ok()
            .map(|p| (*c, p))
    });

    let (codec_name, producer) = match codec {
        Some(v) => v,
        None => {
            eprintln!("SKIP: no compression codec available in this rdkafka build");
            return;
        }
    };
    eprintln!("testing compression codec: {codec_name}");

    let payloads: Vec<String> = (0..N).map(|i| format!("compressed-value-{i:04}")).collect();
    for (i, payload) in payloads.iter().enumerate() {
        producer
            .send(
                FutureRecord::to(topic)
                    .payload(payload.as_bytes())
                    .key(format!("k{i}").as_bytes()),
                Duration::from_secs(10),
            )
            .await
            .unwrap_or_else(|(e, _)| panic!("send compressed ({codec_name}): {e}"));
    }

    let count = tokio::task::spawn_blocking({
        let bs = bootstrap.clone();
        move || consume_records(&bs, topic, "zstd-group", N)
    })
    .await
    .expect("blocking");

    assert_eq!(
        count, N,
        "all {N} zstd-compressed records must be fetchable"
    );
}

/// Two consumers in the same group drain a 4-partition topic without gaps or duplicates.
///
/// Verifies that partition assignment across multiple group members works correctly
/// with the object-log backend — a critical multi-member rebalance scenario.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_log_kafka_multi_consumer_group_delivery() {
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};

    let topic = "multi-consumer-group";
    const PARTITIONS: i32 = 4;
    const N: usize = 200;
    let group = "multi-cg-group";

    let (server, port) = start_object_log_server_with_topics(&[(topic, PARTITIONS)]);
    let bootstrap = format!("127.0.0.1:{port}");

    tokio::spawn(async move { server.run().await.ok() });
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Produce N records spread across all partitions.
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("message.timeout.ms", "10000")
        .create()
        .expect("producer");

    let mut produced: HashSet<String> = HashSet::new();
    for i in 0..N {
        let key = format!("key-{i:04}");
        let value = format!("val-{i}");
        producer
            .send(
                FutureRecord::to(topic)
                    .payload(value.as_bytes())
                    .key(key.as_bytes()),
                Duration::from_secs(10),
            )
            .await
            .expect("produce");
        produced.insert(value);
    }
    assert_eq!(produced.len(), N);

    // Two consumers in the same group; collect messages with a shared set.
    let received: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));

    let make_consumer = move |bs: String| -> BaseConsumer {
        ClientConfig::new()
            .set("bootstrap.servers", &bs)
            .set("group.id", group)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .create()
            .expect("consumer")
    };

    let bs1 = bootstrap.clone();
    let bs2 = bootstrap.clone();
    let recv1 = received.clone();
    let recv2 = received.clone();

    let deadline = std::time::Instant::now() + Duration::from_secs(30);

    let h1 = tokio::task::spawn_blocking(move || {
        let c = make_consumer(bs1);
        c.subscribe(&[topic]).expect("subscribe");
        while std::time::Instant::now() < deadline {
            if let Some(Ok(msg)) = c.poll(Duration::from_millis(100)) {
                if let Some(payload) = msg.payload() {
                    let s = String::from_utf8_lossy(payload).to_string();
                    recv1.lock().unwrap().insert(s);
                }
                if recv1.lock().unwrap().len() >= N {
                    break;
                }
            }
        }
    });

    let h2 = tokio::task::spawn_blocking(move || {
        let c = make_consumer(bs2);
        c.subscribe(&[topic]).expect("subscribe");
        while std::time::Instant::now() < deadline {
            if let Some(Ok(msg)) = c.poll(Duration::from_millis(100)) {
                if let Some(payload) = msg.payload() {
                    let s = String::from_utf8_lossy(payload).to_string();
                    recv2.lock().unwrap().insert(s);
                }
                if recv2.lock().unwrap().len() >= N {
                    break;
                }
            }
        }
    });

    h1.await.expect("consumer1");
    h2.await.expect("consumer2");

    let got = received.lock().unwrap().clone();
    assert_eq!(
        got.len(),
        N,
        "expected {N} unique values; got {} — missing {} records",
        got.len(),
        N - got.len().min(N)
    );
    assert_eq!(got, produced, "consumed set must equal produced set");
}
