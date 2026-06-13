// @covers Slices 5-6 per-trait suites
use fjord_broker::{FjordLog, FjordOffsetStore};
use heimq_testkit::suites;

#[test]
fn fjord_log_backend_suite() {
    let log = FjordLog::new();
    suites::log_backend::run_all(&log);
}

#[test]
fn fjord_offset_store_suite() {
    let store = FjordOffsetStore::new();
    suites::offset_store::run_all(store.as_ref());
}
