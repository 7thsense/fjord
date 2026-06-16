//! Consumer-group coordination invariants at the coordinator contract (TD-007):
//! generation bumps on membership change, deterministic leader, offsets that
//! survive a rebalance and stay isolated per group. Run against the in-memory
//! reference and (gated) Postgres as a differential, so both share the contract.

use fjord_coordinator::{memory::MemoryCoordinator, CoordinatorStore};

fn check_groups(c: &dyn CoordinatorStore) {
    // --- membership + generation ---
    let j1 = c.join_group("grp", "m2").unwrap();
    assert_eq!(j1.members, vec!["m2"]);
    assert_eq!(j1.leader, "m2");
    let gen1 = j1.generation;

    let j2 = c.join_group("grp", "m1").unwrap();
    assert!(j2.generation > gen1, "a new member must bump the generation");
    assert_eq!(j2.leader, "m1", "leader is the lexicographically smallest member");
    assert_eq!(j2.members, vec!["m1", "m2"]);
    let gen2 = j2.generation;

    // Re-join of an EXISTING member is not a membership change → no bump.
    let j3 = c.join_group("grp", "m1").unwrap();
    assert_eq!(j3.generation, gen2, "re-joining an existing member must not bump the generation");
    assert_eq!(j3.members, vec!["m1", "m2"]);

    // --- describe ---
    let d = c.describe_group("grp").unwrap().expect("group exists");
    assert_eq!(d.generation, gen2);
    assert_eq!(d.leader, Some("m1".to_string()));
    assert_eq!(d.members, vec!["m1", "m2"]);
    assert!(c.describe_group("nope").unwrap().is_none());

    // --- offsets: committed, isolated per group ---
    c.create_topic("t", 2).unwrap();
    c.offset_commit("grp", "t", 0, 50).unwrap();
    c.offset_commit("grp", "t", 1, 60).unwrap();
    c.offset_commit("other", "t", 0, 999).unwrap();
    assert_eq!(c.offset_fetch("grp", "t", 0).unwrap(), Some(50));
    assert_eq!(c.offset_fetch("other", "t", 0).unwrap(), Some(999), "groups must be isolated");

    // --- offsets survive a rebalance (m2 leaves, m3 joins) ---
    c.leave_group("grp", "m2").unwrap();
    let j4 = c.join_group("grp", "m3").unwrap();
    assert_eq!(j4.members, vec!["m1", "m3"]);
    assert_eq!(j4.leader, "m1");
    assert!(j4.generation > gen2, "leave + join must advance the generation");
    assert_eq!(c.offset_fetch("grp", "t", 0).unwrap(), Some(50), "offsets must survive rebalance");
    assert_eq!(c.offset_fetch("grp", "t", 1).unwrap(), Some(60));

    // --- list / delete ---
    let mut lst = c.list_group_offsets("grp").unwrap();
    lst.sort();
    assert_eq!(lst, vec![("t".into(), 0, 50), ("t".into(), 1, 60)]);
    assert_eq!(c.list_group_offsets("other").unwrap(), vec![("t".into(), 0, 999)]);

    c.delete_offset("grp", "t", 0).unwrap();
    assert_eq!(c.offset_fetch("grp", "t", 0).unwrap(), None);
    assert_eq!(c.offset_fetch("grp", "t", 1).unwrap(), Some(60), "delete_offset must be partition-scoped");

    c.delete_group_offsets("grp").unwrap();
    assert!(c.list_group_offsets("grp").unwrap().is_empty());
    assert_eq!(c.offset_fetch("other", "t", 0).unwrap(), Some(999), "delete_group_offsets must not touch other groups");

    // --- empty group: generation advances, leader becomes None ---
    c.leave_group("grp", "m1").unwrap();
    c.leave_group("grp", "m3").unwrap();
    let d2 = c.describe_group("grp").unwrap().unwrap();
    assert!(d2.members.is_empty());
    assert_eq!(d2.leader, None, "empty group has no leader");
}

#[test]
fn memory_group_invariants() {
    check_groups(&MemoryCoordinator::new());
}

#[cfg(feature = "postgres-backend")]
#[test]
fn postgres_group_invariants() {
    let Ok(url) = std::env::var("FJORD_PG_URL") else {
        eprintln!("skipping postgres_group_invariants: FJORD_PG_URL not set");
        return;
    };
    let pg = fjord_coordinator::postgres::PgCoordinator::connect_fresh(&url).expect("pg connect");
    check_groups(&pg);
}
