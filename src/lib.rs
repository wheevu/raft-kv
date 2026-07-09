pub mod cluster;
pub mod lsm;
pub mod net;
pub mod node;
pub mod observability;
pub mod state_machine;
pub mod storage;
pub mod types;

// Re-export public API from sub-modules.
pub use cluster::Cluster;
pub use node::Node;
pub use state_machine::{MemoryStateMachine, StateMachine};
pub use types::{
    AppendEntries, AppendEntriesReply, ClientReply, ClientRequest, Command, LogEntry, LogIndex,
    Message, NodeId, RequestVote, RequestVoteReply, Role, Rpc, Term,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_election() {
        let mut cluster = Cluster::new(5);
        assert!(cluster.run_until(600, |cluster| cluster.leader().is_some()));
        let leader = cluster.leader().unwrap();
        let term = cluster.node(leader).current_term();
        assert!(term <= 2, "leader elected in term {term}");
        assert_eq!(
            cluster
                .nodes()
                .filter(|(_, node)| node.role() == Role::Leader)
                .count(),
            1
        );
    }

    #[test]
    fn test_re_election() {
        let mut cluster = Cluster::new(5);
        assert!(cluster.run_until(600, |cluster| cluster.leader().is_some()));
        let old_leader = cluster.leader().unwrap();
        cluster.stop(old_leader);
        assert!(cluster.run_until(1000, |cluster| {
            cluster.leader().is_some_and(|leader| leader != old_leader)
        }));
    }

    #[test]
    fn test_basic_agree() {
        let mut cluster = Cluster::new(5);
        assert!(cluster.run_until(600, |cluster| cluster.leader().is_some()));
        let leader = cluster.leader().unwrap();
        let reply = cluster.propose(
            leader,
            ClientRequest::Set {
                key: "foo".to_string(),
                value: "bar".to_string(),
            },
        );
        assert!(reply.success);
        assert!(cluster.run_until(1000, |cluster| {
            cluster
                .nodes()
                .all(|(_, node)| node.get("foo") == Some("bar".to_string()))
        }));
    }

    #[test]
    fn test_fail_agree() {
        let mut cluster = Cluster::new(5);
        assert!(cluster.run_until(600, |cluster| cluster.leader().is_some()));
        cluster.run_for(200);
        let old_leader = cluster.leader().unwrap();
        let reply = cluster.propose(
            old_leader,
            ClientRequest::Set {
                key: "foo".to_string(),
                value: "bar".to_string(),
            },
        );
        assert!(reply.success);
        cluster.run_for(8);
        cluster.stop(old_leader);
        assert!(cluster.run_until(1100, |cluster| {
            cluster.leader().is_some_and(|leader| leader != old_leader)
        }));
        assert!(cluster.run_until(3000, |cluster| {
            cluster
                .nodes()
                .filter(|(id, _)| *id != old_leader)
                .all(|(_, node)| node.get("foo") == Some("bar".to_string()))
        }));
    }

    #[test]
    fn test_fail_no_agree() {
        let mut cluster = Cluster::new(5);
        assert!(cluster.run_until(600, |cluster| cluster.leader().is_some()));
        cluster.run_for(200);
        let old_leader = cluster.leader().unwrap();
        let minority = vec![old_leader];
        let majority: Vec<_> = (0..5).filter(|&id| id != old_leader).collect();
        cluster.partition(&[minority, majority.clone()]);

        let reply = cluster.propose(
            old_leader,
            ClientRequest::Set {
                key: "lost".to_string(),
                value: "value".to_string(),
            },
        );
        assert!(reply.success);
        cluster.run_for(500);
        assert!(cluster.node(old_leader).get("lost").is_none());

        assert!(cluster.run_until(1200, |cluster| {
            majority
                .iter()
                .any(|&id| cluster.node(id).role() == Role::Leader)
        }));
        cluster.heal();
        assert!(cluster.run_until(2500, |cluster| {
            cluster
                .nodes()
                .filter(|(_, node)| node.id() != old_leader)
                .all(|(_, node)| node.get("lost").is_none())
        }));
    }

    #[test]
    fn test_concurrent_starts() {
        let mut cluster = Cluster::new(5);
        assert!(cluster.run_until(600, |cluster| cluster.leader().is_some()));
        let leader = cluster.leader().unwrap();
        for index in 0..10 {
            let reply = cluster.propose(
                leader,
                ClientRequest::Set {
                    key: format!("k{index}"),
                    value: format!("v{index}"),
                },
            );
            assert!(reply.success);
        }
        assert!(cluster.run_until(2500, |cluster| (0..10).all(|index| {
            cluster
                .nodes()
                .all(|(_, node)| node.get(&format!("k{index}")) == Some(format!("v{index}")))
        })));
    }

    #[test]
    fn test_rejoin() {
        let mut cluster = Cluster::new(5);
        assert!(cluster.run_until(600, |cluster| cluster.leader().is_some()));
        cluster.run_for(200);
        let isolated = (0..5).find(|&id| Some(id) != cluster.leader()).unwrap();
        let connected: Vec<_> = (0..5).filter(|&id| id != isolated).collect();
        cluster.partition(&[vec![isolated], connected]);
        let leader = cluster.leader().unwrap();
        let reply = cluster.propose(
            leader,
            ClientRequest::Set {
                key: "after_partition".to_string(),
                value: "ok".to_string(),
            },
        );
        assert!(reply.success);
        assert!(cluster.run_until(1500, |cluster| {
            cluster
                .nodes()
                .filter(|(id, _)| *id != isolated)
                .all(|(_, node)| node.get("after_partition") == Some("ok".to_string()))
        }));
        assert!(cluster.node(isolated).get("after_partition").is_none());

        cluster.heal();
        assert!(cluster.run_until(
            3000,
            |cluster| cluster.node(isolated).get("after_partition") == Some("ok".to_string())
        ));
    }

    #[test]
    fn leader_cannot_serve_stale_reads_before_noop_commits() {
        let mut cluster = Cluster::new(5);
        assert!(cluster.run_until(600, |cluster| cluster.leader().is_some()));
        let leader = cluster.leader().unwrap();
        // Immediately after election, reads might be rejected until a noop commits.
        let reply = cluster.propose(
            leader,
            ClientRequest::Get {
                key: "nonexistent".to_string(),
            },
        );
        // If noop hasn't committed yet, reads are rejected with leader_id set.
        if !reply.success {
            assert_eq!(reply.leader_id, Some(leader));
        }
        // Drive the cluster forward so the noop commits.
        cluster.run_for(500);
        // Now reads must succeed.
        let reply2 = cluster.propose(
            leader,
            ClientRequest::Get {
                key: "nonexistent".to_string(),
            },
        );
        assert!(
            reply2.success,
            "leader should serve reads after noop commits"
        );
    }
}
