use raft_kv::{Cluster, Command, NodeId, Role};
use std::fs;
use std::io;
use std::path::Path;
use std::process::ExitCode;
use std::time::{Duration, Instant};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("raft-demo: {err}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> io::Result<()> {
    let docs = Path::new("docs");
    fs::create_dir_all(docs)?;

    let election = election_trace();
    let failover = failover_trace();
    let replication = replication_table();
    let metrics = metrics_table();

    fs::write(docs.join("election.svg"), render_svg("Election", &election))?;
    fs::write(docs.join("failover.svg"), render_svg("Failover", &failover))?;
    fs::write(docs.join("cluster-dashboard.svg"), render_dashboard_svg())?;
    fs::write(docs.join("failover-story.svg"), render_failover_story_svg())?;
    fs::write(docs.join("log-ledger.svg"), render_log_ledger_svg())?;
    fs::write(docs.join("replication.md"), &replication)?;
    fs::write(docs.join("metrics.md"), &metrics)?;
    update_readme("README.md", &replication, &metrics)?;
    Ok(())
}

fn election_trace() -> Vec<Sample> {
    let mut cluster = Cluster::new(5);
    sample_until(&mut cluster, 600, |cluster| cluster.leader().is_some())
}

fn failover_trace() -> Vec<Sample> {
    let mut cluster = Cluster::new(5);
    assert!(cluster.run_until(600, |cluster| cluster.leader().is_some()));
    cluster.run_for(200);
    let old_leader = cluster.leader().expect("leader");
    let mut samples = vec![sample(&cluster, 0, Some(format!("kill node {old_leader}")))];
    cluster.stop(old_leader);
    for offset in (50..=600).step_by(50) {
        cluster.run_for(50);
        samples.push(sample(&cluster, offset, None));
    }
    samples
}

fn sample_until(
    cluster: &mut Cluster,
    deadline_ms: u64,
    done: impl Fn(&Cluster) -> bool,
) -> Vec<Sample> {
    let mut samples = Vec::new();
    for time in (0..=deadline_ms).step_by(50) {
        samples.push(sample(cluster, time, None));
        if done(cluster) {
            break;
        }
        cluster.run_for(50);
    }
    samples
}

fn sample(cluster: &Cluster, time_ms: u64, note: Option<String>) -> Sample {
    let mut roles: Vec<_> = cluster
        .nodes
        .iter()
        .map(|(&id, node)| (id, node.role))
        .collect();
    roles.sort_by_key(|(id, _)| *id);
    Sample {
        time_ms,
        roles,
        note,
    }
}

fn replication_table() -> String {
    let mut cluster = Cluster::new(5);
    assert!(cluster.run_until(600, |cluster| cluster.leader().is_some()));
    let leader = cluster.leader().expect("leader");
    let reply = cluster.propose(
        leader,
        Command::Set {
            key: "foo".to_string(),
            value: "bar".to_string(),
        },
    );
    assert!(reply.success);
    assert!(cluster.run_until(1200, |cluster| {
        cluster
            .nodes
            .values()
            .all(|node| node.get("foo") == Some("bar"))
    }));

    let mut out = String::from(
        "| node | role | term | commit | applied | log | kv |\n|---:|---|---:|---:|---:|---|---|\n",
    );
    let mut ids: Vec<_> = cluster.nodes.keys().copied().collect();
    ids.sort_unstable();
    for id in ids {
        let node = &cluster.nodes[&id];
        let log = node
            .log
            .iter()
            .map(|entry| command_label(&entry.command))
            .collect::<Vec<_>>()
            .join(", ");
        out.push_str(&format!(
            "| {id} | {:?} | {} | {} | {} | [{}] | foo={} |\n",
            node.role,
            node.current_term,
            node.commit_index,
            node.last_applied,
            log,
            node.get("foo").unwrap_or("∅")
        ));
    }
    out
}

fn metrics_table() -> String {
    let mut election = Cluster::new(5);
    let election_ms = first_time_until(&mut election, 1000, |cluster| cluster.leader().is_some());

    let mut failover = Cluster::new(5);
    assert!(failover.run_until(600, |cluster| cluster.leader().is_some()));
    failover.run_for(200);
    let old_leader = failover.leader().expect("leader");
    failover.stop(old_leader);
    let failover_ms = first_time_until(&mut failover, 1000, |cluster| {
        cluster.leader().is_some_and(|leader| leader != old_leader)
    });

    let mut replication = Cluster::new(5);
    assert!(replication.run_until(600, |cluster| cluster.leader().is_some()));
    let leader = replication.leader().expect("leader");
    let _ = replication.propose(
        leader,
        Command::Set {
            key: "foo".to_string(),
            value: "bar".to_string(),
        },
    );
    let replication_ms = first_time_until(&mut replication, 1000, |cluster| {
        cluster
            .nodes
            .values()
            .all(|node| node.get("foo") == Some("bar"))
    });
    let bench = benchmark_simulated_writes(3, 1_000);

    format!(
        "| metric | value |\n|---|---:|\n| cluster size tested | 5 nodes |\n| election timeout | 150–300 ms |\n| heartbeat interval | 50 ms |\n| first leader elected | {election_ms} ms simulated |\n| failover after leader kill | {failover_ms} ms simulated |\n| write visible on all nodes | {replication_ms} ms simulated |\n| simulator write throughput | {} writes/sec |\n| benchmark writes | {} writes |\n| benchmark wall time | {} ms |\n| fault tolerance | 2 failed nodes in a 5-node cluster |\n| process-level TCP tests | 1 kill/restart test |\n",
        bench.writes_per_second,
        bench.writes,
        bench.elapsed.as_millis()
    )
}

fn benchmark_simulated_writes(cluster_size: usize, writes: usize) -> BenchResult {
    let mut cluster = Cluster::new(cluster_size);
    assert!(cluster.run_until(600, |cluster| cluster.leader().is_some()));
    let leader = cluster.leader().expect("leader");
    let started = Instant::now();
    for index in 0..writes {
        let reply = cluster.propose(
            leader,
            Command::Set {
                key: format!("bench-{index}"),
                value: index.to_string(),
            },
        );
        assert!(reply.success);
    }
    assert!(cluster.run_until(20_000, |cluster| {
        cluster
            .nodes
            .values()
            .all(|node| node.get(&format!("bench-{}", writes - 1)).is_some())
    }));
    let elapsed = started.elapsed();
    let writes_per_second = ((writes as f64) / elapsed.as_secs_f64()).round() as u64;
    BenchResult {
        writes,
        elapsed,
        writes_per_second,
    }
}

struct BenchResult {
    writes: usize,
    elapsed: Duration,
    writes_per_second: u64,
}

fn first_time_until(
    cluster: &mut Cluster,
    deadline_ms: u64,
    done: impl Fn(&Cluster) -> bool,
) -> u64 {
    for time in 0..=deadline_ms {
        if done(cluster) {
            return time;
        }
        cluster.run_for(1);
    }
    deadline_ms
}

fn command_label(command: &Command) -> String {
    match command {
        Command::Noop => "noop".to_string(),
        Command::Get { key } => format!("get {key}"),
        Command::Set { key, value } => format!("set {key}={value}"),
    }
}

fn committed_cluster() -> Cluster {
    let mut cluster = Cluster::new(5);
    assert!(cluster.run_until(600, |cluster| cluster.leader().is_some()));
    let leader = cluster.leader().expect("leader");
    assert!(
        cluster
            .propose(
                leader,
                Command::Set {
                    key: "foo".to_string(),
                    value: "bar".to_string()
                }
            )
            .success
    );
    assert!(
        cluster
            .propose(
                leader,
                Command::Set {
                    key: "baz".to_string(),
                    value: "qux".to_string()
                }
            )
            .success
    );
    assert!(cluster.run_until(1400, |cluster| {
        cluster
            .nodes
            .values()
            .all(|node| node.get("baz") == Some("qux"))
    }));
    cluster
}

fn render_dashboard_svg() -> String {
    let cluster = committed_cluster();
    let leader = cluster.leader().expect("leader");
    let term = cluster.nodes[&leader].current_term;
    let commit = cluster.nodes[&leader].commit_index;
    let mut svg = svg_shell(960, 430, "raft-kv · live cluster snapshot");
    svg.push_str(&format!(
        r##"<text x="36" y="74" fill="#8b949e" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="14">term {term}</text>
<text x="160" y="74" fill="#3fb950" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="14">leader node-{leader}</text>
<text x="360" y="74" fill="#8b949e" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="14">commit index {commit}</text>
"##
    ));
    let mut ids: Vec<_> = cluster.nodes.keys().copied().collect();
    ids.sort_unstable();
    for (row, id) in ids.iter().enumerate() {
        let node = &cluster.nodes[id];
        let y = 112 + row as i32 * 54;
        let (fill, label) = role_style(node.role);
        svg.push_str(&format!(
            r##"<rect x="36" y="{}" width="888" height="40" rx="10" fill="#171a21" stroke="#30363d"/>
<text x="58" y="{}" fill="#e6e1d9" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="14">node-{id}</text>
<rect x="150" y="{}" width="92" height="24" rx="12" fill="{fill}"/>
<text x="166" y="{}" fill="#0f1115" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="12" font-weight="700">{label}</text>
<text x="278" y="{}" fill="#8b949e" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="13">log</text>
<text x="318" y="{}" fill="#3fb950" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="16">██████████</text>
<text x="520" y="{}" fill="#8b949e" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="13">commit {}</text>
<text x="650" y="{}" fill="#e6e1d9" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="13">foo={}</text>
<text x="770" y="{}" fill="#e6e1d9" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="13">baz={}</text>
"##,
            y, y + 25, y + 8, y + 24, y + 25, y + 25, y + 25, node.commit_index, y + 25, node.get("foo").unwrap_or("∅"), y + 25, node.get("baz").unwrap_or("∅")
        ));
    }
    finish_svg(svg)
}

fn render_failover_story_svg() -> String {
    let mut svg = svg_shell(960, 360, "leader failure · election · recovery");
    let panels = [
        (
            "1",
            "steady state",
            "node-4 leads",
            "logs are aligned",
            "#3fb950",
        ),
        (
            "2",
            "leader crashes",
            "node-4 stops",
            "heartbeats expire",
            "#f85149",
        ),
        (
            "3",
            "new election",
            "node-3 asks",
            "majority votes",
            "#d29922",
        ),
        (
            "4",
            "recovered",
            "node-3 leads",
            "writes continue",
            "#3fb950",
        ),
    ];
    for (index, (num, title, line1, line2, color)) in panels.iter().enumerate() {
        let x = 36 + index as i32 * 226;
        svg.push_str(&format!(
            r##"<rect x="{x}" y="86" width="198" height="210" rx="16" fill="#171a21" stroke="#30363d"/>
<circle cx="{}" cy="122" r="18" fill="{color}"/>
<text x="{}" y="128" text-anchor="middle" fill="#0f1115" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="14" font-weight="700">{num}</text>
<text x="{}" y="168" fill="#e6e1d9" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="16" font-weight="700">{title}</text>
<text x="{}" y="206" fill="#8b949e" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="13">{line1}</text>
<text x="{}" y="232" fill="#8b949e" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="13">{line2}</text>
"##,
            x + 99, x + 99, x + 22, x + 22, x + 22
        ));
    }
    finish_svg(svg)
}

fn render_log_ledger_svg() -> String {
    let cluster = committed_cluster();
    let mut svg = svg_shell(960, 410, "replicated log ledger");
    let mut ids: Vec<_> = cluster.nodes.keys().copied().collect();
    ids.sort_unstable();
    for (row, id) in ids.iter().enumerate() {
        let node = &cluster.nodes[id];
        let y = 88 + row as i32 * 58;
        let (fill, label) = role_style(node.role);
        svg.push_str(&format!(
            r##"<text x="38" y="{}" fill="#e6e1d9" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="14">node-{id}</text>
<rect x="120" y="{}" width="92" height="24" rx="12" fill="{fill}"/>
<text x="136" y="{}" fill="#0f1115" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="12" font-weight="700">{label}</text>
"##,
            y + 22, y + 4, y + 20
        ));
        for (col, entry) in node.log.iter().enumerate() {
            let x = 248 + col as i32 * 150;
            svg.push_str(&format!(
                r##"<rect x="{x}" y="{y}" width="132" height="32" rx="8" fill="#21262d" stroke="#30363d"/>
<text x="{}" y="{}" fill="#e6e1d9" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="12">{}</text>
"##,
                x + 12,
                y + 21,
                escape(&command_label(&entry.command))
            ));
        }
        svg.push_str(&format!(
            r##"<text x="820" y="{}" fill="#3fb950" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="14">commit ✓</text>
"##,
            y + 22
        ));
    }
    finish_svg(svg)
}

fn svg_shell(width: i32, height: i32, title: &str) -> String {
    format!(
        r##"<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
<rect width="100%" height="100%" rx="18" fill="#0f1115"/>
<rect x="18" y="18" width="{}" height="{}" rx="14" fill="#11161d" stroke="#30363d"/>
<text x="36" y="48" fill="#e6e1d9" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="20" font-weight="700">{}</text>
"##,
        width - 36,
        height - 36,
        escape(title)
    )
}

fn finish_svg(mut svg: String) -> String {
    svg.push_str("</svg>\n");
    svg
}

fn role_style(role: Role) -> (&'static str, &'static str) {
    match role {
        Role::Follower => ("#8b949e", "FOLLOWER"),
        Role::Candidate => ("#d29922", "CANDIDATE"),
        Role::Leader => ("#3fb950", "LEADER"),
    }
}

#[derive(Clone, Debug)]
struct Sample {
    time_ms: u64,
    roles: Vec<(NodeId, Role)>,
    note: Option<String>,
}

fn render_svg(title: &str, samples: &[Sample]) -> String {
    let node_count = samples.first().map_or(0, |sample| sample.roles.len());
    let width = 920;
    let left = 88;
    let top = 72;
    let cell_w = 56;
    let cell_h = 34;
    let row_gap = 14;
    let height = top + node_count as i32 * (cell_h + row_gap) + 48;
    let mut svg = format!(
        r##"<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
<rect width="100%" height="100%" fill="#101114"/>
<text x="24" y="34" fill="#f4f1ea" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="20" font-weight="700">raft-kv · {title}</text>
<text x="24" y="56" fill="#9ca3af" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="12">green leader · yellow candidate · gray follower</text>
"##
    );
    for node in 0..node_count {
        let y = top + node as i32 * (cell_h + row_gap);
        svg.push_str(&format!(
            r##"<text x="24" y="{}" fill="#d1d5db" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="13">node {node}</text>
"##,
            y + 22
        ));
    }
    for (column, sample) in samples.iter().enumerate() {
        let x = left + column as i32 * cell_w;
        svg.push_str(&format!(
            r##"<text x="{}" y="66" fill="#6b7280" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="10">{}ms</text>
"##,
            x, sample.time_ms
        ));
        if let Some(note) = &sample.note {
            svg.push_str(&format!(
                r##"<text x="{}" y="{}" fill="#f87171" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="11">{}</text>
"##,
                x,
                height - 18,
                escape(note)
            ));
        }
        for (row, (_, role)) in sample.roles.iter().enumerate() {
            let y = top + row as i32 * (cell_h + row_gap);
            let (fill, label) = match role {
                Role::Follower => ("#374151", "F"),
                Role::Candidate => ("#d97706", "C"),
                Role::Leader => ("#16a34a", "L"),
            };
            svg.push_str(&format!(
                r##"<rect x="{x}" y="{y}" width="44" height="28" rx="6" fill="{fill}"/>
<text x="{}" y="{}" fill="#fff7ed" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="13" font-weight="700">{label}</text>
"##,
                x + 17,
                y + 19
            ));
        }
    }
    svg.push_str("</svg>\n");
    svg
}

fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn update_readme(path: &str, replication: &str, metrics: &str) -> io::Result<()> {
    let Ok(readme) = fs::read_to_string(path) else {
        return Ok(());
    };
    let readme = replace_section(&readme, "replication", replication);
    let readme = replace_section(&readme, "metrics", metrics);
    fs::write(path, readme)
}

fn replace_section(readme: &str, name: &str, content: &str) -> String {
    let start = format!("<!-- {name}:start -->");
    let end = format!("<!-- {name}:end -->");
    let Some(start_index) = readme.find(&start) else {
        return readme.to_string();
    };
    let Some(end_index) = readme.find(&end) else {
        return readme.to_string();
    };
    let before = &readme[..start_index + start.len()];
    let after = &readme[end_index..];
    format!("{before}\n{}\n{after}", content.trim_end())
}
