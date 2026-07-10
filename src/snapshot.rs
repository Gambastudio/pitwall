//! Serde projection of a [`Tick`] into the `/api/state` JSON shape. The two
//! color channels (`load`, `mem_level`) are kept separate exactly as the TUI
//! renders them — a single flattened tier would drop the busy signal for a
//! runner in the memory warn band. The projection re-serializes what
//! [`crate::model::join`] already computed; it does not re-derive any tier.

use crate::app::Tick;
use crate::model::{
    elapsed_secs, mem_level, slice_total_bytes, DeployStatus, HostedStatus, Load, MemLevel,
    SourceKind,
};
use serde::Serialize;
use std::time::{SystemTime, UNIX_EPOCH};

/// The full read-only snapshot served at `GET /api/state`.
#[derive(Debug, Serialize)]
pub struct Snapshot {
    /// Epoch seconds of the last *applied* poll — see `web`'s refresh loop.
    pub generated_at: u64,
    pub runners: Vec<RunnerSnap>,
    pub hosted: Vec<HostedSnap>,
    pub vercel: Vec<VercelSnap>,
    pub slice: SliceSnap,
    /// Source-error banners in docker → native → jobs precedence.
    pub errors: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct RunnerSnap {
    pub name: String,
    /// `"docker"` or `"native"`.
    pub kind: &'static str,
    pub cpu_pct: f64,
    pub mem_bytes: u64,
    /// `0` for uncapped native runners.
    pub mem_limit: u64,
    /// CPU history (percent, oldest→newest).
    pub cpu: Vec<f64>,
    /// Memory history (fraction 0..1, oldest→newest).
    pub mem: Vec<f64>,
    /// Row channel: `"idle" | "busy" | "near_cap"` (job state; `near_cap` only
    /// when memory is Critical).
    pub load: &'static str,
    /// Mem-cell channel: `"normal" | "warn" | "critical"`, independent of `load`.
    pub mem_level: &'static str,
    /// `null` when the runner is idle.
    pub job: Option<JobSnap>,
}

#[derive(Debug, Serialize)]
pub struct JobSnap {
    pub workflow: String,
    pub job: String,
    pub branch: String,
    pub elapsed_secs: u64,
}

#[derive(Debug, Serialize)]
pub struct HostedSnap {
    pub repo: String,
    pub workflow: String,
    pub job: String,
    pub label: String,
    pub branch: String,
    /// `"in_progress"` or `"queued"`.
    pub status: &'static str,
    /// Elapsed (running) or wait (queued) seconds.
    pub elapsed_secs: u64,
}

#[derive(Debug, Serialize)]
pub struct VercelSnap {
    pub repo: String,
    pub project: String,
    pub target: String,
    pub branch: String,
    pub commit: String,
    /// `"building"` or `"queued"`.
    pub status: &'static str,
    /// Elapsed (building) or wait (queued) seconds.
    pub elapsed_secs: u64,
}

#[derive(Debug, Serialize)]
pub struct SliceSnap {
    /// Summed memory of the docker-slice runners (native runners excluded).
    pub docker_mem: u64,
    pub cap: u64,
    /// `"normal" | "warn" | "critical"` for the docker-slice total vs. `cap`.
    pub mem_level: &'static str,
    /// Slice-memory history (bytes, oldest→newest).
    pub hist: Vec<f64>,
}

fn kind_str(k: SourceKind) -> &'static str {
    match k {
        SourceKind::Docker => "docker",
        SourceKind::Native => "native",
    }
}

fn load_str(l: Load) -> &'static str {
    match l {
        Load::Idle => "idle",
        Load::Busy => "busy",
        Load::NearCap => "near_cap",
    }
}

fn mem_level_str(m: MemLevel) -> &'static str {
    match m {
        MemLevel::Normal => "normal",
        MemLevel::Warn => "warn",
        MemLevel::Critical => "critical",
    }
}

fn epoch_secs(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

impl Snapshot {
    /// Project a tick. `now` is the single clock used for `generated_at` and every
    /// elapsed/wait, so an error-only refresh (which passes the last applied time)
    /// never makes elapsed jump.
    pub fn from_tick(
        tick: &Tick,
        slice_cap_bytes: u64,
        warn_ratio: f64,
        crit_ratio: f64,
        now: SystemTime,
    ) -> Snapshot {
        let runners = tick
            .rows
            .iter()
            .map(|r| RunnerSnap {
                name: r.name.clone(),
                kind: kind_str(r.kind),
                cpu_pct: r.cpu_pct,
                mem_bytes: r.mem_bytes,
                mem_limit: r.mem_limit,
                cpu: r.cpu_hist.clone(),
                mem: r.mem_hist.clone(),
                load: load_str(r.load),
                mem_level: mem_level_str(r.mem_level),
                job: r.job.as_ref().map(|j| JobSnap {
                    workflow: j.workflow.clone(),
                    job: j.job.clone(),
                    branch: j.branch.clone(),
                    elapsed_secs: elapsed_secs(j.started_at, now),
                }),
            })
            .collect();

        let hosted = tick
            .hosted
            .iter()
            .map(|h| HostedSnap {
                repo: h.repo.clone(),
                workflow: h.workflow.clone(),
                job: h.job.clone(),
                label: h.label.clone(),
                branch: h.branch.clone(),
                status: match h.status {
                    HostedStatus::InProgress => "in_progress",
                    HostedStatus::Queued => "queued",
                },
                elapsed_secs: elapsed_secs(h.since, now),
            })
            .collect();

        let vercel = tick
            .deployments
            .iter()
            .map(|d| VercelSnap {
                repo: d.repo.clone(),
                project: d.project.clone(),
                target: d.target.clone(),
                branch: d.branch.clone(),
                commit: d.commit_summary.clone(),
                status: match d.status {
                    DeployStatus::Building => "building",
                    DeployStatus::Queued => "queued",
                },
                elapsed_secs: elapsed_secs(d.started_at, now),
            })
            .collect();

        let docker_mem = slice_total_bytes(&tick.rows);
        let slice = SliceSnap {
            docker_mem,
            cap: slice_cap_bytes,
            mem_level: mem_level_str(mem_level(
                docker_mem,
                slice_cap_bytes,
                warn_ratio,
                crit_ratio,
            )),
            hist: tick.slice_mem_hist.to_vec(),
        };

        Snapshot {
            generated_at: epoch_secs(now),
            runners,
            hosted,
            vercel,
            slice,
            errors: tick.errors.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::Tick;
    use crate::model::{Deployment, HostedJob, JobInfo, RunnerRow};
    use std::time::Duration;

    const CAP: u64 = 8 * 1024 * 1024 * 1024;

    fn row(name: &str, kind: SourceKind, load: Load, mem_level: MemLevel) -> RunnerRow {
        RunnerRow {
            name: name.into(),
            cpu_pct: 3.0,
            mem_bytes: 100,
            mem_limit: if kind == SourceKind::Native { 0 } else { CAP },
            job: None,
            load,
            mem_level,
            kind,
            cpu_hist: vec![1.0, 2.0, 3.0],
            mem_hist: vec![0.1, 0.2],
        }
    }

    #[test]
    fn projects_two_channels_and_sparklines() {
        // A busy runner in the memory warn band: load stays busy AND mem_level is
        // warn — the two channels must not collapse into one.
        let rows = vec![row(
            "ci-runner-1",
            SourceKind::Docker,
            Load::Busy,
            MemLevel::Warn,
        )];
        let (hist, hosted, deps): (Vec<f64>, Vec<HostedJob>, Vec<Deployment>) =
            (vec![100.0], vec![], vec![]);
        let tick = Tick {
            rows,
            errors: vec!["native: denied".into()],
            slice_mem_hist: &hist,
            matched_seen: 1,
            unmatched_seen: 0,
            hosted: &hosted,
            deployments: &deps,
        };
        let snap = Snapshot::from_tick(&tick, CAP, 0.85, 0.90, SystemTime::now());

        assert_eq!(snap.runners[0].load, "busy");
        assert_eq!(snap.runners[0].mem_level, "warn");
        assert_eq!(snap.runners[0].kind, "docker");
        assert_eq!(snap.runners[0].cpu, vec![1.0, 2.0, 3.0]);
        assert_eq!(snap.runners[0].mem, vec![0.1, 0.2]);
        assert!(snap.runners[0].job.is_none());
        assert_eq!(snap.slice.docker_mem, 100);
        assert_eq!(snap.slice.cap, CAP);
        assert_eq!(snap.errors, vec!["native: denied"]);
    }

    #[test]
    fn near_cap_and_critical_map_through() {
        let rows = vec![row(
            "ci-runner-2",
            SourceKind::Docker,
            Load::NearCap,
            MemLevel::Critical,
        )];
        let (hist, hosted, deps): (Vec<f64>, Vec<HostedJob>, Vec<Deployment>) =
            (vec![], vec![], vec![]);
        let tick = Tick {
            rows,
            errors: vec![],
            slice_mem_hist: &hist,
            matched_seen: 0,
            unmatched_seen: 0,
            hosted: &hosted,
            deployments: &deps,
        };
        let snap = Snapshot::from_tick(&tick, CAP, 0.85, 0.90, SystemTime::now());
        assert_eq!(snap.runners[0].load, "near_cap");
        assert_eq!(snap.runners[0].mem_level, "critical");
    }

    #[test]
    fn job_elapsed_computed_against_now() {
        let now = SystemTime::now();
        let mut r = row(
            "ci-runner-1",
            SourceKind::Docker,
            Load::Busy,
            MemLevel::Normal,
        );
        r.job = Some(JobInfo {
            workflow: "CI".into(),
            job: "Build".into(),
            branch: "main".into(),
            started_at: now - Duration::from_secs(30),
        });
        let (hist, hosted, deps): (Vec<f64>, Vec<HostedJob>, Vec<Deployment>) =
            (vec![], vec![], vec![]);
        let tick = Tick {
            rows: vec![r],
            errors: vec![],
            slice_mem_hist: &hist,
            matched_seen: 0,
            unmatched_seen: 0,
            hosted: &hosted,
            deployments: &deps,
        };
        let snap = Snapshot::from_tick(&tick, CAP, 0.85, 0.90, now);
        let job = snap.runners[0].job.as_ref().unwrap();
        assert_eq!(job.elapsed_secs, 30);
        assert_eq!(job.branch, "main");
    }
}
