use crate::config::Config;
use crate::history::History;
use crate::jobs::{self, JobsUpdate};
use crate::model::{
    join, Deployment, HostedJob, JobInfo, RunnerKey, RunnerResource, RunnerRow, SourceKind,
};
use crate::resource::ResourceUpdate;
use crate::resource_native::discover;
use crate::theme::Palette;
use crate::ui::{self, View};
use crate::vercel::{self, VercelUpdate};
use crate::{resource_docker, resource_native};
use crossterm::event::{Event, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use futures_util::StreamExt;
use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc;
use tokio::time::interval;

#[derive(Default)]
struct AppState {
    docker_resources: Vec<RunnerResource>,
    native_resources: Vec<RunnerResource>,
    docker_err: Option<String>,
    native_err: Option<String>,
    jobs: HashMap<RunnerKey, Option<JobInfo>>,
    jobs_err: Option<String>,
    hosted: Vec<HostedJob>,
    /// In-flight Vercel deployments from the last successful poll.
    deployments: Vec<Deployment>,
    history: History,
    /// From the last successful docker poll: containers whose name matched the
    /// prefix, and those that didn't. Drives the empty-state hint (a docker-only
    /// concept — native runners have no prefix).
    matched_seen: usize,
    unmatched_seen: usize,
}

impl AppState {
    /// Every currently-known runner across both sources — the join input and the
    /// history sample set.
    fn all_resources(&self) -> Vec<RunnerResource> {
        let mut all = self.docker_resources.clone();
        all.extend(self.native_resources.clone());
        all
    }
}

/// What a single [`Engine::recv`] produced. `Applied` carries a real data change
/// (advances snapshot freshness); `ErrorOnly` set a banner without changing data
/// (a docker top-level error preserving last-known-good, or a poller channel
/// closing); `Closed` means every poller channel has closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvOutcome {
    Applied,
    ErrorOnly,
    Closed,
}

/// A single projected tick: the joined rows plus the section data and error
/// banners derived from the current [`Engine`] state. Both the TUI and the portal
/// build their output from this — the one place the model is projected, so the
/// two surfaces cannot drift.
pub struct Tick<'a> {
    pub rows: Vec<RunnerRow>,
    /// Source errors in banner precedence order (docker → native → jobs); the TUI
    /// shows `.first()`, the portal exposes them all as `errors[]`.
    pub errors: Vec<String>,
    pub slice_mem_hist: &'a [f64],
    pub matched_seen: usize,
    pub unmatched_seen: usize,
    pub hosted: &'a [HostedJob],
    pub deployments: &'a [Deployment],
}

/// Owns the poll loop's accumulated state and the poller receivers, and projects
/// a [`Tick`] on demand. Discovers native runners once and spawns the docker +
/// native resource pollers, the jobs poller, and the vercel poller. Frontends
/// (the TUI `run` and the portal) drive it by awaiting [`Engine::recv`] and
/// rendering [`Engine::tick`]. Degradation: a source error never clears
/// last-known-good data — docker and native resources keep independent
/// last-known slices, jobs preservation lives in the poller, and the errors carry
/// the docker → native → jobs precedence.
pub struct Engine {
    state: AppState,
    rx_res: mpsc::Receiver<ResourceUpdate>,
    rx_jobs: mpsc::Receiver<JobsUpdate>,
    rx_vercel: mpsc::Receiver<VercelUpdate>,
    res_alive: bool,
    jobs_alive: bool,
    vercel_alive: bool,
    pub slice_cap_bytes: u64,
    pub warn_ratio: f64,
    pub crit_ratio: f64,
    pub prefix: String,
    pub multi_repo: bool,
}

impl Engine {
    /// Discover native runners once, derive the jobs poll-lists from their scopes,
    /// and spawn the four pollers. Must be called from within a tokio runtime
    /// (both frontends are `async`).
    pub fn spawn(mut cfg: Config) -> Engine {
        let slice_cap_bytes = cfg.slice_cap_bytes;
        let prefix = cfg.prefix.clone();
        let warn_ratio = cfg.warn_ratio;
        let crit_ratio = cfg.crit_ratio;

        // Discover native runners once; derive the jobs poll-lists from their scopes.
        let natives = discover();
        let (repos, orgs) = resource_native::derive_scopes(&cfg.configured_repos, &natives);
        cfg.repos = repos;
        cfg.orgs = orgs;
        // Gate the hosted `repo` column: only repo scopes can surface hosted jobs, so
        // the column is worth showing exactly when more than one repo is polled.
        let multi_repo = cfg.repos.len() > 1;

        let (tx_res, rx_res) = mpsc::channel::<ResourceUpdate>(8);
        let (tx_jobs, rx_jobs) = mpsc::channel::<JobsUpdate>(8);
        let (tx_vercel, rx_vercel) = mpsc::channel::<VercelUpdate>(8);

        tokio::spawn(resource_docker::run(cfg.clone(), tx_res.clone()));
        tokio::spawn(resource_native::run(natives, tx_res));
        tokio::spawn(jobs::run(cfg.clone(), tx_jobs));
        tokio::spawn(vercel::run(cfg.clone(), tx_vercel));

        Engine {
            state: AppState::default(),
            rx_res,
            rx_jobs,
            rx_vercel,
            res_alive: true,
            jobs_alive: true,
            vercel_alive: true,
            slice_cap_bytes,
            warn_ratio,
            crit_ratio,
            prefix,
            multi_repo,
        }
    }

    /// Await the next poller update, apply it, and report the outcome. The `else`
    /// arm is required: a `select!` over only the three `if *_alive` branches
    /// panics once every channel has closed (no branch enabled), so all-closed
    /// returns [`RecvOutcome::Closed`] instead. A closed channel flips its own
    /// `*_alive` flag so its branch is disabled on the next call.
    pub async fn recv(&mut self) -> RecvOutcome {
        tokio::select! {
            res = self.rx_res.recv(), if self.res_alive => match res {
                Some(update) => outcome(apply_resource_update(&mut self.state, update)),
                None => {
                    self.res_alive = false;
                    RecvOutcome::ErrorOnly
                }
            },
            jobs = self.rx_jobs.recv(), if self.jobs_alive => match jobs {
                Some(update) => outcome(apply_jobs_update(&mut self.state, update)),
                None => {
                    self.jobs_alive = false;
                    RecvOutcome::ErrorOnly
                }
            },
            dep = self.rx_vercel.recv(), if self.vercel_alive => match dep {
                Some(update) => outcome(apply_vercel_update(&mut self.state, update)),
                None => {
                    self.vercel_alive = false;
                    RecvOutcome::ErrorOnly
                }
            },
            else => RecvOutcome::Closed,
        }
    }

    /// Project the current state: join resources with jobs, gather the section
    /// data, and collect the source errors in banner precedence order.
    pub fn tick(&self) -> Tick<'_> {
        project_tick(&self.state, self.warn_ratio, self.crit_ratio)
    }
}

/// The projection behind [`Engine::tick`], split out so it can be unit-tested
/// against a hand-built [`AppState`] without spawning pollers.
fn project_tick(state: &AppState, warn_ratio: f64, crit_ratio: f64) -> Tick<'_> {
    let rows = join(
        state.all_resources(),
        &state.jobs,
        &state.history,
        warn_ratio,
        crit_ratio,
    );
    // Banner precedence: docker → native → jobs.
    let errors = [&state.docker_err, &state.native_err, &state.jobs_err]
        .into_iter()
        .flatten()
        .cloned()
        .collect();
    Tick {
        rows,
        errors,
        slice_mem_hist: state.history.slice_mem(),
        matched_seen: state.matched_seen,
        unmatched_seen: state.unmatched_seen,
        hosted: &state.hosted,
        deployments: &state.deployments,
    }
}

/// Map an `applied` flag from an `apply_*` call to the recv outcome.
fn outcome(applied: bool) -> RecvOutcome {
    if applied {
        RecvOutcome::Applied
    } else {
        RecvOutcome::ErrorOnly
    }
}

/// Runs the pitwall TUI event loop: spawns the [`Engine`], then drives a
/// `tokio::select!` over terminal input, engine updates, and a 1s redraw tick.
/// `engine_alive` gates the `recv` branch so that once the engine reports
/// `Closed` the branch is disabled and the loop idles on the ticker — a `Closed`
/// future is instantly-ready, so re-arming it every pass would spin the CPU.
pub async fn run(mut terminal: ratatui::DefaultTerminal, cfg: Config) -> anyhow::Result<()> {
    let palette = Palette::for_flavor(cfg.flavor);
    let mut engine = Engine::spawn(cfg);
    let mut events = EventStream::new();
    let mut ticker = interval(Duration::from_secs(1));
    let mut engine_alive = true;

    draw(&mut terminal, &engine, &palette)?;

    loop {
        tokio::select! {
            maybe_event = events.next() => {
                match maybe_event {
                    Some(Ok(Event::Key(key))) if is_quit(&key) => return Ok(()),
                    Some(Ok(_)) => {}
                    Some(Err(_)) => {}
                    None => return Ok(()), // input stream closed
                }
            }
            outcome = engine.recv(), if engine_alive => {
                if outcome == RecvOutcome::Closed {
                    engine_alive = false;
                }
            }
            _ = ticker.tick() => {}
        }
        draw(&mut terminal, &engine, &palette)?;
    }
}

/// Applies a resource poll result to the slice named by `update.source`, and (on
/// applied data) appends a history sample for *only the updated source's* runners
/// — so each runner gets one sample per its own 2s poll (the ~40s window holds)
/// — while pruning against the union so the other source's series survive.
///
/// The two sources handle their `error` differently, matching what an error
/// means for each. A **docker** error is a top-level list/connect failure: the
/// whole poll is invalid, so the last-known-good docker slice is preserved and
/// nothing is recorded. A **native** error only names the individual runners a
/// cgroup read failed for; the poller still sends the complete healthy set, so
/// it is applied (and recorded) regardless of the banner — one failed runner
/// never freezes the healthy rows.
///
/// Returns whether a real data change was applied — `false` for a docker
/// top-level error that only preserved last-known-good — so the caller can tell
/// fresh data from an error-only poll.
fn apply_resource_update(state: &mut AppState, update: ResourceUpdate) -> bool {
    let applied = match update.source {
        SourceKind::Docker => {
            state.docker_err = update.error;
            if state.docker_err.is_none() {
                state.docker_resources = update.resources;
                // matched/unmatched are a docker-prefix concept only.
                state.matched_seen = update.matched_seen;
                state.unmatched_seen = update.unmatched_seen;
                true
            } else {
                false
            }
        }
        SourceKind::Native => {
            state.native_err = update.error;
            state.native_resources = update.resources;
            true
        }
    };
    if applied {
        let all = state.all_resources();
        let sample = match update.source {
            SourceKind::Docker => &state.docker_resources,
            SourceKind::Native => &state.native_resources,
        }
        .clone();
        state.history.record(&sample, &all);
        // The memory section's slice sparkline tracks the docker-slice total only,
        // so record one aggregate sample per applied *docker* poll. Skip the
        // transient "stats not ready" empty (containers match the prefix but
        // Docker hasn't returned stats yet: `matched_seen > 0`, no resources) so
        // it can't inject a persistent baseline dip; a genuine zero
        // (`matched_seen == 0`: nothing matches) is the truth and is recorded.
        if update.source == SourceKind::Docker
            && (!state.docker_resources.is_empty() || state.matched_seen == 0)
        {
            let total: u64 = state.docker_resources.iter().map(|r| r.mem_bytes).sum();
            state.history.record_slice(total);
        }
    }
    applied
}

/// Applies a jobs poll result. Per-scope last-known-good preservation lives in
/// `jobs::run`, so here we simply replace both the data and the banner. Returns
/// whether the poll succeeded (`error` is `None`); an errored poll re-sends the
/// preserved data, i.e. no fresh change.
fn apply_jobs_update(state: &mut AppState, update: JobsUpdate) -> bool {
    let applied = update.error.is_none();
    state.jobs_err = update.error;
    state.jobs = update.jobs;
    state.hosted = update.hosted;
    applied
}

/// Applies a Vercel poll result. Sorting + the clear-on-error policy (an error
/// yields an empty list, unlike jobs' last-known-good) live in `vercel::run`, so
/// here we simply replace. Always a real change (fresh list or cleared-on-error).
fn apply_vercel_update(state: &mut AppState, update: VercelUpdate) -> bool {
    state.deployments = update.deployments;
    true
}

fn is_quit(key: &KeyEvent) -> bool {
    if key.kind != KeyEventKind::Press {
        return false;
    }
    matches!(key.code, KeyCode::Char('q') | KeyCode::Esc)
        || (key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c'))
}

fn draw(
    terminal: &mut ratatui::DefaultTerminal,
    engine: &Engine,
    palette: &Palette,
) -> anyhow::Result<()> {
    let tick = engine.tick();
    // Banner precedence: docker → native → jobs (the order `tick.errors` collects).
    let status = tick.errors.first().cloned();
    terminal.draw(|f| {
        ui::render(
            f,
            &View {
                rows: &tick.rows,
                slice_cap_bytes: engine.slice_cap_bytes,
                slice_mem_hist: tick.slice_mem_hist,
                now: SystemTime::now(),
                status,
                palette,
                prefix: &engine.prefix,
                matched_seen: tick.matched_seen,
                unmatched_seen: tick.unmatched_seen,
                warn_ratio: engine.warn_ratio,
                crit_ratio: engine.crit_ratio,
                hosted: tick.hosted,
                multi_repo: engine.multi_repo,
                deployments: tick.deployments,
            },
        );
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::HostedStatus;

    fn resource(name: &str, kind: SourceKind) -> RunnerResource {
        RunnerResource {
            name: name.into(),
            cpu_pct: 1.0,
            mem_bytes: 100,
            mem_limit: 8 * 1024 * 1024 * 1024,
            key: None,
            kind,
        }
    }

    fn docker_update(resources: Vec<RunnerResource>, error: Option<String>) -> ResourceUpdate {
        let (m, u) = if error.is_none() {
            (resources.len(), 0)
        } else {
            (0, 0)
        };
        ResourceUpdate {
            source: SourceKind::Docker,
            resources,
            matched_seen: m,
            unmatched_seen: u,
            error,
        }
    }

    fn native_update(resources: Vec<RunnerResource>, error: Option<String>) -> ResourceUpdate {
        ResourceUpdate {
            source: SourceKind::Native,
            resources,
            matched_seen: 0,
            unmatched_seen: 0,
            error,
        }
    }

    fn job() -> JobInfo {
        JobInfo {
            workflow: "ci".into(),
            job: "test".into(),
            branch: "main".into(),
            started_at: SystemTime::now(),
        }
    }

    #[test]
    fn docker_error_preserves_last_known_and_leaves_native_untouched() {
        let mut state = AppState {
            docker_resources: vec![
                resource("pulse-ci-runner-1", SourceKind::Docker),
                resource("pulse-ci-runner-2", SourceKind::Docker),
            ],
            native_resources: vec![resource("ltdovr", SourceKind::Native)],
            ..Default::default()
        };

        apply_resource_update(
            &mut state,
            docker_update(vec![], Some("docker: x".to_string())),
        );

        assert_eq!(state.docker_resources.len(), 2);
        assert_eq!(state.docker_err, Some("docker: x".to_string()));
        // Native slice untouched by a docker failure.
        assert_eq!(state.native_resources.len(), 1);
    }

    #[test]
    fn resource_success_replaces_its_own_slice() {
        let mut state = AppState {
            native_resources: vec![
                resource("ltdovr", SourceKind::Native),
                resource("scoop-vanscout", SourceKind::Native),
            ],
            native_err: Some("stale error".to_string()),
            ..Default::default()
        };

        apply_resource_update(
            &mut state,
            native_update(vec![resource("ltdovr", SourceKind::Native)], None),
        );

        assert_eq!(state.native_resources.len(), 1);
        assert!(state.native_err.is_none());
    }

    #[test]
    fn native_partial_error_applies_healthy_and_still_banners() {
        // One native runner failed to read; the poller drops it and sends the
        // healthy set with a banner. The app must apply the fresh healthy rows
        // (not freeze the whole slice) while surfacing the banner.
        let mut state = AppState {
            native_resources: vec![resource("stale-runner", SourceKind::Native)],
            ..Default::default()
        };

        apply_resource_update(
            &mut state,
            native_update(
                vec![resource("ltdovr", SourceKind::Native)],
                Some("native: scoop-vanscout: denied".to_string()),
            ),
        );

        // Healthy fresh row applied (old stale slice replaced), banner shown, and
        // the healthy runner's history recorded despite the error.
        assert_eq!(state.native_resources.len(), 1);
        assert_eq!(state.native_resources[0].name, "ltdovr");
        assert_eq!(
            state.native_err.as_deref(),
            Some("native: scoop-vanscout: denied")
        );
        assert!(!state.history.cpu("ltdovr").is_empty());
    }

    #[test]
    fn success_poll_records_history_for_both_sources() {
        let mut state = AppState::default();
        apply_resource_update(
            &mut state,
            docker_update(
                vec![resource("pulse-ci-runner-1", SourceKind::Docker)],
                None,
            ),
        );
        apply_resource_update(
            &mut state,
            native_update(vec![resource("ltdovr", SourceKind::Native)], None),
        );
        // Both runners now have history, and neither poll pruned the other's series.
        assert!(!state.history.cpu("pulse-ci-runner-1").is_empty());
        assert!(!state.history.cpu("ltdovr").is_empty());
    }

    #[test]
    fn error_poll_does_not_touch_history() {
        let mut state = AppState::default();
        apply_resource_update(
            &mut state,
            docker_update(
                vec![resource("pulse-ci-runner-1", SourceKind::Docker)],
                None,
            ),
        );
        let before = state.history.cpu("pulse-ci-runner-1").len();
        apply_resource_update(
            &mut state,
            docker_update(vec![], Some("docker: x".to_string())),
        );
        // The error poll neither appended a point nor cleared the series.
        assert_eq!(state.history.cpu("pulse-ci-runner-1").len(), before);
    }

    #[test]
    fn docker_poll_with_resources_appends_slice_point() {
        let mut state = AppState::default();
        apply_resource_update(
            &mut state,
            docker_update(
                vec![resource("pulse-ci-runner-1", SourceKind::Docker)],
                None,
            ),
        );
        // resource() has mem_bytes 100 → the slice total is that one runner.
        assert_eq!(state.history.slice_mem(), &[100.0]);
    }

    #[test]
    fn native_poll_does_not_touch_slice_series() {
        // Native runners aren't in the docker slice, so a native poll must not
        // append a slice-memory sample.
        let mut state = AppState::default();
        apply_resource_update(
            &mut state,
            native_update(vec![resource("ltdovr", SourceKind::Native)], None),
        );
        assert!(state.history.slice_mem().is_empty());
    }

    #[test]
    fn transient_empty_docker_poll_skips_slice_sample() {
        // Containers match the prefix but stats aren't ready yet (applied, empty
        // resources, matched_seen > 0). Recording 0 here would inject a baseline
        // dip, so the sample is skipped.
        let mut state = AppState::default();
        apply_resource_update(
            &mut state,
            ResourceUpdate {
                source: SourceKind::Docker,
                resources: vec![],
                matched_seen: 2,
                unmatched_seen: 0,
                error: None,
            },
        );
        assert!(state.history.slice_mem().is_empty());
    }

    #[test]
    fn genuine_zero_docker_poll_records_zero_slice_sample() {
        // Nothing matches the prefix (matched_seen == 0): 0 is the truth, so it
        // is recorded to keep the graph honest.
        let mut state = AppState::default();
        apply_resource_update(&mut state, docker_update(vec![], None));
        assert_eq!(state.history.slice_mem(), &[0.0]);
    }

    #[test]
    fn jobs_update_always_replaces() {
        // Preservation lives in the poller; the app just mirrors each update.
        let mut stale = HashMap::new();
        stale.insert(
            RunnerKey {
                scope: "erwins-enkel/pulse".into(),
                name: "runner-1".into(),
            },
            Some(job()),
        );
        let mut state = AppState {
            jobs: stale,
            jobs_err: Some("stale error".to_string()),
            ..Default::default()
        };

        let mut fresh = HashMap::new();
        fresh.insert(
            RunnerKey {
                scope: "erwins-enkel/pulse".into(),
                name: "runner-2".into(),
            },
            Some(job()),
        );
        apply_jobs_update(
            &mut state,
            JobsUpdate {
                jobs: fresh,
                hosted: Vec::new(),
                error: None,
            },
        );

        assert_eq!(state.jobs.len(), 1);
        assert!(state.jobs.contains_key(&RunnerKey {
            scope: "erwins-enkel/pulse".into(),
            name: "runner-2".into()
        }));
        assert!(state.jobs_err.is_none());
    }

    #[test]
    fn jobs_update_sets_hosted() {
        let mut state = AppState::default();
        let hosted = vec![HostedJob {
            repo: "o/r".into(),
            workflow: "CI".into(),
            job: "Build".into(),
            label: "ubuntu-latest".into(),
            branch: "main".into(),
            status: HostedStatus::InProgress,
            since: SystemTime::now(),
        }];
        apply_jobs_update(
            &mut state,
            JobsUpdate {
                jobs: HashMap::new(),
                hosted,
                error: None,
            },
        );
        assert_eq!(state.hosted.len(), 1);
        assert_eq!(state.hosted[0].job, "Build");
    }

    #[test]
    fn tick_errors_follow_docker_native_jobs_precedence() {
        // All three sources erroring: the banner list is docker → native → jobs.
        let state = AppState {
            docker_err: Some("docker: x".into()),
            native_err: Some("native: y".into()),
            jobs_err: Some("jobs: z".into()),
            ..Default::default()
        };
        let tick = project_tick(&state, 0.85, 0.90);
        assert_eq!(tick.errors, vec!["docker: x", "native: y", "jobs: z"]);

        // With docker healthy, native leads and the list only carries the present ones.
        let state = AppState {
            native_err: Some("native: y".into()),
            jobs_err: Some("jobs: z".into()),
            ..Default::default()
        };
        let tick = project_tick(&state, 0.85, 0.90);
        assert_eq!(tick.errors, vec!["native: y", "jobs: z"]);
    }

    #[test]
    fn tick_rows_match_join_for_the_same_state() {
        // The projected rows must equal a direct `join` over the union of both
        // slices — the tick is a projection, not a re-derivation.
        let state = AppState {
            docker_resources: vec![resource("pulse-ci-runner-1", SourceKind::Docker)],
            native_resources: vec![resource("ltdovr", SourceKind::Native)],
            ..Default::default()
        };
        let tick = project_tick(&state, 0.85, 0.90);
        let expected = join(
            state.all_resources(),
            &state.jobs,
            &state.history,
            0.85,
            0.90,
        );
        let got: Vec<&str> = tick.rows.iter().map(|r| r.name.as_str()).collect();
        let want: Vec<&str> = expected.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(got, want);
        assert_eq!(tick.rows.len(), 2);
    }
}
