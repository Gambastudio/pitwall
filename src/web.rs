//! The portal web server: a background task that projects the [`Engine`]'s ticks
//! into an `Arc<RwLock<Snapshot>>`, and an axum server that serves the embedded
//! single page plus `GET /api/state`.
//!
//! Refresh discipline mirrors the TUI's last-known-good rule: `generated_at` (and
//! every elapsed) tracks the last *applied* poll, so a docker error-only poll
//! surfaces its banner in `errors[]` without advancing the data timestamp.

use crate::app::{Engine, RecvOutcome};
use crate::snapshot::Snapshot;
use axum::http::header::CONTENT_TYPE;
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use axum::Router;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, RwLock};
use std::time::SystemTime;

type Shared = Arc<RwLock<Snapshot>>;

const INDEX_HTML: &str = include_str!("../assets/index.html");
const APP_JS: &str = include_str!("../assets/app.js");
const STYLE_CSS: &str = include_str!("../assets/style.css");

const DEFAULT_PORT: u16 = 8787;

/// Resolve the bind address: `127.0.0.1:$PITWALL_PORTAL_PORT` (default 8787). An
/// empty or unparseable port falls back to the default. Loopback only — the
/// tailnet (`tailscale serve`) is the exposure boundary, not this bind.
pub fn addr_from_env() -> SocketAddr {
    let port = std::env::var("PITWALL_PORTAL_PORT")
        .ok()
        .and_then(|s| s.trim().parse::<u16>().ok())
        .filter(|&p| p != 0)
        .unwrap_or(DEFAULT_PORT);
    SocketAddr::from((Ipv4Addr::LOCALHOST, port))
}

/// Build the current snapshot from the engine's tick against `now`.
fn project(engine: &Engine, now: SystemTime) -> Snapshot {
    Snapshot::from_tick(
        &engine.tick(),
        engine.slice_cap_bytes,
        engine.warn_ratio,
        engine.crit_ratio,
        now,
    )
}

/// Serve the portal: seed the shared snapshot, spawn the refresh task, and run
/// the axum server bound to `addr` until it stops.
pub async fn serve(mut engine: Engine, addr: SocketAddr) -> anyhow::Result<()> {
    let shared: Shared = Arc::new(RwLock::new(project(&engine, SystemTime::now())));

    let refresh = shared.clone();
    tokio::spawn(async move {
        // `generated_at` tracks the last applied poll, so an error-only refresh
        // updates the banner without advancing the data timestamp or elapsed.
        let mut last_applied_at = SystemTime::now();
        loop {
            match engine.recv().await {
                RecvOutcome::Closed => break, // pollers gone: keep serving the last snapshot
                outcome => {
                    if outcome == RecvOutcome::Applied {
                        last_applied_at = SystemTime::now();
                    }
                    let snap = project(&engine, last_applied_at);
                    *refresh.write().expect("snapshot lock poisoned") = snap;
                }
            }
        }
    });

    let app = Router::new()
        .route("/", get(index))
        .route("/app.js", get(app_js))
        .route("/style.css", get(style_css))
        .route("/api/state", get(api_state))
        .with_state(shared);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn app_js() -> impl IntoResponse {
    ([(CONTENT_TYPE, "application/javascript")], APP_JS)
}

async fn style_css() -> impl IntoResponse {
    ([(CONTENT_TYPE, "text/css")], STYLE_CSS)
}

async fn api_state(
    axum::extract::State(shared): axum::extract::State<Shared>,
) -> impl IntoResponse {
    let body = {
        let snap = shared.read().expect("snapshot lock poisoned");
        serde_json::to_string(&*snap).expect("snapshot serializes")
    };
    ([(CONTENT_TYPE, "application/json")], body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::{RunnerSnap, SliceSnap};
    use axum::extract::State;

    fn seeded() -> Snapshot {
        Snapshot {
            generated_at: 42,
            runners: vec![RunnerSnap {
                name: "ci-runner-1".into(),
                kind: "docker",
                cpu_pct: 12.0,
                mem_bytes: 100,
                mem_limit: 200,
                cpu: vec![1.0, 2.0],
                mem: vec![0.1, 0.2],
                load: "busy",
                mem_level: "warn",
                job: None,
            }],
            hosted: vec![],
            vercel: vec![],
            slice: SliceSnap {
                docker_mem: 100,
                cap: 200,
                mem_level: "normal",
                hist: vec![100.0],
            },
            errors: vec!["docker: boom".into()],
        }
    }

    #[tokio::test]
    async fn api_state_serves_the_current_snapshot_json() {
        let shared: Shared = Arc::new(RwLock::new(seeded()));
        let resp = api_state(State(shared)).await.into_response();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get(CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("application/json")
        );
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["generated_at"], 42);
        assert_eq!(v["runners"][0]["load"], "busy");
        assert_eq!(v["runners"][0]["mem_level"], "warn");
        assert_eq!(v["errors"][0], "docker: boom");
    }

    #[test]
    fn addr_defaults_to_loopback_8787() {
        // Env is not set in this test; the default must be 127.0.0.1:8787.
        let addr = addr_from_env();
        assert_eq!(addr.ip().to_string(), "127.0.0.1");
        assert_eq!(addr.port(), DEFAULT_PORT);
    }
}
