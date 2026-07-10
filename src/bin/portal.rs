//! The read-only web portal: pitwall's collectors, served over HTTP instead of
//! rendered as a TUI. Reuses the shared `Config` and `Engine`; the only new
//! surface is `web::serve`. Built with `--features web`.

use pitwall::app::Engine;
use pitwall::{config, web};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load config before binding so a bad config file reports cleanly to stderr.
    let cfg = config::Config::load()?;
    let engine = Engine::spawn(cfg);
    let addr = web::addr_from_env();
    eprintln!("pitwall portal listening on http://{addr}");
    web::serve(engine, addr).await
}
