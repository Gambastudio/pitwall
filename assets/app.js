// Dumb glance frontend: poll /api/state every 2s and render. All tiers are
// computed server-side (load + mem_level); this file only maps them to classes.
"use strict";

const POLL_MS = 2000;
const GIB = 1024 * 1024 * 1024;
const MIB = 1024 * 1024;

function fmtMem(bytes, limit) {
  const used = fmtBytes(bytes);
  return limit > 0 ? `${used}/${fmtBytes(limit)}` : used;
}
function fmtBytes(b) {
  if (b >= GIB) return (b / GIB).toFixed(1) + "G";
  if (b >= MIB) return Math.round(b / MIB) + "M";
  return Math.round(b / 1024) + "K";
}
function fmtDuration(secs) {
  if (secs < 60) return secs + "s";
  const m = Math.floor(secs / 60);
  if (m < 60) return m + "m" + String(secs % 60).padStart(2, "0") + "s";
  const h = Math.floor(m / 60);
  return h + "h" + String(m % 60).padStart(2, "0") + "m";
}

// A compact inline-SVG sparkline. `norm` maps a value to 0..1.
function sparkline(values, norm) {
  const w = 60, h = 14;
  if (!values || values.length === 0) return "";
  const n = values.length;
  const step = n > 1 ? w / (n - 1) : 0;
  const pts = values.map((v, i) => {
    const y = h - 1 - Math.max(0, Math.min(1, norm(v))) * (h - 2);
    return `${(i * step).toFixed(1)},${y.toFixed(1)}`;
  });
  return `<svg class="spark" width="${w}" height="${h}" viewBox="0 0 ${w} ${h}"><path d="M${pts.join(" L")}"/></svg>`;
}

function el(tag, attrs, html) {
  const e = document.createElement(tag);
  if (attrs) for (const k in attrs) if (attrs[k] != null) e.setAttribute(k, attrs[k]);
  if (html != null) e.innerHTML = html;
  return e;
}

function renderRunners(runners) {
  const tbody = document.getElementById("runners");
  tbody.replaceChildren();
  for (const r of runners) {
    const tr = el("tr", { class: "load-" + r.load });
    const cpuMax = Math.max(10, ...r.cpu); // 10% floor, like the TUI
    const job = r.job
      ? `<span class="job">${esc(r.job.workflow)} › ${esc(r.job.job)}</span>`
      : `<span class="job idle">${r.load === "busy" ? "busy" : "— idle"}</span>`;
    tr.innerHTML =
      `<td>${esc(r.name)}</td>` +
      `<td class="num">${r.cpu_pct.toFixed(0)}%</td>` +
      `<td>${sparkline(r.cpu, (v) => v / cpuMax)}</td>` +
      `<td class="num mem-${r.mem_level}">${fmtMem(r.mem_bytes, r.mem_limit)}</td>` +
      `<td>${sparkline(r.mem, (v) => v)}</td>` +
      `<td>${job}</td>` +
      `<td class="num">${r.job ? fmtDuration(r.job.elapsed_secs) : "-"}</td>`;
    tbody.appendChild(tr);
  }
}

function renderSection(id, rows, buildRow) {
  const tbody = document.getElementById(id);
  const section = document.getElementById(id + "-section");
  tbody.replaceChildren();
  section.hidden = rows.length === 0;
  for (const row of rows) tbody.appendChild(buildRow(row));
}

function dot(running) {
  return running ? `<span class="dot-running">●</span>` : `<span class="dot-queued">○</span>`;
}

function renderHosted(hosted) {
  renderSection("hosted", hosted, (h) => {
    const tr = el("tr");
    tr.innerHTML =
      `<td>${dot(h.status === "in_progress")}</td>` +
      `<td>${esc(h.workflow)} › ${esc(h.job)}</td>` +
      `<td>${esc(h.label)}</td>` +
      `<td>${esc(h.branch)}</td>` +
      `<td class="num">${fmtDuration(h.elapsed_secs)}${h.status === "queued" ? " wait" : ""}</td>`;
    return tr;
  });
}

function renderVercel(vercel) {
  renderSection("vercel", vercel, (d) => {
    const tr = el("tr");
    tr.innerHTML =
      `<td>${dot(d.status === "building")}</td>` +
      `<td>${esc(d.project)}</td>` +
      `<td>${esc(d.target)}</td>` +
      `<td>${esc(d.branch)}</td>` +
      `<td>${esc(d.commit)}</td>` +
      `<td class="num">${fmtDuration(d.elapsed_secs)}${d.status === "queued" ? " wait" : ""}</td>`;
    return tr;
  });
}

function esc(s) {
  return String(s).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
}

function renderErrors(errors) {
  const box = document.getElementById("errors");
  if (errors && errors.length) {
    box.textContent = errors.join("\n");
    box.hidden = false;
  } else {
    box.hidden = true;
  }
}

function setUpdated(generatedAt, ok) {
  const span = document.getElementById("updated");
  if (!ok) {
    span.textContent = "disconnected — retrying…";
    return;
  }
  const age = Math.max(0, Math.floor(Date.now() / 1000) - generatedAt);
  span.textContent = `updated ${age}s ago`;
}

async function poll() {
  try {
    const res = await fetch("/api/state", { cache: "no-store" });
    if (!res.ok) throw new Error("HTTP " + res.status);
    const state = await res.json();
    renderErrors(state.errors);
    renderRunners(state.runners);
    renderHosted(state.hosted);
    renderVercel(state.vercel);
    setUpdated(state.generated_at, true);
  } catch (e) {
    setUpdated(0, false);
  }
}

poll();
setInterval(poll, POLL_MS);
