// ------------ ROTC Tools Frontend ------------
// Robust tab wiring + API calls (GET only) + defensive rendering
// Requires the page to have panels with ids: panel-availability, panel-person, panel-attendance (optional)
// And tab buttons/links with [data-tab="availability|person|attendance"]

(function () {
  const log = (...args) => console.log("[ui]", ...args);
  const warn = (...args) => console.warn("[ui]", ...args);
  const err = (...args) => console.error("[ui]", ...args);

  // --- PW handling (from URL) ---
  const url = new URL(window.location.href);
  const PW = url.searchParams.get("pw") || "";
  if (!PW) {
    warn("No ?pw=… in URL. Most endpoints will 401.");
  }

  // --- Helpers ---
  function qs(sel) { return document.querySelector(sel); }
  function qsa(sel) { return Array.from(document.querySelectorAll(sel)); }

  function show(id) {
    const el = qs(`#${id}`);
    if (el) el.style.display = "";
  }
  function hide(id) {
    const el = qs(`#${id}`);
    if (el) el.style.display = "none";
  }
  function setActiveTab(name) {
    // Toggle active state on any [data-tab] triggers
    qsa("[data-tab]").forEach(el => {
      if (el.getAttribute("data-tab") === name) {
        el.classList.add("active");
        el.setAttribute("aria-selected", "true");
      } else {
        el.classList.remove("active");
        el.setAttribute("aria-selected", "false");
      }
    });
    // Show the target panel, hide others (if they exist)
    const panels = ["availability", "person", "attendance", "reports"];
    panels.forEach(p => {
      const pid = `panel-${p}`;
      if (p === name) show(pid); else hide(pid);
    });
  }

  async function fetchJSON(endpoint, params = {}) {
    // always GET; never HEAD
    const u = new URL(endpoint, window.location.origin);
    // append pw to every request
    u.searchParams.set("pw", PW);
    Object.entries(params).forEach(([k, v]) => {
      if (v !== undefined && v !== null && v !== "") u.searchParams.set(k, v);
    });
    log("GET", u.toString());
    const resp = await fetch(u.toString(), { method: "GET", headers: { "Accept": "application/json" } });
    // Special-case 401/403 to bubble clearer errors
    if (!resp.ok) {
      const text = await resp.text().catch(() => "");
      throw new Error(`HTTP ${resp.status} on ${u.pathname} – ${text || resp.statusText}`);
    }
    // Some endpoints may return empty body for HEAD – but we never use HEAD now.
    const json = await resp.json();
    return json;
  }

  // --- Availability UI ---
  function wireAvailability() {
    const form = qs("#availability-form");
    const btn = qs("#availability-search");
    const out = qs("#availability-results");

    if (!form || !btn || !out) {
      log("Availability widgets not found; skipping wiring.");
      return;
    }

    btn.addEventListener("click", async (e) => {
      e.preventDefault();
      const day = (qs("#availability-day")?.value || "Mon").trim();
      const start = (qs("#availability-start")?.value || "0900").trim();
      const end = (qs("#availability-end")?.value || "1000").trim();
      const org = (qs("#availability-org")?.value || "").trim();

      out.innerHTML = `<div class="muted">Searching…</div>`;
      try {
        const data = await fetchJSON("/api/available", { day, start, end, org });
        if (!data.ok) throw new Error(data.error || "Unknown error");
        const rows = data.rows || [];
        if (rows.length === 0) {
          out.innerHTML = `<div class="muted">No cadets free in that window.</div>`;
          return;
        }
        // Render simple table
        const html = [
          `<table class="table"><thead><tr><th>First</th><th>Last</th><th>MS</th></tr></thead><tbody>`,
          ...rows.map(r => `<tr><td>${esc(r.first)}</td><td>${esc(r.last)}</td><td>${esc(r.ms)}</td></tr>`),
          `</tbody></table>`
        ].join("");
        out.innerHTML = html;
      } catch (ex) {
        err("availability search failed:", ex);
        out.innerHTML = `<div class="error">Availability error: ${esc(ex.message)}</div>`;
      }
    });
  }

  // --- Person search UI ---
  function wirePerson() {
    const btn = qs("#person-search");
    const input = qs("#person-query");
    const out = qs("#person-result");

    if (!btn || !input || !out) {
      log("Person lookup widgets not found; skipping wiring.");
      return;
    }

    btn.addEventListener("click", async (e) => {
      e.preventDefault();
      const q = input.value.trim();
      if (!q) {
        out.innerHTML = `<div class="muted">Type a name or email.</div>`;
        return;
      }
      out.innerHTML = `<div class="muted">Searching…</div>`;
      try {
        const data = await fetchJSON("/api/person", { q });
        if (!data.ok) throw new Error(data.error || "Unknown error");
        const p = data.person || {};
        out.innerHTML = renderPersonCard(p);
      } catch (ex) {
        err("person search failed:", ex);
        out.innerHTML = `<div class="error">Person search error: ${esc(ex.message)}</div>`;
      }
    });
  }

  function renderPersonCard(p) {
    const ex = p.extras || {};
    const commute = ex.commute_minutes || {};
    const busy = ex.busy_blocks || {};

    const boolBadge = (label, val) => {
      if (val == null || val === "") return "";
      const yes = String(val).toLowerCase().startsWith("y");
      return `<span class="chip ${yes ? "chip-yes" : "chip-no"}">${label}: ${yes ? "Yes" : "No"}</span>`;
    };

    return `
      <div class="card">
        <div class="card-title">${esc(p.first)} ${esc(p.last)} <span class="muted">(${esc(p.ms||"")})</span></div>
        <div class="grid">
          <div>
            <div><strong>Email:</strong> ${esc(p.email||"")}</div>
            <div><strong>Phone:</strong> ${esc(p.phone||"")}</div>
            <div><strong>School:</strong> ${esc(p.school||"")}</div>
            <div><strong>Major:</strong> ${esc(p.major||"")}</div>
            <div><strong>Contracted:</strong> ${esc(p.contracted||"")}</div>
            <div><strong>Prior Service:</strong> ${esc(p.prior_service||"")}</div>
            <div><strong>Vehicle:</strong> ${esc(p.vehicle||"")}</div>
          </div>
          <div>
            <div class="chips">
              ${boolBadge("AGSU", ex.agsu)}
              ${boolBadge("OCPs", ex.ocps)}
              ${boolBadge("PT Uniform", ex.pt_uniform)}
              ${boolBadge("Compass", ex.compass)}
              ${boolBadge("Ranger Challenge", ex.ranger_challenge)}
            </div>
            <div class="muted small">Commute (mins): GSU ${esc(commute.gsu_track||"-")}, Tech ${esc(commute.tech_track||"-")}, ULM ${esc(commute.ulm||"-")}, NSU ${esc(commute.nsu||"-")}</div>
            <details class="mt">
              <summary>Weekly Busy Blocks</summary>
              <pre class="pre-wrap small">${fmtBusy(busy)}</pre>
            </details>
          </div>
        </div>
      </div>
    `;
  }

  function fmtBusy(b) {
    const days = ["monday","tuesday","wednesday","thursday","friday"];
    return days.map(d => {
      const v = b[d] || "";
      return `${cap(d)}: ${v || "—"}`;
    }).join("\n");
  }

  // --- Attendance tab (read-only wiring; optional) ---
  function wireAttendance() {
    const btn = qs("#att-load-events");
    const out = qs("#att-events");
    if (!btn || !out) { log("Attendance widgets not found; skipping."); return; }

    btn.addEventListener("click", async (e) => {
      e.preventDefault();
      out.innerHTML = `<div class="muted">Loading events…</div>`;
      try {
        const data = await fetchJSON("/api/att/events");
        if (!data.ok) throw new Error(data.error || "Unknown error");
        const events = data.events || [];
        if (!events.length) { out.innerHTML = `<div class="muted">No events.</div>`; return; }
        out.innerHTML = `<ul class="list">${events.map(e => `<li>${esc(e.date)} — ${esc(e.header)}</li>`).join("")}</ul>`;
      } catch (ex) {
        err("attendance load failed:", ex);
        out.innerHTML = `<div class="error">Attendance error: ${esc(ex.message)}</div>`;
      }
    });
  }

  // --- Tabs wiring ---
  function wireTabs() {
    const triggers = qsa("[data-tab]");
    if (!triggers.length) {
      log("No [data-tab] triggers found; skipping tabs wiring.");
      return;
    }
    triggers.forEach(el => {
      el.addEventListener("click", (e) => {
        e.preventDefault();
        const name = el.getAttribute("data-tab");
        if (!name) return;
        setActiveTab(name);
      });
    });

    // pick initial tab from hash (#person etc.) or default to availability
    const initial = (location.hash || "").replace(/^#/, "") || "availability";
    setActiveTab(initial);
  }

  // --- tiny utils ---
  function cap(s){ return s ? s.charAt(0).toUpperCase() + s.slice(1) : s; }
  function esc(s){
    return String(s ?? "")
      .replace(/&/g, "&amp;").replace(/</g, "&lt;")
      .replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#39;");
  }

  // --- init ---
  document.addEventListener("DOMContentLoaded", () => {
    try {
      wireTabs();
      wireAvailability();
      wirePerson();
      wireAttendance();
      log("script initialized");
    } catch (ex) {
      err("fatal init error:", ex);
    }
  });
})();
