/* ===== ROTC Tools – Frontend JS (full rewrite) =====
   - Auto-reads ?pw= from URL and attaches to every API call
   - Availability search with validation, loading state, and table render
   - Attendance tab scaffold (roster load + single-select marking UI)
   - Reports tab tries endpoints, degrades gracefully if not present
   - Robust error surfacing (status + JSON error message if any)
   - No external deps
===================================================== */

(() => {
  // ---------- Basic DOM helpers ----------
  const $ = (sel, ctx = document) => ctx.querySelector(sel);
  const $$ = (sel, ctx = document) => Array.from(ctx.querySelectorAll(sel));
  const byId = id => document.getElementById(id);

  // ---------- Global state ----------
  const state = {
    pw: new URLSearchParams(location.search).get("pw") || "",
    busy: false,
    endpoints: {
      available: "/api/available",
      person: "/api/person",                  // optional; we probe before using
      roster: "/api/attendance/roster",       // expected: ?campus=GSU|ULM
      save: "/api/attendance/save",           // body: { campus, day, eventType, date, slotLabel, rows:[{first,last,ms,status}] }
      leaderboard: "/api/reports/leaderboard" // optional
    },
    endpointExists: {
      person: false,
      roster: false,
      save: false,
      leaderboard: false
    }
  };

  // ---------- UI roots ----------
  const rootMsg = byId("msg");            // <div id="msg"></div> (one-line status/errors)
  const tabAvailability = byId("tab-availability");
  const tabAttendance   = byId("tab-attendance");
  const tabReports      = byId("tab-reports");

  // Availability controls
  const daySel   = byId("day-select");
  const startInp = byId("start-input");
  const endInp   = byId("end-input");
  const btnSearch = byId("btn-search");
  const availOut = byId("availability-out");

  // Attendance controls
  const campusSel   = byId("campus-select");    // GSU | ULM
  const eventSel    = byId("event-type-select");// PT | Lab | Other
  const dateInp     = byId("date-input");       // yyyy-mm-dd
  const slotInp     = byId("slot-label");       // e.g., "8/19/25 + PT"
  const btnLoadRoster = byId("btn-load-roster");
  const btnSaveAttendance = byId("btn-save-attendance");
  const rosterOut   = byId("attendance-roster");

  // Reports controls
  const btnLoadLeaderboard = byId("btn-load-leaderboard");
  const reportsOut = byId("reports-out");

  // ---------- Utilities ----------
  function setBusy(on, label = "") {
    state.busy = on;
    document.body.classList.toggle("busy", !!on);
    status(label || (on ? "Working…" : ""));
  }

  function status(msg) {
    if (!rootMsg) return;
    rootMsg.textContent = msg || "";
    rootMsg.classList.remove("error");
  }

  function error(errMsg) {
    if (!rootMsg) return;
    rootMsg.textContent = errMsg || "Something went wrong.";
    rootMsg.classList.add("error");
  }

  function msSortDesc(a, b) {
    const ai = Number(a.ms || a.MS || 0);
    const bi = Number(b.ms || b.MS || 0);
    return bi - ai;
  }

  function pad4(s) {
    s = (s || "").replace(/\D/g, "");
    if (!s) return "";
    if (s.length <= 2) return s.padStart(4, "0") + "0";
    return s.padStart(4, "0");
  }

  async function fetchJSON(url, opts = {}) {
    // Always attach pw
    const u = new URL(url, location.origin);
    if (state.pw) u.searchParams.set("pw", state.pw);

    let res;
    try {
      res = await fetch(u.toString(), {
        headers: { "Content-Type": "application/json" },
        ...opts
      });
    } catch (e) {
      throw new Error(`Network error: ${e.message}`);
    }

    let data = null;
    const text = await res.text();
    try { data = text ? JSON.parse(text) : {}; } catch {
      // not JSON – still show raw body
      throw new Error(`HTTP ${res.status} – ${text || "non-JSON response"}`);
    }

    if (!res.ok || data.error) {
      const msg = data.error || `HTTP ${res.status}`;
      throw new Error(msg);
    }
    return data;
  }

  async function endpointExists(path) {
    try {
      const u = new URL(path, location.origin);
      if (state.pw) u.searchParams.set("pw", state.pw);
      const res = await fetch(u.toString(), { method: "HEAD" });
      return res.ok;
    } catch {
      return false;
    }
  }

  // ---------- Availability ----------
  function validateAvailabilityInputs() {
    const validDays = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"];
    const d = (daySel?.value || "").trim();
    const s = pad4(startInp?.value || "");
    const e = pad4(endInp?.value || "");
    if (!validDays.includes(d)) throw new Error(`day must be one of ${JSON.stringify(validDays)}`);
    if (!/^\d{4}$/.test(s)) throw new Error("Start time must be HHMM (e.g., 0830).");
    if (!/^\d{4}$/.test(e)) throw new Error("End time must be HHMM (e.g., 1030).");
    if (Number(e) <= Number(s)) throw new Error("End time must be after Start time.");
    return { day: d, start: s, end: e };
  }

  function renderAvailability(rows) {
    availOut.innerHTML = "";
    if (!rows || !rows.length) {
      availOut.innerHTML = `<p class="muted">No one is available in that window.</p>`;
      return;
    }
    // Sort by MS level desc, then last name
    rows.sort((a, b) => {
      const m = msSortDesc(a, b);
      if (m !== 0) return m;
      const la = (a.last || a.Last || "").toLowerCase();
      const lb = (b.last || b.Last || "").toLowerCase();
      return la.localeCompare(lb);
    });

    const table = document.createElement("table");
    table.className = "table";
    table.innerHTML = `
      <thead>
        <tr>
          <th>Name</th>
          <th>MS</th>
        </tr>
      </thead>
      <tbody></tbody>
    `;
    const tb = $("tbody", table);
    rows.forEach(r => {
      const first = r.first || r.First || "";
      const last = r.last || r.Last || "";
      const ms = r.ms || r.MS || "";
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td><button class="link person-btn" data-first="${encodeURIComponent(first)}" data-last="${encodeURIComponent(last)}">${first} ${last}</button></td>
        <td>${ms}</td>
      `;
      tb.appendChild(tr);
    });
    availOut.appendChild(table);

    // Only enable detail clicks if endpoint exists
    if (state.endpointExists.person) {
      availOut.addEventListener("click", async (ev) => {
        const btn = ev.target.closest(".person-btn");
        if (!btn) return;
        try {
          setBusy(true, "Loading profile…");
          const first = decodeURIComponent(btn.dataset.first || "");
          const last  = decodeURIComponent(btn.dataset.last || "");
          const data = await fetchJSON(state.endpoints.person + `?first=${encodeURIComponent(first)}&last=${encodeURIComponent(last)}`);
          showPersonModal(data);
          status("");
        } catch (e) {
          error(e.message);
        } finally {
          setBusy(false);
        }
      }, { once: true }); // attach once per render
    }
  }

  function showPersonModal(data) {
    const modal = document.createElement("div");
    modal.className = "modal";
    modal.innerHTML = `
      <div class="modal-card">
        <div class="modal-head">
          <h3>${(data.name || "Details")}</h3>
          <button class="close-x">&times;</button>
        </div>
        <div class="modal-body">
          <pre>${escapeHTML(JSON.stringify(data, null, 2))}</pre>
        </div>
      </div>
    `;
    document.body.appendChild(modal);
    modal.addEventListener("click", (e) => {
      if (e.target.classList.contains("modal") || e.target.classList.contains("close-x")) {
        modal.remove();
      }
    });
  }

  // ---------- Attendance ----------
  function renderRoster(rows) {
    rosterOut.innerHTML = "";
    if (!rows || !rows.length) {
      rosterOut.innerHTML = `<p class="muted">No roster rows returned.</p>`;
      return;
    }
    // Sort MS desc then last
    rows.sort((a, b) => {
      const m = msSortDesc(a, b);
      if (m !== 0) return m;
      return String(a.last || "").localeCompare(String(b.last || ""));
    });

    // Column: name | MS | Present | FTR | Excused
    const table = document.createElement("table");
    table.className = "table compact";
    table.innerHTML = `
      <thead>
        <tr>
          <th>Name</th><th>MS</th>
          <th>Present</th><th>FTR</th><th>Excused</th>
        </tr>
      </thead>
      <tbody></tbody>
    `;
    const tb = $("tbody", table);

    rows.forEach((r, idx) => {
      const first = r.first || r.First || "";
      const last  = r.last  || r.Last  || "";
      const ms    = r.ms    || r.MS    || "";
      const rowId = `r${idx}`;
      const tr = document.createElement("tr");
      tr.dataset.first = first;
      tr.dataset.last  = last;
      tr.dataset.ms    = ms;

      tr.innerHTML = `
        <td>${first} ${last}</td>
        <td>${ms}</td>
        <td><input type="radio" name="${rowId}" value="Present"></td>
        <td><input type="radio" name="${rowId}" value="FTR"></td>
        <td><input type="radio" name="${rowId}" value="Excused"></td>
      `;
      tb.appendChild(tr);
    });

    rosterOut.appendChild(table);
    rosterOut.dataset.ready = "1";
  }

  function collectAttendancePayload() {
    if (rosterOut.dataset.ready !== "1") throw new Error("Load a roster first.");
    const campus = campusSel.value;
    const eventType = eventSel.value;
    const date = (dateInp.value || "").trim();
    const slotLabel = (slotInp.value || "").trim();
    if (!campus) throw new Error("Campus is required.");
    if (!eventType) throw new Error("Event type is required.");
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) throw new Error("Date must be yyyy-mm-dd.");
    if (!slotLabel) throw new Error("Slot label is required.");

    const rows = [];
    $$("tbody tr", rosterOut).forEach(tr => {
      const first = tr.dataset.first;
      const last  = tr.dataset.last;
      const ms    = tr.dataset.ms;
      const checked = $$("input[type=radio]:checked", tr)[0];
      const status = checked ? checked.value : "";
      rows.push({ first, last, ms, status });
    });
    return { campus, eventType, date, slotLabel, rows };
  }

  // ---------- Reports ----------
  function renderLeaderboard(data) {
    reportsOut.innerHTML = "";
    if (!data || !data.rows || !data.rows.length) {
      reportsOut.innerHTML = `<p class="muted">No leaderboard data.</p>`;
      return;
    }
    const table = document.createElement("table");
    table.className = "table";
    table.innerHTML = `
      <thead><tr><th>Rank</th><th>Name</th><th>MS</th><th>Score</th></tr></thead>
      <tbody></tbody>
    `;
    const tb = $("tbody", table);
    data.rows.forEach((r, i) => {
      const tr = document.createElement("tr");
      tr.innerHTML = `<td>${i + 1}</td><td>${r.name || ""}</td><td>${r.ms || ""}</td><td>${r.score ?? ""}</td>`;
      tb.appendChild(tr);
    });
    reportsOut.appendChild(table);
  }

  // ---------- Event wiring ----------
  btnSearch?.addEventListener("click", async () => {
    try {
      availOut.innerHTML = "";
      status("");
      const { day, start, end } = validateAvailabilityInputs();
      setBusy(true, "Searching availability…");
      const data = await fetchJSON(`${state.endpoints.available}?day=${encodeURIComponent(day)}&start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}`);
      if (!data || data.ok === false) throw new Error(data?.error || "Unknown error");
      renderAvailability(data.rows || data.people || []);
      status(`Found ${ (data.rows || []).length } cadet(s).`);
    } catch (e) {
      error(e.message);
    } finally {
      setBusy(false);
    }
  });

  btnLoadRoster?.addEventListener("click", async () => {
    if (!state.endpointExists.roster) {
      error("Roster API is not available on the server.");
      return;
    }
    try {
      rosterOut.innerHTML = "";
      status("");
      const campus = campusSel.value || "GSU";
      setBusy(true, `Loading ${campus} roster…`);
      const data = await fetchJSON(`${state.endpoints.roster}?campus=${encodeURIComponent(campus)}`);
      renderRoster(data.rows || data.roster || []);
      status(`Loaded ${ (data.rows || []).length } cadet(s).`);
    } catch (e) {
      error(e.message);
    } finally {
      setBusy(false);
    }
  });

  btnSaveAttendance?.addEventListener("click", async () => {
    if (!state.endpointExists.save) {
      error("Save API is not available on the server.");
      return;
    }
    try {
      const payload = collectAttendancePayload();
      setBusy(true, "Saving attendance…");
      const res = await fetchJSON(state.endpoints.save, {
        method: "POST",
        body: JSON.stringify(payload)
      });
      status(res?.message || "Attendance saved.");
    } catch (e) {
      error(e.message);
    } finally {
      setBusy(false);
    }
  });

  btnLoadLeaderboard?.addEventListener("click", async () => {
    if (!state.endpointExists.leaderboard) {
      error("Leaderboard endpoint is not available.");
      return;
    }
    try {
      setBusy(true, "Loading leaderboard…");
      const data = await fetchJSON(state.endpoints.leaderboard);
      renderLeaderboard(data);
      status("Leaderboard loaded.");
    } catch (e) {
      error(e.message);
    } finally {
      setBusy(false);
    }
  });

  // ---------- Tabs (simple) ----------
  function showTab(which) {
    const maps = {
      availability: tabAvailability,
      attendance: tabAttendance,
      reports: tabReports
    };
    Object.values(maps).forEach(node => node?.classList.add("hidden"));
    maps[which]?.classList.remove("hidden");

    // Highlight nav buttons (if present)
    $$(".navbtn").forEach(b => b.classList.remove("active"));
    $(`.navbtn[data-tab="${which}"]`)?.classList.add("active");
  }
  $$(".navbtn").forEach(btn => {
    btn.addEventListener("click", () => showTab(btn.dataset.tab));
  });

  // ---------- Init / Capability probe ----------
  async function init() {
    // If no pw, tell the user early (but allow availability from public sheet if backend allows)
    if (!state.pw) {
      status("Tip: append ?pw=YOURPASSWORD to the URL.");
    }

    // wire default values if not present
    if (startInp && !startInp.value) startInp.value = "0830";
    if (endInp   && !endInp.value)   endInp.value   = "1030";

    // Probe optional endpoints so UI can degrade gracefully
    state.endpointExists.person      = await endpointExists(state.endpoints.person);
    state.endpointExists.roster      = await endpointExists(state.endpoints.roster);
    state.endpointExists.save        = await endpointExists(state.endpoints.save);
    state.endpointExists.leaderboard = await endpointExists(state.endpoints.leaderboard);

    // Show/hide features based on probes
    if (!state.endpointExists.roster) {
      byId("attendance-tools")?.classList.add("muted-block");
      byId("attendance-note")?.classList.remove("hidden");
    }
    if (!state.endpointExists.leaderboard) {
      byId("reports-note")?.classList.remove("hidden");
    }

    // Default to Availability tab
    showTab("availability");
  }

  // ---------- Misc ----------
  function escapeHTML(s) {
    return String(s).replace(/[&<>"']/g, c => (
      { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]
    ));
  }

  // Go!
  window.addEventListener("DOMContentLoaded", init);
})();
