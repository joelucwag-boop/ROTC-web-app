// ROTC Tools frontend wired to your current index.html
// - Tabs: .tab[data-for] toggles .panel.on
// - Availability: #day, #a_start, #a_end, #a_go, #a_out
// - Attendance: #evt_block, #evt_date, #evt_type, #evt_other, #save_att, #rost
// - Reports: #rep_block, #rep_date, #rep_day_btn, #rep_day_out, #lb_from, #lb_to, #lb_btn, #lb_out

(function () {
  const log = (...a) => console.log("[ui]", ...a);
  const warn = (...a) => console.warn("[ui]", ...a);
  const err = (...a) => console.error("[ui]", ...a);

  // pull pw/admin_pw from URL (?pw=...&admin_pw=...)
  const url = new URL(location.href);
  const PW = url.searchParams.get("pw") || "";
  const ADMIN_PW = url.searchParams.get("admin_pw") || "";

  // -------- utils ----------
  const qs = (s, r = document) => r.querySelector(s);
  const qsa = (s, r = document) => Array.from(r.querySelectorAll(s));
  const esc = (s) =>
    String(s ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");

  function getJSON(path, params = {}) {
    const u = new URL(path, location.origin);
    if (PW) u.searchParams.set("pw", PW);
    Object.entries(params).forEach(([k, v]) => {
      if (v !== undefined && v !== null && v !== "") u.searchParams.set(k, v);
    });
    log("GET", u.toString());
    return fetch(u.toString(), { method: "GET", headers: { Accept: "application/json" } })
      .then(async (r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status} ${r.statusText}`);
        return r.json();
      });
  }

  function postJSON(path, body) {
    const u = new URL(path, location.origin);
    log("POST", u.toString(), body);
    return fetch(u.toString(), {
      method: "POST",
      headers: { "Content-Type": "application/json", Accept: "application/json" },
      body: JSON.stringify(body),
    }).then(async (r) => {
      let t = null;
      try { t = await r.json(); } catch {}
      if (!r.ok) {
        const msg = (t && (t.error || t.message)) || `HTTP ${r.status} ${r.statusText}`;
        throw new Error(msg);
      }
      return t || {};
    });
  }

  // -------- tabs wiring ----------
  function wireTabs() {
    const tabs = qsa(".tabs .tab");
    const panels = qsa(".panel");

    function show(name) {
      // toggle button state
      tabs.forEach((b) => {
        const on = b.getAttribute("data-for") === name;
        b.classList.toggle("on", on);
        b.setAttribute("aria-selected", on ? "true" : "false");
      });
      // toggle panels
      panels.forEach((p) => p.classList.toggle("on", p.id === name));
    }

    tabs.forEach((b) => {
      b.addEventListener("click", (e) => {
        e.preventDefault();
        const name = b.getAttribute("data-for");
        if (name) show(name);
      });
    });

    // initial: keep whatever has .on, else default to availability
    const current = qs(".panel.on")?.id || "availability";
    show(current);
  }

  // -------- availability ----------
  function wireAvailability() {
    const daySel = qs("#day");
    const sInp = qs("#a_start");
    const eInp = qs("#a_end");
    const btn = qs("#a_go");
    const out = qs("#a_out");

    if (!daySel || !sInp || !eInp || !btn || !out) {
      log("availability controls not found, skipping");
      return;
    }

    btn.addEventListener("click", async (e) => {
      e.preventDefault();
      const day = (daySel.value || "").trim();
      const start = (sInp.value || "0900").trim();
      const end = (eInp.value || "1030").trim();
      out.innerHTML = `<div class="muted">Searching…</div>`;
      try {
        const data = await getJSON("/api/available", { day, start, end });
        if (!data.ok) throw new Error(data.error || "Unknown error");
        const rows = data.rows || [];
        if (!rows.length) {
          out.innerHTML = `<div class="muted">No cadets free for that window.</div>`;
          return;
        }
        const html = [
          `<table class="table"><thead><tr><th>First</th><th>Last</th><th>MS</th></tr></thead><tbody>`,
          ...rows.map((r) => `<tr><td>${esc(r.first)}</td><td>${esc(r.last)}</td><td>${esc(r.ms)}</td></tr>`),
          `</tbody></table>`,
        ].join("");
        out.innerHTML = html;
      } catch (ex) {
        err("availability error:", ex);
        out.innerHTML = `<div class="error">Availability failed: ${esc(ex.message)}</div>`;
      }
    });
  }

  // -------- attendance (roster + save) ----------
  function wireAttendance() {
    const blockSel = qs("#evt_block");
    const dateInp = qs("#evt_date");
    const typeSel = qs("#evt_type");
    const otherInp = qs("#evt_other");
    const saveBtn = qs("#save_att");
    const rostTable = qs("#rost tbody");

    if (!blockSel || !dateInp || !typeSel || !saveBtn || !rostTable) {
      log("attendance controls not found, skipping");
      return;
    }

    async function loadRoster() {
      const label = (blockSel.value || "gsu").toLowerCase();
      rostTable.innerHTML = `<tr><td colspan="5" class="muted">Loading roster…</td></tr>`;
      try {
        const data = await getJSON("/api/roster", { label });
        if (!data.ok) throw new Error(data.error || "Unknown error");
        const rows = data.rows || [];
        if (!rows.length) {
          rostTable.innerHTML = `<tr><td colspan="5" class="muted">No roster entries.</td></tr>`;
          return;
        }
        rostTable.innerHTML = rows
          .map((r, i) => {
            const name = [r.first, r.last].filter(Boolean).join(" ");
            const ms = r.ms || "";
            return `
              <tr data-i="${i}">
                <td>${esc(name)}</td>
                <td>${esc(ms)}</td>
                <td><input type="checkbox" class="present"></td>
                <td><input type="checkbox" class="ftr"></td>
                <td><input type="checkbox" class="excused"></td>
              </tr>
            `;
          })
          .join("");
      } catch (ex) {
        err("roster load error:", ex);
        rostTable.innerHTML = `<tr><td colspan="5" class="error">Roster error: ${esc(ex.message)}</td></tr>`;
      }
    }

    blockSel.addEventListener("change", loadRoster);
    // initial load
    loadRoster();

    // Save attendance -> POST /api/att/add_event_and_mark  (requires admin_pw)
    saveBtn.addEventListener("click", async (e) => {
      e.preventDefault();
      const label = (blockSel.value || "gsu").toLowerCase();
      const date = (dateInp.value || "").trim();
      const event_type = (typeSel.value || "PT").toUpperCase();
      const event_other = (otherInp.value || "").trim();

      if (!date) {
        alert("Enter a date (e.g., 8/27/2025)");
        return;
      }
      if (!ADMIN_PW) {
        alert("This action requires ?admin_pw=… in the URL.");
        return;
      }

      // collect marks from table
      const marks = qsa("#rost tbody tr").map((tr) => {
        const tds = tr.querySelectorAll("td");
        const name = tds[0]?.textContent?.trim() || "";
        return {
          name,
          present: tr.querySelector(".present")?.checked || false,
          ftr: tr.querySelector(".ftr")?.checked || false,
          excused: tr.querySelector(".excused")?.checked || false,
        };
      });

      try {
        const body = { admin_pw: ADMIN_PW, label, date, event_type, event_other, marks };
        const res = await postJSON("/api/att/add_event_and_mark", body);
        alert(`Saved: ${res.header || "event"} (${(res.updated_cells || []).length || "OK"})`);
      } catch (ex) {
        err("save attendance error:", ex);
        alert(`Save failed: ${ex.message}`);
      }
    });
  }

  // -------- reports ----------
  function wireReports() {
    const blockSel = qs("#rep_block");
    const dateInp = qs("#rep_date");
    const dayBtn = qs("#rep_day_btn");
    const dayOut = qs("#rep_day_out");

    const lbFrom = qs("#lb_from");
    const lbTo = qs("#lb_to");
    const lbBtn = qs("#lb_btn");
    const lbOut = qs("#lb_out");

    // day view
    if (blockSel && dateInp && dayBtn && dayOut) {
      dayBtn.addEventListener("click", async (e) => {
        e.preventDefault();
        const label = (blockSel.value || "gsu").toLowerCase();
        const date = (dateInp.value || "").trim();
        if (!date) {
          dayOut.innerHTML = `<div class="muted">Enter a date.</div>`;
          return;
        }
        dayOut.innerHTML = `<div class="muted">Loading…</div>`;
        try {
          const data = await getJSON("/api/att/day", { label, date });
          if (!data.ok) throw new Error(data.error || "Unknown error");
          const recs = data.records || [];
          const head = data.header || "";
          if (!recs.length) {
            dayOut.innerHTML = `<div class="muted">No records for ${esc(head || date)}.</div>`;
            return;
          }
          const html = [
            `<div class="h3">${esc(head)}</div>`,
            `<table class="table"><thead><tr><th>Name</th><th>Present</th><th>FTR</th><th>Excused</th></tr></thead><tbody>`,
            ...recs.map((r) => `<tr><td>${esc(r.name || "")}</td><td>${r.present ? "✓" : ""}</td><td>${r.ftr ? "✓" : ""}</td><td>${r.excused ? "✓" : ""}</td></tr>`),
            `</tbody></table>`,
          ].join("");
          dayOut.innerHTML = html;
        } catch (ex) {
          err("day view error:", ex);
          dayOut.innerHTML = `<div class="error">Day view failed: ${esc(ex.message)}</div>`;
        }
      });
    }

    // leaderboard
    if (lbFrom && lbTo && lbBtn && lbOut) {
      lbBtn.addEventListener("click", async (e) => {
        e.preventDefault();
        const label = (blockSel?.value || "gsu").toLowerCase();
        const dfrom = (lbFrom.value || "").trim();
        const dto = (lbTo.value || "").trim();
        lbOut.innerHTML = `<div class="muted">Building…</div>`;
        try {
          const data = await getJSON("/api/att/leaderboard", { label, from: dfrom, to: dto, top: 50 });
          if (!data.ok) throw new Error(data.error || "Unknown error");
          const rows = data.rows || data.leaderboard || [];
          if (!rows.length) {
            lbOut.innerHTML = `<div class="muted">No data in range.</div>`;
            return;
          }
          lbOut.innerHTML = `
            <table class="table">
              <thead><tr><th>#</th><th>Name</th><th>Pts</th></tr></thead>
              <tbody>
                ${rows.map((r, i) => `<tr><td>${i + 1}</td><td>${esc(r.name || "")}</td><td>${esc(r.points ?? r.pts ?? "")}</td></tr>`).join("")}
              </tbody>
            </table>`;
        } catch (ex) {
          err("leaderboard error:", ex);
          lbOut.innerHTML = `<div class="error">Leaderboard failed: ${esc(ex.message)}</div>`;
        }
      });
    }
  }

  // -------- boot ----------
  document.addEventListener("DOMContentLoaded", () => {
    try {
      wireTabs();
      wireAvailability();
      wireAttendance();
      wireReports();
      log("initialized");
      if (!PW) warn("No ?pw=… provided; API calls will 401.");
    } catch (ex) {
      err("init failed:", ex);
    }
  });
})();
