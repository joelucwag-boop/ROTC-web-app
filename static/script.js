/* Tabs + modal + API glue + Charts */

(function () {
  const $ = (s, p) => (p || document).querySelector(s);
  const $$ = (s, p) => Array.from((p || document).querySelectorAll(s));
  const pw = window.APP_PW || new URLSearchParams(location.search).get("pw") || "";

  // -------------------------- Tabs --------------------------
  $$(".tab").forEach(btn => {
    btn.addEventListener("click", () => {
      $$(".tab").forEach(b => b.classList.remove("on"));
      btn.classList.add("on");
      const tgt = btn.getAttribute("data-for");
      $$(".panel").forEach(p => p.classList.remove("on"));
      $("#" + tgt).classList.add("on");

      if (tgt === "directory" && !$("#dir_out").dataset.loaded) {
        loadDirectory();
      }
      if (tgt === "reports" && !window.__charts_loaded__) {
        refreshCharts();  // auto-render on first open
        window.__charts_loaded__ = true;
      }
    });
  });

  // ------------------------- Modal --------------------------
  const modal = $("#modal");
  const mclose = $("#mclose");
  if (mclose) mclose.onclick = () => (modal.style.display = "none");

  function showPersonKV(title, obj) {
    $("#mtitle").textContent = title;
    const kv = $("#kvgrid");
    kv.innerHTML = Object.entries(obj).map(([k,v]) => {
      if (typeof v === "object" && v !== null) {
        return `<div class="kv"><b>${k}</b>: <pre>${JSON.stringify(v, null, 2)}</pre></div>`;
      }
      return `<div class="kv"><b>${k}</b>: ${v}</div>`;
    }).join("");
    modal.style.display = "block";
  }

  // -------------------- Availability Search -----------------
  const go = $("#a_go");
  if (go) {
    go.onclick = async () => {
      const out = $("#a_out");
      out.innerHTML = "Searching…";
      const day = $("#day").value;
      const start = $("#a_start").value.trim();
      const end = $("#a_end").value.trim();
      try {
        const r = await fetch(`/api/available?pw=${pw}&day=${encodeURIComponent(day)}&start=${start}&end=${end}`);
        const j = await r.json();
        if (!j.ok) throw new Error(j.error || "bad response");
        let html = "<table class='table'><thead><tr><th>Name</th><th>MS</th></tr></thead><tbody>";
        for (const row of j.rows) {
          const full = `${row.first} ${row.last}`.trim();
          html += `<tr><td><a href="#" class="plink" data-name="${full}">${full}</a></td><td>${row.ms||""}</td></tr>`;
        }
        html += "</tbody></table>";
        out.innerHTML = html;
        $$(".plink", out).forEach(a => a.onclick = async (ev) => {
          ev.preventDefault();
          const name = ev.target.dataset.name;
          const r2 = await fetch(`/api/cadet/details?pw=${pw}&name=${encodeURIComponent(name)}`);
          const j2 = await r2.json();
          if (!j2.ok) throw new Error(j2.error);
          showPersonKV(name, j2.person);
        });
      } catch (e) {
        out.innerHTML = `<div class="err">Error: ${e.message}</div>`;
      }
    };
  }

  // ------------------------- Directory ----------------------
  async function loadDirectory() {
    const out = $("#dir_out");
    out.innerHTML = "Loading directory…";
    try {
      const r = await fetch(`/api/cadet/list?pw=${pw}`);
      const j = await r.json();
      if (!j.ok) throw new Error(j.error || "bad response");

      let html = `
        <div class="row">
          <input id="dir_filter" class="input" placeholder="Search name… (type to filter)">
        </div>
        <table class='table' id='dir_tbl'>
          <thead><tr><th>Name</th><th>MS</th></tr></thead><tbody>`;
      for (const c of j.rows) {
        const full = `${c.first} ${c.last}`.trim();
        html += `<tr><td><a href="#" class="plink" data-name="${full}">${full}</a></td><td>${c.ms||""}</td></tr>`;
      }
      html += "</tbody></table>";
      out.innerHTML = html;
      out.dataset.loaded = "1";

      $("#dir_filter").addEventListener("input", (e) => {
        const q = e.target.value.trim().toLowerCase();
        $$("#dir_tbl tbody tr").forEach(tr => {
          const nm = tr.children[0].innerText.toLowerCase();
          tr.style.display = nm.includes(q) ? "" : "none";
        });
      });

      $$(".plink", out).forEach(a => a.onclick = async (ev) => {
        ev.preventDefault();
        const name = ev.target.dataset.name;
        const r2 = await fetch(`/api/cadet/details?pw=${pw}&name=${encodeURIComponent(name)}`);
        const j2 = await r2.json();
        if (!j2.ok) throw new Error(j2.error);
        showPersonKV(name, j2.person);
      });
    } catch (e) {
      out.innerHTML = `<div class="err">Directory error: ${e.message}</div>`;
    }
  }

  // -------------------------- Reports (Text) ----------------
  const repBtn = $("#rep_day_btn");
  if (repBtn) {
    repBtn.onclick = async () => {
      const blk = $("#rep_block").value;
      const date = $("#rep_date").value.trim();
      const out = $("#rep_day_out");
      out.textContent = "Loading day…";
      try {
        const r = await fetch(`/api/att/day?pw=${pw}&label=${blk}&date=${encodeURIComponent(date)}`);
        const j = await r.json();
        if (!j.ok) throw new Error(j.error || "bad response");
        out.textContent = j.text || "(no data)";
      } catch (e) {
        out.textContent = `Day error: ${e.message}`;
      }
    };
  }

  const lbBtn = $("#lb_btn");
  if (lbBtn) {
    lbBtn.onclick = async () => {
      const blk = $("#rep_block").value; // reuse same block selector
      const from = $("#lb_from").value.trim();
      const to   = $("#lb_to").value.trim();
      const out  = $("#lb_out");
      out.textContent = "Building leaderboard…";
      try {
        const r = await fetch(`/api/att/leaderboard?pw=${pw}&label=${blk}&from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}&top=10&mode=text`);
        const j = await r.json();
        if (!j.ok) throw new Error(j.error || "bad response");
        out.textContent = j.text || "(no data)";
      } catch (e) {
        out.textContent = `Leaderboard error: ${e.message}`;
      }
    };
  }

  // ------------------------------ Charts --------------------
  let chartTotals = null;
  let chartMS = null;

  async function refreshCharts() {
    const blk = $("#rep_block").value;
    const from = $("#ch_from").value.trim();
    const to   = $("#ch_to").value.trim();
    try {
      const r = await fetch(`/api/att/charts?pw=${pw}&label=${blk}&from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}`);
      const j = await r.json();
      if (!j.ok) throw new Error(j.error || "bad response");
      renderCharts(j);
    } catch (e) {
      console.error("charts load error:", e);
      // show inline message
      const ctx1 = $("#chart_totals").getContext("2d");
      ctx1.font = "14px sans-serif";
      ctx1.fillText("Charts error: " + e.message, 10, 20);
    }
  }

  function renderCharts(data) {
    const labels = data.labels || [];
    const totals = data.totals || {present:[], ftr:[], excused:[]};
    const byms   = data.by_ms || {"MS1":[],"MS2":[],"MS3":[],"MS4":[],"MS5":[]};

    // Totals chart
    if (chartTotals) chartTotals.destroy();
    chartTotals = new Chart($("#chart_totals"), {
      type: "line",
      data: {
        labels,
        datasets: [
          { label: "Present (total)", data: totals.present },
          { label: "FTR (total)",     data: totals.ftr },
          { label: "Excused (total)", data: totals.excused }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { position: "top" }, title: { display: true, text: "Weekly Totals" } },
        scales: { x: { ticks: { autoSkip: true, maxTicksLimit: 10 } } }
      }
    });

    // MS chart
    if (chartMS) chartMS.destroy();
    chartMS = new Chart($("#chart_ms"), {
      type: "line",
      data: {
        labels,
        datasets: [
          { label: "MS1 Present", data: byms.MS1 },
          { label: "MS2 Present", data: byms.MS2 },
          { label: "MS3 Present", data: byms.MS3 },
          { label: "MS4 Present", data: byms.MS4 },
          { label: "MS5 Present", data: byms.MS5 }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { position: "top" }, title: { display: true, text: "Weekly Present by MS" } },
        scales: { x: { ticks: { autoSkip: true, maxTicksLimit: 10 } } }
      }
    });
  }

  const chBtn = $("#ch_refresh");
  if (chBtn) chBtn.onclick = refreshCharts;

})();
