/* global window, document, fetch, location */
(function () {
  const qs  = (sel, p=document) => p.querySelector(sel);
  const qsa = (sel, p=document) => Array.from(p.querySelectorAll(sel));
  const out = (id, html) => (qs(id).innerHTML = html);

  // ----- tabs -----
  qsa(".tab").forEach(b=>{
    b.addEventListener("click", ()=>{
      qsa(".tab").forEach(x=>x.classList.remove("on"));
      qsa(".panel").forEach(x=>x.classList.remove("on"));
      b.classList.add("on");
      qs("#"+b.dataset.for).classList.add("on");
    });
  });

  // ----- Availability -----
  const aBtn = qs("#a_go");
  if (aBtn) aBtn.addEventListener("click", async ()=>{
    const day = qs("#day").value;
    const start = qs("#a_start").value.trim();
    const end   = qs("#a_end").value.trim();
    try {
      const r = await fetch(`/api/available?day=${encodeURIComponent(day)}&start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}${pwParam()}`);
      const j = await r.json();
      if (!j.ok) throw new Error(j.error || "failed");
      // render cards with clickable names
      const html = j.rows.length ? j.rows.map(row => {
        const name = `${row.first} ${row.last}`.trim();
        return `<div class="card line">
          <span class="pill">${row.ms || ""}</span>
          <a href="#" data-person="${encodeURIComponent(name)}" class="person-link">${name}</a>
          <span class="muted">${row.school||""}</span>
        </div>`;
      }).join("") : `<div class="muted">No one available.</div>`;
      out("#a_out", html);
      wirePersonLinks();
    } catch (e) {
      out("#a_out", `Error: ${e}`);
    }
  });

  // directory
  const dirBtn = qs("#open_dir");
  if (dirBtn) dirBtn.addEventListener("click", async ()=>{
    try {
      const r = await fetch(`/api/directory${pwParam()}`);
      const j = await r.json();
      if (!j.ok) throw new Error(j.error || "failed");
      const list = (j.rows||[]).map(x=>{
        const name = `${x.first||""} ${x.last||""}`.trim();
        return `<li><a href="#" class="person-link" data-person="${encodeURIComponent(name)}">${name}</a> <span class="muted">${x.ms||""}</span></li>`;
      }).join("");
      showModal("Cadet Directory", `<ul class="list">${list}</ul>`);
      wirePersonLinks();
    } catch (e) {
      showModal("Error", `<pre>${String(e)}</pre>`);
    }
  });

  function wirePersonLinks(){
    qsa("a.person-link").forEach(a=>{
      a.addEventListener("click", async (ev)=>{
        ev.preventDefault();
        const q = decodeURIComponent(a.dataset.person);
        try{
          const r = await fetch(`/api/person?q=${encodeURIComponent(q)}${pwParam()}`);
          const j = await r.json();
          if(!j.ok) throw new Error(j.error || "lookup failed");
          const p = j.person || {};
          const kv = flattenPerson(p);
          const html = Object.keys(kv).map(k=>`<div><b>${k}:</b> ${kv[k]}</div>`).join("");
          showModal(`${p.first||""} ${p.last||""}`.trim() || "Details", `<div class="kvgrid">${html}</div>`);
        } catch(e){
          showModal("Error", `<pre>${String(e)}</pre>`);
        }
      });
    });
  }

  function flattenPerson(p){
    const o = {};
    const push = (k,v)=>{ if(v==null || v==="") return; o[k]= (typeof v==="object") ? JSON.stringify(v) : String(v); };
    push("First", p.first); push("Last", p.last); push("MS", p.ms);
    push("School", p.school); push("Email", p.email); push("Phone", p.phone);
    push("Contracted", p.contracted); push("Prior Service", p.prior_service);
    push("Major", p.major); push("Vehicle", p.vehicle);
    if (p.extras){
      Object.entries(p.extras).forEach(([k,v])=> push(titleCase(k.replace(/_/g," ")), v));
    }
    return o;
  }
  function titleCase(s){ return s.replace(/\w\S*/g, t=>t[0].toUpperCase()+t.slice(1)); }

  // modal
  const modal = qs("#modal"), mclose = qs("#mclose"), mtitle = qs("#mtitle"), kvgrid = qs("#kvgrid");
  if (mclose) mclose.addEventListener("click", ()=> modal.classList.remove("show"));
  function showModal(title, innerHtml){
    mtitle.textContent = title;
    kvgrid.innerHTML = innerHtml;
    modal.classList.add("show");
  }

  // ----- Attendance -----
  const rostBody = qs("#rost tbody");
  if (rostBody){
    // fetch roster immediately
    loadRoster();
    qs("#save_att").addEventListener("click", saveAttendance);
  }

  async function loadRoster(){
    try{
      const r = await fetch(`/api/roster?label=${encodeURIComponent(qs("#evt_block").value)}${pwParam()}`);
      const j = await r.json();
      if (!j.ok) throw new Error(j.error || "roster failed");
      rostBody.innerHTML = (j.rows||[]).map(r=>{
        const name = `${r.first} ${r.last}`.trim();
        return `<tr>
          <td>${name}</td>
          <td>${r.ms||""}</td>
          <td><input type="checkbox" class="mark" data-name="${name}" data-mark="P"></td>
          <td><input type="checkbox" class="mark" data-name="${name}" data-mark="FTR"></td>
          <td><input type="checkbox" class="mark" data-name="${name}" data-mark="E"></td>
        </tr>`;
      }).join("");
    }catch(e){
      rostBody.innerHTML = `<tr><td colspan="5">Error: ${e}</td></tr>`;
    }
  }

  async function saveAttendance(){
    const admin_pw = qs("#admin_pw").value.trim();
    if (!admin_pw){ alert("Admin password required."); return; }
    const marks = [];
    qsa("input.mark:checked").forEach(ch=>{
      marks.push({ name: ch.dataset.name, mark: ch.dataset.mark });
    });
    const body = {
      admin_pw,
      date: qs("#evt_date").value.trim(),
      event_type: qs("#evt_type").value,
      event_other: qs("#evt_other").value.trim(),
      label: qs("#evt_block").value,
      marks
    };
    try{
      const r = await fetch(`/api/att/add_event_and_mark${pwParam()}`, {
        method:"POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify(body)
      });
      const j = await r.json();
      if(!j.ok) throw new Error(j.error || "save failed");
      alert(`Saved: ${j.header}`);
    }catch(e){
      alert(`Error: ${e}`);
    }
  }

  // ----- Reports -> Day view -----
  const repBtn = qs("#rep_day_btn");
  if (repBtn) repBtn.addEventListener("click", async ()=>{
    const label = qs("#rep_block").value;
    const date  = qs("#rep_date").value.trim();
    try{
      const r = await fetch(`/api/att/day?label=${encodeURIComponent(label)}&date=${encodeURIComponent(date)}${pwParam()}`);
      const j = await r.json();
      if(!j.ok) throw new Error(j.error || "day failed");
      const rows = j.records || [];
      const line = [j.header].concat(rows.map(x=>{
        const flags = [];
        if (String(x.present||"").toUpperCase()==="P") flags.push("P "+(x.first||""));
        if (String(x.ftr||"").toUpperCase()==="FTR") flags.push("FTR "+(x.first||""));
        if (String(x.excused||"").toUpperCase()==="E") flags.push("E "+(x.first||""));
        if (!flags.length) flags.push(x.first||"");
        return flags.join(" — ");
      })).join(" — ");
      out("#rep_day_out", line);
    }catch(e){
      out("#rep_day_out", `Error: ${e}`);
    }
  });

  // ----- Reports -> Leaderboard (pretty) -----
  const lbBtn = qs("#lb_btn");
  if (lbBtn) lbBtn.addEventListener("click", buildLeaderboard);

  async function buildLeaderboard(){
    const from = qs("#lb_from").value.trim();
    const to   = qs("#lb_to").value.trim();
    try{
      const r = await fetch(`/api/att/leaderboard?label=gsu&from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}&top=10${pwParam()}`);
      const j = await r.json();
      if(!j.ok) throw new Error(j.error || "lb failed");
      out("#lb_out", renderLeaderboardPretty(j));
    }catch(e){
      out("#lb_out", `Error: ${e}`);
    }
  }

  function renderLeaderboardPretty(data){
    // expects data like { MS1:[{name,present,ftr,sessions},...], MS2:[], ... }
    const sec = (title, arr, mode) => {
      if (!arr || !arr.length) return "";
      const lines = arr.map((p,i)=>{
        const rank = (i+1).toString().padStart(2," ");
        if (mode==="present")
          return `${rank}. ${p.name}  (${p.present} Present; Sessions: ${p.sessions})`;
        else // fewest FTR
          return `${rank}. ${p.name}  (${p.ftr} FTR; ${p.present} Present; Sessions: ${p.sessions})`;
      }).join("\n");
      return `<pre class="lb-block"><b>${title}</b>\n${lines}</pre>`;
    };
    return [
      sec("MS1 — Top 10 (highest Present)", data.MS1 || [], "present"),
      sec("MS2 — Top 10 (highest Present)", data.MS2 || [], "present"),
      sec("MS3 — Top 10 (fewest FTR)",      data.MS3 || [], "ftr"),
      sec("MS4 — Top 10 (fewest FTR)",      data.MS4 || [], "ftr"),
      sec("MS5 — Top 10 (fewest FTR)",      data.MS5 || [], "ftr")
    ].join("");
  }

  // ----- Charts auto-load -----
  const chartsDiv = qs("#charts");
  if (chartsDiv){
    const files = [
      "attendance_weekly.png",
      "attendance_by_ms.png",
      "present_vs_ftr.png"
    ];
    chartsDiv.innerHTML = files.map(fn => 
      `<img src="/static/charts/${fn}" alt="${fn}" onerror="this.style.display='none'">`
    ).join("");
  }

  // ----- helpers -----
  function pwParam(){
    const m = location.search.match(/[?&]pw=([^&]+)/);
    return m ? `&pw=${m[1]}` : "";
  }
})();

