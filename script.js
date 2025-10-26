// tabs
document.querySelectorAll('.tab').forEach(t=>{
  t.onclick = ()=>{
    document.querySelectorAll('.tab').forEach(x=>x.classList.remove('on'));
    document.querySelectorAll('.panel').forEach(x=>x.classList.remove('on'));
    t.classList.add('on');
    document.getElementById(t.dataset.for).classList.add('on');
  };
});
function getPW(){ const u=new URLSearchParams(location.search).get('pw'); if(u){localStorage.setItem('pw',u);return u;} return localStorage.getItem('pw')||''; }
function escapeHtml(s){return (s||'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}

// Availability
document.getElementById('a_go').onclick = async ()=>{
  const day=a_day.value, start=a_start.value.trim(), end=a_end.value.trim();
  const r=await fetch(`/api/available?day=${encodeURIComponent(day)}&start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}&pw=${encodeURIComponent(getPW())}`);
  const j=await r.json(); const out=document.getElementById('a_out');
  if(!j.ok){ out.textContent=j.error||'Error'; return; }
  out.innerHTML = `<table class=table><thead><tr><th>Name</th><th>MS</th><th>Phone</th><th>Email</th><th>Info</th></tr></thead><tbody>${
    j.people.map(p=>`<tr><td>${escapeHtml((p.first||'')+' '+(p.last||''))}</td><td>${escapeHtml(p.ms)}</td><td>${escapeHtml(p.phone)}</td><td>${escapeHtml(p.email)}</td><td><button class=btn data-row="${p.row}" data-name="${escapeHtml((p.first||'')+' '+(p.last||''))}">View</button></td></tr>`).join('')
  }</tbody></table>`;
  out.querySelectorAll('button[data-row]').forEach(b=> b.onclick=()=>openPerson(b.dataset.row,b.dataset.name));
};
async function openPerson(row,name){
  try{
    const r=await fetch(`/api/person?row=${encodeURIComponent(row)}&drop_days=1&pw=${encodeURIComponent(getPW())}`);
    const j=await r.json(); if(!j.ok) throw new Error(j.error||'Fail');
    mtitle.textContent=name; kvgrid.innerHTML='';
    Object.entries(j.fields).forEach(([k,v])=>{ const a=document.createElement('div');a.textContent=k;a.className='kv'; kvgrid.append(a); const b=document.createElement('div');b.textContent=String(v??''); kvgrid.append(b); });
    modal.style.display='flex';
  }catch(e){ alert('Could not load info: '+e.message); }
}
mclose.onclick = ()=> modal.style.display='none';
modal.onclick = e=>{ if(e.target.id==='modal') modal.style.display='none'; };

// Attendance
let statusByKey = {}; // key -> {status, ms}
async function loadRoster(label){
  const r=await fetch(`/api/roster?label=${encodeURIComponent(label)}&pw=${encodeURIComponent(getPW())}`);
  const j=await r.json(); if(!j.ok){ alert(j.error||'Roster failed'); return; }
  renderRoster(j.rows);
}
function renderRoster(rows){
  const tb=document.querySelector('#rost tbody'); tb.innerHTML='';
  rows.forEach(r=>{
    const name=`${r.first||''} ${r.last||''}`.trim(); const ms=String(r.ms||'').trim(); const key=`${(r.first||'').toLowerCase()}|${(r.last||'').toLowerCase()}`;
    const tr=document.createElement('tr');
    tr.innerHTML=`<td>${escapeHtml(name)}</td><td>${escapeHtml(ms)}</td><td><button class=btn data-kind=Present>Present</button></td><td><button class=btn data-kind=FTR>FTR</button></td><td><button class=btn data-kind=Excused>Excused</button></td>`;
    tb.append(tr);
    const btns=tr.querySelectorAll('button');
    const refresh=()=>{ btns.forEach(b=>b.textContent=b.dataset.kind); if(statusByKey[key]){ const k=statusByKey[key].status; const b=[...btns].find(x=>x.dataset.kind===k); if(b) b.textContent='✓ '+b.dataset.kind; } };
    btns.forEach(b=> b.onclick=()=>{ const desired=b.dataset.kind; const cur=statusByKey[key]?.status; if(cur===desired){ delete statusByKey[key]; } else { statusByKey[key]={status:desired,ms:ms}; } refresh(); });
    refresh();
  });
}
evt_block.addEventListener('change', e=>{ statusByKey={}; loadRoster(e.target.value); });
save_att.onclick = async ()=>{
  const date=evt_date.value.trim(); const t=evt_type.value; const other=evt_other.value.trim(); const label=evt_block.value;
  if(t==='OTHER' && !other){ alert('Provide event name'); return; }
  const marks=Object.entries(statusByKey).map(([k,v])=>{ const [first,last]=k.split('|'); let status=v.status; if(status==='Excused'){ const msNum=parseInt(String(v.ms||'').replace(/\D/g,''),10); status=(msNum===1||msNum===2)?'Excused: NFR':'Excused'; } return {first,last,status}; });
  if(marks.length===0){ alert('No marks'); return; }
  const admin_pw=prompt('Admin password?')||'';
  const r=await fetch('/api/att/add_event_and_mark?admin_pw='+encodeURIComponent(admin_pw),{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({date,event_type:t,event_other:other,label,marks})});
  const j=await r.json(); if(!j.ok){ alert(j.error||'Save failed'); return; } alert('Saved '+j.updated_cells.length+' marks to '+j.header); statusByKey={}; loadRoster(label);
};

// Reports
rep_day_btn.onclick = async ()=>{
  const label=rep_block.value, date=rep_date.value.trim();
  const r=await fetch(`/api/att/day?label=${encodeURIComponent(label)}&date=${encodeURIComponent(date)}&pw=${encodeURIComponent(getPW())}`);
  const j=await r.json(); const out=document.getElementById('rep_day_out'); if(!j.ok){ out.textContent=j.error||'No data'; return; }
  out.innerHTML=`<div>Event: ${escapeHtml(j.header)}</div><table class=table><thead><tr><th>Name</th><th>MS</th><th>Status</th></tr></thead><tbody>${j.records.map(r=>`<tr><td>${escapeHtml((r.first||'')+' '+(r.last||''))}</td><td>${escapeHtml(r.ms)}</td><td>${escapeHtml(r.status)}</td></tr>`).join('')}</tbody></table>`;
};
lb_btn.onclick = async ()=>{
  const label=rep_block.value, from=lb_from.value.trim(), to=lb_to.value.trim();
  const r=await fetch(`/api/att/leaderboard?label=${encodeURIComponent(label)}&from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}&pw=${encodeURIComponent(getPW())}`);
  const j=await r.json(); const out=document.getElementById('lb_out'); if(!j.ok){ out.textContent=j.error||'No data'; return; }
  out.innerHTML=`<div>Events counted: ${j.events.length}</div><table class=table><thead><tr><th>#</th><th>Name</th><th>MS</th><th>Present</th><th>FTR</th><th>Excused</th><th>Absent</th><th>Score</th></tr></thead><tbody>${j.rows.map((p,i)=>`<tr><td>${i+1}</td><td>${escapeHtml((p.first||'')+' '+(p.last||''))}</td><td>${escapeHtml(p.ms)}</td><td>${p.present}</td><td>${p.ftr}</td><td>${p.excused}</td><td>${p.absent}</td><td>${Number(p.score).toFixed(2)}</td></tr>`).join('')}</tbody></table>`;
};

// init
loadRoster('gsu');
