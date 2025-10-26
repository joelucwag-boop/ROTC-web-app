import os, re, json, datetime as dt
import gspread
from google.oauth2.service_account import Credentials

SPREADSHEET_ID  = os.getenv("SPREADSHEET_ID","").strip()
WORKSHEET_NAME  = os.getenv("WORKSHEET_NAME","Attendance Roster").strip()
GSU_HEADER_ROW  = int(os.getenv("GSU_HEADER_ROW","7"))
ULM_HEADER_ROW  = int(os.getenv("ULM_HEADER_ROW","79"))

POINTS_PRESENT  = float(os.getenv("POINTS_PRESENT","1"))
POINTS_LATE     = float(os.getenv("POINTS_LATE","0.5"))
POINTS_EXCUSED  = float(os.getenv("POINTS_EXCUSED","0.25"))
POINTS_ABSENT   = float(os.getenv("POINTS_ABSENT","0"))

DATE_RX = re.compile(r'^\s*(\d{1,2})/(\d{1,2})/(\d{4})\s*\+\s*(.+?)\s*$')

def _client(scopes=None):
    info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    scopes = scopes or ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def _ws():
    return _client().open_by_key(SPREADSHEET_ID).worksheet(WORKSHEET_NAME)

def _by_label(label:str):
    label=(label or 'gsu').lower()
    if label in ('gsu','main'): return {'header_row': GSU_HEADER_ROW, 'first_col':1, 'last_col':2, 'ms_col':3, 'first_event_col':4}
    if label=='ulm': return {'header_row': ULM_HEADER_ROW, 'first_col':1, 'last_col':2, 'ms_col':3, 'first_event_col':4}
    raise ValueError("label must be 'gsu' or 'ulm'")

def _row_bounds(ws,b):
    start=b['header_row']+1
    vals=ws.get(gspread.utils.rowcol_to_a1(start,b['first_col'])+':'+gspread.utils.rowcol_to_a1(start+1000,b['ms_col']))
    for i,row in enumerate(vals):
        if not any((c or '').strip() for c in (row+['',''])[:3]):
            return start, start+i-1
    return start, start+len(vals)-1

def list_roster(label:str):
    ws=_ws(); b=_by_label(label)
    start=b['header_row']+1
    rng=gspread.utils.rowcol_to_a1(start,b['first_col'])+':'+gspread.utils.rowcol_to_a1(start+1000,b['ms_col'])
    vals=ws.get(rng)
    out=[]
    for row in vals:
        row=(row+['','',''])[:3]
        first,last,ms=(row[0] or '').strip(),(row[1] or '').strip(),(row[2] or '').strip()
        if not (first or last or ms): break
        out.append({'first':first,'last':last,'ms':ms})
    def msn(x):
        try: return int(str(x.get('ms','')).strip()[:1])
        except: return -999
    out.sort(key=lambda r:(-msn(r),(r['last'] or ''),(r['first'] or '')))
    return out

def _headers(ws,b):
    vals=ws.row_values(b['header_row'])
    return [(i+1,(v or '').strip()) for i,v in enumerate(vals)]

def guess_or_create_header(label:str, header:str):
    ws=_ws(); b=_by_label(label)
    hdrs=_headers(ws,b)
    for col,text in hdrs:
        if col < b['first_event_col']: continue
        if not text: break
        if text.strip().lower()==header.strip().lower(): return ws,b,col
    target=None
    for col,text in hdrs:
        if col >= b['first_event_col'] and not text:
            target=col; break
    if target is None: target=len(hdrs)+1
    ws.update_cell(b['header_row'], target, header)
    return ws,b,target

def add_event_and_mark(label:str, header:str, marks:list):
    ws,b,col=guess_or_create_header(label, header)
    r1,r2=_row_bounds(ws,b)
    rng=gspread.utils.rowcol_to_a1(r1,b['first_col'])+':'+gspread.utils.rowcol_to_a1(r2,b['last_col'])
    names=ws.get(rng)
    updated=[]
    for m in marks:
        first=(m.get('first') or '').strip().lower()
        last=(m.get('last') or '').strip().lower()
        status=(m.get('status') or '').strip()
        if not (first or last): continue
        found=None
        for i,row in enumerate(names, start=r1):
            f=(row[0] if len(row)>0 else '').strip().lower()
            l=(row[1] if len(row)>1 else '').strip().lower()
            if f==first and l==last:
                found=i; break
        if found is None: continue
        ws.update_cell(found, col, status)
        updated.append({'row':found,'col':col,'status':status})
    return updated

def list_events(label:str):
    ws=_ws(); b=_by_label(label)
    vals=ws.row_values(b['header_row'])
    ev=[]
    for j in range(b['first_event_col'], len(vals)+1):
        t=(vals[j-1] or '').strip()
        if not t: break
        m=DATE_RX.match(t); date_str=None; evname=t
        if m:
            M,D,Y,name=m.groups()
            try:
                date_obj=dt.date(int(Y),int(M),int(D))
                date_str=f"{int(M)}/{int(D)}/{Y}"; evname=name
            except: pass
        ev.append({'col':j,'header':t,'date':date_str,'event':evname})
    return ev

def read_event_records(label:str, col:int):
    ws=_ws(); b=_by_label(label)
    r1,r2=_row_bounds(ws,b)
    rng=gspread.utils.rowcol_to_a1(r1,b['first_col'])+':'+gspread.utils.rowcol_to_a1(r2,col)
    vals=ws.get(rng)
    out=[]
    for row in vals:
        row=row+['','','']
        first,last,ms=row[0],row[1],row[2]
        if not (first or last or ms): continue
        st=(row[col-b['first_col']] if len(row)>(col-b['first_col']) else '')
        out.append({'first':first,'last':last,'ms':ms,'status':st})
    return out

def _score_for(status:str):
    s=(status or '').strip().lower()
    if s=='present': return POINTS_PRESENT
    if s=='ftr': return POINTS_LATE
    if s.startswith('excused'): return POINTS_EXCUSED
    if not s: return POINTS_ABSENT
    return POINTS_ABSENT

def leaderboard(label:str, start_date=None, end_date=None, top=50):
    evs=list_events(label)
    chosen=[]
    for e in evs:
        if not e['date']: continue
        d=dt.datetime.strptime(e['date'],'%m/%d/%Y').date()
        if start_date and d<start_date: continue
        if end_date and d>end_date: continue
        chosen.append(e)
    if not chosen: return {'events':[],'rows':[]}
    people={}
    for e in chosen:
        for r in read_event_records(label, e['col']):
            key=(r['first'],r['last'])
            if key not in people:
                people[key]={'first':r['first'],'last':r['last'],'ms':r['ms'],
                             'present':0,'ftr':0,'excused':0,'absent':0,'score':0.0,'events':0}
            p=people[key]; st=(r['status'] or '').strip().lower()
            p['events']+=1
            if st=='present': p['present']+=1
            elif st=='ftr': p['ftr']+=1
            elif st.startswith('excused'): p['excused']+=1
            else: p['absent']+=1
            p['score']+=_score_for(r['status'])
    rows=sorted(people.values(), key=lambda x:(-x['score'], x['last'], x['first']))[:top]
    return {'events':chosen,'rows':rows}
