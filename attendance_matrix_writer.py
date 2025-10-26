import os, json, re
import gspread
from typing import List, Dict, Optional
from google.oauth2.service_account import Credentials
# For conditional formatting:
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials as SACreds

ATT_SHEET_ID = os.getenv("ATT_SHEET_ID","").strip()
ATT_TAB      = os.getenv("ATT_TAB","Attendance Roster").strip()

GSU_HEADER_ROW = int(os.getenv("GSU_HEADER_ROW","7"))
ULM_HEADER_ROW = int(os.getenv("ULM_HEADER_ROW","79"))

def _gc():
    info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def _ws():
    return _gc().open_by_key(ATT_SHEET_ID).worksheet(ATT_TAB)

def _block(header_row:int):
    return {"header_row": header_row, "first_col":1, "last_col":2, "ms_col":3, "first_event_col":4}

def _by_label(label:str):
    lab = (label or "").strip().lower()
    if lab in ("gsu","main"): return _block(GSU_HEADER_ROW)
    if lab=="ulm": return _block(ULM_HEADER_ROW)
    raise ValueError("label must be 'gsu' or 'ulm'")

def _read_row(ws, row, c1, c2):
    a1 = gspread.utils.rowcol_to_a1(row,c1)+":"+gspread.utils.rowcol_to_a1(row,c2)
    vals = ws.get(a1)
    return (vals[0] if vals else [])

def _headers(ws, b):
    raw = _read_row(ws, b["header_row"], b["first_event_col"], 500)
    out = [ (x or "").strip() for x in raw ]
    while out and out[-1]=="": out.pop()
    return out

def _rightmost_event_col(ws, b)->int:
    hdrs = _headers(ws,b)
    return b["first_event_col"] + len(hdrs) - 1 if hdrs else (b["first_event_col"]-1)

def _ensure_new_header(ws, b, header_text:str)->int:
    last_col = _rightmost_event_col(ws,b)
    new_col  = max(b["first_event_col"], last_col+1)
    ws.update(gspread.utils.rowcol_to_a1(b["header_row"], new_col), header_text)
    return new_col

def _data_bounds(ws, b):
    start = b["header_row"]+1
    rng = gspread.utils.rowcol_to_a1(start,b["first_col"])+":"+gspread.utils.rowcol_to_a1(start+1000,b["ms_col"])
    block = ws.get(rng)
    for i,row in enumerate(block):
        row = (row+["",""])[:3]
        if not any((c or "").strip() for c in row):
            return (start, start+i-1)
    return (start, start+len(block)-1)

def _find_row(ws,b,first,last)->Optional[int]:
    first = (first or "").strip().lower(); last=(last or "").strip().lower()
    r1,r2 = _data_bounds(ws,b)
    if r2<r1: return None
    rng = gspread.utils.rowcol_to_a1(r1,b["first_col"])+":"+gspread.utils.rowcol_to_a1(r2,b["last_col"])
    vals = ws.get(rng)
    for i,row in enumerate(vals):
        fn = (row[0] if len(row)>0 else "").strip().lower()
        ln = (row[1] if len(row)>1 else "").strip().lower()
        if fn==first and ln==last: return r1+i
    return None

def _sheet_id(ws)->int:
    try: return int(ws.id)
    except:  # fallback parse
        import urllib.parse as up
        return int(up.parse_qs(up.urlsplit(ws.url).query).get("gid",["0"])[0])

def _apply_column_colors(spreadsheet_id:str, sheet_id:int, col:int, header_row:int):
    info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    creds = SACreds.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    svc = build("sheets","v4",credentials=creds)

    start_row = header_row # you can use header_row+1 to avoid coloring header
    col0 = col-1
    green = {"red":0.80,"green":0.94,"blue":0.80}
    red   = {"red":0.96,"green":0.80,"blue":0.80}
    blue  = {"red":0.80,"green":0.87,"blue":0.95}

    rules = [
      {"type":"TEXT_EQ","val":"Present","fmt":green},
      {"type":"TEXT_EQ","val":"FTR","fmt":red},
      {"type":"TEXT_CONTAINS","val":"Excused","fmt":blue},
    ]
    reqs=[]
    for r in rules:
        cond = {"type": r["type"], "values":[{"userEnteredValue": r["val"]}]}
        fmt  = {"backgroundColor": r["fmt"]}
        reqs.append({"addConditionalFormatRule":{
            "rule":{"ranges":[{"sheetId":sheet_id,"startRowIndex":start_row,"startColumnIndex":col0,"endColumnIndex":col0+1}],
                    "condition":cond,"format":fmt},
            "index":0}})

    svc.spreadsheets().batchUpdate(spreadsheetId=spreadsheets_id, body={"requests":reqs}).execute()

def add_event_and_mark(label:str, date_mdyyyy:str, event_text:str,
                       marks:List[Dict[str,str]])->Dict:
    ws = _ws(); b=_by_label(label)
    header = f"{date_mdyyyy} + {event_text}".strip()
    new_col = _ensure_new_header(ws,b,header)

    # try to apply colors; ignore if library missing
    try:
        _apply_column_colors(ATT_SHEET_ID, _sheet_id(ws), new_col, b["header_row"])
    except Exception:
        pass

    updated=[]
    for m in marks:
        row = _find_row(ws,b,m.get("first"), m.get("last"))
        if not row: continue
        a1 = gspread.utils.rowcol_to_a1(row,new_col)
        ws.update(a1, (m.get("status") or "").strip())
        updated.append(a1)
    return {"header":header,"col":new_col,"updated_cells":updated}
