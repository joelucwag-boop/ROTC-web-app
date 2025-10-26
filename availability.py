import os, io, pandas as pd, requests, re

CSV_URL = os.getenv("GOOGLE_SHEET_URL","").strip()

DAY_MAP = {'mon':'monday','tue':'tuesday','wed':'wednesday','thu':'thursday','fri':'friday'}

def _load_df():
    if not CSV_URL: raise RuntimeError("GOOGLE_SHEET_URL is not set")
    resp = requests.get(CSV_URL, timeout=30); resp.raise_for_status()
    buf = io.StringIO(resp.text)
    df = pd.read_csv(buf)
    df.columns = [str(c).strip() for c in df.columns]
    return df

def _intervals_from_cell(cell):
    s = str(cell or '').strip()
    if not s: return []
    parts = re.split(r'[;,]\s*', s)
    out = []
    for p in parts:
        p = p.replace(':','')
        m = re.match(r'^(\d{3,4})-(\d{3,4})$', p)
        if not m: continue
        a,b = int(m.group(1)), int(m.group(2))
        if a<1000: a += 0
        if b<1000: b += 0
        out.append((a,b))
    return out

def _overlap(a1,a2,b1,b2):
    return max(a1,b1) < min(a2,b2)

def _find_day_col(df, day_key):
    want = DAY_MAP.get(day_key.lower()[:3],'monday')
    for c in df.columns:
        if c.strip().lower().startswith(want):
            return c
    raise KeyError(f'Day column not found for {day_key}')

def find_available(day, start, end):
    s = int(str(start).replace(':','')); e = int(str(end).replace(':',''))
    df = _load_df(); day_col = _find_day_col(df, day)
    email_cols=[c for c in df.columns if 'email' in c.lower()]; phone_cols=[c for c in df.columns if 'phone' in c.lower()]
    ms_cols=[c for c in df.columns if 'ms' in c.lower()]
    out=[]
    for idx,row in df.iterrows():
        busy=_intervals_from_cell(row.get(day_col))
        if all(not _overlap(s,e,a,b) for (a,b) in busy):
            first,last='',''
            if 'First Name' in df.columns: first=str(row.get('First Name') or '')
            if 'Last Name' in df.columns: last=str(row.get('Last Name') or '')
            if not first and 'Name' in df.columns:
                full=str(row.get('Name') or '')
                if ' ' in full: first,last=full.split(' ',1)
                else: first=full
            email=str(row.get(email_cols[0]) or '') if email_cols else ''
            phone=str(row.get(phone_cols[0]) or '') if phone_cols else ''
            ms=str(row.get(ms_cols[0]) or '') if ms_cols else ''
            out.append({'row': idx+2, 'first': first, 'last': last, 'ms': ms, 'email': email, 'phone': phone})
    def msn(x):
        try: return int(str(x.get('ms','')).strip()[:1])
        except: return -999
    out.sort(key=lambda r:(-msn(r), r.get('last',''), r.get('first','')))
    return out

def person_info(row, drop_days=False):
    df=_load_df()
    try:
        r=df.iloc[int(row)-2]
    except Exception:
        return None
    fields={}
    for c in df.columns:
        if drop_days and c.lower().startswith(('monday','tuesday','wednesday','thursday','friday')): 
            continue
        fields[c]=r.get(c,'')
    return fields
