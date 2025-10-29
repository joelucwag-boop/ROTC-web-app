#!/usr/bin/env python3
"""
Google Sheets Attendance Helper

Features
- get_attendance_by_date(sheet_id, tab_name, target_date, ms_level)
    -> returns dict with "Present", "FTR", "Excused" lists of cadet names for that MS level.
- get_cadet_record(sheet_id, tab_name, first_name=None, last_name=None, full_name=None)
    -> returns pandas Series of all date columns for that cadet.

Assumptions about the sheet (based on the screenshot):
- Columns include "NAME First", "NAME Last", "MS Level".
- Each attendance date column header contains a date in the form "M/D/YYYY" (possibly followed by text like " + PT").
- Cell values include "Present", "FTR", or strings starting with "Excused" (case-insensitive).

Setup
-----
1) pip install gspread google-auth pandas
2) Create a Service Account in Google Cloud & download the JSON key.
3) Share the Google Sheet with the service account's email (Viewer is enough for read-only).
4) Set the KEY_FILE constant below to your JSON filename (or pass it into the helper functions).

You can also adapt the code to OAuth Client ID if preferred, but service accounts are simplest for automations.
"""
from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials
import pandas as pd

# === CONFIG ===
# Path to your downloaded service-account JSON key file
KEY_FILE = r"c:\Users\joelu\OneDrive\Pictures\Desktop\sharp-cosmos-472916-i0-ea951b47926f.json"


@dataclass
class SheetConfig:
    sheet_id: str
    tab_name: str
    key_file: str = KEY_FILE


def _client_from_key(key_file: str) -> gspread.Client:
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = Credentials.from_service_account_file(key_file, scopes=scopes)
    return gspread.authorize(creds)


def _open_ws(cfg: SheetConfig) -> gspread.Worksheet:
    gc = _client_from_key(cfg.key_file)
    sh = gc.open_by_key(cfg.sheet_id)
    return sh.worksheet(cfg.tab_name)


def _sheet_to_df(ws: gspread.Worksheet) -> pd.DataFrame:
    rows = ws.get_all_values()
    if not rows:
        raise ValueError("Worksheet appears to be empty.")

    hdr_idx = _detect_header_row(rows, max_scan=10)
    header = rows[hdr_idx]
    data = rows[hdr_idx + 1:]

    # Trim trailing empty columns so headers align
    last_nonempty = max((i for i, h in enumerate(header) if (h or "").strip() != ""), default=-1)
    if last_nonempty >= 0:
        header = header[:last_nonempty + 1]
        data = [r[:last_nonempty + 1] for r in data]

    df = pd.DataFrame(data, columns=header)
    return df


def _normalize_name(first: str, last: str) -> str:
    return f"{first.strip()} {last.strip()}".strip()


def _extract_date_str(text: str) -> Optional[str]:
    """
    Pull 'M/D/YYYY' from header text like '8/11/2025 + PT'.
    Returns None if not found.
    """
    if not text:
        return None
    m = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', text)
    return m.group(1) if m else None


def _target_date_formats(target: str) -> List[str]:
    """
    Accept variants like '2025-08-11', '8/11/2025', '08/11/2025', etc.
    Returns a list of canonical M/D/YYYY strings to match against headers.
    """
    candidates = set()

    # ISO try
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%-m/%-d/%Y", "%m/%-d/%Y", "%-m/%d/%Y"):
        try:
            dt = datetime.strptime(target, fmt)
            candidates.add(f"{dt.month}/{dt.day}/{dt.year}")   # M/D/YYYY
        except Exception:
            pass

    # As-is fallback if it already looks like M/D/YYYY
    if re.fullmatch(r"\d{1,2}/\d{1,2}/\d{4}", target.strip()):
        candidates.add(target.strip())

    return list(candidates)


from datetime import date as _date

def _find_date_column(df: pd.DataFrame, target_date: str) -> Optional[str]:
    """
    Return the column name whose embedded date matches target_date
    (normalize both sides to real date objects so 09/11/2025 == 9/11/2025).
    """
    # Build set of target dates as date objects
    targets = set()
    for s in _target_date_formats(target_date):
        try:
            m, d, y = (int(x) for x in s.split("/"))
            targets.add(_date(y, m, d))
        except Exception:
            pass
    if not targets:
        raise ValueError(
            f"Could not parse target_date '{target_date}'. "
            "Try formats like 2025-08-11 or 8/11/2025."
        )

    # Scan headers; if a header contains a date, compare as date objects
    for col in df.columns:
        mdyyyy = _extract_date_str(col)
        if not mdyyyy:
            continue
        try:
            m, d, y = (int(x) for x in mdyyyy.split("/"))
            col_date = _date(y, m, d)
        except Exception:
            continue
        if col_date in targets:
            return col

    return None



def _classify_status(cell_value: str) -> Optional[str]:
    """Return 'Present', 'FTR', or 'Excused' (normalized), or None if not a known status."""
    v = (cell_value or "").strip().lower()
    if not v:
        return None
    if v == "present":
        return "Present"
    if v == "ftr":
        return "FTR"
    if v.startswith("excused"):
        return "Excused"
    return None

import re

def _norm(s: str) -> str:
    """Lowercase and strip to alphanumerics only (so 'NAME First' -> 'namefirst')."""
    return re.sub(r'[^a-z0-9]+', '', (s or '').strip().lower())

def _detect_header_row(rows, max_scan=10) -> int:
    """
    Return the 0-based row index to use as the header.
    Scans the first `max_scan` rows to find one that looks like real headers
    (contains 'name' and 'ms' style labels).
    """
    def norm(s):
        return re.sub(r'[^a-z0-9]+', '', (s or '').strip().lower())
    for i in range(min(max_scan, len(rows))):
        r = rows[i]
        if not r: 
            continue
        norms = [norm(c) for c in r]
        has_nameish = any(k in norms for k in ["namefirst","firstname","first","namelast","lastname","last"])
        has_msish   = any(k in norms for k in ["mslevel","ms","mslvl","msyear","msclass","mscohort"])
        # also ensure it's not just one giant merged title + empties
        nonempty = sum(1 for c in r if (c or "").strip() != "")
        if (has_nameish or has_msish) and nonempty >= 3:
            return i
    # fallback to row 0
    return 0


def _find_col(df: pd.DataFrame, wanted_keys: list[str]) -> Optional[str]:
    """
    Find a column whose normalized header matches any of the wanted_keys.
    Also supports regex patterns in wanted_keys when prefixed with 're:'.
    """
    headers = list(df.columns)
    norm_map = {col: _norm(col) for col in headers}
    wanted_norm = {w for w in wanted_keys if not w.startswith('re:')}
    wanted_regex = [re.compile(w[3:], re.I) for w in wanted_keys if w.startswith('re:')]

    # direct normalized match
    for col, nm in norm_map.items():
        if nm in wanted_norm:
            return col

    # regex fallback
    for col in headers:
        for rx in wanted_regex:
            if rx.search(col):
                return col

    return None

def _guess_ms_col(df: pd.DataFrame) -> Optional[str]:
    """
    Heuristic: pick a column whose values look like MS levels (1-5) or 'MS1'...'MS5'.
    """
    for col in df.columns:
        vals = pd.Series(df[col]).astype(str).str.strip().str.lower()
        sample = vals[vals != ''].head(30)
        if sample.empty:
            continue
        # accept if most values are 1..5, 'ms1'..'ms5', or 'ms 1'..'ms 5'
        ok = sample.apply(lambda v: v in {'1','2','3','4','5'} or re.fullmatch(r'ms\s*\d', v) is not None).mean()
        if ok >= 0.7:  # 70% look like MS levels
            return col
    return None

def _get_series(df: pd.DataFrame, colname: str) -> pd.Series:
    """
    Return a 1-D Series for a column label, even if the label is duplicated.
    If there are duplicates, take the first matching column.
    """
    obj = df[colname]
    if isinstance(obj, pd.DataFrame):
        return obj.iloc[:, 0]
    return obj

def _guess_ms_col(df: pd.DataFrame) -> Optional[str]:
    """
    Heuristic: choose a column whose values look like MS levels (1-5 or 'MS 1'..'MS 5').
    Works even if there are duplicate column headers elsewhere.
    """
    headers = list(df.columns)
    for i, col in enumerate(headers):
        s = df.iloc[:, i]              # always 1-D by position
        vals = s.astype(str).str.strip().str.lower()
        sample = vals[vals != ''].head(30)
        if sample.empty:
            continue
        ok = sample.apply(lambda v: v in {'1','2','3','4','5'} or re.fullmatch(r'ms\s*\d', v) is not None).mean()
        if ok >= 0.7:
            return col
    return None


def get_attendance_by_date(
    sheet_id: str,
    tab_name: str,
    target_date: str,
    ms_level: str | int,
    key_file: str = KEY_FILE,
) -> Dict[str, List[str]]:
    """
    Return {"Present": [...], "FTR": [...], "Excused": [...]} for the given MS level on the target date.
    """
    cfg = SheetConfig(sheet_id=sheet_id, tab_name=tab_name, key_file=key_file)
    ws = _open_ws(cfg)
    df = _sheet_to_df(ws)

    # --- FLEXIBLE header matching (requires _find_col, _guess_ms_col helpers) ---
    first_col = _find_col(df, [
        'namefirst','firstname','first','fname','givenname',
        're:^name.*first$', 're:^first\\b'
    ])
    last_col = _find_col(df, [
        'namelast','lastname','last','lname','surname','familyname',
        're:^name.*last$', 're:^last\\b'
    ])
    ms_col = _find_col(df, [
        'mslevel','ms','mslvl','msyear','msclass','mscohort',
        're:^ms\\s*level$', 're:^ms\\b'
    ])

    # Fallback: guess MS column by its values if not found
    if ms_col is None:
        ms_col = _guess_ms_col(df)

    if not (first_col and last_col and ms_col):
        raise ValueError(
            "Could not find required columns for First/Last/MS.\n"
            f"Headers seen: {list(df.columns)}\n"
            f"Detected -> first_col: {first_col}, last_col: {last_col}, ms_col: {ms_col}\n"
            "Tip: ensure your sheet has columns for first name, last name, and an MS level (1–5) column."
        )

    # --- Find the date column by the M/D/YYYY embedded in the header ---
    date_col = _find_date_column(df, target_date)
    if not date_col:
        raise ValueError(f"Could not find a date column matching {target_date}. Check your header row text.")

    # --- Normalize MS values and filter the target MS ---
    # Accept values like "3", " MS 3", "ms3", etc.
    
    df["_MS"] = (_get_series(df, ms_col).astype(str).str.lower().str.replace(r"^ms\s*", "", regex=True).str.strip()
    )
    
    wanted_ms = str(ms_level).lower().replace("ms", "").strip()
    df_ms = df[df["_MS"] == wanted_ms].copy()

    # --- Build buckets ---
    out = {"Present": [], "FTR": [], "Excused": []}
    for _, row in df_ms.iterrows():
        status = _classify_status(row.get(date_col, ""))
        if status:
            name = _normalize_name(str(row.get(first_col, "")), str(row.get(last_col, "")))
            out[status].append(name)

    return out


def get_cadet_record(
    sheet_id: str,
    tab_name: str,
    first_name: Optional[str] = None,
    last_name: Optional[str] = None,
    full_name: Optional[str] = None,
    key_file: str = KEY_FILE,
) -> pd.Series:
    """
    Return a Series of all date columns for the requested cadet.
    You can pass either (first_name, last_name) or full_name.
    """
    # ---- Normalize target name ----
    if full_name and full_name.strip():
        if " " in full_name.strip():
            target_first, target_last = [p.strip() for p in full_name.split(" ", 1)]
        else:
            target_first, target_last = full_name.strip(), ""
    else:
        target_first = (first_name or "").strip()
        target_last  = (last_name  or "").strip()
    target_full = _normalize_name(target_first, target_last).lower()

    # ---- Load sheet -> DataFrame ----
    cfg = SheetConfig(sheet_id=sheet_id, tab_name=tab_name, key_file=key_file)
    ws = _open_ws(cfg)
    df = _sheet_to_df(ws)

    # ---- FLEXIBLE header matching (requires _find_col / _guess_ms_col) ----
    first_col = _find_col(df, [
        'namefirst','firstname','first','fname','givenname',
        're:^name.*first$', 're:^first\\b'
    ])
    last_col = _find_col(df, [
        'namelast','lastname','last','lname','surname','familyname',
        're:^name.*last$', 're:^last\\b'
    ])
    ms_col = _find_col(df, [
        'mslevel','ms','mslvl','msyear','msclass','mscohort',
        're:^ms\\s*level$', 're:^ms\\b'
    ])
    if ms_col is None:
        ms_col = _guess_ms_col(df)

    if not (first_col and last_col and ms_col):
        raise ValueError(
            "Could not find required columns for First/Last/MS.\n"
            f"Headers seen: {list(df.columns)}\n"
            f"Detected -> first_col: {first_col}, last_col: {last_col}, ms_col: {ms_col}\n"
            "Tip: ensure your sheet has columns for first name, last name, and an MS level (1–5) column."
        )

    # ---- Identify all attendance date columns (headers containing M/D/YYYY) ----
    date_cols = [c for c in df.columns if _extract_date_str(c)]
    if not date_cols:
        raise ValueError("No attendance date columns found (headers with M/D/YYYY).")

    # ---- Find the cadet row ----
    df["_full"] = (
        _get_series(df, first_col).astype(str).str.strip() + " " +
        _get_series(df, last_col).astype(str).str.strip()
        ).str.lower()


    match = df[df["_full"] == target_full]
    if match.empty:
        # Try a soft fallback: compare only last name if first not found (optional)
        # fallback = df[df[last_col].astype(str).str.strip().str.lower() == target_last.lower()]
        # if not fallback.empty: match = fallback
        raise ValueError(f"Cadet '{target_full}' not found. Check spelling/casing.")

    row = match.iloc[0]
    return row[date_cols]


def daily_report(
    sheet_id: str,
    tab_name: str,
    target_date: str,
    ms_levels=("1","2","3","4","5"),
    include_name_lists: bool = False,
    key_file: str = KEY_FILE,
):
    """
    Build a per-MS summary for one day.
    Returns:
      {
        "table": [{"MS Level": "3", "Present": X, "FTR": Y, "Excused": Z, "Total": T}, ...],
        "overall": {"MS Level": "Overall", "Present": ..., "FTR": ..., "Excused": ..., "Total": ...},
        # "names_by_ms": { "3": {"Present":[...], "FTR":[...], "Excused":[...] }, ... }   # only if include_name_lists=True
      }
    """
    ms_levels = [str(x).strip() for x in ms_levels]

    rows = []
    names_by_ms = {}

    total_present = total_ftr = total_excused = 0

    for ms in ms_levels:
        buckets = get_attendance_by_date(
            sheet_id=sheet_id,
            tab_name=tab_name,
            target_date=target_date,
            ms_level=ms,
            key_file=key_file,
        )
        p = len(buckets.get("Present", []))
        f = len(buckets.get("FTR", []))
        e = len(buckets.get("Excused", []))

        rows.append({
            "MS Level": ms,
            "Present": p,
            "FTR": f,
            "Excused": e,
            "Total": p + f + e,
        })

        total_present += p
        total_ftr += f
        total_excused += e

        if include_name_lists:
            names_by_ms[ms] = {
                "Present": buckets.get("Present", []),
                "FTR": buckets.get("FTR", []),
                "Excused": buckets.get("Excused", []),
            }

    overall = {
        "MS Level": "Overall",
        "Present": total_present,
        "FTR": total_ftr,
        "Excused": total_excused,
        "Total": total_present + total_ftr + total_excused,
    }

    result = {"table": rows, "overall": overall}
    if include_name_lists:
        result["names_by_ms"] = names_by_ms
    return result


def print_daily_report(report: dict):
    """Pretty print the daily_report result to the console."""
    rows = report["table"]
    overall = report["overall"]

    # header
    print(f"{'MS Level':<8} {'Present':>7} {'FTR':>7} {'Excused':>8} {'Total':>7}")
    print("-" * 42)
    # rows
    for r in rows:
        print(f"{r['MS Level']:<8} {r['Present']:>7} {r['FTR']:>7} {r['Excused']:>8} {r['Total']:>7}")
    print("-" * 42)
    print(f"{overall['MS Level']:<8} {overall['Present']:>7} {overall['FTR']:>7} {overall['Excused']:>8} {overall['Total']:>7}")
    

# ---------- Example CLI usage ----------
# Run from terminal (after editing KEY_FILE above):
#   python google_sheets_attendance.py by-date SHEET_ID "Roster" 2025-08-11 4
#   python google_sheets_attendance.py cadet SHEET_ID "Roster" "Joshua" "Elzie"
#
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:\n"
              "  by-date SHEET_ID TAB_NAME TARGET_DATE MS_LEVEL\n"
              "  cadet   SHEET_ID TAB_NAME FIRST LAST\n"
              "  daily   SHEET_ID TAB_NAME TARGET_DATE [MS_LEVELS_COMMA_SEPARATED]\n"
              "Examples:\n"
              "  python google_sheets_attendance.py by-date 1AbC... \"Attendance Roster\" 2025-08-11 4\n"
              "  python google_sheets_attendance.py cadet  1AbC... \"Attendance Roster\" Joshua Elzie\n"
              "  python google_sheets_attendance.py daily  1AbC... \"Attendance Roster\" 2025-08-11 3,4,5")
        sys.exit(1)

    mode = sys.argv[1].lower()

    if mode == "by-date":
        if len(sys.argv) != 6:
            print("Usage: by-date SHEET_ID TAB_NAME TARGET_DATE MS_LEVEL")
            sys.exit(1)
        _, _, sheet_id, tab_name, target_date, ms_level = sys.argv
        res = get_attendance_by_date(sheet_id, tab_name, target_date, ms_level)
        print(pd.Series({k: len(v) for k, v in res.items()}))
        print(res)

    elif mode == "cadet":
        if len(sys.argv) != 6:
            print("Usage: cadet SHEET_ID TAB_NAME FIRST LAST")
            sys.exit(1)
        _, _, sheet_id, tab_name, first, last = sys.argv
        s = get_cadet_record(sheet_id, tab_name, first_name=first, last_name=last)
        out = pd.DataFrame({"DateCol": s.index, "Status": s.values})
        print(out.to_string(index=False))

    elif mode == "daily":
        if len(sys.argv) < 5:
            print("Usage: daily SHEET_ID TAB_NAME TARGET_DATE [MS_LEVELS_COMMA_SEPARATED]\n"
                  "Example: python google_sheets_attendance.py daily 1AbC... \"Attendance Roster\" 2025-08-11 3,4,5")
            sys.exit(1)
        _, _, sheet_id, tab_name, target_date, *rest = sys.argv
        ms_levels = ("1", "2", "3", "4", "5")
        if rest:
            ms_levels = tuple(str(rest[0]).split(","))
        rep = daily_report(sheet_id, tab_name, target_date, ms_levels=ms_levels)
        print_daily_report(rep)

    else:
        print("Unknown mode. Use 'by-date', 'cadet', or 'daily'.")
