#!/usr/bin/env python3
"""
Google Sheets Attendance Helper (Render-safe)

This version reads the Google service account credentials from the environment
variable GOOGLE_SERVICE_ACCOUNT_JSON instead of a local JSON file path.
All other behavior is identical to the original.
"""

from __future__ import annotations
import os
import re
import sys
import json
from dataclasses import dataclass
from datetime import datetime, date as _date
from typing import Dict, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials
import pandas as pd

# === CONFIG ===
# Load credentials JSON from environment variable
ENV_KEY = "GOOGLE_SERVICE_ACCOUNT_JSON"

def _client_from_env() -> gspread.Client:
    """
    Build an authenticated gspread client using the JSON key stored in
    the environment variable GOOGLE_SERVICE_ACCOUNT_JSON.
    Falls back to local KEY_FILE if env var missing.
    """
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]

    creds_json = os.getenv(ENV_KEY)
    if creds_json:
        info = json.loads(creds_json)
        creds = Credentials.from_service_account_info(info, scopes=scopes)
    else:
        raise RuntimeError(
            f"Environment variable {ENV_KEY} not found. "
            "Set it in Render with the full JSON contents of your service key."
        )

    return gspread.authorize(creds)


@dataclass
class SheetConfig:
    sheet_id: str
    tab_name: str


def _open_ws(cfg: SheetConfig) -> gspread.Worksheet:
    gc = _client_from_env()
    sh = gc.open_by_key(cfg.sheet_id)
    return sh.worksheet(cfg.tab_name)


def _sheet_to_df(ws: gspread.Worksheet) -> pd.DataFrame:
    rows = ws.get_all_values()
    if not rows:
        raise ValueError("Worksheet appears to be empty.")

    hdr_idx = _detect_header_row(rows, max_scan=10)
    header = rows[hdr_idx]
    data = rows[hdr_idx + 1:]

    last_nonempty = max((i for i, h in enumerate(header) if (h or "").strip() != ""), default=-1)
    if last_nonempty >= 0:
        header = header[:last_nonempty + 1]
        data = [r[:last_nonempty + 1] for r in data]

    return pd.DataFrame(data, columns=header)


def _normalize_name(first: str, last: str) -> str:
    return f"{first.strip()} {last.strip()}".strip()


def _extract_date_str(text: str) -> Optional[str]:
    if not text:
        return None
    m = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', text)
    return m.group(1) if m else None


def _target_date_formats(target: str) -> List[str]:
    candidates = set()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%-m/%-d/%Y", "%m/%-d/%Y", "%-m/%d/%Y"):
        try:
            dt = datetime.strptime(target, fmt)
            candidates.add(f"{dt.month}/{dt.day}/{dt.year}")
        except Exception:
            pass
    if re.fullmatch(r"\d{1,2}/\d{1,2}/\d{4}", target.strip()):
        candidates.add(target.strip())
    return list(candidates)


def _find_date_column(df: pd.DataFrame, target_date: str) -> Optional[str]:
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


def _norm(s: str) -> str:
    return re.sub(r'[^a-z0-9]+', '', (s or '').strip().lower())


def _detect_header_row(rows, max_scan=10) -> int:
    def norm(s):
        return re.sub(r'[^a-z0-9]+', '', (s or '').strip().lower())
    for i in range(min(max_scan, len(rows))):
        r = rows[i]
        if not r:
            continue
        norms = [norm(c) for c in r]
        has_nameish = any(k in norms for k in ["namefirst","firstname","first","namelast","lastname","last"])
        has_msish   = any(k in norms for k in ["mslevel","ms","mslvl","msyear","msclass","mscohort"])
        nonempty = sum(1 for c in r if (c or "").strip() != "")
        if (has_nameish or has_msish) and nonempty >= 3:
            return i
    return 0


def _find_col(df: pd.DataFrame, wanted_keys: list[str]) -> Optional[str]:
    headers = list(df.columns)
    norm_map = {col: _norm(col) for col in headers}
    wanted_norm = {w for w in wanted_keys if not w.startswith('re:')}
    wanted_regex = [re.compile(w[3:], re.I) for w in wanted_keys if w.startswith('re:')]
    for col, nm in norm_map.items():
        if nm in wanted_norm:
            return col
    for col in headers:
        for rx in wanted_regex:
            if rx.search(col):
                return col
    return None


def _guess_ms_col(df: pd.DataFrame) -> Optional[str]:
    headers = list(df.columns)
    for i, col in enumerate(headers):
        s = df.iloc[:, i]
        vals = s.astype(str).str.strip().str.lower()
        sample = vals[vals != ''].head(30)
        if sample.empty:
            continue
        ok = sample.apply(lambda v: v in {'1','2','3','4','5'} or re.fullmatch(r'ms\s*\d', v) is not None).mean()
        if ok >= 0.7:
            return col
    return None


def _get_series(df: pd.DataFrame, colname: str) -> pd.Series:
    obj = df[colname]
    if isinstance(obj, pd.DataFrame):
        return obj.iloc[:, 0]
    return obj


# ---------- Core functions ----------

def get_attendance_by_date(sheet_id, tab_name, target_date, ms_level) -> Dict[str, List[str]]:
    cfg = SheetConfig(sheet_id=sheet_id, tab_name=tab_name)
    ws = _open_ws(cfg)
    df = _sheet_to_df(ws)

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
    ]) or _guess_ms_col(df)

    if not (first_col and last_col and ms_col):
        raise ValueError("Missing columns for First/Last/MS.")

    date_col = _find_date_column(df, target_date)
    if not date_col:
        raise ValueError(f"No column for date {target_date}")

    df["_MS"] = (
        _get_series(df, ms_col)
        .astype(str)
        .str.lower()
        .str.replace(r"^ms\s*", "", regex=True)
        .str.strip()
    )
    wanted_ms = str(ms_level).lower().replace("ms", "").strip()
    df_ms = df[df["_MS"] == wanted_ms].copy()

    out = {"Present": [], "FTR": [], "Excused": []}
    for _, row in df_ms.iterrows():
        status = _classify_status(row.get(date_col, ""))
        if status:
            name = _normalize_name(str(row.get(first_col, "")), str(row.get(last_col, "")))
            out[status].append(name)
    return out


def get_cadet_record(sheet_id, tab_name, first_name=None, last_name=None, full_name=None) -> pd.Series:
    if full_name and full_name.strip():
        if " " in full_name.strip():
            target_first, target_last = [p.strip() for p in full_name.split(" ", 1)]
        else:
            target_first, target_last = full_name.strip(), ""
    else:
        target_first = (first_name or "").strip()
        target_last  = (last_name  or "").strip()
    target_full = _normalize_name(target_first, target_last).lower()

    cfg = SheetConfig(sheet_id=sheet_id, tab_name=tab_name)
    ws = _open_ws(cfg)
    df = _sheet_to_df(ws)

    first_col = _find_col(df, ['namefirst','firstname','first','fname','givenname','re:^name.*first$', 're:^first\\b'])
    last_col = _find_col(df,  ['namelast','lastname','last','lname','surname','familyname','re:^name.*last$', 're:^last\\b'])
    ms_col = _find_col(df,   ['mslevel','ms','mslvl','msyear','msclass','mscohort','re:^ms\\s*level$', 're:^ms\\b']) or _guess_ms_col(df)

    if not (first_col and last_col and ms_col):
        raise ValueError("Missing columns for First/Last/MS.")

    date_cols = [c for c in df.columns if _extract_date_str(c)]
    if not date_cols:
        raise ValueError("No attendance date columns found.")

    df["_full"] = (
        _get_series(df, first_col).astype(str).str.strip() + " " +
        _get_series(df, last_col).astype(str).str.strip()
    ).str.lower()

    match = df[df["_full"] == target_full]
    if match.empty:
        raise ValueError(f"Cadet '{target_full}' not found.")
    return match.iloc[0][date_cols]


def daily_report(sheet_id, tab_name, target_date, ms_levels=("1","2","3","4","5"), include_name_lists=False):
    ms_levels = [str(x).strip() for x in ms_levels]
    rows, names_by_ms = [], {}
    total_present = total_ftr = total_excused = 0

    for ms in ms_levels:
        buckets = get_attendance_by_date(sheet_id, tab_name, target_date, ms)
        p, f, e = len(buckets["Present"]), len(buckets["FTR"]), len(buckets["Excused"])
        rows.append({"MS Level": ms, "Present": p, "FTR": f, "Excused": e, "Total": p+f+e})
        total_present += p; total_ftr += f; total_excused += e
        if include_name_lists:
            names_by_ms[ms] = buckets

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
    rows = report["table"]; overall = report["overall"]
    print(f"{'MS Level':<8} {'Present':>7} {'FTR':>7} {'Excused':>8} {'Total':>7}")
    print("-"*42)
    for r in rows:
        print(f"{r['MS Level']:<8} {r['Present']:>7} {r['FTR']:>7} {r['Excused']:>8} {r['Total']:>7}")
    print("-"*42)
    print(f"{overall['MS Level']:<8} {overall['Present']:>7} {overall['FTR']:>7} "
          f"{overall['Excused']:>8} {overall['Total']:>7}")


# ---------- CLI ----------
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:\n  by-date SHEET_ID TAB_NAME TARGET_DATE MS_LEVEL\n"
              "  cadet SHEET_ID TAB_NAME FIRST LAST\n"
              "  daily SHEET_ID TAB_NAME TARGET_DATE [MS_LEVELS_COMMA_SEPARATED]")
        sys.exit(1)

    mode = sys.argv[1].lower()

    if mode == "by-date":
        _, _, sheet_id, tab_name, target_date, ms_level = sys.argv
        res = get_attendance_by_date(sheet_id, tab_name, target_date, ms_level)
        print(pd.Series({k: len(v) for k, v in res.items()})); print(res)
    elif mode == "cadet":
        _, _, sheet_id, tab_name, first, last = sys.argv
        s = get_cadet_record(sheet_id, tab_name, first_name=first, last_name=last)
        out = pd.DataFrame({"DateCol": s.index, "Status": s.values}); print(out.to_string(index=False))
    elif mode == "daily":
        _, _, sheet_id, tab_name, target_date, *rest = sys.argv
        ms_levels = tuple(str(rest[0]).split(",")) if rest else ("1","2","3","4","5")
        rep = daily_report(sheet_id, tab_name, target_date, ms_levels=ms_levels); print_daily_report(rep)
    else:
        print("Unknown mode. Use 'by-date', 'cadet', or 'daily'.")
