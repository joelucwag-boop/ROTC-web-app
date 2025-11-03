#!/usr/bin/env python3
"""High level helpers for interacting with the attendance workbook.

This module is intentionally verbose – each function performs a single
responsibility so that troubleshooting a bad dataset only requires
touching one small unit at a time.  The functions here are used both by
the caching layer and directly by the writer blueprint.
"""

from __future__ import annotations

import io
import json
import logging
import os
import re
import sys
from dataclasses import dataclass
from datetime import date as _date
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import gspread
import pandas as pd
import requests
from gspread.cell import Cell
from gspread.utils import rowcol_to_a1
from google.oauth2.service_account import Credentials

log = logging.getLogger(__name__)

STATUS_KEYS = ("Present", "FTR", "Excused")

DAY_ALIASES = {
    "monday": {"monday", "mon", "mo"},
    "tuesday": {"tuesday", "tue", "tues", "tu"},
    "wednesday": {"wednesday", "wed", "we"},
    "thursday": {"thursday", "thu", "thur", "thurs", "th"},
    "friday": {"friday", "fri", "fr"},
    "saturday": {"saturday", "sat", "sa"},
    "sunday": {"sunday", "sun", "su"},
}


# ---------------------------------------------------------------------------
# Google client utilities
# ---------------------------------------------------------------------------


ENV_KEY = "GOOGLE_SERVICE_ACCOUNT_JSON"


def _client_from_env() -> gspread.Client:
    log.debug("Initialising Google Sheets client using %s", ENV_KEY)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    creds_json = os.getenv(ENV_KEY)
    if not creds_json:
        log.error("Environment variable %s is not configured.", ENV_KEY)
        raise RuntimeError(
            f"Environment variable {ENV_KEY} not found. "
            "Set it to the full JSON payload of your service account key."
        )

    try:
        info = json.loads(creds_json)
    except json.JSONDecodeError as exc:
        log.exception("Failed to parse service account JSON from %s", ENV_KEY)
        raise RuntimeError("Invalid service account JSON payload.") from exc

    try:
        creds = Credentials.from_service_account_info(info, scopes=scopes)
    except Exception:
        log.exception("Failed to build Google credentials from service account info.")
        raise

    try:
        client = gspread.authorize(creds)
    except Exception:
        log.exception("Failed to authorise Google Sheets client.")
        raise

    log.debug("Google Sheets client initialised successfully.")
    return client


@dataclass
class SheetConfig:
    sheet_id: str
    tab_name: str


def _open_ws(cfg: SheetConfig) -> gspread.Worksheet:
    log.debug("Opening worksheet: sheet_id=%s tab_name=%s", cfg.sheet_id, cfg.tab_name)
    try:
        gc = _client_from_env()
        sh = gc.open_by_key(cfg.sheet_id)
        ws = sh.worksheet(cfg.tab_name)
    except Exception:
        log.exception(
            "Failed to open worksheet", extra={"sheet_id": cfg.sheet_id, "tab_name": cfg.tab_name}
        )
        raise

    log.debug("Worksheet opened successfully: %s / %s", cfg.sheet_id, cfg.tab_name)
    return ws


# ---------------------------------------------------------------------------
# DataFrame helpers (ported from the original scripts)
# ---------------------------------------------------------------------------


def _detect_header_row(rows: Iterable[Iterable[str]], max_scan: int = 10) -> int:
    def norm(s):
        return re.sub(r"[^a-z0-9]+", "", (s or "").strip().lower())

    for i in range(min(max_scan, len(rows))):
        r = list(rows[i])
        if not r:
            continue
        norms = [norm(c) for c in r]
        has_nameish = any(k in norms for k in ["namefirst", "firstname", "first", "namelast", "lastname", "last"])
        has_msish = any(k in norms for k in ["mslevel", "ms", "mslvl", "msyear", "msclass", "mscohort"])
        nonempty = sum(1 for c in r if (c or "").strip() != "")
        if (has_nameish or has_msish) and nonempty >= 3:
            return i
    return 0


def _sheet_to_df(ws: gspread.Worksheet, return_meta: bool = False):
    log.debug("Fetching all values for worksheet %s", ws.title)
    rows = ws.get_all_values()
    log.debug("Worksheet %s returned %d rows", ws.title, len(rows))
    if not rows:
        log.error("Worksheet %s appears to be empty.", ws.title)
        raise ValueError("Worksheet appears to be empty.")

    hdr_idx = _detect_header_row(rows, max_scan=10)
    header = rows[hdr_idx]
    data = rows[hdr_idx + 1 :]

    last_nonempty = max((i for i, h in enumerate(header) if (h or "").strip() != ""), default=-1)
    if last_nonempty >= 0:
        header = header[: last_nonempty + 1]
        data = [r[: last_nonempty + 1] for r in data]

    df = pd.DataFrame(data, columns=header)
    log.debug(
        "DataFrame for worksheet %s created with shape %s (header row %d)",
        ws.title,
        df.shape,
        hdr_idx,
    )
    if return_meta:
        return df, hdr_idx, last_nonempty + 1
    return df


def _normalize_name(first: str, last: str) -> str:
    return f"{first.strip()} {last.strip()}".strip()


def _extract_date_str(text: str) -> Optional[str]:
    if not text:
        return None
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", text)
    return m.group(1) if m else None


def _event_from_header(header: str) -> str:
    if not header:
        return ""
    m = re.search(r"\d{1,2}/\d{1,2}/\d{4}\s*([+\-–—:]\s*(.+))?$", header.strip())
    if m and m.group(2):
        return m.group(2).strip()
    return ""


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
    return re.sub(r"[^a-z0-9]+", "", (s or "").strip().lower())


def _find_col(df: pd.DataFrame, wanted_keys: List[str]) -> Optional[str]:
    headers = list(df.columns)
    norm_map = {col: _norm(col) for col in headers}
    wanted_norm = {w for w in wanted_keys if not w.startswith("re:")}
    wanted_regex = [re.compile(w[3:], re.I) for w in wanted_keys if w.startswith("re:")]

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
        sample = vals[vals != ""].head(30)
        if sample.empty:
            continue
        ok = sample.apply(lambda v: v in {"1", "2", "3", "4", "5"} or re.fullmatch(r"ms\s*\d", v) is not None).mean()
        if ok >= 0.7:
            return col
    return None


def _get_series(df: pd.DataFrame, colname: str) -> pd.Series:
    obj = df[colname]
    if isinstance(obj, pd.DataFrame):
        return obj.iloc[:, 0]
    return obj


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.strip().lower()).strip("-")
    return slug or "cadet"


def _iso_from_mdyyyy(mdyyyy: str) -> str:
    m, d, y = (int(x) for x in mdyyyy.split("/"))
    return f"{y:04d}-{m:02d}-{d:02d}"


def _mdyyyy_from_iso(iso_date: str) -> str:
    y, m, d = (int(x) for x in iso_date.split("-"))
    return f"{m}/{d}/{y}"


def _program_column(df: pd.DataFrame, hint: str = "") -> Optional[str]:
    preferred = []
    if hint:
        preferred.append(_norm(hint))
    preferred.extend(["school", "campus", "program", "university", "college", "institution"])

    norm_map = {col: _norm(col) for col in df.columns}
    for want in preferred:
        for col, norm_val in norm_map.items():
            if norm_val == want:
                return col
    return None


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def _bool_from_response(text: str) -> Optional[bool]:
    value = (text or "").strip().lower()
    if not value:
        return None
    positives = ["yes", "available", "open", "present", "can", "y"]
    negatives = ["no", "not", "unavailable", "cannot", "can't", "n"]
    if any(p in value for p in positives):
        return True
    if any(n in value for n in negatives):
        return False
    return None


def _tokenise(text: str) -> List[str]:
    cleaned = re.sub(r"[^a-z0-9:]+", " ", (text or "").lower())
    parts = [p for p in cleaned.split() if p]
    extra = []
    for part in parts:
        if "-" in part:
            extra.extend(part.split("-"))
        if ":" in part:
            extra.extend(part.split(":"))
    return list({*parts, *extra})


# ---------------------------------------------------------------------------
# Attendance cache builder
# ---------------------------------------------------------------------------


def build_attendance_cache(sheet_id: str, tab_name: str, program_hint: str = "") -> Dict[str, Any]:
    log.info("Building attendance cache", extra={"sheet_id": sheet_id, "tab_name": tab_name})
    try:
        result = _build_attendance_cache_core(sheet_id, tab_name, program_hint)
    except Exception:
        log.exception(
            "Failed to build attendance cache", extra={"sheet_id": sheet_id, "tab_name": tab_name}
        )
        raise

    log.info(
        "Attendance cache built: %d cadets, %d events",
        len(result.get("cadets", [])),
        len(result.get("events", [])),
    )
    return result


def _build_attendance_cache_core(sheet_id: str, tab_name: str, program_hint: str) -> Dict[str, Any]:
    ws = _open_ws(SheetConfig(sheet_id, tab_name))
    df, header_idx, last_col = _sheet_to_df(ws, return_meta=True)

    first_col = _find_col(
        df,
        [
            "namefirst",
            "firstname",
            "first",
            "fname",
            "givenname",
            "re:^name.*first$",
            "re:^first\\b",
        ],
    )
    last_name_col = _find_col(
        df,
        [
            "namelast",
            "lastname",
            "last",
            "lname",
            "surname",
            "familyname",
            "re:^name.*last$",
            "re:^last\\b",
        ],
    )
    ms_col = _find_col(
        df,
        ["mslevel", "ms", "mslvl", "msyear", "msclass", "mscohort", "re:^ms\\s*level$", "re:^ms\\b"],
    ) or _guess_ms_col(df)

    if not (first_col and last_name_col and ms_col):
        raise ValueError("Could not detect first/last/MS columns in attendance sheet.")

    program_col = _program_column(df, program_hint)

    date_columns: List[Dict[str, Any]] = []
    for idx, col in enumerate(df.columns):
        md = _extract_date_str(col)
        if not md:
            continue
        try:
            iso = _iso_from_mdyyyy(md)
        except Exception:
            continue
        date_columns.append(
            {
                "header": col,
                "iso": iso,
                "event": _event_from_header(col),
                "column_index": idx + 1,
            }
        )

    date_columns.sort(key=lambda c: c["iso"])

    ms_series = (
        _get_series(df, ms_col)
        .astype(str)
        .str.lower()
        .str.replace(r"^ms\s*", "", regex=True)
        .str.strip()
    )
    program_series = (
        _get_series(df, program_col).astype(str).str.strip() if program_col else pd.Series(["" for _ in range(len(df))])
    )

    first_series = _get_series(df, first_col).astype(str)
    last_series = _get_series(df, last_name_col).astype(str)

    per_event: Dict[str, Any] = {}
    cadets: List[Dict[str, Any]] = []
    cadet_index: Dict[str, Dict[str, Any]] = {}
    by_name: Dict[str, Dict[str, Any]] = {}
    ms_levels_set = set()

    for idx, row in df.iterrows():
        first = first_series.iloc[idx].strip()
        last = last_series.iloc[idx].strip()
        name = _normalize_name(first, last)
        if not name:
            continue

        ms_value = ms_series.iloc[idx] or ""
        if ms_value:
            ms_levels_set.add(ms_value)
        program_value = program_series.iloc[idx].strip()

        slug_base = _slugify(name)
        slug = f"{slug_base}-{header_idx + 2 + idx}"

        attendance_entries: List[Dict[str, Any]] = []
        status_counts = {status: 0 for status in STATUS_KEYS}

        for col_info in date_columns:
            header = col_info["header"]
            raw_value = str(row.get(header, "") or "").strip()
            normalized = _classify_status(raw_value)
            entry = {
                "date": col_info["iso"],
                "label": header,
                "event": col_info["event"],
                "status": normalized or raw_value,
                "normalized_status": normalized or "",
            }
            attendance_entries.append(entry)

            if normalized:
                status_counts[normalized] += 1
                event_bucket = per_event.setdefault(
                    col_info["iso"],
                    {
                        "iso": col_info["iso"],
                        "header": header,
                        "event": col_info["event"],
                        "counts": {status: 0 for status in STATUS_KEYS},
                        "per_ms": {},
                        "names": {status: [] for status in STATUS_KEYS},
                    },
                )
                event_bucket["counts"][normalized] += 1
                ms_counts = event_bucket["per_ms"].setdefault(ms_value, {status: 0 for status in STATUS_KEYS})
                ms_counts[normalized] += 1
                event_bucket["names"][normalized].append(
                    {
                        "name": name,
                        "slug": slug,
                        "ms": ms_value,
                        "school": program_value,
                    }
                )

        cadet_payload = {
            "id": slug,
            "name": name,
            "first": first,
            "last": last,
            "ms": ms_value,
            "school": program_value,
            "normalized_name": _norm(name),
            "sheet_row": header_idx + 2 + idx,
            "attendance": attendance_entries,
            "status_counts": status_counts,
        }

        cadets.append(cadet_payload)
        cadet_index[slug] = cadet_payload
        by_name[_norm(name)] = cadet_payload

    events = []
    for iso in sorted(per_event.keys()):
        bucket = per_event[iso]
        bucket["per_ms"] = {
            ms: {status: counts.get(status, 0) for status in STATUS_KEYS}
            for ms, counts in bucket["per_ms"].items()
        }
        events.append(
            {
                "iso": iso,
                "header": bucket["header"],
                "event": bucket["event"],
                "counts": {status: bucket["counts"].get(status, 0) for status in STATUS_KEYS},
                "per_ms": bucket["per_ms"],
                "names": bucket["names"],
            }
        )

    latest_event = events[-1] if events else None

    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "header_row": header_idx + 1,
        "last_column_index": last_col,
        "ms_levels": sorted(ms_levels_set),
        "date_columns": date_columns,
        "events": events,
        "latest_event": latest_event,
        "cadets": cadets,
        "cadet_index": cadet_index,
        "by_name": by_name,
        "per_event": per_event,
    }


# ---------------------------------------------------------------------------
# Availability cache builder
# ---------------------------------------------------------------------------


def build_availability_cache(csv_url: str, name_column_override: str = "") -> Dict[str, Any]:
    log.info("Building availability cache", extra={"csv_url": csv_url})
    try:
        result = _build_availability_cache_core(csv_url, name_column_override)
    except Exception:
        log.exception("Failed to build availability cache", extra={"csv_url": csv_url})
        raise

    log.info(
        "Availability cache built with %d entries", len(result.get("entries", []))
    )
    return result


def _build_availability_cache_core(csv_url: str, name_column_override: str) -> Dict[str, Any]:
    response = requests.get(csv_url, timeout=30)
    log.debug(
        "Availability CSV response: status=%s bytes=%d", response.status_code, len(response.content)
    )
    response.raise_for_status()

    df = pd.read_csv(io.StringIO(response.text), dtype=str, keep_default_na=False)
    log.debug("Availability CSV parsed with shape %s", df.shape)
    name_column = ""

    if name_column_override and name_column_override in df.columns:
        name_column = name_column_override
        log.debug("Using override name column: %s", name_column)
    else:
        candidates = [
            "full name",
            "name",
            "cadet name",
            "cadet",
            "preferred name",
            "re:^name$",
        ]
        norm_map = {col: _norm(col) for col in df.columns}
        for cand in candidates:
            norm_cand = _norm(cand)
            for col, nm in norm_map.items():
                if nm == norm_cand:
                    name_column = col
                    log.debug("Detected name column: %s", name_column)
                    break
            if name_column:
                break

    first_col = _find_col(df, ["firstname", "first", "fname", "re:^first\\b"])
    last_col = _find_col(df, ["lastname", "last", "lname", "re:^last\\b"])

    entries: List[Dict[str, Any]] = []
    index: Dict[str, Dict[str, Any]] = {}
    by_name: Dict[str, Dict[str, Any]] = {}

    for _, row in df.iterrows():
        if name_column:
            name = str(row.get(name_column, "")).strip()
        else:
            first = str(row.get(first_col, "")).strip()
            last = str(row.get(last_col, "")).strip()
            name = _normalize_name(first, last)

        name = _clean_text(name)
        if not name:
            continue

        slug = f"{_slugify(name)}-{len(entries)+1}"
        row_dict = {col: _clean_text(str(row[col])) for col in df.columns}

        day_map: Dict[str, List[Dict[str, Any]]] = {day: [] for day in DAY_ALIASES}

        for column, raw_value in row_dict.items():
            lower_header = column.lower()
            target_day = None
            for canonical, aliases in DAY_ALIASES.items():
                if any(alias in lower_header for alias in aliases):
                    target_day = canonical
                    break
            if not target_day:
                continue

            tokens = _tokenise(column + " " + raw_value)
            day_map[target_day].append(
                {
                    "column": column,
                    "value": raw_value,
                    "tokens": tokens,
                    "available": _bool_from_response(raw_value),
                }
            )

        entry = {
            "id": slug,
            "name": name,
            "normalized_name": _norm(name),
            "raw": row_dict,
            "days": day_map,
        }

        entries.append(entry)
        index[slug] = entry
        by_name[_norm(name)] = entry

    entries.sort(key=lambda item: item["name"].split(" ")[-1].lower())

    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "entries": entries,
        "index": index,
        "by_name": by_name,
    }


# ---------------------------------------------------------------------------
# UMR cache builder
# ---------------------------------------------------------------------------


def build_umr_cache(sheet_id: str, tab_name: str, mapping_json: str = "") -> Dict[str, Any]:
    log.info(
        "Building UMR cache", extra={"sheet_id": sheet_id, "tab_name": tab_name, "mapping_configured": bool(mapping_json)}
    )
    try:
        result = _build_umr_cache_core(sheet_id, tab_name, mapping_json)
    except Exception:
        log.exception(
            "Failed to build UMR cache", extra={"sheet_id": sheet_id, "tab_name": tab_name}
        )
        raise

    log.info("UMR cache built with %d entries", len(result.get("entries", [])))
    return result


def _build_umr_cache_core(sheet_id: str, tab_name: str, mapping_json: str) -> Dict[str, Any]:
    ws = _open_ws(SheetConfig(sheet_id, tab_name))
    entries: List[Dict[str, Any]] = []

    if mapping_json:
        try:
            mapping = json.loads(mapping_json)
        except json.JSONDecodeError as exc:
            log.exception("Invalid JSON provided for UMR mapping.")
            raise ValueError("UMR_MAPPING_JSON is not valid JSON") from exc

        if not isinstance(mapping, list):
            raise ValueError("UMR mapping must be a list of objects")

        for item in mapping:
            if not isinstance(item, dict):
                continue
            position_cell = item.get("position_cell")
            name_cell = item.get("name_cell")
            title_cell = item.get("title_cell")
            if not (position_cell and name_cell):
                continue
            position = ws.acell(position_cell).value
            name = ws.acell(name_cell).value
            title = ws.acell(title_cell).value if title_cell else ""
            entries.append(
                {
                    "position": _clean_text(position),
                    "name": _clean_text(name),
                    "title": _clean_text(title),
                    "cells": {
                        "position": position_cell,
                        "name": name_cell,
                        "title": title_cell or "",
                    },
                }
            )
    else:
        rows = ws.get_all_values()
        log.debug("UMR worksheet returned %d rows", len(rows))
        for i, row in enumerate(rows, start=1):
            if len(row) < 2:
                continue
            position = _clean_text(row[0])
            name = _clean_text(row[1])
            title = _clean_text(row[2]) if len(row) > 2 else ""
            if not (position or name or title):
                continue
            entries.append(
                {
                    "position": position,
                    "name": name,
                    "title": title,
                    "cells": {
                        "position": f"A{i}",
                        "name": f"B{i}",
                        "title": f"C{i}",
                    },
                }
            )

    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "entries": entries,
    }


# ---------------------------------------------------------------------------
# Writer helpers
# ---------------------------------------------------------------------------


def _hex_to_rgb(hex_color: str) -> Dict[str, float]:
    value = hex_color.lstrip("#")
    if len(value) != 6:
        raise ValueError("Color must be 6 hex characters")
    r = int(value[0:2], 16) / 255.0
    g = int(value[2:4], 16) / 255.0
    b = int(value[4:6], 16) / 255.0
    return {"red": r, "green": g, "blue": b}


def ensure_date_column(
    ws: gspread.Worksheet,
    header_row: int,
    date_columns: List[Dict[str, Any]],
    target_iso: str,
    event_label: str,
    last_column_index: int,
) -> Tuple[int, List[Dict[str, Any]]]:
    for col_info in date_columns:
        if col_info["iso"] == target_iso:
            header_value = col_info["header"]
            if event_label and event_label.lower() not in header_value.lower():
                new_header = f"{_mdyyyy_from_iso(target_iso)} + {event_label}".strip()
                ws.update_cell(header_row, col_info["column_index"], new_header)
                col_info["header"] = new_header
                col_info["event"] = event_label
                log.debug(
                    "Updated existing header for %s with event %s at column %d",
                    target_iso,
                    event_label,
                    col_info["column_index"],
                )
            return col_info["column_index"], date_columns

    new_col_index = last_column_index + 1
    header_text = _mdyyyy_from_iso(target_iso)
    if event_label:
        header_text += f" + {event_label}" if "+" not in event_label else f" {event_label}"
    ws.update_cell(header_row, new_col_index, header_text)
    log.debug(
        "Added new attendance column %d with header '%s'", new_col_index, header_text
    )
    date_columns.append(
        {
            "header": header_text,
            "iso": target_iso,
            "event": event_label,
            "column_index": new_col_index,
        }
    )
    date_columns.sort(key=lambda c: c["iso"])
    return new_col_index, date_columns


def write_attendance_entries(
    sheet_id: str,
    tab_name: str,
    header_row: int,
    date_columns: List[Dict[str, Any]],
    last_column_index: int,
    target_iso: str,
    event_label: str,
    updates: List[Dict[str, Any]],
    color_present: str,
    color_ftr: str,
    color_excused: str,
):
    if not updates:
        log.info(
            "No attendance updates received for %s (event=%s)", target_iso, event_label or ""
        )
        return {"updated": 0}

    log.info(
        "Writing %d attendance updates", len(updates), extra={"target_iso": target_iso, "event_label": event_label}
    )
    try:
        ws = _open_ws(SheetConfig(sheet_id, tab_name))
        column_index, updated_columns = ensure_date_column(
            ws, header_row, date_columns, target_iso, event_label, last_column_index
        )

        value_cells = []
        format_requests = []

        color_map = {
            "Present": _hex_to_rgb(color_present),
            "FTR": _hex_to_rgb(color_ftr),
            "Excused": _hex_to_rgb(color_excused),
        }

        for payload in updates:
            row_number = payload["sheet_row"]
            status = payload["status"]
            if not status:
                continue
            value = status
            note = payload.get("note", "").strip()
            if status == "Excused" and note:
                value = f"Excused - {note}" if not status.lower().startswith("excused") else f"{status} - {note}"

            a1 = rowcol_to_a1(row_number, column_index)
            value_cells.append(Cell(row_number, column_index, value))
            if status in color_map:
                format_requests.append(
                    {
                        "range": a1,
                        "format": {"userEnteredFormat": {"backgroundColor": color_map[status]}},
                    }
                )

        if value_cells:
            ws.update_cells(value_cells, value_input_option="USER_ENTERED")
            log.debug("Updated %d cells in column %d", len(value_cells), column_index)

        for request in format_requests:
            ws.format(request["range"], request["format"])

        log.info(
            "Successfully wrote %d attendance updates", len(value_cells), extra={"column_index": column_index}
        )
        return {"updated": len(value_cells), "column_index": column_index, "date_columns": updated_columns}
    except Exception:
        log.exception(
            "Failed to write attendance entries",
            extra={"sheet_id": sheet_id, "tab_name": tab_name, "target_iso": target_iso},
        )
        raise


# ---------------------------------------------------------------------------
# Legacy CLI compatibility wrappers (used by the CLI + cache helpers)
# ---------------------------------------------------------------------------


def get_attendance_by_date(sheet_id, tab_name, target_date, ms_level) -> Dict[str, List[str]]:
    log.debug(
        "Fetching attendance by date",
        extra={
            "sheet_id": sheet_id,
            "tab_name": tab_name,
            "target_date": target_date,
            "ms_level": ms_level,
        },
    )
    cfg = SheetConfig(sheet_id=sheet_id, tab_name=tab_name)
    ws = _open_ws(cfg)
    df = _sheet_to_df(ws)

    first_col = _find_col(
        df,
        [
            "namefirst",
            "firstname",
            "first",
            "fname",
            "givenname",
            "re:^name.*first$",
            "re:^first\\b",
        ],
    )
    last_col = _find_col(
        df,
        [
            "namelast",
            "lastname",
            "last",
            "lname",
            "surname",
            "familyname",
            "re:^name.*last$",
            "re:^last\\b",
        ],
    )
    ms_col = _find_col(
        df,
        ["mslevel", "ms", "mslvl", "msyear", "msclass", "mscohort", "re:^ms\\s*level$", "re:^ms\\b"],
    ) or _guess_ms_col(df)

    if not (first_col and last_col and ms_col):
        raise ValueError("Missing columns for First/Last/MS.")

    date_col = _find_date_column(df, target_date)
    if not date_col:
        raise ValueError(f"No column for date {target_date}")

    df["_MS"] = (
        _get_series(df, ms_col).astype(str).str.lower().str.replace(r"^ms\s*", "", regex=True).str.strip()
    )
    wanted_ms = str(ms_level).lower().replace("ms", "").strip()
    df_ms = df[df["_MS"] == wanted_ms].copy()

    out = {"Present": [], "FTR": [], "Excused": []}
    for _, row in df_ms.iterrows():
        status = _classify_status(row.get(date_col, ""))
        if status:
            name = _normalize_name(str(row.get(first_col, "")), str(row.get(last_col, "")))
            out[status].append(name)
    log.debug(
        "Attendance by date fetched",
        extra={
            "target_date": target_date,
            "ms_level": ms_level,
            "present": len(out.get("Present", [])),
            "ftr": len(out.get("FTR", [])),
            "excused": len(out.get("Excused", [])),
        },
    )
    return out


def get_cadet_record(sheet_id, tab_name, first_name=None, last_name=None, full_name=None) -> pd.Series:
    log.debug(
        "Fetching cadet record",
        extra={
            "sheet_id": sheet_id,
            "tab_name": tab_name,
            "first_name": first_name,
            "last_name": last_name,
            "full_name": full_name,
        },
    )
    if full_name and full_name.strip():
        if " " in full_name.strip():
            target_first, target_last = [p.strip() for p in full_name.split(" ", 1)]
        else:
            target_first, target_last = full_name.strip(), ""
    else:
        target_first = (first_name or "").strip()
        target_last = (last_name or "").strip()
    target_full = _normalize_name(target_first, target_last).lower()

    cfg = SheetConfig(sheet_id=sheet_id, tab_name=tab_name)
    ws = _open_ws(cfg)
    df = _sheet_to_df(ws)

    first_col = _find_col(df, ["namefirst", "firstname", "first", "fname", "givenname", "re:^name.*first$", "re:^first\\b"])
    last_col = _find_col(df, ["namelast", "lastname", "last", "lname", "surname", "familyname", "re:^name.*last$", "re:^last\\b"])
    ms_col = _find_col(df, ["mslevel", "ms", "mslvl", "msyear", "msclass", "mscohort", "re:^ms\\s*level$", "re:^ms\\b"]) or _guess_ms_col(df)

    if not (first_col and last_col and ms_col):
        raise ValueError("Missing columns for First/Last/MS.")

    date_cols = [c for c in df.columns if _extract_date_str(c)]
    if not date_cols:
        raise ValueError("No attendance date columns found.")

    df["_full"] = (
        _get_series(df, first_col).astype(str).str.strip() + " " + _get_series(df, last_col).astype(str).str.strip()
    ).str.lower()

    match = df[df["_full"] == target_full]
    if match.empty:
        raise ValueError(f"Cadet '{target_full}' not found.")
    result = match.iloc[0][date_cols]
    log.debug(
        "Cadet record retrieved",
        extra={"cadet": target_full, "columns": len(date_cols)},
    )
    return result


def daily_report(sheet_id, tab_name, target_date, ms_levels=("1", "2", "3", "4", "5"), include_name_lists=False):
    log.debug(
        "Generating daily report",
        extra={
            "sheet_id": sheet_id,
            "tab_name": tab_name,
            "target_date": target_date,
            "ms_levels": ms_levels,
            "include_names": include_name_lists,
        },
    )
    ms_levels = [str(x).strip() for x in ms_levels]
    rows, names_by_ms = [], {}
    total_present = total_ftr = total_excused = 0

    for ms in ms_levels:
        buckets = get_attendance_by_date(sheet_id, tab_name, target_date, ms)
        p, f, e = len(buckets["Present"]), len(buckets["FTR"]), len(buckets["Excused"])
        rows.append({"MS Level": ms, "Present": p, "FTR": f, "Excused": e, "Total": p + f + e})
        total_present += p
        total_ftr += f
        total_excused += e
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
    log.debug(
        "Daily report generated",
        extra={
            "target_date": target_date,
            "overall_present": overall["Present"],
            "overall_ftr": overall["FTR"],
            "overall_excused": overall["Excused"],
        },
    )
    return result


def print_daily_report(report: dict):
    rows = report["table"]
    overall = report["overall"]
    print(f"{'MS Level':<8} {'Present':>7} {'FTR':>7} {'Excused':>8} {'Total':>7}")
    print("-" * 42)
    for r in rows:
        print(f"{r['MS Level']:<8} {r['Present']:>7} {r['FTR']:>7} {r['Excused']:>8} {r['Total']:>7}")
    print("-" * 42)
    print(
        f"{overall['MS Level']:<8} {overall['Present']:>7} {overall['FTR']:>7} "
        f"{overall['Excused']:>8} {overall['Total']:>7}"
    )


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(
            "Usage:\n  by-date SHEET_ID TAB_NAME TARGET_DATE MS_LEVEL\n"
            "  cadet SHEET_ID TAB_NAME FIRST LAST\n"
            "  daily SHEET_ID TAB_NAME TARGET_DATE [MS_LEVELS_COMMA_SEPARATED]"
        )
        sys.exit(1)

    mode = sys.argv[1].lower()

    if mode == "by-date":
        _, _, sheet_id, tab_name, target_date, ms_level = sys.argv
        res = get_attendance_by_date(sheet_id, tab_name, target_date, ms_level)
        print(pd.Series({k: len(v) for k, v in res.items()}))
        print(res)
    elif mode == "cadet":
        _, _, sheet_id, tab_name, first, last = sys.argv
        s = get_cadet_record(sheet_id, tab_name, first_name=first, last_name=last)
        out = pd.DataFrame({"DateCol": s.index, "Status": s.values})
        print(out.to_string(index=False))
    elif mode == "daily":
        _, _, sheet_id, tab_name, target_date, *rest = sys.argv
        ms_levels = tuple(str(rest[0]).split(",")) if rest else ("1", "2", "3", "4", "5")
        rep = daily_report(sheet_id, tab_name, target_date, ms_levels=ms_levels)
        print_daily_report(rep)
    else:
        print("Unknown mode. Use 'by-date', 'cadet', or 'daily'.")

