# -*- coding: utf-8 -*-
"""
Henter DOFbasens CSV for en valgt dato (DD-MM-YYYY), gemmer den,
detekterer første observation pr. (Artnavn × Loknr) pr. dag (DK-tid)
og leverer resultater pr. klient-profil via stdout/fil/webpush.

Denne version (L1-hybrid klar):
- CLI-output er kompakt: én linje pr. observation (uden noter).
- [alm | sub | su | bemaerk] vises i CLI og medsendes pr. item i batch-JSON.
- 'bemaerk' bestemmes ud fra regionale tærskler pr. art (data/*bemaerk*.csv).
- Webpush/digest udsendes parallelt; klient-POSTs bruger Session + konfigurerbar timeout.
- Per-klient filtrering bevares (species, adfærd, lokation, tid, bbox, antal, kategori).
- Ændringer fra alle grupper sendes samlet pr. polling-runde.
- NYT: payload til server inkluderer 'regions' + 'categories' (gør server-side grovfilter mulig),
       samt 'urgency' (high ved su/bemaerk, ellers normal).
- NYT: atomic writes for state- og batchfiler.
- FIX: only_with_coords-masken brugte forkert operator—rettet til OR/AND.
"""
import sys
import re
import mimetypes
import unicodedata
import warnings

import pandas as pd
import yaml
import argparse
import datetime as dt
from zoneinfo import ZoneInfo
DK_TZ = ZoneInfo("Europe/Copenhagen")

import json
from pathlib import Path
import os           # NYT
import tempfile     # NYT
import time         # NYT
from typing import Dict, List, Tuple, Optional


import requests
import yaml
from concurrent.futures import ThreadPoolExecutor, as_completed

# ───────────────────────────────── Konstanter og stier ─────────────────────────────────
warnings.simplefilter(action="ignore", category=FutureWarning)
DK_TZ = ZoneInfo("Europe/Copenhagen")
DOF_URL = (
    "https://dofbasen.dk/excel/search_result1.php"
    "?design=excel&soeg=soeg&periode=dato&dato={dato}"
    "&obstype=observationer&species=alle&sortering=dato"
)

APP_DIR = Path.cwd()
STATE_DIR = APP_DIR / "state"
DL_DIR = APP_DIR / "downloads"
OUT_DIR = APP_DIR / "out"
DATA_DIR = APP_DIR / "data"

FEED_FILE = Path("web") / "feed.jsonl"
META_FILE = Path("web") / "meta.json"
LATEST_PUSH_FILE = Path("web") / "latest-push.json"
STATE_FILE = STATE_DIR / "dof_state.json"
SU_LIST = DATA_DIR / "SU-arter.csv"
SUB_LIST = DATA_DIR / "SUB-arter.csv"
BATCH_DIR = Path("web") / "batches"
OBS_BASE = Path("web") / "obs"
REQUIRED_COLS = ["Artnavn", "Loknr", "Loknavn", "Antal", "Dato", "Obsid"]

mimetypes.add_type("application/manifest+json", ".webmanifest")

def _ensure_dir(p: Path):
    try:
        Path(p).mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

def _atomic_write_text(path: Path, text: str, encoding: str = "utf-8"):
    path = Path(path)
    _ensure_dir(path.parent)
    fd, tmp = tempfile.mkstemp(prefix=path.name, dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding=encoding, newline="") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except Exception:
        try: os.unlink(tmp)
        except Exception: pass
        raise

def _atomic_write_json(path: Path, obj):
    path = Path(path)
    _ensure_dir(path.parent)
    fd, tmp = tempfile.mkstemp(prefix=path.name, dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, separators=(",", ":"), sort_keys=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except Exception:
        try: os.unlink(tmp)
        except Exception: pass
        raise

def _safe_str(v):
    return "" if v is None else str(v)

def _event_obs_ts(ev: dict) -> str:
    return ev.get("ts_obs") or ev.get("ts_seen") or ""

def _load_prev_thread_payload(tpath: Path) -> dict:
    if not tpath.exists():
        return {}
    try:
        return json.loads(tpath.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}

def _prev_obs_state(prev_payload: dict) -> dict:
    # Version 2+ har 'obs_state'; normalisér nøgler til rene tal
    st = prev_payload.get("obs_state")
    if isinstance(st, dict):
        return { _canon_obsid(k): v for (k, v) in st.items() }
    st = {}
    evs = prev_payload.get("events") if isinstance(prev_payload, dict) else None
    if isinstance(evs, list):
        for ev in evs:
            try:
                oid = _canon_obsid(ev.get("obsid"))
                ts = _event_obs_ts(ev)
                an = ev.get("antal_num")
                if oid and ts:
                    st[oid] = {
                        "first_seen_ts": ev.get("ts_thread_first") or ts,
                        "last_update_ts": ev.get("ts_thread_last_update") or ts,
                        "last_antal_num": an if isinstance(an, (int, float)) else None,
                    }
            except Exception:
                pass
    return st

def _atomic_write_json_pretty(path: Path, obj):
    path = Path(path); _ensure_dir(path.parent)
    fd, tmp = tempfile.mkstemp(prefix=path.name, dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
            f.write("\n")
            f.flush(); os.fsync(f.fileno())
        os.replace(tmp, path)
    except Exception:
        try: os.unlink(tmp)
        except Exception: pass
        raise

def _slugify(s: str) -> str:
    t = unicodedata.normalize("NFKD", str(s or "").lower())
    t = t.replace("æ","ae").replace("ø","oe").replace("å","aa")
    t = "".join(ch for ch in t if not unicodedata.combining(ch))
    t = re.sub(r"[^a-z0-9]+", "-", t).strip("-")
    return t or "x"

def _date_ymd_dk(now: dt.datetime | None = None, reset_hour: int = 3) -> str:
    now = now or dt.datetime.now(DK_TZ)
    if now.hour < reset_hour:
        now = now - dt.timedelta(hours=reset_hour)
    return now.date().isoformat()

def _canon_obsid(s) -> str:
    """Returnér kun cifre fra et obsid-felt (fx 'obs-37859710' -> '37859710')."""
    m = re.search(r"\d+", str(s or ""))
    return m.group(0) if m else (str(s or "").strip())

def _row_obsid(row) -> str:
    # Forsøg at finde ObsID i kendte kolonner og returnér kun tal
    for k in ("ObsID", "ObsId", "Obsid", "obsid", "ID", "id"):
        v = row.get(k)
        if pd.notna(v):
            s = str(v).strip()
            if s:
                return _canon_obsid(s)
    # Fallback: stabilt numerisk hash (kun tal)
    key = (row.get("Artnavn",""), row.get("Loknr",""), row.get("Dato") or row.get("Dato-tid") or "")
    return str(abs(hash(key)) % 10_000_000)

def _kategori_from_row(row) -> str:
    try:
        val = art_kategori(row.get("Artnavn",""), row, row.get("_antal_num"))
    except TypeError:
        val = art_kategori(row.get("Artnavn",""), row)
    if isinstance(val, (list, tuple)): val = val[0] if val else ""
    if isinstance(val, dict): val = val.get("kategori") or val.get("cat") or val.get("type") or ""
    return str(val or "").strip().lower()

def _ensure_kategori(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["_antal_num"] = df["Antal"].apply(_parse_antal) if "Antal" in df.columns else None
    if "kategori" not in df.columns or df["kategori"].dtype != object:
        df["kategori"] = df.apply(_kategori_from_row, axis=1)
    else:
        # normalisér til ren str
        df["kategori"] = df["kategori"].astype(str).str.lower()
    return df

def _parse_date_any(s: str) -> dt.date | None:
    s = (s or "").strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d-%m-%Y"):
        try:
            return dt.datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    # fx "2025-10-18 08:32"
    for fmt in ("%Y-%m-%d %H:%M", "%d-%m-%Y %H:%M", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return dt.datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None

def _parse_time_any(s: str) -> dt.time | None:
    s = (s or "").strip()
    if not s:
        return None
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return dt.datetime.strptime(s, fmt).time()
        except ValueError:
            continue
    # tolerér "HH.MM"
    m = re.match(r"^(\d{1,2})[.:](\d{2})$", s)
    if m:
        try:
            return dt.time(int(m.group(1)), int(m.group(2)))
        except Exception:
            return None
    return None

def _row_ts_obs_iso(row) -> str:
    # 1) Hvis der findes en egentlig datetime-kolonne, brug den
    dt_raw = (row.get("Dato-tid") or row.get("DatoTid") or "").strip()
    if dt_raw:
        for fmt in ("%Y-%m-%d %H:%M", "%d-%m-%Y %H:%M", "%Y-%m-%dT%H:%M",
                    "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                dt_obj = dt.datetime.strptime(dt_raw, fmt).replace(tzinfo=DK_TZ)
                return dt_obj.isoformat()
            except ValueError:
                continue
        # fallback: hvis kun dato i feltet
        d = _parse_date_any(dt_raw)
        if d:
            return dt.datetime.combine(d, dt.time(0, 0, 0), tzinfo=DK_TZ).isoformat()

    # 2) Kombinér Dato + tid (Obstidfra/Turtidfra; fallback til til-tid)
    d = _parse_date_any(row.get("Dato", ""))
    t = (_parse_time_any(row.get("Obstidfra", "")) or
         _parse_time_any(row.get("Turtidfra", "")) or
         _parse_time_any(row.get("Obstidtil", "")) or
         _parse_time_any(row.get("Turtidtil", "")))
    if d and t:
        return dt.datetime.combine(d, t, tzinfo=DK_TZ).isoformat()
    if d:
        # som sidste udvej, behold dato (kan vises som 00:00 hvis tid mangler)
        return dt.datetime.combine(d, dt.time(0, 0, 0), tzinfo=DK_TZ).isoformat()
    return ""

# Stop skrivning af enkeltfiler i events/
def _write_event_upsert(day_dir, thread_id, ev):
    return False

# Byg ét event-objekt (inkl. rå kolonner)
def _build_event_from_row(row) -> dict:
    try:
        raw = row.to_dict()
    except Exception:
        raw = {}

    # DOF-lokalafdeling
    region_name = (row.get("DOF_afdeling") or row.get("Region") or row.get("region") or "") or None

    # Observatør (fornavn + efternavn → fallback)
    fn = (row.get("Fornavn") or "").strip()
    en = (row.get("Efternavn") or "").strip()
    observer = " ".join([x for x in (fn, en) if x]).strip() or \
               (row.get("ObsNavn") or row.get("Observatør") or row.get("Observator") or "")

    # Adfærd
    adf = (row.get("Adfbeskrivelse") or row.get("Adfærd") or row.get("Adfaerd") or row.get("Adf") or "").strip()

    # Noter
    turnoter = (row.get("Turnoter") or row.get("TurNoter") or "").strip()
    fuglnoter = (row.get("Fuglnoter") or row.get("Fuglenoter") or "").strip()

    ev = {
        "event_type": "obs",
        "obsid": _canon_obsid(_row_obsid(row)),
        "art": row.get("Artnavn"),
        "lok": row.get("Lokalitet") or row.get("Loknavn"),
        "loknr": row.get("Loknr"),
        "region": region_name,
        "coords": row.get("Coords") or row.get("Koordinater"),
        "antal_num": row.get("_antal_num"),
        "antal_text": (str(row.get("Antal")) if getattr(pd, "notna", lambda x: x is not None)(row.get("Antal")) else None),
        "kategori": row.get("kategori"),
        "adf": adf,
        "observer": observer.strip() or None,
        "ts_obs": _row_ts_obs_iso(row),
        "ts_seen": dt.datetime.now(DK_TZ).isoformat(),
        # NYT: noter som top-level felter
        "turnoter": turnoter if turnoter else None,
        "fuglnoter": fuglnoter if fuglnoter else None,
        "raw": raw,
    }
    return {k: v for k, v in ev.items() if v not in (None, "", [])}

# Skriv thread.json med alle observationer for art × lokalitet
def _update_thread_rollup(day_dir: Path, thread_id: str, evs_for_thread: list[dict], date_ymd: str) -> dict:
    """Byg trådsammenfatning og skriv thread.json med ALLE events samt obs_state."""
    tdir = Path(day_dir) / "threads" / thread_id
    _ensure_dir(tdir)
    tpath = tdir / "thread.json"

    events = [e for e in (evs_for_thread or []) if isinstance(e, dict)]
    if not events:
        payload = {
            "version": 2,
            "thread": {
                "day": date_ymd,
                "thread_id": thread_id,
                "status": "withdrawn",
                "num_events": 0,
                "has_nonzero_today": False,
            },
            "events": [],
            "stats": {"num_events": 0},
            "obs_state": {},
        }
        _atomic_write_json(tpath, payload)
        return payload["thread"]

    events_desc = sorted(events, key=lambda e: _event_obs_ts(e), reverse=True)
    events_asc = list(reversed(events_desc))

    prev_payload = _load_prev_thread_payload(tpath)
    prev_state = _prev_obs_state(prev_payload)

    new_state: dict[str, dict] = {}
    for ev in events_desc:
        oid = _canon_obsid(ev.get("obsid"))
        if not oid:
            continue
        ev["obsid"] = oid  # SIKR: events i filen har rene tal
        obs_ts = _event_obs_ts(ev)
        curr_antal = ev.get("antal_num")
        prev = prev_state.get(oid) or {}
        prev_first = prev.get("first_seen_ts") or obs_ts
        prev_last_upd = prev.get("last_update_ts") or obs_ts
        prev_antal = prev.get("last_antal_num")
        changed = (prev_antal is not None) and (curr_antal != prev_antal)
        last_update_ts = obs_ts if changed else prev_last_upd
        ts_display = last_update_ts if changed else prev_first
        ev["ts_thread_first"] = prev_first
        ev["ts_thread_last_update"] = last_update_ts
        ev["ts_thread_display"] = ts_display
        ev["thread_count_changed"] = bool(changed)
        new_state[oid] = {
            "first_seen_ts": prev_first,
            "last_update_ts": last_update_ts,
            "last_antal_num": curr_antal if isinstance(curr_antal, (int, float)) else None,
        }

    def _ts_display(e): return e.get("ts_thread_display") or _event_obs_ts(e)
    first_ts_obs = _ts_display(min(events_desc, key=_ts_display))
    last_ts_obs = _ts_display(max(events_desc, key=_ts_display))

    active_by_display = [e for e in events_desc if isinstance(e.get("antal_num"), (int, float)) and (e.get("antal_num") or 0) > 0]
    last_active_ts_obs = max(active_by_display, key=_ts_display).get("ts_thread_display") if active_by_display else None
    has_nonzero_today = bool(active_by_display)

    max_antal_num = None
    for e in events_desc:
        v = e.get("antal_num")
        if isinstance(v, (int, float)) and (max_antal_num is None or v > max_antal_num):
            max_antal_num = v

    last_antal_num = None
    for e in events_desc:
        v = e.get("antal_num")
        if isinstance(v, (int, float)):
            last_antal_num = v
            break

    last_kategori = None
    for e in events_desc:
        k = e.get("kategori")
        if k:
            last_kategori = str(k).lower()
            break

    def pick(key: str):
        for e in events_desc:
            v = e.get(key)
            if v not in (None, "", []):
                return v
        return None

    last_event = events_desc[0]

    thread = {
        "day": date_ymd,
        "thread_id": thread_id,
        "art": pick("art"),
        "lok": pick("lok"),
        "loknr": pick("loknr"),
        "region": pick("region"),
        "region_slug": pick("region_slug"),
        "coords": pick("coords"),
        "first_ts_obs": first_ts_obs,
        "last_ts_obs": last_ts_obs,
        "last_active_ts_obs": last_active_ts_obs,
        "last_kategori": last_kategori,
        "status": "active",
        "max_antal_num": max_antal_num,
        "last_antal_num": last_antal_num,
        "num_events": len(events_desc),
        "has_nonzero_today": has_nonzero_today,
        "last_adf": last_event.get("adf"),
        "last_observer": last_event.get("observer"),
        "last_obsid": _canon_obsid(last_event.get("obsid")),  # SIKR: tal
    }

    max_item = None
    for e in events_desc:
        v = e.get("antal_num")
        if isinstance(v, (int, float)) and (max_item is None or v > (max_item.get("antal_num") or float("-inf"))):
            max_item = e
    stats = {
        "num_events": len(events_desc),
        "max_antal_num": (max_item or {}).get("antal_num"),
        "max_antal_ts_obs": (max_item or {}).get("ts_obs"),
    }

    payload = {
        "version": 2,
        "thread": thread,
        "events": events_desc,
        "stats": stats,
        "obs_state": new_state,
    }

    _atomic_write_json(tpath, payload)
    return thread
def _thread_id_for(row: pd.Series) -> str:
    return f"{_slugify(row.get('Artnavn',''))}-{str(row.get('Loknr','')).strip()}"

def _write_event_if_new(base_dir: Path, thread_id: str, ev: dict) -> bool:
    evdir = base_dir / "threads" / thread_id / "events"
    _ensure_dir(evdir)
    if not ev.get("obsid"):
        # fallback: ts_seen som id
        ev_id = f"e-{int(dt.datetime.now(DK_TZ).timestamp())}"
    else:
        ev_id = str(ev["obsid"]).strip()
    path = evdir / f"{ev_id}.json"
    if path.exists():
        return False
    _atomic_write_json(path, ev)
    return True

# DEPRECATED: skriv ikke længere enkeltfiler under threads/<id>/events/
def _write_event_upsert(day_dir, thread_id, ev):
    return False

def _read_json_safe(p: Path) -> dict:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _json_equal(a, b) -> bool:
    try:
        return json.dumps(a, sort_keys=True, ensure_ascii=False) == json.dumps(b, sort_keys=True, ensure_ascii=False)
    except Exception:
        return False

# NYT: map tråd -> index-item (samme felter som observations-/trådlister bruger)
def _index_item_from_thread(thread: dict) -> dict:
    return {
        "day": thread.get("day"),
        "thread_id": thread.get("thread_id"),
        "art": thread.get("art"),
        "lok": thread.get("lok"),
        "loknr": thread.get("loknr"),
        "region": thread.get("region"),
        "region_slug": thread.get("region_slug"),
        "coords": thread.get("coords"),
        "status": thread.get("status"),
        "last_kategori": thread.get("last_kategori") or thread.get("kategori"),
        "first_ts_obs": thread.get("first_ts_obs"),
        "last_ts_obs": thread.get("last_ts_obs"),
        "last_active_ts_obs": thread.get("last_active_ts_obs"),
        "max_antal_num": thread.get("max_antal_num"),
        "last_antal_num": thread.get("last_antal_num"),
        "event_count": thread.get("num_events"),
        "last_adf": thread.get("last_adf"),
        "last_observer": thread.get("last_observer"),
        "last_obsid": thread.get("last_obsid"),  # SIKR: tal
        # NYT: noter medtages i index når kun 1 obs findes
        "turnoter": thread.get("turnoter"),
        "fuglnoter": thread.get("fuglnoter"),
    }

# Byg trådsammenfatning og skriv thread.json med ALLE events.
def _update_thread_rollup(day_dir: Path, thread_id: str, evs_for_thread: list[dict], date_ymd: str) -> dict:
    tdir = Path(day_dir) / "threads" / thread_id
    _ensure_dir(tdir)
    tpath = tdir / "thread.json"

    def ts(e: dict) -> str:
        return e.get("ts_obs") or e.get("ts_seen") or ""

    events = [e for e in (evs_for_thread or []) if isinstance(e, dict)]
    if not events:
        payload = {"version": 2, "thread": {
            "day": date_ymd, "thread_id": thread_id, "status": "withdrawn",
            "num_events": 0, "has_nonzero_today": False
        }, "events": [], "stats": {"num_events": 0}}
        _atomic_write_json(tpath, payload)
        return payload["thread"]

    # Sorteringer
    events_desc = sorted(events, key=lambda e: ts(e), reverse=True)
    events_asc = list(reversed(events_desc))

    # Første/sidste tidspunkter
    first_ts_obs = events_asc[0].get("ts_obs") or events_asc[0].get("ts_seen")
    last_ts_obs = events_desc[0].get("ts_obs") or events_desc[0].get("ts_seen")

    # Sidste aktive (antal > 0)
    pos = [e for e in events_desc if isinstance(e.get("antal_num"), (int, float)) and (e.get("antal_num") or 0) > 0]
    last_active_ts_obs = pos[0].get("ts_obs") if pos else None
    has_nonzero_today = bool(pos)

    # Max og sidste antal
    max_antal_num = None
    for e in events:
        v = e.get("antal_num")
        if isinstance(v, (int, float)) and (max_antal_num is None or v > max_antal_num):
            max_antal_num = v

    last_antal_num = None
    for e in events_desc:
        v = e.get("antal_num")
        if isinstance(v, (int, float)):
            last_antal_num = v
            break

    # Sidste kategori
    last_kategori = None
    for e in events_desc:
        k = e.get("kategori")
        if k:
            last_kategori = str(k).lower()
            break

    # Hjælpere til at løfte fælles felter fra seneste ikke-tomme
    def pick(key: str):
        for e in events_desc:
            v = e.get(key)
            if v not in (None, "", []):
                return v
        return None

    last_event = events_desc[0]

    thread = {
        "day": date_ymd,
        "thread_id": thread_id,
        "art": pick("art"),
        "lok": pick("lok"),
        "loknr": pick("loknr"),
        "region": pick("region"),
        "coords": pick("coords"),
        "first_ts_obs": first_ts_obs,
        "last_ts_obs": last_ts_obs,
        "last_active_ts_obs": last_active_ts_obs,
        "last_kategori": last_kategori,
        "status": "active",
        "max_antal_num": max_antal_num,
        "last_antal_num": last_antal_num,
        "num_events": len(events),
        "has_nonzero_today": has_nonzero_today,
        "last_adf": last_event.get("adf"),
        "last_observer": last_event.get("observer"),
    }

    # NYT: Hvis kun én observation i tråden, medtag noter i thread
    if len(events_desc) == 1:
        only = events_desc[0]
        tnote = (only.get("turnoter") or "").strip()
        fnote = (only.get("fuglnoter") or "").strip()
        if tnote:
            thread["turnoter"] = tnote
        if fnote:
            thread["fuglnoter"] = fnote

    # Stats til reference
    max_item = None
    for e in events_desc:
        v = e.get("antal_num")
        if isinstance(v, (int, float)) and (max_item is None or v > (max_item.get("antal_num") or float("-inf"))):
            max_item = e
    stats = {
        "num_events": len(events_desc),
        "max_antal_num": (max_item or {}).get("antal_num"),
        "max_antal_ts_obs": (max_item or {}).get("ts_obs"),
    }

    # NYT: thread.json indeholder både sammendrag og ALLE events
    payload = {
        "version": 2,
        "thread": thread,
        "events": events_desc,  # alle observationer for art × lokalitet (den dag)
        "stats": stats,
    }

    _atomic_write_json(tpath, payload)
    return thread

def _write_events_all(day_dir: Path, thread_id: str, thread: dict, evs_for_thread: list[dict], date_ymd: str) -> bool:
    """
    Skriv samlet fil med alle events for tråden:
    web/obs/<YYYY-MM-DD>/threads/<thread_id>/events.all.json
    Returnerer True hvis filen blev skrevet/ændret.
    """
    path = Path(day_dir) / "threads" / thread_id / "events.all.json"
    _ensure_dir(path.parent)

    # Pak relevante event-felter og sorter nyeste først
    events = []
    for ev in evs_for_thread:
        if ev.get("event_type") != "obs":
            continue
        events.append({
            "obsid": ev.get("obsid"),
            "ts_obs": ev.get("ts_obs") or ev.get("ts_seen"),
            "antal_num": ev.get("antal_num"),
            "antal_text": ev.get("antal_text"),
            "kategori": ev.get("kategori"),
            "adf": ev.get("adf"),
            "observer": ev.get("observer"),
        })
    events.sort(key=lambda e: e.get("ts_obs") or "", reverse=True)

    # Stats
    max_item = None
    for e in events:
        v = e.get("antal_num")
        if isinstance(v, (int, float)):
            if (max_item is None) or (v > (max_item.get("antal_num") or float("-inf"))):
                max_item = e
    stats = {
        "num_events": len(events),
        "max_antal_num": (max_item or {}).get("antal_num"),
        "max_antal_ts_obs": (max_item or {}).get("ts_obs"),
    }

    # Tråd-metadata (subset)
    payload = {
        "version": 1,
        "thread": {
            "day": date_ymd,
            "thread_id": thread_id,
            "art": thread.get("art"),
            "lok": thread.get("lok"),
            "loknr": thread.get("loknr"),
            "region": thread.get("region"),
            "region_slug": thread.get("region_slug"),  # NYT
            "coords": thread.get("coords"),
            "first_ts_obs": thread.get("first_ts_obs"),
            "last_ts_obs": thread.get("last_ts_obs"),
            "status": thread.get("status"),
        },
        "events": events,
        "stats": stats,
    }

    old = None
    if path.exists():
        try:
            old = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            old = None

    if old is None or not _json_equal(old, payload):
        _atomic_write_json(path, payload)
        return True
    return False

def _write_index_for_day(day_dir: Path, threads: list[dict], date_ymd: str) -> bool:
    """
    Skriv web/obs/<date>/index.json som pæn, multilinjers JSON.
    """
    items = []
    for t in (threads or []):
        if not t: continue
        item = _index_item_from_thread(t)
        items.append(item)

    # Nyeste først
    items.sort(key=lambda x: x.get("last_ts_obs") or "", reverse=True)

    path = Path(day_dir) / "index.json"
    old = None
    if path.exists():
        try: old = json.loads(path.read_text(encoding="utf-8"))
        except Exception: old = None

    if old is None or not _json_equal(old, items):
        _atomic_write_json_pretty(path, items)
        return True
    return False

def _purge_old_obs(retain_days: int = 2):
    base = OBS_BASE
    if not base.exists():
        return
    today = _date_ymd_dk()
    try:
        dates = sorted([p.name for p in base.iterdir() if p.is_dir()])
    except Exception:
        return
    # Behold i alt 'retain_days' inklusive today og i går
    # Slet resten
    keep = set()
    try:
        d = dt.date.fromisoformat(today)
        for i in range(retain_days):
            keep.add((d - dt.timedelta(days=i)).isoformat())
    except Exception:
        keep = {today}
    for dname in dates:
        if dname not in keep:
            try:
                for pp in (base / dname).rglob("*"):
                    try:
                        if pp.is_file():
                            pp.unlink()
                    except Exception:
                        pass
                for pp in sorted((base / dname).rglob("*"), reverse=True):
                    try:
                        if pp.is_dir():
                            pp.rmdir()
                    except Exception:
                        pass
                (base / dname).rmdir()
            except Exception:
                continue

def _send_withdraw_push(thread: dict, server_url: str = "http://localhost:8000/api/publish"):
    try:
        url = server_url
        title = f"Tilbagekaldt: {thread.get('art','')} – {thread.get('lok','')}"
        last_pos = thread.get("last_active_ts_obs") or thread.get("last_ts_obs")
        body = f"Dagens observation(er) rettet til 0 / fjernet. Sidst positivt: {last_pos}"
        link = f"/thread.html?date={thread.get('first_ts_obs','')[:10]}&id={thread.get('thread_id','')}"
        payload = {
            "title": title, "body": body, "url": link,
            "tag": f"withdraw-{thread.get('thread_id')}",
            "urgency": "normal"
        }
        requests.post(url, json=payload, timeout=(3.05, 15))
    except Exception:
        pass

def build_obs_storage_for_day(df: pd.DataFrame, date_ymd: str, *, categories: tuple[str,...]=("su","sub"), send_withdraw_push: bool = True):
    if df.empty:
        _ensure_dir(OBS_BASE / date_ymd)
        _atomic_write_json((OBS_BASE / date_ymd / "index.json"), [])
        return

    day_dir = OBS_BASE / date_ymd
    _ensure_dir(day_dir)

    df = _ensure_kategori(df)
    rows = df[df["kategori"].isin(categories)]

    thread_events: dict[str, list[dict]] = {}

    for _, r in rows.iterrows():
        ev = _build_event_from_row(r)
        tid = _thread_id_for(r)  # art × lokalitet
        thread_events.setdefault(tid, []).append(ev)

    threads_out: list[dict] = []
    for tid, evs in thread_events.items():
        thread = _update_thread_rollup(day_dir, tid, evs, date_ymd)
        threads_out.append(thread)

    # Markér withdrawn for tråde, der mangler i denne sync
    present = set(thread_events.keys())
    threads_dir = day_dir / "threads"
    if threads_dir.exists():
        for tdir in threads_dir.iterdir():
            if not tdir.is_dir(): continue
            tid = tdir.name
            if tid in present: continue
            tpath = tdir / "thread.json"
            if not tpath.exists(): continue
            try:
                before = json.loads(tpath.read_text(encoding="utf-8"))
            except Exception:
                continue
            if before.get("has_nonzero_today") and before.get("status") != "withdrawn":
                before["has_nonzero_today"] = False
                before["status"] = "withdrawn"
                _atomic_write_json(tpath, before)

    # Skriv dagsindex (med alle nødvendige felter til thread.js liste)
    _write_index_for_day(day_dir, threads_out, date_ymd)

    # Debug: antal tråde (fjern 'writes' som ikke længere bruges)
    print(f"[obs] {date_ymd}: threads={len(thread_events)}")

# ───────────────────────────────── Utilities ─────────────────────────────────
def ensure_dirs():
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    DL_DIR.mkdir(parents=True, exist_ok=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

def build_url(date_dd_mm_yyyy: str) -> str:
    try:
        dt.datetime.strptime(date_dd_mm_yyyy, "%d-%m-%Y")
    except ValueError as e:
        raise SystemExit(f"Ugyldig dato '{date_dd_mm_yyyy}' (forventet DD-MM-YYYY)") from e
    return DOF_URL.format(dato=date_dd_mm_yyyy)

def fetch_csv(url: str) -> Optional[Path]:
    ts = dt.datetime.now(DK_TZ).strftime("%Y%m%d_%H%M%S")
    dest = DL_DIR / f"search_result_{ts}.csv"
    headers = {"User-Agent": "birdnotification/1.7 (+local)"}
    try:
        with requests.get(url, headers=headers, timeout=60) as r:
            r.raise_for_status()
            dest.write_bytes(r.content)
            return dest
    except requests.RequestException as e:
        print(f"[Advarsel] Kunne ikke hente CSV: {e}", file=sys.stderr)
        return None

def _trim_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    return df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

def _get_series(df: pd.DataFrame, col: str) -> pd.Series:
    return df[col] if col in df.columns else pd.Series([""] * len(df), index=df.index)

def _inject_time_fallbacks(df: pd.DataFrame) -> pd.DataFrame:
    s_obsidfra = _get_series(df, "Obstidfra")
    s_turtidfra = _get_series(df, "Turtidfra")
    s_obsidtil = _get_series(df, "Obsidtil")
    s_turtidtil = _get_series(df, "Turtidtil")
    df["Obstidfra"] = s_obsidfra.where(s_obsidfra != "", s_turtidfra)
    df["Obstidtil"] = s_obsidtil.where(s_obsidtil != "", s_turtidtil)
    return df

def _try_read(path: Path, encoding: str) -> pd.DataFrame:
    df = pd.read_csv(path, sep=";", encoding=encoding, dtype=str, keep_default_na=False)
    return _trim_whitespace(df)

def read_csv_with_fallback(path: Path) -> Tuple[pd.DataFrame, str]:
    encodings = ["utf-8", "utf-8-sig", "cp1252", "latin1"]
    last_err = None
    for enc in encodings:
        try:
            df = _try_read(path, enc)
            missing = [c for c in REQUIRED_COLS if c not in df.columns]
            if missing:
                raise SystemExit(f"Mangler forventede kolonner i CSV: {missing}")
            df["group_key"] = df["Artnavn"] + "\n" + df["Loknr"]
            df = _inject_time_fallbacks(df)
            return df, enc
        except (UnicodeDecodeError, pd.errors.ParserError) as e:
            last_err = e
            continue
    raise SystemExit(f"Kunne ikke læse CSV med kendte encodings: {last_err}")

def _region_key(text: str) -> str:
    return (text or "").strip()

# ─────────────────────── State ───────────────────────
def load_state() -> Dict[str, dict]:
    if not STATE_FILE.exists():
        return {}
    try:
        raw = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        state: Dict[str, dict] = {}
        today_iso = dt.datetime.now(DK_TZ).date().isoformat()
        if not isinstance(raw, dict):
            return {}
        for k, v in raw.items():
            if isinstance(v, dict) and ("date" in v or "antal" in v):
                date_val = v.get("date")
                antal_val = v.get("antal")
                try:
                    if isinstance(antal_val, str):
                        antal_val = float(antal_val.replace(",", "."))
                except Exception:
                    antal_val = None
                state[k] = {"date": date_val, "antal": antal_val}
            else:
                state[k] = {"date": today_iso, "antal": None}
        return state
    except Exception:
        return {}

def save_state(state: Dict[str, dict]):
    _atomic_write_text(STATE_FILE, json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

def clear_state_and_downloads():
    try:
        if STATE_FILE.exists():
            STATE_FILE.unlink()
    except Exception as e:
        print(f"[Advarsel] Kunne ikke slette state: {e}", file=sys.stderr)
    try:
        if DL_DIR.exists():
            for p in DL_DIR.glob("*.csv"):
                try:
                    p.unlink()
                except Exception as e:
                    print(f"[Advarsel] Kunne ikke slette fil: {p.name} ({e})", file=sys.stderr)
    except Exception as e:
        print(f"[Advarsel] Fejl ved oprydning i downloads/: {e}", file=sys.stderr)

# ─────────────────────── Parsning/normalisering ───────────────────────
def parse_float_str(s: str) -> str:
    return s.replace(",", ".") if s else ""

def _current_ts_dk() -> str:
    return dt.datetime.now(DK_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")

def obs_timestamp_dk(row: pd.Series) -> str:
    date_str = (row.get("Dato") or "").strip()
    time_str = (row.get("Obstidfra") or row.get("Turtidfra") or "").strip()
    if not date_str:
        return ""
    dt_obj = None
    fmts = ["%Y-%m-%d %H:%M", "%d-%m-%Y %H:%M", "%Y-%m-%d", "%d-%m-%Y"]
    for fmt in fmts:
        try:
            dt_obj = dt.datetime.strptime(
                (f"{date_str} {time_str}".strip() if "%H:%M" in fmt else date_str),
                fmt,
            )
            break
        except ValueError:
            continue
    if dt_obj is None:
        return ""
    return dt_obj.replace(tzinfo=DK_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")

def _parse_antal(val):
    """Træk et tal ud af 'Antal' (tolerant)."""
    if val is None:
        return None
    s = str(val).strip().lower().replace(",", ".")
    if not s:
        return None
    s = s.replace("ca.", "").replace("ca ", "").replace("~", "")
    nums = re.findall(r"\d+(?:\.\d+)?", s)
    if not nums:
        return None
    return max(float(x) for x in nums)

# ─────────────────────── Region- og navne-normalisering ───────────────────────
def _strip_accents(s: str) -> str:
    return "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))

def _slug_region(s: str) -> str:
    """Normaliser DOF-afdelingsnavn til slug uden diakritika (fx 'København' -> 'kobenhavn')."""
    s0 = (s or "").strip().lower()
    if not s0:
        return ""
    s1 = _strip_accents(s0)
    s1 = re.sub(r"[^a-z0-9]+", "", s1)
    aliases = {
        "kbh": "kobenhavn",
        "kobenhavn": "kobenhavn",
        "kobenhavns": "kobenhavn",
        "bornholm": "bornholm",
        "fyn": "fyn",
        "nordsjaelland": "nordsjaelland",
        "nordjylland": "nordjylland",
        "nordvestjylland": "nordvestjylland",
        "oestjylland": "oestjylland",
        "sydoestjylland": "sydoestjylland",
        "sydvestjylland": "sydvestjylland",
        "vestjylland": "vestjylland",
        "vestsjaelland": "vestsjaelland",
        "storstrom": "storstrom",
        "sonderjylland": "sonderjylland",
        "kobenhavnskommune": "kobenhavn",
    }
    return aliases.get(s1, s1)

def _norm_art(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())

# ─────────────────────── SU/SUB- og BEMÆRK-data ───────────────────────
def _read_semicolon_csv_with_fallback(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    for enc in ["utf-8", "utf-8-sig", "cp1252", "latin1"]:
        try:
            df = pd.read_csv(path, sep=";", dtype=str, keep_default_na=False, encoding=enc)
            return df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        except Exception:
            continue
    return pd.DataFrame()

def load_rare_sets() -> tuple[set[str], set[str]]:
    su_df = _read_semicolon_csv_with_fallback(SU_LIST) if SU_LIST.exists() else pd.DataFrame()
    sub_df = _read_semicolon_csv_with_fallback(SUB_LIST) if SUB_LIST.exists() else pd.DataFrame()

    def to_set(df: pd.DataFrame) -> set[str]:
        return set(df["artsnavn"].astype(str).str.strip()) if not df.empty and "artsnavn" in df.columns else set()

    return to_set(su_df), to_set(sub_df)

try:
    SU_SET, SUB_SET = load_rare_sets()
except Exception as e:
    print(f"[Advarsel] Kunne ikke læse SU/SUB-lister: {e} – markerer alle som 'alm'.", file=sys.stderr)
    SU_SET, SUB_SET = set(), set()

# ↓ NYT: bemaerk-tærskler per region
def load_bemaerk_thresholds() -> Dict[str, Dict[str, float]]:
    """
    Indlæs alle data/*bemaerk*.csv med kolonner 'artsnavn;bemaerk_antal'
    og byg mapping: region_key -> { artsnavn_norm -> terskel_float }.
    region_key er case-insensitiv men bevarer diakritik (fx 'københavn').
    """
    out: Dict[str, Dict[str, float]] = {}
    for p in DATA_DIR.glob("*bemaerk*.csv"):
        base = p.stem  # fx 'københavn_bemaerk_parsed'
        region_hint_raw = base.split("_")[0]  # 'københavn'
        region_key = _region_key(region_hint_raw)  # 'københavn'
        if not region_key:
            continue
        df = _read_semicolon_csv_with_fallback(p)
        if df.empty:
            continue
        cols = {c.strip().lower(): c for c in df.columns}
        if "artsnavn" not in cols or "bemaerk_antal" not in cols:
            possible = [c for c in df.columns if "bemaerk" in c.lower() and "antal" in c.lower()]
            if not possible:
                print(f"[Advarsel] Springer {p.name} over (mangler 'artsnavn'/'bemaerk_antal')", file=sys.stderr)
                continue
            cols["bemaerk_antal"] = possible[0]
            cols.setdefault("artsnavn", df.columns[0])
        a_col = cols["artsnavn"]; t_col = cols["bemaerk_antal"]
        reg_map = out.setdefault(region_key, {})
        for _, r in df.iterrows():
            art = _norm_art(r.get(a_col, ""))
            if not art:
                continue
            v = str(r.get(t_col, "")).strip().replace(",", ".")
            try:
                thr = float(v)
            except Exception:
                continue
            reg_map[art] = thr
    return out

try:
    BEMAERK_MAP = load_bemaerk_thresholds()
except Exception as e:
    print(f"[Advarsel] Kunne ikke læse bemaerk-lister: {e}", file=sys.stderr)
    BEMAERK_MAP = {}

# ─────────────────────── Kategorisering (hierarki) ───────────────────────
def art_kategori(artnavn: str, row: Optional[pd.Series] = None,
                 antal_override: Optional[float] = None, debug: bool = False) -> str:
    a_raw = (artnavn or "").strip()
    if not a_raw:
        return "alm"
    # SU / SUB først
    if a_raw in SU_SET:
        return "su"
    if a_raw in SUB_SET:
        return "sub"
    # bemaerk: fleksibelt opslag mod BEMAERK_MAP (DOF_afdeling forbliver uændret)
    if row is not None and BEMAERK_MAP:
        region_raw = (row.get("DOF_afdeling") or "").strip()
        if region_raw:
            region_candidates = [region_raw, region_raw.lower()]
            region_wo_dof = re.sub(r"^DOF\s+", "", region_raw, flags=re.IGNORECASE).strip()
            if region_wo_dof:
                region_candidates += [region_wo_dof, region_wo_dof.lower()]
            reg_map = None
            chosen_key = None
            for key in region_candidates:
                reg_map = BEMAERK_MAP.get(key)
                if reg_map:
                    chosen_key = key
                    break
            if reg_map:
                thr = reg_map.get(_norm_art(a_raw))
                if thr is not None:
                    antal_val = antal_override
                    if antal_val is None:
                        antal_val = _parse_antal(row.get("Antal", ""))
                    if antal_val is not None and antal_val >= thr:
                        return "bemaerk"
                if debug:
                    print(f"[bemaerk:no] art='{a_raw}' antal={antal_val} < thr={thr} region_key='{chosen_key}'")
            elif debug:
                print(f"[bemaerk:no] ingen region-match for '{region_raw}' (prøvede: {region_candidates})")
    return "alm"

# ─────────────────────── Filtrering (regler) ───────────────────────
def load_clients_config(path: Optional[str]) -> List[dict]:
    if not path:
        return [{"id": "default", "sinks": [{"type": "stdout"}], "rules": {}}]
    p = Path(path)
    if not p.exists():
        raise SystemExit(f"Client-config ikke fundet: {p}")
    with p.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    clients = data.get("clients", [])
    if not clients:
        clients = [{"id": "default", "sinks": [{"type": "stdout"}], "rules": {}}]
    return clients

def build_mask(df: pd.DataFrame, rules: dict) -> pd.Series:
    """
    Understøttede regler:
    species, exclude_species,
    adf_contains, loknavn_contains, (kompat) adf_regex/loknavn_regex -> literal substring
    loknr, dof_afdelinger,
    time_range {from,to}, min_antal,
    only_with_coords, bbox [lon_min, lat_min, lon_max, lat_max],
    kategori ["alm","sub","su","bemaerk"]
    """
    m = pd.Series(True, index=df.index)

    # 1) Arter
    species = rules.get("species")
    if species:
        m &= df["Artnavn"].isin(species)
    ex = rules.get("exclude_species")
    if ex:
        m &= ~df["Artnavn"].isin(ex)

    # 2) Adfærd / Loknavn (literal substring, case-insensitive)
    adf_lit = rules.get("adf_contains") or rules.get("adf_regex")
    if adf_lit:
        m &= df["Adfbeskrivelse"].str.contains(str(adf_lit), case=False, na=False, regex=False)
    lok_lit = rules.get("loknavn_contains") or rules.get("loknavn_regex")
    if lok_lit:
        m &= df["Loknavn"].str.contains(str(lok_lit), case=False, na=False, regex=False)

    # 3) Lokation
    loknr = rules.get("loknr")
    if loknr:
        m &= df["Loknr"].isin(loknr)
    dof = rules.get("dof_afdelinger")
    if dof and "DOF_afdeling" in df.columns:
        m &= df["DOF_afdeling"].isin(dof)

    # 4) Tid
    tr = rules.get("time_range")
    if tr and (tr.get("from") or tr.get("to")):
        t_from_series = df["Obstidfra"].fillna("").str.strip()
        have_time = t_from_series.str.len() >= 4
        if tr.get("from"):
            m &= have_time & (t_from_series >= tr["from"])
        if tr.get("to"):
            m &= have_time & (t_from_series <= tr["to"])

    # 5) Antal
    min_a = rules.get("min_antal")
    if (min_a is not None) and ("Antal" in df.columns):
        antal = pd.to_numeric(df["Antal"], errors="coerce").fillna(0)
        m &= antal >= float(min_a)

    # 6) Koordinater / BBox
    if rules.get("only_with_coords"):
        # FIX: OR/AND-logik (obs OR lok) pr. akse, derefter AND mellem akser
        has_lon = df["obs_laengdegrad"].ne("") | df["lok_laengdegrad"].ne("")
        has_lat = df["obs_breddegrad"].ne("") | df["lok_breddegrad"].ne("")
        m &= has_lon & has_lat
    bbox = rules.get("bbox")
    if bbox:
        lon_min, lat_min, lon_max, lat_max = bbox
        lon = df["obs_laengdegrad"].replace("", pd.NA).fillna(df["lok_laengdegrad"])
        lat = df["obs_breddegrad"].replace("", pd.NA).fillna(df["lok_breddegrad"])
        lon = pd.to_numeric(lon.astype(str).str.replace(",", "."), errors="coerce")
        lat = pd.to_numeric(lat.astype(str).str.replace(",", "."), errors="coerce")
        m &= (lon >= lon_min) & (lon <= lon_max) & (lat >= lat_min) & (lat <= lat_max)

    # 7) Kategori
    cats = rules.get("kategori")
    if cats:
        if isinstance(cats, str):
            cats = [cats]
        cats_norm = {str(c).strip().lower() for c in cats if str(c).strip()}
        s_cat = df["kategori"] if "kategori" in df.columns else (
            df.apply(lambda r: art_kategori(r.get("Artnavn",""), r), axis=1)
        )
        m &= s_cat.isin(cats_norm)

    return m

# ─────────────────────── CLI-rendering ───────────────────────
def render_output_line(r: pd.Series, timestamp_mode: str, kategori: str) -> str:
    ts = obs_timestamp_dk(r) if timestamp_mode == "obs" else _current_ts_dk()
    antal = (r.get("Antal", "") or "").strip()
    art = (r.get("Artnavn", "") or "").strip()
    adf = (r.get("Adfbeskrivelse", "") or "").strip()
    lok = (r.get("Loknavn", "") or "").strip()
    dof_afd = (r.get("DOF_afdeling", "") or "").strip()
    fn = (r.get("Fornavn", "") or "").strip()
    en = (r.get("Efternavn", "") or "").strip()
    name = " ".join([x for x in [fn, en] if x]).strip()
    lon = parse_float_str((r.get("obs_laengdegrad", "") or r.get("lok_laengdegrad", "") or "").strip())
    lat = parse_float_str((r.get("obs_breddegrad", "") or r.get("lok_breddegrad", "") or "").strip())
    coords = f"{lon}, {lat}" if lon and lat else ""
    t_from = (r.get("Obstidfra", "") or "").strip()
    t_to = (r.get("Obstidtil", "") or "").strip()
    t_span = f"{t_from}-{t_to}" if t_from and t_to else (t_from or t_to)
    antal_art = f"{antal} {art}".strip()
    parts = [ts, f"[{kategori}]", antal_art, adf, lok, dof_afd, name, coords, t_span]
    return " · ".join([p for p in parts if p])

# ─────────────────────── Fanout (stdout/file + webpush) ───────────────────────
def _write_digest_file(cid: str, rows_c: pd.DataFrame) -> Tuple[str, List[dict], List[str], List[str]]:
    """
    Skriver batchfil (atomisk) for klientens chunk og returnerer:
      (url_path, items, regions, categories)

    regions/categories bruges både til at vise i UI og til server-side grovfilter.
    """
    batch_dir = BATCH_DIR
    batch_dir.mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.now(DK_TZ).strftime("%Y%m%d%H%M%S")
    fname = f"batch-{ts}-{cid}.json"

    items: List[dict] = []
    regions_set, cats_set = set(), set()

    for _, r in rows_c.iterrows():
        cat = art_kategori(r.get("Artnavn", ""), r)  # kategori pr. item
        afd = (r.get("DOF_afdeling", "") or "").strip()
        if afd:
            regions_set.add(afd)
        if cat:
            cats_set.add(cat)

        items.append({
            "obsid": r.get("Obsid",""),
            "loknr": r.get("Loknr",""),          # ← NYT: send loknr med
            "art": r.get("Artnavn",""),
            "antal": r.get("Antal",""),
            "adf": r.get("Adfbeskrivelse",""),
            "lok": r.get("Loknavn",""),
            "dof_afdeling": afd,
            "fornavn": r.get("Fornavn",""),
            "efternavn": r.get("Efternavn",""),
            "lon": r.get("obs_laengdegrad") or r.get("lok_laengdegrad",""),
            "lat": r.get("obs_breddegrad") or r.get("lok_breddegrad",""),
            "tid_fra": r.get("Obstidfra",""),
            "tid_til": r.get("Obstidtil",""),
            "kategori": cat
        })

    payload = {
        "client": cid,
        "count": len(items),
        "generated": dt.datetime.now(DK_TZ).isoformat(),
        "items": items
    }

    # Atomisk skrivning
    out_path = (batch_dir / fname)
    _atomic_write_text(out_path, json.dumps(payload, ensure_ascii=False, indent=0), encoding="utf-8")

    return f"/batches/{fname}", items, sorted(regions_set), sorted(cats_set)

def purge_old_batches(max_age_hours: int = 24, verbose: bool = False) -> int:
    """
    Slet batch-JSON-filer i web/batches/, der er ældre end max_age_hours.
    Returnerer antal slettede filer.
    """
    base = BATCH_DIR
    if not base.exists():
        return 0
    now_ts = dt.datetime.now(DK_TZ).timestamp()
    cutoff = now_ts - (max_age_hours * 3600)
    deleted = 0
    for p in base.glob("batch-*.json"):
        try:
            if p.stat().st_mtime < cutoff:
                p.unlink()
                deleted += 1
        except Exception as e:
            print(f"[Advarsel] Kunne ikke slette batch '{p.name}': {e}", file=sys.stderr)
    if verbose and deleted:
        print(f"[Cleanup] Slettede {deleted} batch(es) ældre end {max_age_hours} timer.")
    return deleted

def fanout_to_clients(new_rows: pd.DataFrame, clients: List[dict], timestamp_mode: str):
    """
    Sender nye rækker til hver klient i 'clients' efter deres regler.
    - Filtrerer rækker med build_mask(...)
    - Printer kompakt linje til stdout (hvor valgt)
    - Logger til fil (append) (hvor valgt)
    - Publicerer webpush-digests (hvor valgt)
    - Skriver til web/feed.jsonl og opdaterer web/meta.json
    - Skriver seneste pushoverblik til web/latest-push.json (debug)
    """
    import requests

    def _normalize_timeout(val) -> Tuple[float, float]:
        if isinstance(val, (int, float)):
            return (3.05, float(val))
        if isinstance(val, (list, tuple)) and len(val) == 2:
            return (float(val[0]), float(val[1]))
        return (3.05, 30.0)

    for c in clients:
        cid = c.get("id", "default")
        rules = c.get("rules", {}) or {}
        sinks = c.get("sinks", [{"type": "stdout"}])

        # Sikr kategori-kolonne inkl. bemaerk (row-wise pga. region/antal)
        if "kategori" not in new_rows.columns:
            tmp = new_rows.copy()
            tmp["_antal_num"] = tmp["Antal"].apply(_parse_antal)
            tmp["kategori"] = tmp.apply(lambda r: art_kategori(r.get("Artnavn",""), r, r.get("_antal_num")), axis=1)
            rows_for_mask = tmp
        else:
            rows_for_mask = new_rows

        # Filtrer rækker for denne klient
        rows_c = rows_for_mask[build_mask(rows_for_mask, rules)]
        if rows_c.empty:
            continue

        # FEED + META
        _append_to_feed(cid, rows_c)
        _update_meta(rows_c)

        # STDOUT/FILE
        for _, r in rows_c.iterrows():
            cat = r.get("kategori") or art_kategori(r.get("Artnavn", ""), r)
            line = render_output_line(r, timestamp_mode, cat)
            out = f"[{cid}] {line}"
            for s in (s for s in sinks if s.get("type", "stdout").lower() == "stdout"):
                print(out)
            for s in (s for s in sinks if s.get("type", "file").lower() == "file"):
                path = Path(s.get("path") or (OUT_DIR / f"{cid}.log"))
                path.parent.mkdir(parents=True, exist_ok=True)
                with path.open("a", encoding="utf-8") as f:
                    f.write(out + "\n")

        # WEBPUSH (digest pr. batch) – PARALLEL POST til server
        wps = [s for s in sinks if s.get("type", "").lower() == "webpush"]
        if wps:
            sink_cfg = wps[0]
            CHUNK = int(sink_cfg.get("chunk_size", 100))
            api = sink_cfg.get("url", "http://localhost:8000/api/publish")
            parallel_posts = int(sink_cfg.get("parallel_posts", 4))
            timeout = _normalize_timeout(sink_cfg.get("timeout", (3.05, 30.0)))

            tasks = []
            for start in range(0, len(rows_c), CHUNK):
                chunk = rows_c.iloc[start:start + CHUNK]

                # Skriv batchfil og udled regions/categories for denne chunk
                url_path, items, regions, categories = _write_digest_file(cid, chunk)

                # Byg titel/teaser
                n = len(chunk)
                head = ", ".join(
                    f"{r.get('Antal','')} {r.get('Artnavn','')}".strip()
                    for _, r in chunk.head(3).iterrows()
                )
                more = f" … +{n-3} flere" if n > 3 else ""

                # Urgency: "high" hvis chunk har su/bemaerk, ellers "normal"
                cats_lc = {str(x).lower() for x in categories}
                urgency = "high" if ("su" in cats_lc or "bemaerk" in cats_lc) else "normal"

                payload = {
                    "type": "digest",
                    "title": f"[{cid}] {n} nye obs",
                    "body": (head + more).strip(" ,·"),
                    "url": url_path,
                    "tag": f"bird-digest-{cid}",
                    "renotify": True,
                    # NYT: server-side filter hints
                    "regions": regions,         # fx ["DOF København", "DOF Fyn"]
                    "categories": list(cats_lc),# fx ["su","bemaerk"]
                    "urgency": urgency          # bruges af serverens header-merge
                }

                _write_latest_push({
                    "client": cid, "title": payload["title"], "body": payload["body"],
                    "url": payload["url"], "count": n, "tag": payload["tag"]
                })
                tasks.append(payload)

            def _post_one(session: requests.Session, payload: dict) -> None:
                try:
                    session.post(api, json=payload, timeout=timeout)
                except Exception as e:
                    print(f"[webpush] POST {api} fejlede: {e}", file=sys.stderr)

            if tasks:
                with requests.Session() as sess:
                    adapter = requests.adapters.HTTPAdapter(pool_connections=parallel_posts,
                                                            pool_maxsize=parallel_posts)
                    sess.mount("http://", adapter); sess.mount("https://", adapter)
                    with ThreadPoolExecutor(max_workers=parallel_posts) as pool:
                        futs = [pool.submit(_post_one, sess, payload) for payload in tasks]
                        for f in as_completed(futs):
                            _ = f.result()

# ─────────────────────── FEED/META helpers ───────────────────────
def _append_to_feed(cid: str, rows_c: pd.DataFrame) -> None:
    FEED_FILE.parent.mkdir(parents=True, exist_ok=True)
    now_iso = dt.datetime.now(DK_TZ).isoformat()
    with FEED_FILE.open("a", encoding="utf-8") as f:
        for _, r in rows_c.iterrows():
            payload = {
                "client": cid,
                "generated": now_iso,
                "obsid": r.get("Obsid",""),
                "art": r.get("Artnavn",""),
                "antal": r.get("Antal",""),
                "adf": r.get("Adfbeskrivelse",""),
                "lok": r.get("Loknavn",""),
                "dof_afdeling": r.get("DOF_afdeling",""),
                "fornavn": r.get("Fornavn",""),
                "efternavn": r.get("Efternavn",""),
                "lon": r.get("obs_laengdegrad") or r.get("lok_laengdegrad",""),
                "lat": r.get("obs_breddegrad") or r.get("lok_breddegrad",""),
                "tid_fra": r.get("Obstidfra",""),
                "tid_til": r.get("Obstidtil",""),
                "kategori": art_kategori(r.get("Artnavn",""), r),
            }
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")

def _update_meta(rows_c: pd.DataFrame) -> None:
    META_FILE.parent.mkdir(parents=True, exist_ok=True)
    now_iso = dt.datetime.now(DK_TZ).isoformat()
    meta = {"afdelinger": [], "lastUpdated": now_iso}
    if META_FILE.exists():
        try:
            meta = json.loads(META_FILE.read_text(encoding="utf-8"))
            if not isinstance(meta, dict):
                meta = {"afdelinger": [], "lastUpdated": now_iso}
        except Exception:
            meta = {"afdelinger": [], "lastUpdated": now_iso}
    existing = set(meta.get("afdelinger", []))
    new_vals = set(v for v in rows_c.get("DOF_afdeling", pd.Series(dtype=str)).astype(str).tolist() if v)
    meta["afdelinger"] = sorted(existing | new_vals)
    meta["lastUpdated"] = now_iso
    _atomic_write_text(META_FILE, json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

def _write_latest_push(payload: dict) -> None:
    try:
        LATEST_PUSH_FILE.parent.mkdir(parents=True, exist_ok=True)
        payload = dict(payload)
        payload["generated"] = dt.datetime.now(DK_TZ).isoformat()
        _atomic_write_text(LATEST_PUSH_FILE, json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        print(f"[Advarsel] Kunne ikke skrive latest-push.json: {e}", file=sys.stderr)

# ─────────────────────── Ændringsdetektion (loknr/dag) ───────────────────────
def _choose_latest_row(gdf: pd.DataFrame) -> pd.Series:
    if "Obstidfra" in gdf.columns:
        g2 = gdf.copy()
        g2["_t"] = g2["Obstidfra"].fillna("").astype(str)
        g2 = g2.sort_values("_t", kind="stable")
        r = g2.iloc[-1]
        try:
            r = r.drop(labels=["_t"])
        except Exception:
            pass
        return r
    return gdf.iloc[-1]

def detect_and_report_changes(
    df: pd.DataFrame,
    state: Dict[str, dict],
    clients: List[dict],
    timestamp_mode: str,
) -> Dict[str, dict]:
    """
    Finder nye førstegangs-observationer pr. (Artnavn × Loknr) pr. dag
    og registrerer desuden ANTAL-stigninger samme dag.
    """
    updated_state: Dict[str, dict] = {k: {"date": v.get("date"), "antal": v.get("antal")} for k, v in state.items()}
    batches = []
    today_iso = dt.datetime.now(DK_TZ).date().isoformat()

    # Sikr kategori-kolonne (row-wise pga. bemaerk)
    if "kategori" not in df.columns:
        df = df.copy()
        df["_antal_num"] = df["Antal"].apply(_parse_antal)
        df["kategori"] = df.apply(lambda r: art_kategori(r.get("Artnavn",""), r, r.get("_antal_num")), axis=1)

    for gkey, gdf in df.groupby("group_key", dropna=False):
        rec = updated_state.get(gkey, {"date": None, "antal": None})
        last_date = rec.get("date")
        last_antal = rec.get("antal")
        r = _choose_latest_row(gdf)
        a_new = _parse_antal(r.get("Antal", ""))

        if last_date != today_iso:
            batches.append(r)
            updated_state[gkey] = {"date": today_iso, "antal": a_new}
            continue

        if a_new is not None and last_antal is not None and a_new > last_antal:
            try:
                art = (r.get("Artnavn", "") or "").strip()
                lok = (r.get("Loknavn", "") or "").strip()
                print(f"[Δ] {art} @ {lok}: {last_antal} → {a_new}")
            except Exception:
                pass
            batches.append(r)
            updated_state[gkey]["antal"] = a_new
            continue

        if last_antal is None and a_new is not None:
            updated_state[gkey]["antal"] = a_new

    if batches:
        all_new = pd.DataFrame(batches)
        sort_cols = [c for c in ["Obstidfra", "Obsid"] if c in all_new.columns]
        if sort_cols:
            all_new = all_new.sort_values(sort_cols, kind="stable")
        fanout_to_clients(all_new, clients, timestamp_mode)

    return updated_state

# ─────────────────────── Kørsel ───────────────────────
def run_once(date_str: str, clients: List[dict], timestamp_mode: str) -> bool:
    """
    Kører en enkelt iteration af scriptet.
    Returnerer True hvis kørslen var succesfuld, False hvis der var fejl ved CSV-hentning.
    """
    ensure_dirs()
    url = build_url(date_str)
    csv_path = fetch_csv(url)

    if csv_path is None:
        return False

    try:
        df, enc = read_csv_with_fallback(csv_path)
    finally:
        try:
            csv_path.unlink()
        except FileNotFoundError:
            pass
        except Exception as e:
            print(f"[Advarsel] Kunne ikke slette downloadet CSV: {csv_path.name} ({e})", file=sys.stderr)

    # Når df er klar:
    try:
        date_ymd = dt.datetime.strptime(date_str.strip(), "%d-%m-%Y").date().isoformat()
    except Exception:
        date_ymd = dt.datetime.now(DK_TZ).date().isoformat()
    try:
        build_obs_storage_for_day(df, date_ymd=date_ymd, send_withdraw_push=True)
    except Exception as e:
        print(f"[Advarsel] Bygning af obs-lager fejlede: {e}", file=sys.stderr)

    state = load_state()

    # Bestem dagsmappe fra CLI-datoen
    def _date_ymd_dk(now: dt.datetime | None = None, reset_hour: int = 3) -> str:
        now = now or dt.datetime.now(DK_TZ)
        if now.hour < reset_hour:
            now = now - dt.timedelta(hours=reset_hour)
        return now.date().isoformat()

    def _cli_date_to_iso(date_str: Optional[str]) -> str:
        s = (date_str or "").strip()
        if s:
            try:
                return dt.datetime.strptime(s, "%d-%m-%Y").date().isoformat()
            except Exception:
                pass
        return dt.datetime.now(DK_TZ).date().isoformat()

    state = load_state()
    initial = len(state) == 0
    if initial:
        today_iso = dt.datetime.now(DK_TZ).date().isoformat()
        new_state: Dict[str, dict] = {}
        for gkey, gdf in df.groupby("group_key", dropna=False):
            r = _choose_latest_row(gdf)
            new_state[gkey] = {"date": today_iso, "antal": _parse_antal(r.get("Antal",""))}
        save_state(new_state)
        print(f"[Init] Lydløs baseline oprettet for {len(new_state)} grupper. (encoding={enc})")
        purge_old_batches(max_age_hours=24, verbose=False)
        return True

    new_state = detect_and_report_changes(df, state, clients, timestamp_mode)
    save_state(new_state)
    return True

def run_watch(initial_date_str: str, clients: List[dict], interval_sec: int, timestamp_mode: str) -> None:
    ensure_dirs()
    active_date = initial_date_str
    last_day = dt.datetime.now(DK_TZ).date()
    consecutive_failures = 0
    print(f"Starter overvågning hver {interval_sec} sek. for dato={active_date} …")
    
    while True:
        try:
            today = dt.datetime.now(DK_TZ).date()
            if today != last_day:
                clear_state_and_downloads()
                active_date = today.strftime("%d-%m-%Y")
                print(f"[Midnat] Ny dag {active_date} – state nulstillet og tidligere downloads slettet.")
                last_day = today
            
            success = run_once(active_date, clients, timestamp_mode=timestamp_mode)
            
            if success:
                if consecutive_failures > 0:
                    print(f"[Info] Forbindelse genoprettet efter {consecutive_failures} fejlede forsøg")
                consecutive_failures = 0
            else:
                consecutive_failures += 1
                if consecutive_failures == 1 or consecutive_failures % 10 == 0:
                    print(f"[Advarsel] Kunne ikke hente data (forsøg #{consecutive_failures})")
                    
        except Exception as e:
            print(f"[Fejl] {e}", file=sys.stderr)
            consecutive_failures += 1
            
        time.sleep(interval_sec)

def main():
    parser = argparse.ArgumentParser(
        description="Overvåg DOFbasens CSV pr. (Artnavn × Loknr) og lever klient-filtrerede output."
    )
    parser.add_argument("--date", "-d", default=dt.datetime.now(DK_TZ).strftime("%d-%m-%Y"),
                        help="Dato i format DD-MM-YYYY (default: i dag i DK-tid)")
    parser.add_argument("--watch", "-w", action="store_true", help="Kør i loop og hent/scan periodisk.")
    parser.add_argument("--interval", "-i", type=int, default=300, help="Interval i sekunder ved --watch (default: 300)")
    parser.add_argument("--timestamp", choices=["now", "obs"], default="now",
                        help="Tidsstempel i output: 'now' = ændringstidspunkt (DK), 'obs' = observationens tid (DK).")
    parser.add_argument("--config", help="YAML med klient-profiler og regler (fx clients.yaml).")
    args = parser.parse_args()

    # Genindlæs bemaerk (hvis man vil regenerere filer uden at genstarte hele processen)
    global BEMAERK_MAP
    try:
        BEMAERK_MAP = load_bemaerk_thresholds()
    except Exception as e:
        print(f"[Advarsel] Kunne ikke genindlæse bemaerk-lister: {e}", file=sys.stderr)

    clients = load_clients_config(args.config)
    if args.watch:
        run_watch(args.date, clients, args.interval, timestamp_mode=args.timestamp)
    else:
        run_once(args.date, clients, timestamp_mode=args.timestamp)

if __name__ == "__main__":
    main()

