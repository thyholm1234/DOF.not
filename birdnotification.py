# birdnotification.py
# -*- coding: utf-8 -*-
"""
Henter DOFbasens CSV for en valgt dato (DD-MM-YYYY), gemmer den,
detekterer første observation pr. (Artnavn × Loknr) pr. dag (DK-tid)
og leverer resultater pr. klient-profil via stdout/fil/webpush.

Denne version:
- CLI-output er kompakt: én linje pr. observation (uden noter).
- [alm|sub|su] vises i CLI og medsendes pr. item i batch-JSON.
- Webpush/digest uændret.
- Per-klient filtrering bevares (species, adfærd, lokation, tid, bbox, antal, kategori).
- Ændringer fra alle grupper sendes samlet pr. polling-runde.
"""

import re  # ← til robust parsning af 'Antal'
import argparse
import datetime as dt
import json
import sys
import time
import warnings
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional

import pandas as pd
import requests
import yaml
from zoneinfo import ZoneInfo

# --- Konstanter og stier ---
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

REQUIRED_COLS = [
    "Artnavn", "Loknr", "Adfbeskrivelse", "Loknavn",
    "Fornavn", "Efternavn", "Obsid",
    "obs_laengdegrad", "obs_breddegrad",
    "lok_laengdegrad", "lok_breddegrad",
]

# --- Utility ---
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

def fetch_csv(url: str) -> Path:
    ts = dt.datetime.now(DK_TZ).strftime("%Y%m%d_%H%M%S")
    dest = DL_DIR / f"search_result_{ts}.csv"
    headers = {"User-Agent": "birdnotification/1.4 (+local)"}
    try:
        with requests.get(url, headers=headers, timeout=60) as r:
            r.raise_for_status()
            dest.write_bytes(r.content)
    except requests.RequestException as e:
        raise SystemExit(f"Kunne ikke hente CSV: {e}") from e
    return dest

def _trim_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    # DataFrame har ikke .map – brug applymap for robust trim
    return df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

def _get_series(df: pd.DataFrame, col: str) -> pd.Series:
    return df[col] if col in df.columns else pd.Series([""] * len(df), index=df.index)

def _inject_time_fallbacks(df: pd.DataFrame) -> pd.DataFrame:
    s_obstidfra = _get_series(df, "Obstidfra")
    s_turtidfra = _get_series(df, "Turtidfra")
    s_obstidtil = _get_series(df, "Obstidtil")
    s_turtidtil = _get_series(df, "Turtidtil")
    df["Obstidfra"] = s_obstidfra.where(s_obstidfra != "", s_turtidfra)
    df["Obstidtil"] = s_obstidtil.where(s_obstidtil != "", s_turtidtil)
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

# Ny state-struktur: dict[group_key] = {"date": "YYYY-MM-DD"|None, "antal": float|None}
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
                # legacy (obsid-lister/sets) -> migrér til "allerede notificeret i dag"
                state[k] = {"date": today_iso, "antal": None}
        return state
    except Exception:
        return {}

def save_state(state: Dict[str, dict]):
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

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

# Hjælpere til antal og "seneste" række
def _parse_antal(val):
    """
    Tolerant parsning:
    - '1+' -> 1
    - 'ca 5', 'ca. 5', '~5' -> 5
    - '2-3' -> 3 (vælg max i interval)
    - '10,5' -> 10.5
    Returnerer None hvis ingen tal kan udtrækkes.
    """
    if val is None:
        return None
    s = str(val).strip().lower().replace(",", ".")
    if not s:
        return None
    # fjern bløde præfikser/tegn
    s = s.replace("ca.", "").replace("ca ", "").replace("~", "")
    # find alle tal (inkl. decimaltal) og vælg det største som "antal"
    nums = re.findall(r"\d+(?:\.\d+)?", s)
    if not nums:
        return None
    return max(float(x) for x in nums)

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
                "kategori": art_kategori(r.get("Artnavn","")),
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
    META_FILE.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

def _write_latest_push(payload: dict) -> None:
    """Skriv seneste push/digest payload til web/latest-push.json (debug/diagnose)."""
    try:
        LATEST_PUSH_FILE.parent.mkdir(parents=True, exist_ok=True)
        payload = dict(payload)  # kopi
        payload["generated"] = dt.datetime.now(DK_TZ).isoformat()
        # Hold den lille
        LATEST_PUSH_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        print(f"[Advarsel] Kunne ikke skrive latest-push.json: {e}", file=sys.stderr)
def _update_meta(rows_c: pd.DataFrame) -> None:
    """
    Opdater web/meta.json med nye DOF-afdelinger (unikke) og timestamp for seneste opdatering.
    Struktur: { "afdelinger": [...], "lastUpdated": "<ISO>" }
    """
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

    # Saml nye afdelinger fra rows_c
    existing = set(meta.get("afdelinger", []))
    new_vals = set(v for v in rows_c.get("DOF_afdeling", pd.Series(dtype=str)).astype(str).tolist() if v)
    all_vals = sorted(existing | new_vals)
    meta["afdelinger"] = all_vals
    meta["lastUpdated"] = now_iso


# --- SU/SUB-lister og klassifikation ---
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

def art_kategori(artnavn: str) -> str:
    a = (artnavn or "").strip()
    if not a:
        return "alm"
    if a in SU_SET:
        return "su"
    if a in SUB_SET:
        return "sub"
    return "alm"

# --- Klient-config & filtrering ---
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
      kategori ["alm","sub","su"]
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
        s_cat = df["kategori"] if "kategori" in df.columns else df["Artnavn"].astype(str).map(art_kategori)
        m &= s_cat.isin(cats_norm)

    return m

# --- Kompakt CLI-line builder ---
def render_output_line(r: pd.Series, timestamp_mode: str, kategori: str) -> str:
    """
    Kompakt én-linje: [client] [kategori] TS · Antal Art · Adfærd · Loknavn · DOF-afdeling · Navn · lon, lat · tid
    """
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

# --- Fanout (stdout/file + webpush) ---
def _write_digest_file(cid: str, rows_c: pd.DataFrame) -> str:
    batch_dir = Path("web") / "batches"
    batch_dir.mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.now(DK_TZ).strftime("%Y%m%d%H%M%S")
    fname = f"batch-{ts}-{cid}.json"

    items = []
    for _, r in rows_c.iterrows():
        cat = art_kategori(r.get("Artnavn", ""))  # kategori pr. item
        items.append({
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
            "kategori": cat
        })

    payload = {
        "client": cid,
        "count": len(items),
        "generated": dt.datetime.now(DK_TZ).isoformat(),
        "items": items
    }
    (batch_dir / fname).write_text(json.dumps(payload, ensure_ascii=False, indent=0), encoding="utf-8")
    return f"/batches/{fname}"

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
    import requests  # lokal import for at matche tidligere struktur

    for c in clients:
        cid = c.get("id", "default")
        rules = c.get("rules", {}) or {}
        sinks = c.get("sinks", [{"type": "stdout"}])

        # Filtrér rækker for denne klient
        rows_c = new_rows[build_mask(new_rows, rules)]
        if rows_c.empty:
            continue

        # FEED + META (efter rows_c er fastlagt)
        _append_to_feed(cid, rows_c)    # skriver til web/feed.jsonl (JSON Lines)
        _update_meta(rows_c)            # opdaterer web/meta.json (afdelinger + timestamp)

        # --- STDOUT + FILE (kompakt linje) ---
        for _, r in rows_c.iterrows():
            cat = art_kategori(r.get("Artnavn", ""))
            line = render_output_line(r, timestamp_mode, cat)
            out = f"[{cid}] {line}"

            # stdout-sinks
            for s in (s for s in sinks if s.get("type", "stdout").lower() == "stdout"):
                print(out)

            # file-sinks (append til path)
            for s in (s for s in sinks if s.get("type", "file").lower() == "file"):
                path = Path(s.get("path") or (OUT_DIR / f"{cid}.log"))
                path.parent.mkdir(parents=True, exist_ok=True)
                with path.open("a", encoding="utf-8") as f:
                    f.write(out + "\n")

        # --- WEBPUSH (digest pr. batch) ---
        wps = [s for s in sinks if s.get("type", "").lower() == "webpush"]
        if wps:
            CHUNK = int(wps[0].get("chunk_size", 100))
            api = wps[0].get("url", "http://localhost:8000/api/publish")

            for start in range(0, len(rows_c), CHUNK):
                chunk = rows_c.iloc[start:start + CHUNK]

                # skriv batchfil og få web-stien
                url_path = _write_digest_file(cid, chunk)

                # kort opsummering til notifikations-body
                n = len(chunk)
                head = ", ".join(
                    f"{r.get('Antal','')} {r.get('Artnavn','')}".strip()
                    for _, r in chunk.head(3).iterrows()
                )
                more = f" … +{n-3} flere" if n > 3 else ""

                payload = {
                    "type": "digest",
                    "title": f"[{cid}] {n} nye obs",
                    "body": (head + more).strip(" ,·"),
                    "url": url_path,
                    "tag": f"bird-digest-{cid}",  # samme tag => notifikationer kollapser
                    "renotify": True
                }

                # DEBUG: skriv seneste push til web/latest-push.json (sidste chunk 'vinder')
                _write_latest_push({
                    "client": cid,
                    "title": payload["title"],
                    "body": payload["body"],
                    "url": payload["url"],
                    "count": n,
                    "tag": payload["tag"]
                })

                try:
                    requests.post(api, json=payload, timeout=15)
                except Exception as e:
                    print(f"[webpush] POST {api} fejlede: {e}", file=sys.stderr)

# --- Ændringsdetektion (loknr/dag) ---
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

    # Sikr kategori-kolonne
    if "kategori" not in df.columns:
        df = df.copy()
        df["kategori"] = df["Artnavn"].astype(str).map(art_kategori)

    # Gennemgå grupper (Artnavn × Loknr)
    for gkey, gdf in df.groupby("group_key", dropna=False):
        rec = updated_state.get(gkey, {"date": None, "antal": None})
        last_date = rec.get("date")
        last_antal = rec.get("antal")

        r = _choose_latest_row(gdf)
        a_new = _parse_antal(r.get("Antal", ""))

        # Første gang i dag → altid notificér
        if last_date != today_iso:
            batches.append(r)
            updated_state[gkey] = {"date": today_iso, "antal": a_new}
            continue

        # Samme dag: antal-stigning → notificér
        if a_new is not None and last_antal is not None and a_new > last_antal:
            # lille CLI-hint for tydelig "stigning opdaget"
            try:
                art = (r.get("Artnavn", "") or "").strip()
                lok = (r.get("Loknavn", "") or "").strip()
                print(f"[Δ] {art} @ {lok}: {last_antal} → {a_new}")
            except Exception:
                pass

            batches.append(r)
            updated_state[gkey]["antal"] = a_new
            continue

        # Hvis last_antal mangler, men vi nu kan aflæse a_new, så opdatér state lydløst
        if last_antal is None and a_new is not None:
            updated_state[gkey]["antal"] = a_new

    # Fan-out hvis der er noget nyt
    if batches:
        all_new = pd.DataFrame(batches)
        sort_cols = [c for c in ["Obstidfra", "Obsid"] if c in all_new.columns]
        if sort_cols:
            all_new = all_new.sort_values(sort_cols, kind="stable")
        fanout_to_clients(all_new, clients, timestamp_mode)

    return updated_state
# --- Kørsel ---
def run_once(date_str: str, clients: List[dict], timestamp_mode: str) -> None:
    ensure_dirs()
    url = build_url(date_str)
    csv_path = fetch_csv(url)

    # Slet den hentede CSV efter parsing, også ved fejl (finally)
    try:
        df, enc = read_csv_with_fallback(csv_path)
    finally:
        try:
            csv_path.unlink()
        except FileNotFoundError:
            pass
        except Exception as e:
            print(f"[Advarsel] Kunne ikke slette downloadet CSV: {csv_path.name} ({e})", file=sys.stderr)

    state = load_state()
    initial = len(state) == 0
    if initial:
        # Lydløs baseline: marker alle nuværende grupper som notificeret i dag
        today_iso = dt.datetime.now(DK_TZ).date().isoformat()
        new_state: Dict[str, dict] = {}
        for gkey, gdf in df.groupby("group_key", dropna=False):
            r = _choose_latest_row(gdf)
            new_state[gkey] = {"date": today_iso, "antal": _parse_antal(r.get("Antal",""))}
        save_state(new_state)
        print(f"[Init] Lydløs baseline oprettet for {len(new_state)} grupper. (encoding={enc})")
        return

    new_state = detect_and_report_changes(df, state, clients, timestamp_mode)
    save_state(new_state)
def run_watch(initial_date_str: str, clients: List[dict], interval_sec: int, timestamp_mode: str) -> None:
    ensure_dirs()
    active_date = initial_date_str
    last_day = dt.datetime.now(DK_TZ).date()
    print(f"Starter overvågning hver {interval_sec} sek. for dato={active_date} …")
    while True:
        try:
            today = dt.datetime.now(DK_TZ).date()
            if today != last_day:
                clear_state_and_downloads()
                active_date = today.strftime("%d-%m-%Y")
                print(f"[Midnat] Ny dag {active_date} – state nulstillet og tidligere downloads slettet.")
                last_day = today
            run_once(active_date, clients, timestamp_mode=timestamp_mode)
        except Exception as e:
            # Udvidet fejllogning kan tilføjes her ved behov
            print(f"[Fejl] {e}", file=sys.stderr)
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

    clients = load_clients_config(args.config)
    if args.watch:
        run_watch(args.date, clients, args.interval, timestamp_mode=args.timestamp)
    else:
        run_once(args.date, clients, timestamp_mode=args.timestamp)

if __name__ == "__main__":
    main()
