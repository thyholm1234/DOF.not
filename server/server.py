# server.py
from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import time
import uuid
import mimetypes
from base64 import urlsafe_b64decode
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Dict, List, Tuple, Iterable, Set
from concurrent.futures import ThreadPoolExecutor, as_completed

from fastapi import FastAPI, HTTPException, Request, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pywebpush import WebPushException, webpush

mimetypes.add_type("application/manifest+json", ".webmanifest")

# ─────────────────────────── Stier & konfiguration ───────────────────────────
APP_DIR = Path(__file__).resolve().parent.parent
WEB_DIR = APP_DIR / "web"
DB_PATH = Path(__file__).resolve().parent / "subscriptions.db"

# VAPID-nøgler
VAPID_PUBLIC = os.getenv(
    "VAPID_PUBLIC",
    "BGKYkcfHBux7tUko6RVVAunqqqPJf3M4VIcyjiqsxJCy5i8IY2fwWbxj1Two49P72cwyazIquavH-vNPVsNilZQ",
)
VAPID_PRIVATE = os.getenv(
    "VAPID_PRIVATE",
    "_2Bgw807BRsVxl1VvXRKN4XEh_D4DLpEsz7gi2SnGoA",
)
VAPID_CLAIMS = {"sub": os.getenv("VAPID_SUB", "mailto:cvh.privat@gmail.com")}

# Parallelisering
PUSH_MAX_WORKERS = int(os.getenv("PUSH_MAX_WORKERS", "12"))

# Server-side filter toggle (L1 / hybrid)
SERVER_SIDE_FILTER = os.getenv("SERVER_SIDE_FILTER", "true").strip().lower() in ("1","true","yes","on")

# Hvis brugerens valg er "alle" (udvides til {"su","sub","alm"}), skal "bemaerk" så tælles med i overlap?
INCLUDE_BEM_IN_ALLE = os.getenv("INCLUDE_BEM_IN_ALLE", "true").strip().lower() in ("1","true","yes","on")

# “Seneste push” til forsiden
LATEST_FILE = WEB_DIR / "latest-push.json"
_latest_lock = Lock()

# Logger
logger = logging.getLogger("dofpush.server")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

app = FastAPI(title="DOF Web Push (lokal)", version="3.0")

# ───────────── PREFS: udvidelser af valg -> effektive kategorier ─────────────
# NB: 'alle' udvides her IKKE med 'bemaerk' for at bevare tidligere API-kontrakt,
# men i server-side filter kan vi vælge at tælle bemaerk med via INCLUDE_BEM_IN_ALLE
_PREFS_CATEGORY_MAP = {
    "none": set(),
    "su": {"su"},
    "sub": {"su", "sub"},
    "alle": {"su", "sub", "alm"},
}

def _prefs_expand_value(val: str) -> list[str]:
    return sorted(_PREFS_CATEGORY_MAP.get(str(val or "").strip().lower(), set()))

def _prefs_expand_all(prefs: dict) -> dict[str, list[str]]:
    if not isinstance(prefs, dict):
        return {}
    return {k: _prefs_expand_value(v) for k, v in prefs.items()}

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8000", "http://127.0.0.1:8000",
        "http://localhost:5173", "http://127.0.0.1:5173",
    ],
    allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

# ─────────── helper: flet evt. eksisterende headers med 'Urgency' ────────────
def _merge_push_headers(payload: dict, default_urgency: str = "high") -> dict:
    headers = {}
    src = payload.get("headers") if isinstance(payload, dict) else None
    if isinstance(src, dict):
        headers.update({str(k): str(v) for k, v in src.items()})
    headers.setdefault("Urgency", str(payload.get("urgency", default_urgency)))
    return headers

# ───────────────────────────────── DB-hjælpere ──────────────────────────────
def ensure_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH); cur = con.cursor()
    # Subscriptions
    cur.execute("""
    CREATE TABLE IF NOT EXISTS subs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      endpoint TEXT UNIQUE NOT NULL,
      p256dh TEXT NOT NULL,
      auth TEXT NOT NULL,
      user_id TEXT
    )""")
    # Users
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
      id TEXT PRIMARY KEY,
      created_at INTEGER NOT NULL,
      last_seen INTEGER NOT NULL
    )""")
    # User prefs
    cur.execute("""
    CREATE TABLE IF NOT EXISTS user_prefs (
      user_id TEXT PRIMARY KEY,
      prefs TEXT NOT NULL,
      ts INTEGER,
      FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    )""")
    con.commit(); con.close()

def add_subscription(sub: Dict) -> None:
    endpoint = sub.get("endpoint"); keys = sub.get("keys", {})
    p256dh = keys.get("p256dh"); auth = keys.get("auth")
    if not endpoint or not p256dh or not auth:
        raise ValueError("Subscription mangler endpoint/p256dh/auth")
    con = sqlite3.connect(DB_PATH)
    con.execute("INSERT OR REPLACE INTO subs(endpoint, p256dh, auth) VALUES (?, ?, ?)", (endpoint, p256dh, auth))
    con.commit(); con.close()

def all_subscriptions() -> List[Dict]:
    con = sqlite3.connect(DB_PATH)
    rows = con.execute("SELECT endpoint, p256dh, auth FROM subs").fetchall()
    con.close()
    return [{"endpoint": r[0], "keys": {"p256dh": r[1], "auth": r[2]}} for r in rows]

def _subs_with_user_ids() -> List[Dict]:
    """
    Returnér subscriptions inkl. user_id, så vi kan filtrere pr. bruger.
    """
    con = sqlite3.connect(DB_PATH)
    rows = con.execute("SELECT endpoint, p256dh, auth, COALESCE(user_id,'') FROM subs").fetchall()
    con.close()
    out = []
    for endpoint, p256dh, auth, uid in rows:
        out.append({"endpoint": endpoint, "keys": {"p256dh": p256dh, "auth": auth}, "user_id": uid or None})
    return out

def delete_subscription(endpoint: str) -> None:
    con = sqlite3.connect(DB_PATH)
    con.execute("DELETE FROM subs WHERE endpoint=?", (endpoint,))
    con.commit(); con.close()

def subscriptions_count() -> int:
    con = sqlite3.connect(DB_PATH)
    cnt = con.execute("SELECT COUNT(*) FROM subs").fetchone()[0]
    con.close(); return int(cnt)

# ───────────────────────────── VAPID-validering ──────────────────────────────
_B64URL_RE = re.compile(r"^[A-Za-z0-9\-\_]+$")

def _b64url_decode(s: str) -> bytes:
    if not s: raise ValueError("tom streng")
    if not _B64URL_RE.fullmatch(s): raise ValueError("ikke base64url-tegn")
    padding = "=" * ((4 - len(s) % 4) % 4)
    return urlsafe_b64decode(s + padding)

def validate_vapid_keys() -> Dict[str, object]:
    result = {"public_ok": False, "private_ok": False, "errors": []}
    try:
        pb = _b64url_decode(VAPID_PUBLIC)
        if len(pb) != 65 or pb[0] != 0x04: result["errors"].append("VAPID_PUBLIC: forventet 65 bytes")
        else: result["public_ok"] = True
    except Exception as e:
        result["errors"].append(f"VAPID_PUBLIC decode-fejl: {e}")
    try:
        pr = _b64url_decode(VAPID_PRIVATE)
        if len(pr) != 32: result["errors"].append("VAPID_PRIVATE: forventet 32 bytes")
        else: result["private_ok"] = True
    except Exception as e:
        result["errors"].append(f"VAPID_PRIVATE decode-fejl: {e}")
    result["valid"] = bool(result["public_ok"] and result["private_ok"])
    return result

# ───────────────────────────── Helpers (latest) ──────────────────────────────
def _write_latest(items: List[Dict], kind: str) -> None:
    WEB_DIR.mkdir(parents=True, exist_ok=True)
    doc = {
        "kind": kind, "count": len(items),
        "received_at": datetime.now(timezone.utc).astimezone().isoformat(),
        "items": items,
    }
    with _latest_lock:
        LATEST_FILE.write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")

def _safe_read_json(p: Path) -> dict:
    try: return json.loads(p.read_text(encoding="utf-8"))
    except Exception: return {}

def _enrich_payload_for_latest(payload: dict) -> dict:
    out = dict(payload or {}); url = out.get("url") or ""
    if isinstance(url, str) and url.startswith("/batches/"):
        batch_path = (WEB_DIR / url.lstrip("/")).resolve()
        if batch_path.is_file():
            data = _safe_read_json(batch_path); items = data.get("items") or []
            if items:
                it = items[0]
                antal = str(it.get("antal", "") or "").strip()
                art = str(it.get("art", "") or "").strip()
                lok = str(it.get("lok", "") or "").strip()
                fn = str(it.get("fornavn","") or "").strip()
                en = str(it.get("efternavn","") or "").strip()
                obsid = str(it.get("obsid","") or "").strip()
                t1 = " ".join(x for x in [antal, art] if x).strip()
                title = ", ".join(x for x in [t1, lok] if x)
                name_parts = [p for p in [fn, en] if p]; body = ", ".join(name_parts)
                if obsid:
                    out["url"] = f"https://dofbasen.dk/popobs.php?obsid={obsid}&summering=tur&obs=obs"
                if title: out["title"] = title
                if body: out["body"] = body
    return out

# ───────────────────────────── User helpers ──────────────────────────────────
VALID_PREF_VALUES = {"none", "su", "sub", "alle"}

def _normalize_prefs(prefs: dict) -> dict:
    if not isinstance(prefs, dict): return {}
    out = {}
    for k, v in prefs.items():
        val = str(v).strip().lower()
        if val not in VALID_PREF_VALUES:
            raise HTTPException(status_code=400, detail=f"Ugyldig værdi for '{k}': {v}")
        out[str(k)] = val
    return out

def _ensure_user(con, user_id: str):
    now = int(time.time() * 1000)
    row = con.execute("SELECT id FROM users WHERE id=?", (user_id,)).fetchone()
    if row: con.execute("UPDATE users SET last_seen=? WHERE id=?", (now, user_id))
    else:   con.execute("INSERT INTO users(id, created_at, last_seen) VALUES (?,?,?)",(user_id, now, now))

def _safe_lower(s: str) -> str:
    return str(s or "").strip().lower()

# ────────────────────── Server-side filter helpers (L1) ──────────────────────
def _load_all_user_prefs() -> dict[str, dict[str, Set[str]]]:
    """
    Læs alle brugerpræferencer fra DB og returnér:
      user_id -> { region_lower -> set(allowed_categories) }
    Bemærk: 'alle' udvides til {'su','sub','alm'} af _prefs_expand_all.
    I selve match kan vi vælge at inkludere 'bemaerk' når 'alm' findes,
    styret af INCLUDE_BEM_IN_ALLE.
    """
    con = sqlite3.connect(DB_PATH)
    rows = con.execute("SELECT user_id, prefs FROM user_prefs").fetchall()
    con.close()
    out: dict[str, dict[str, Set[str]]] = {}
    for uid, prefs_json in rows:
        try:
            raw = json.loads(prefs_json) if prefs_json else {}
        except Exception:
            raw = {}
        eff = _prefs_expand_all(raw)  # {"DOF København": ["su","sub"], ...}
        # normaliser nøgler til lower for case-insensitive match
        out[uid] = { _safe_lower(k): set(v or []) for k, v in eff.items() }
    return out

def _allowed_plus(allowed: Set[str]) -> Set[str]:
    """
    Hvis INCLUDE_BEM_IN_ALLE er sand og brugeren reelt har 'alle' (der i eff. set
    indeholder 'alm'), så medtag 'bemaerk' i overlap.
    """
    if not INCLUDE_BEM_IN_ALLE:
        return set(allowed)
    # heuristik: 'alle' udvides til su, sub, alm -> vi kan identificere den ved 'alm' i allowed
    if "alm" in allowed:
        return set(allowed) | {"bemaerk"}
    return set(allowed)

def _extract_regions_categories_from_batch_file(url_path: str) -> Tuple[Set[str], Set[str]]:
    """
    Læs web/batches/*.json og udled set af (regions, categories) i lower-case.
    """
    regions: Set[str] = set()
    cats: Set[str] = set()
    p = (WEB_DIR / url_path.lstrip("/")).resolve()
    try:
        data = _safe_read_json(p)
        for it in data.get("items", []) or []:
            reg = _safe_lower(it.get("dof_afdeling", ""))
            cat = _safe_lower(it.get("kategori", ""))
            if reg: regions.add(reg)
            if cat: cats.add(cat)
    except Exception:
        pass
    return regions, cats

def _load_batch_info_from_payload(payload: dict) -> Tuple[Set[str], Set[str]]:
    """
    Giv (regions, categories) for et payload:
      1) Hvis payload selv indeholder 'regions'/'categories', brug dem.
      2) Ellers hvis 'url' peger på /batches/*.json, læs filen og udled dem.
      3) Ellers tomme sæt (ukendt) -> filter kan vælge at tillade alle.
    """
    regions: Set[str] = set()
    cats: Set[str] = set()
    if not isinstance(payload, dict):
        return regions, cats

    # direkte medsendt
    for k in ("regions", "categories"):
        if k in payload and isinstance(payload[k], (list, tuple, set)):
            vals = { _safe_lower(x) for x in payload[k] if str(x).strip() }
            if k == "regions": regions |= vals
            else: cats |= vals

    # batchfil
    url = payload.get("url")
    if isinstance(url, str) and url.startswith("/batches/"):
        r2, c2 = _extract_regions_categories_from_batch_file(url)
        regions |= r2; cats |= c2

    return regions, cats

def _user_allows_categories_for_region(user_prefs: dict[str, dict[str, Set[str]]],
                                       user_id: str,
                                       region_lc: str,
                                       batch_cats: Set[str]) -> bool:
    """
    Returnér True hvis brugeren (user_id) for den givne region_lc har
    tilladte kategorier, der overlapper batch_cats. Medtag evt. 'bemaerk'
    når 'alle' er valgt (INCLUDE_BEM_IN_ALLE).
    """
    if not user_id:  # ingen bruger tilknyttet -> tillad
        return True
    regmap = user_prefs.get(user_id) or {}
    allowed = regmap.get(region_lc)
    if not allowed:
        return False
    allowed_expanded = _allowed_plus(allowed)
    return bool(allowed_expanded & batch_cats)

def _sub_allows_payload(sub: Dict, payload: dict, user_prefs: dict[str, dict[str, Set[str]]]) -> bool:
    """
    Afgør om en given subscription (med evt. user_id) skal have payloaden.
    For digests (url=/batches/...) baseret på overlap (region, kategori).
    """
    uid = sub.get("user_id")
    # Hvis der ikke er bruger knyttet, sender vi som før
    if not uid:
        return True

    regions, cats = _load_batch_info_from_payload(payload)
    # Hvis vi ikke kan udlede noget, lad den passere (fail-open)
    if not regions and not cats:
        return True

    # Hvis ingen kategorier er udledt fra batchen, antag alle fire (defensivt)
    if not cats:
        cats = {"su", "sub", "alm", "bemaerk"}

    # Tillad hvis mindst én batch-region overlapper med brugerens allowed kategorier
    for reg in regions:
        if _user_allows_categories_for_region(user_prefs, uid, reg, cats):
            return True
    return False

# ───────────────────────────────── Middleware ────────────────────────────────
@app.middleware("http")
async def log_requests(request: Request, call_next):
    req_id = uuid.uuid4().hex[:8]; start = time.perf_counter()
    client = request.client.host if request.client else "-"
    clen = request.headers.get("content-length")
    try:
        response = await call_next(request); dur_ms = (time.perf_counter()-start)*1000
        logger.info("[API %s] %s %s -> %s (%.1f ms) size=%sB ip=%s",
                    req_id, request.method, request.url.path, response.status_code, dur_ms, clen or "?", client)
        return response
    except Exception as e:
        dur_ms = (time.perf_counter()-start)*1000
        logger.exception("[API %s] %s %s FAILED (%.1f ms) size=%sB ip=%s err=%s",
                         req_id, request.method, request.url.path, dur_ms, clen or "?", client, e)
        raise

# ───────────────────────────────────── API ───────────────────────────────────
@app.on_event("startup")
def _on_startup():
    ensure_db()
    info = validate_vapid_keys()
    if info["valid"]:
        logger.info("VAPID-nøgler OK. Sub: %s", VAPID_CLAIMS.get("sub"))
    else:
        logger.warning("VAPID-nøgler ugyldige: %s", info["errors"])
    logger.info("Web dir: %s · DB: %s", WEB_DIR, DB_PATH)
    logger.info("SERVER_SIDE_FILTER=%s · INCLUDE_BEM_IN_ALLE=%s · WORKERS=%d",
                SERVER_SIDE_FILTER, INCLUDE_BEM_IN_ALLE, PUSH_MAX_WORKERS)

@app.get("/ping")
def ping(): return {"pong": True}

@app.get("/vapid-public-key")
def vapid_public_key():
    info = validate_vapid_keys()
    if not VAPID_PUBLIC:
        logger.warning("[VAPID] PUBLIC mangler (env VAPID_PUBLIC)")
        return JSONResponse({"publicKey": "", "valid": False, "details": info})
    logger.info("[VAPID] valid=%s public.len=%d private=%s",
                info["valid"], len(VAPID_PUBLIC), "OK" if info["private_ok"] else "NOK")
    return JSONResponse({"publicKey": VAPID_PUBLIC, "valid": info["valid"], "details": info})

@app.get("/latest")
def latest():
    if not LATEST_FILE.exists(): return JSONResponse(status_code=204, content=None)
    try:
        data = json.loads(LATEST_FILE.read_text(encoding="utf-8")); return JSONResponse(data)
    except Exception:
        return JSONResponse(status_code=204, content=None)

# ─────── GLOBAL /api/prefs ER UDFASET (returnerer 410 Gone) ───────
@app.get("/api/prefs")
def get_prefs_deprecated():
    raise HTTPException(status_code=410, detail="Global prefs er udfaset – brug /api/prefs/user")

@app.post("/api/prefs")
async def save_prefs_deprecated(req: Request):
    raise HTTPException(status_code=410, detail="Global prefs er udfaset – brug /api/prefs/user")

# ──────────────────────── PWA filer (SW og manifest) ────────────────────────
@app.get("/sw.js")
def serve_sw():
    path = WEB_DIR / "sw.js"
    if not path.exists():
        raise HTTPException(status_code=404, detail="sw.js mangler i /web")
    # Undgå hård caching af service worker
    return FileResponse(path, media_type="application/javascript",
                        headers={"Cache-Control": "no-cache"})

@app.get("/manifest.webmanifest")
def serve_manifest():
    path = WEB_DIR / "manifest.webmanifest"
    if not path.exists():
        raise HTTPException(status_code=404, detail="manifest.webmanifest mangler i /web")
    # Manifest må gerne caches lidt
    return FileResponse(path, media_type="application/manifest+json",
                        headers={"Cache-Control": "public, max-age=3600, immutable"})

# ─────────────── NYT: bruger-initialisering og prefs pr. bruger ─────────────
@app.post("/api/user/init")
async def user_init(req: Request):
    data = await req.json()
    user_id = str(data.get("user_id") or "").strip()
    if not user_id: raise HTTPException(status_code=400, detail="user_id mangler")
    con = sqlite3.connect(DB_PATH); _ensure_user(con, user_id); con.commit(); con.close()
    return {"ok": True, "user_id": user_id}

@app.get("/api/prefs/user")
def get_user_prefs(user_id: str = Query(..., min_length=8)):
    con = sqlite3.connect(DB_PATH)
    row = con.execute("SELECT prefs, ts FROM user_prefs WHERE user_id=?", (user_id,)).fetchone()
    con.close()
    if not row: return JSONResponse({"prefs": {}, "ts": None, "source": "user-none"})
    try: prefs = json.loads(row[0]) if row[0] else {}
    except Exception: prefs = {}
    return JSONResponse({"prefs": prefs, "ts": row[1], "source": "user"})

@app.post("/api/prefs/user")
async def save_user_prefs(req: Request):
    data = await req.json()
    user_id = str(data.get("user_id") or "").strip()
    if not user_id: raise HTTPException(status_code=400, detail="user_id mangler")
    prefs = _normalize_prefs(data.get("prefs", {}))
    ts = int(data.get("ts") or 0) or int(time.time() * 1000)
    con = sqlite3.connect(DB_PATH); _ensure_user(con, user_id)
    con.execute(
        "INSERT INTO user_prefs(user_id, prefs, ts) VALUES (?,?,?) "
        "ON CONFLICT(user_id) DO UPDATE SET prefs=excluded.prefs, ts=excluded.ts",
        (user_id, json.dumps(prefs, ensure_ascii=False), ts),
    )
    con.commit(); con.close()
    return {"ok": True, "prefs": prefs, "effective": _prefs_expand_all(prefs), "ts": ts}

@app.post("/api/subscribe")
async def subscribe(req: Request):
    data = await req.json()
    sub = data; user_id = str(data.get("user_id") or "").strip() or None
    try:
        add_subscription(sub)
        if user_id:
            con = sqlite3.connect(DB_PATH); _ensure_user(con, user_id)
            con.execute("UPDATE subs SET user_id=? WHERE endpoint=?", (user_id, sub.get("endpoint")))
            con.commit(); con.close()
        endpoint = sub.get("endpoint",""); short = (endpoint[:64] + "...") if len(endpoint) > 67 else endpoint
        total = subscriptions_count()
        logger.info("[SUB] Gemt subscription: %s · total=%d · user=%s", short, total, user_id or "-")
        return {"ok": True, "total": total}
    except Exception as e:
        logger.warning("[SUB] Ugyldig subscription: %s", e)
        raise HTTPException(status_code=400, detail=f"Ugyldig subscription: {e}")

# ──────────────── Parallel baggrundsafsendelse (med L1-filter) ───────────────
def _send_one_push(sub: Dict, payload: dict, ttl: int, headers: dict) -> str:
    """Afsend én webpush til en subscription (køres i trådpool)."""
    try:
        webpush(
            subscription_info=sub,
            data=json.dumps(payload),
            vapid_private_key=VAPID_PRIVATE,
            vapid_claims=VAPID_CLAIMS,
            ttl=ttl,
            headers=headers,
        )
        return "sent"
    except WebPushException as ex:
        status = getattr(ex, "response", None).status_code if getattr(ex, "response", None) else None
        if status in (404, 410):
            delete_subscription(sub["endpoint"])
            return "deleted"
        return "error"
    except Exception:
        return "error"

def _send_pushes_parallel(payload: dict) -> None:
    """Send push til alle (eller filtrerede) subscriptions parallelt i en trådpool."""
    if SERVER_SIDE_FILTER:
        subs = _subs_with_user_ids()
    else:
        subs = all_subscriptions()

    if not subs:
        logger.info("[PUSH/bg] Ingen subscriptions (skip).")
        return

    ttl = int(payload.get("ttl") or 86400)
    headers = _merge_push_headers(payload, default_urgency="high")

    # Server-side filter (L1): filtrér pr. bruger baseret på batchens region/kategori
    if SERVER_SIDE_FILTER:
        t0f = time.perf_counter()
        user_prefs = _load_all_user_prefs()
        before = len(subs)
        subs = [s for s in subs if _sub_allows_payload(s, payload, user_prefs)]
        durf_ms = (time.perf_counter() - t0f) * 1000
        logger.info("[PUSH/filter] subs=%d -> %d (%.1f ms) payload.url=%s",
                    before, len(subs), durf_ms, payload.get("url"))

    if not subs:
        logger.info("[PUSH/bg] Ingen modtagere efter server-side filter.")
        return

    t0 = time.perf_counter()
    acc = {"sent": 0, "deleted": 0, "errors": 0}
    with ThreadPoolExecutor(max_workers=PUSH_MAX_WORKERS) as pool:
        futures = [pool.submit(_send_one_push, sub, payload, ttl, headers) for sub in subs]
        for fut in as_completed(futures):
            res = fut.result()
            if res in acc: acc[res] += 1
    dur_ms = (time.perf_counter() - t0) * 1000
    logger.info("[PUSH/bg] subs=%d -> sent=%d deleted=%d errors=%d (%.1f ms) ttl=%s urgency=%s workers=%d",
                len(subs), acc["sent"], acc["deleted"], acc["errors"], dur_ms, ttl, headers.get("Urgency"), PUSH_MAX_WORKERS)

@app.post("/api/publish")
async def publish(req: Request, background_tasks: BackgroundTasks):
    payload = await req.json()
    check = validate_vapid_keys()
    if not check["valid"]:
        logger.error("[PUSH] VAPID ugyldig: %s", check["errors"])
        raise HTTPException(status_code=500, detail={"msg": "VAPID-nøgler er ikke gyldige", "details": check})
    try:
        _write_latest([_enrich_payload_for_latest(payload)], kind="single")
    except Exception:
        logger.warning("[PUSH] Kunne ikke gemme latest-push.json")
    # Start afsendelse i baggrunden og svar straks
    background_tasks.add_task(_send_pushes_parallel, payload)
    return {"accepted": True, "count_subscriptions": subscriptions_count(), "workers": PUSH_MAX_WORKERS,
            "server_side_filter": SERVER_SIDE_FILTER}

# ───────────────────────────── /api/publish-batch ────────────────────────────
def _send_batch_to_sub(sub: Dict, payloads: list, ttl: int, headers: dict) -> tuple[int, int]:
    """Send hele batchen (sekventielt) til én subscription. Returnér (sent, errors)."""
    sent = err = 0
    for item in payloads:
        try:
            webpush(
                subscription_info=sub,
                data=json.dumps(item),
                vapid_private_key=VAPID_PRIVATE,
                vapid_claims=VAPID_CLAIMS,
                ttl=ttl,
                headers=headers,
            )
            sent += 1
        except WebPushException as ex:
            status = getattr(ex, "response", None).status_code if getattr(ex, "response", None) else None
            if status in (404, 410):
                delete_subscription(sub["endpoint"])
            else:
                err += 1
        except Exception:
            err += 1
    return sent, err

def _send_batch_parallel(payloads: list) -> None:
    if SERVER_SIDE_FILTER:
        subs = _subs_with_user_ids()
    else:
        subs = all_subscriptions()

    if not subs:
        logger.info("[BATCH/bg] Ingen subscriptions (skip).")
        return

    first = payloads[0] if payloads else {}
    ttl = int((first or {}).get("ttl") or 86400)
    headers = _merge_push_headers(first or {}, default_urgency="high")

    # Server-side filter (L1): behold kun subs der tillades af *mindst ét* payload i batchen
    if SERVER_SIDE_FILTER:
        t0f = time.perf_counter()
        user_prefs = _load_all_user_prefs()
        before = len(subs)
        def _allows_any(sub):
            for p in payloads:
                if _sub_allows_payload(sub, p, user_prefs):
                    return True
            return False
        subs = [s for s in subs if _allows_any(s)]
        durf_ms = (time.perf_counter() - t0f) * 1000
        logger.info("[BATCH/filter] subs=%d -> %d (%.1f ms) payloads=%d",
                    before, len(subs), durf_ms, len(payloads))

    if not subs:
        logger.info("[BATCH/bg] Ingen modtagere efter server-side filter.")
        return

    t0 = time.perf_counter()
    sent_total = err_total = 0
    with ThreadPoolExecutor(max_workers=PUSH_MAX_WORKERS) as pool:
        futures = {pool.submit(_send_batch_to_sub, sub, payloads, ttl, headers): sub for sub in subs}
        for fut in as_completed(futures):
            try:
                s, e = fut.result()
                sent_total += s; err_total += e
            except Exception:
                err_total += 1
    dur_ms = (time.perf_counter() - t0) * 1000
    logger.info("[BATCH/bg] payloads=%d subs=%d -> sent=%d errors=%d (%.1f ms) ttl=%s urgency=%s workers=%d",
                len(payloads), len(subs), sent_total, err_total, dur_ms, ttl, headers.get("Urgency"), PUSH_MAX_WORKERS)

@app.post("/api/publish-batch")
async def publish_batch(req: Request, background_tasks: BackgroundTasks):
    try:
        payloads = await req.json()
        if not isinstance(payloads, list):
            raise HTTPException(status_code=400, detail="Body skal være en liste af objekter")
        if not payloads:
            logger.info("[BATCH] Tom liste -> intet sendt")
            _write_latest([], kind="batch")
            return {"accepted": True, "count_payloads": 0, "count_subscriptions": subscriptions_count(),
                    "workers": PUSH_MAX_WORKERS, "server_side_filter": SERVER_SIDE_FILTER}
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=400, detail="Ugyldig JSON body")

    check = validate_vapid_keys()
    if not check["valid"]:
        logger.error("[BATCH] VAPID ugyldig: %s", check["errors"])
        raise HTTPException(status_code=500, detail={"msg": "VAPID-nøgler er ikke gyldige", "details": check})

    _write_latest(payloads, kind="batch")
    background_tasks.add_task(_send_batch_parallel, payloads)
    return {"accepted": True, "count_payloads": len(payloads), "count_subscriptions": subscriptions_count(),
            "workers": PUSH_MAX_WORKERS, "server_side_filter": SERVER_SIDE_FILTER}

@app.post("/api/publish-latest")
def publish_latest():
    try:
        if not LATEST_FILE.exists(): raise HTTPException(status_code=404, detail="latest-push.json mangler")
        data = json.loads(LATEST_FILE.read_text(encoding="utf-8"))
        items = data.get("items", [])
        if not items: raise HTTPException(status_code=400, detail="Ingen items i latest-push.json")
        payload = items[0]
        subs = _subs_with_user_ids() if SERVER_SIDE_FILTER else all_subscriptions()
        if not subs: return {"sent": 0, "deleted": 0, "errors": 0, "count_subscriptions": 0}

        # L1-filter her også (enkeltpayload)
        if SERVER_SIDE_FILTER:
            user_prefs = _load_all_user_prefs()
            before = len(subs)
            subs = [s for s in subs if _sub_allows_payload(s, payload, user_prefs)]
            logger.info("[LATEST/filter] subs=%d -> %d url=%s", before, len(subs), payload.get("url"))

        results = {"sent": 0, "deleted": 0, "errors": 0}
        for sub in subs:
            try:
                webpush(subscription_info=sub, data=json.dumps(payload),
                        vapid_private_key=VAPID_PRIVATE, vapid_claims=VAPID_CLAIMS)
                results["sent"] += 1
            except WebPushException as ex:
                status = getattr(ex, "response", None).status_code if getattr(ex, "response", None) else None
                if status in (404, 410): delete_subscription(sub["endpoint"]); results["deleted"] += 1
                else: results["errors"] += 1
            except Exception:
                results["errors"] += 1
        results.update({"count_subscriptions": len(subs)})
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fejl: {e}")

# ───────────────────────────────── Statiske filer ────────────────────────────
@app.get("/")
def index():
    index_path = WEB_DIR / "index.html"
    if not index_path.exists(): raise HTTPException(status_code=404, detail="index.html mangler i /web")
    return FileResponse(index_path)

app.mount("/", StaticFiles(directory=WEB_DIR, html=True), name="web")