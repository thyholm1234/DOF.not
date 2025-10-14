# server.py
from __future__ import annotations
import json
import logging
import os
import re
import sqlite3
import time
import uuid
from base64 import urlsafe_b64decode
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Dict, List

from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pywebpush import WebPushException, webpush

# ────────────────────────────── Stier & konfiguration ─────────────────────────
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

# "Seneste push" til forsiden
LATEST_FILE = WEB_DIR / "latest-push.json"
_latest_lock = Lock()

# Logger
logger = logging.getLogger("dofpush.server")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

app = FastAPI(title="DOF Web Push (lokal)", version="2.3")

# --- PREFS: udvidelser af valg -> effektive kategorier ---
_PREFS_CATEGORY_MAP = {
    "none": set(),
    "su": {"su"},
    "sub": {"su", "sub"},
    "alle": {"su", "sub", "alm"},
}
def _prefs_expand_value(val: str) -> list[str]:
    return sorted(_PREFS_CATEGORY_MAP.get(str(val or "").strip().lower(), set()))
def _prefs_expand_all(prefs: dict) -> dict[str, list[str]]:
    if not isinstance(prefs, dict): return {}
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


# --- helper: flet evt. eksisterende headers med 'Urgency' ---
def _merge_push_headers(payload: dict, default_urgency: str = "high") -> dict:
    headers = {}
    # Hvis afsenderen allerede har leveret headers, brug dem som base
    src = payload.get("headers") if isinstance(payload, dict) else None
    if isinstance(src, dict):
        headers.update({str(k): str(v) for k, v in src.items()})
    # Sæt 'Urgency' hvis ikke sat
    headers.setdefault("Urgency", str(payload.get("urgency", default_urgency)))
    return headers


# ────────────────────────────── DB-hjælpere ──────────────────────────────────
def ensure_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH); cur = con.cursor()

    # Subscriptions
    cur.execute("""
      CREATE TABLE IF NOT EXISTS subs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        endpoint TEXT UNIQUE NOT NULL,
        p256dh TEXT NOT NULL,
        auth    TEXT NOT NULL
      )
    """)

    # Users
    cur.execute("""
      CREATE TABLE IF NOT EXISTS users (
        id         TEXT PRIMARY KEY,
        created_at INTEGER NOT NULL,
        last_seen  INTEGER NOT NULL
      )
    """)

    # User prefs
    cur.execute("""
      CREATE TABLE IF NOT EXISTS user_prefs (
        user_id TEXT PRIMARY KEY,
        prefs   TEXT NOT NULL,
        ts      INTEGER,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
      )
    """)

    # Migration: subs.user_id
    cols = {r[1] for r in cur.execute("PRAGMA table_info(subs)").fetchall()}
    if "user_id" not in cols:
        cur.execute("ALTER TABLE subs ADD COLUMN user_id TEXT")

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

def delete_subscription(endpoint: str) -> None:
    con = sqlite3.connect(DB_PATH)
    con.execute("DELETE FROM subs WHERE endpoint=?", (endpoint,))
    con.commit(); con.close()

def subscriptions_count() -> int:
    con = sqlite3.connect(DB_PATH)
    cnt = con.execute("SELECT COUNT(*) FROM subs").fetchone()[0]
    con.close(); return int(cnt)

# ────────────────────────────── VAPID-validering ─────────────────────────────
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
        if len(pb) != 65 or pb[0] != 0x04: result["errors"].append("VAPID_PUBLIC: forventet 65 bytes"); 
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

# ────────────────────────────── Helpers (latest) ─────────────────────────────
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
                if body:  out["body"]  = body
    return out

# ────────────────────────────── User helpers ──────────────────────────────────
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

# ────────────────────────────── Middleware ────────────────────────────────────
@app.middleware("http")
async def log_requests(request: Request, call_next):
    req_id = uuid.uuid4().hex[:8]; start = time.perf_counter()
    client = request.client.host if request.client else "-"; clen = request.headers.get("content-length")
    try:
        response = await call_next(request); dur_ms = (time.perf_counter()-start)*1000
        logger.info("[API %s] %s %s -> %s (%.1f ms) size=%sB ip=%s", req_id, request.method, request.url.path, response.status_code, dur_ms, clen or "?", client)
        return response
    except Exception as e:
        dur_ms = (time.perf_counter()-start)*1000
        logger.exception("[API %s] %s %s FAILED (%.1f ms) size=%sB ip=%s err=%s", req_id, request.method, request.url.path, dur_ms, clen or "?", client, e)
        raise

# ────────────────────────────── API ──────────────────────────────────────────
@app.on_event("startup")
def _on_startup():
    ensure_db()
    info = validate_vapid_keys()
    if info["valid"]:
        logger.info("VAPID-nøgler OK. Sub: %s", VAPID_CLAIMS.get("sub"))
    else:
        logger.warning("VAPID-nøgler ugyldige: %s", info["errors"])
    logger.info("Web dir: %s · DB: %s", WEB_DIR, DB_PATH)

@app.get("/ping")
def ping(): return {"pong": True}

@app.get("/vapid-public-key")
def vapid_public_key():
    info = validate_vapid_keys()
    if not VAPID_PUBLIC:
        logger.warning("[VAPID] PUBLIC mangler (env VAPID_PUBLIC)")
        return JSONResponse({"publicKey": "", "valid": False, "details": info})
    logger.info("[VAPID] valid=%s public.len=%d private=%s", info["valid"], len(VAPID_PUBLIC), "OK" if info["private_ok"] else "NOK")
    return JSONResponse({"publicKey": VAPID_PUBLIC, "valid": info["valid"], "details": info})

@app.get("/latest")
def latest():
    if not LATEST_FILE.exists(): return JSONResponse(status_code=204, content=None)
    try:
        data = json.loads(LATEST_FILE.read_text(encoding="utf-8")); return JSONResponse(data)
    except Exception:
        return JSONResponse(status_code=204, content=None)

# --- GLOBAL /api/prefs ER UDFASET (returnerer 410 Gone) ----------------------
@app.get("/api/prefs")
def get_prefs_deprecated():
    raise HTTPException(status_code=410, detail="Global prefs er udfaset – brug /api/prefs/user")

@app.post("/api/prefs")
async def save_prefs_deprecated(req: Request):
    raise HTTPException(status_code=410, detail="Global prefs er udfaset – brug /api/prefs/user")

# --- NYT: bruger-initialisering og prefs pr. bruger --------------------------
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

# --- Publish (uændret) -------------------------------------------------------

@app.post("/api/publish")
async def publish(req: Request):
    payload = await req.json()
    check = validate_vapid_keys()
    if not check["valid"]:
        logger.error("[PUSH] VAPID ugyldig: %s", check["errors"])
        raise HTTPException(status_code=500, detail={"msg": "VAPID-nøgler er ikke gyldige", "details": check})

    subs = list(all_subscriptions())
    if not subs:
        logger.info("[PUSH] Ingen subscriptions. Gemmer latest og returnerer.")
        try:
            _write_latest([_enrich_payload_for_latest(payload)], kind="single")
        except Exception:
            logger.warning("[PUSH] Kunne ikke gemme latest-push.json")
        return {"sent": 0, "deleted": 0, "errors": 0, "count_subscriptions": 0}

    # Ny: ttl + urgency headers
    ttl = int(payload.get("ttl") or 86400)  # 24 timer default
    headers = _merge_push_headers(payload, default_urgency="high")

    t0 = time.perf_counter()
    results = {"sent": 0, "deleted": 0, "errors": 0}
    for sub in subs:
        try:
            webpush(
                subscription_info=sub,
                data=json.dumps(payload),
                vapid_private_key=VAPID_PRIVATE,
                vapid_claims=VAPID_CLAIMS,
                ttl=ttl,
                headers=headers,
            )
            results["sent"] += 1
        except WebPushException as ex:
            status = getattr(ex, "response", None).status_code if getattr(ex, "response", None) else None
            if status in (404, 410):
                delete_subscription(sub["endpoint"])
                results["deleted"] += 1
            else:
                results["errors"] += 1
        except Exception:
            results["errors"] += 1

    dur_ms = (time.perf_counter() - t0) * 1000
    logger.info(
        "[PUSH] url=%s subs=%d -> sent=%d deleted=%d errors=%d (%.1f ms) ttl=%s urgency=%s",
        payload.get("url"), len(subs), results["sent"], results["deleted"], results["errors"],
        dur_ms, ttl, headers.get("Urgency"),
    )
    results.update({"count_subscriptions": len(subs), "duration_ms": round(dur_ms, 1)})

    try:
        _write_latest([_enrich_payload_for_latest(payload)], kind="single")
    except Exception:
        logger.warning("[PUSH] Kunne ikke gemme latest-push.json")
    return results

# --- /api/publish-batch (én webpush pr. sub pr. batch, med ttl/urgency) ---
@app.post("/api/publish-batch")
async def publish_batch(req: Request):
    try:
        payloads = await req.json()
        if not isinstance(payloads, list):
            raise HTTPException(status_code=400, detail="Body skal være en liste af objekter")
        if not payloads:
            logger.info("[BATCH] Tom liste -> intet sendt")
            _write_latest([], kind="batch")
            return {"sent": 0, "deleted": 0, "errors": 0,
                    "count_payloads": 0, "count_subscriptions": subscriptions_count()}
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=400, detail="Ugyldig JSON body")

    check = validate_vapid_keys()
    if not check["valid"]:
        logger.error("[BATCH] VAPID ugyldig: %s", check["errors"])
        raise HTTPException(status_code=500, detail={"msg": "VAPID-nøgler er ikke gyldige", "details": check})

    subs = list(all_subscriptions())
    if not subs:
        logger.info("[BATCH] Ingen subscriptions. payloads=%d", len(payloads))
        _write_latest(payloads, kind="batch")
        return {"sent": 0, "deleted": 0, "errors": 0,
                "count_payloads": len(payloads), "count_subscriptions": 0}

    # Ny: ttl/urgency – ens for hele batchen (kan evt. udledes fra 1. payload)
    first = payloads[0] if payloads else {}
    ttl = int((first or {}).get("ttl") or 86400)
    headers = _merge_push_headers(first or {}, default_urgency="high")

    # Vi sender (som før) hver payload til hver sub – men nu med ttl/urgency
    t0 = time.perf_counter()
    results = {"sent": 0, "deleted": 0, "errors": 0}
    for sub in subs:
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
                results["sent"] += 1
            except WebPushException as ex:
                status = getattr(ex, "response", None).status_code if getattr(ex, "response", None) else None
                if status in (404, 410):
                    delete_subscription(sub["endpoint"])
                    results["deleted"] += 1
                else:
                    results["errors"] += 1
            except Exception:
                results["errors"] += 1

    dur_ms = (time.perf_counter() - t0) * 1000
    logger.info(
        "[BATCH] payloads=%d subs=%d -> sent=%d deleted=%d errors=%d (%.1f ms) ttl=%s urgency=%s",
        len(payloads), len(subs), results["sent"], results["deleted"], results["errors"],
        dur_ms, ttl, headers.get("Urgency"),
    )
    results.update({
        "count_payloads": len(payloads),
        "count_subscriptions": len(subs),
        "duration_ms": round(dur_ms, 1),
    })

    _write_latest(payloads, kind="batch")
    return results

@app.post("/api/publish-latest")
def publish_latest():
    try:
        if not LATEST_FILE.exists(): raise HTTPException(status_code=404, detail="latest-push.json mangler")
        data = json.loads(LATEST_FILE.read_text(encoding="utf-8"))
        items = data.get("items", [])
        if not items: raise HTTPException(status_code=400, detail="Ingen items i latest-push.json")
        payload = items[0]
        subs = list(all_subscriptions())
        if not subs: return {"sent": 0, "deleted": 0, "errors": 0, "count_subscriptions": 0}
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
            except Exception: results["errors"] += 1
        results.update({"count_subscriptions": len(subs)}); return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fejl: {e}")

# ────────────────────────────── Statiske filer ───────────────────────────────
@app.get("/")
def index():
    index_path = WEB_DIR / "index.html"
    if not index_path.exists(): raise HTTPException(status_code=404, detail="index.html mangler i /web")
    return FileResponse(index_path)

app.mount("/", StaticFiles(directory=WEB_DIR, html=True), name="web")