// app.js – DOF.not (v3.2+) · samlet & ordnet
// ============================================================================
// 1) COMMON UTILS (fælles for hele appen)
// ============================================================================

// Basal URL-hjælper (bevarer understøttelse af base href)
const BASE = document.baseURI || document.location.href;
const abs = (p) => new URL(p, BASE).toString();

// Element helpers (tåler at element mangler)
const $id = (id) => document.getElementById(id);

// Diagnostik/status (valgfrit)
const elStatus = $id('status');
function setDiag(msg, color = '#f2a900', ttlMs = 0) {
  if (!elStatus) return;
  elStatus.textContent = msg;
  elStatus.style.color = color;
  if (ttlMs > 0) setTimeout(() => (elStatus.textContent = ''), ttlMs);
}
(function setupDiagnostics() {
  const stamp = new Date().toISOString();
  setDiag(`app.js loaded @ ${stamp}`);
  window.addEventListener('error', (e) =>
    setDiag(`JS-fejl: ${e.message}`, '#d32f2f')
  );
  window.addEventListener('unhandledrejection', (e) =>
    setDiag(`Promise-fejl: ${e?.reason?.message ?? String(e.reason)}`, '#d32f2f')
  );
})();

async function fetchFirstOk(urls) {
  for (const url of urls) {
    try {
      const r = await fetch(abs(url), { cache: 'no-cache' });
      if (r.ok) return await r.text();
    } catch { /* prøv næste */ }
  }
  return '';
}

// Robust bruger-ID (én definition – fjernede dublet)
function getOrCreateUserId() {
  try {
    let id = localStorage.getItem('dofnot-user-id');
    if (!id) {
      id = (crypto?.randomUUID ? crypto.randomUUID()
        : (Date.now() + '-' + Math.random().toString(16).slice(2)));
      localStorage.setItem('dofnot-user-id', id);
    }
    return id;
  } catch {
    return Date.now() + '-' + Math.random().toString(16).slice(2);
  }
}

function getOrCreateDeviceId() {
  try {
    let id = localStorage.getItem('dofnot-device-id');
    if (!id) {
      id = (crypto?.randomUUID ? crypto.randomUUID()
           : (Date.now() + '-' + Math.random().toString(16).slice(2)));
      localStorage.setItem('dofnot-device-id', id);
    }
    return id;
  } catch {
    return Date.now() + '-' + Math.random().toString(16).slice(2);
  }
}

// Lokal opt-out flag (bruges også som cache)
const OPT_OUT_KEY = 'dofnot-push-optout';
const isOptedOut = () => { try { return localStorage.getItem(OPT_OUT_KEY) === '1'; } catch { return false; } };
const setOptOut  = (v)   => { try { localStorage.setItem(OPT_OUT_KEY, v ? '1' : '0'); } catch {} };

// Server-sync helpers
async function getServerOptOut(userId, deviceId) {
  const url = abs(`api/push/optout?user_id=${encodeURIComponent(userId)}&device_id=${encodeURIComponent(deviceId)}`);
  const r = await fetch(url, { cache: 'no-cache' });
  if (!r.ok) return { opted_out: false };
  return r.json();
}
async function setServerOptOut(userId, deviceId, opted) {
  try {
    await fetch(abs('api/push/optout'), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ user_id: userId, device_id: deviceId, opted_out: !!opted, ts: Date.now() })
    });
  } catch (e) {
    console.warn('Kunne ikke sync’e opt-out til serveren:', e);
  }
}


// Platform capabilities
const isIOS = /iphone|ipad|ipod/i.test(navigator.userAgent);
const isStandalone = () =>
  window.matchMedia('(display-mode: standalone)').matches ||
  window.navigator.standalone === true;
const supportsPush = () =>
  'serviceWorker' in navigator && 'PushManager' in window && 'Notification' in window;

// SW utils
async function ensureSW() {
  if (!('serviceWorker' in navigator)) return;
  try {
    await navigator.serviceWorker.register('sw.js', { scope: './' });
    await navigator.serviceWorker.ready;
  } catch (e) {
    console.warn('SW-registrering fejlede:', e);
  }
}
function postToSW(msg) {
  return navigator.serviceWorker.getRegistration().then((reg) => {
    try { reg?.active?.postMessage(msg); } catch {}
  });
}

// Sikker HTML-escaping (rigtig)
function escapeHtml(s) {
  return String(s ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

// Stabil slugify
function slugify(s) {
  return String(s ?? '')
    .toLowerCase()
    .replace(/æ/g,'ae').replace(/ø/g,'oe').replace(/å/g,'aa')
    .normalize('NFD').replace(/[\u0300-\u036f]/g,'')
    .replace(/[^a-z0-9]+/g,'-').replace(/(^-|-$)/g,'');
}

// Afdelinger (bruges i præferencetabellen)
const DOF_AFDELINGER = [
  "DOF København","DOF Nordsjælland","DOF Vestsjælland","DOF Storstrøm","DOF Bornholm",
  "DOF Fyn","DOF Sønderjylland","DOF Sydvestjylland","DOF Sydøstjylland","DOF Vestjylland",
  "DOF Østjylland","DOF Nordvestjylland","DOF Nordjylland"
];
const CHOICE_ORDER = ['none', 'su', 'sub', 'alle'];
const CHOICE_LABELS = { none: 'Ingen', su: 'SU', sub: 'SUB', alle: 'alle' };
const _norm = (s) => String(s ?? '').trim().toLowerCase();

// === Klassifikation fra én CSV: arter_filter_klassificeret.csv ===
// Forventet header: "artsid;artsnavn;klassifikation"
let __CLASS_MAP = null; // Map<normName, "SU"|"SUB"|"Alm">
async function loadClassificationMap() {
  if (__CLASS_MAP) return __CLASS_MAP;
  // Prøv begge stier, så det virker både i /web/data og /data
  const text = await fetchFirstOk([
    './web/data/arter_filter_klassificeret.csv',
    './data/arter_filter_klassificeret.csv'
  ]);
  if (!text) {
    console.warn('[klassifikation] Fandt ikke arter_filter_klassificeret.csv i /web/data eller /data');
    __CLASS_MAP = new Map(); // default: alt bliver 'Alm'
    return __CLASS_MAP;
  }
  const lines = text.split(/\r?\n/).filter(Boolean);
  if (!lines.length) { __CLASS_MAP = new Map(); return __CLASS_MAP; }

  // Find kolonneindeks
  const hdr = lines[0].split(';').map(h => h.trim().toLowerCase());
  const iNavn = hdr.indexOf('artsnavn');
  const iKls  = hdr.indexOf('klassifikation');

  const start = (iNavn >= 0 && iKls >= 0) ? 1 : 0;
  const map = new Map();
  for (let i = start; i < lines.length; i++) {
    const cols = lines[i].split(';');
    const navn  = (iNavn >= 0 ? cols[iNavn] : cols[1]) ?? '';
    const klass = (iKls  >= 0 ? cols[iKls]  : cols[2]) ?? '';
    const key = normArtKey(navn);
    const val = (klass ?? '').trim();
    if (key) {
      const K = (val === 'SU' || val === 'SUB') ? val : 'Alm';
      const prev = map.get(key);
      map.set(key, prev === 'SU' ? 'SU' : K); // SU > SUB > Alm
    }
  }
  __CLASS_MAP = map;
  return __CLASS_MAP;
}
// Sikker klassifikation (kaldes fra render-funktioner)
async function ensureClassificationLoaded() { await loadClassificationMap(); }
// Returnér 'su' | 'sub' | 'alm' (CSS-venlig)
function classifySpeciesName(name) {
  const key = normArtKey(name);
  const k = __CLASS_MAP?.get(key); // "SU" | "SUB" | "Alm" | undefined
  return (k === 'SU') ? 'su' : (k === 'SUB') ? 'sub' : 'alm';
}

// ============================================================================
// 2) PREFERENCES & PUSH (abonnementsmatrix + push-subscription)
// ============================================================================
const elGrid = $id('grid');
const elSave = $id('save'); // hovedknap (Gem/Abonnér/Opdater)
const elUnsub = $id('unsubscribe'); // afmeld push

function renderPrefsTable(prefs) {
  if (!elGrid) return;
  let html = `
<table class="prefs-table" aria-label="Abonnementsfiltre pr. lokalafdeling">
  <thead>
    <tr><th>Lokalafdeling</th><th>Ingen</th><th>SU</th><th>SUB</th><th>BV</th></tr>
  </thead>
  <tbody>`;
  for (const afd of DOF_AFDELINGER) {
    const slug = slugify(afd);
    const current = (prefs && prefs[afd]) ? String(prefs[afd]).toLowerCase() : 'none';
    html += `<tr><td class="afd">${escapeHtml(afd)}</td>`;
    for (const v of CHOICE_ORDER) {
      const id = `pref-${slug}-${v}`;
      const label = CHOICE_LABELS[v] || v.toUpperCase();
      html += `
<td class="sel">
  <input type="radio" id="${id}" name="pref-${slug}" value="${v}" ${current===v?'checked':''} />
  <label for="${id}" title="${label}"><span class="box"></span></label>
</td>`;
    }
    html += `</tr>`;
  }
  html += `</tbody></table>`;
  elGrid.innerHTML = html;
  // Klik på hele cellen for at vælge radio
  elGrid.querySelectorAll('.prefs-table td.sel').forEach((td) => {
    td.addEventListener('click', () => {
      const input = td.querySelector('input[type=radio]');
      if (input) { input.checked = true; input.dispatchEvent(new Event('change', { bubbles: true })); }
    });
  });
}

function setSaveBtnState(state, label) {
  const btn = elSave;
  if (!btn) return;
  // state: subscribed | unsubscribed | blocked | unsupported
  btn.dataset.state = state;
  btn.classList.remove('is-subscribed','is-unsubscribed','is-blocked','is-unsupported');
  btn.classList.add(`is-${state}`);
  const labelEl = btn.querySelector('[data-role="label"]');
  if (labelEl) labelEl.textContent = label; else btn.textContent = label;
  btn.setAttribute('aria-label', label);
  btn.disabled = (state === 'blocked' || state === 'unsupported');
  if (elUnsub) elUnsub.disabled = (state !== 'subscribed');
}

let _labelRunId = 0;
function updateSaveButtonLabel() {
  if (!elSave) return;
  const myRun = ++_labelRunId;
  const safe = (state, label) => { if (myRun === _labelRunId) setSaveBtnState(state, label); };
  (async () => {
    if (!supportsPush()) { safe('unsupported', 'Gem præferencer'); return; }
    try {
      await ensureSW();
      await navigator.serviceWorker.ready;
      const reg = await navigator.serviceWorker.getRegistration();
      if (!reg || !('pushManager' in reg)) { safe('unsupported', 'Gem præferencer'); return; }
      const perm = Notification.permission;
      if (perm === 'denied') { safe('blocked', 'Notifikationer blokeret'); return; }
      const sub = await reg.pushManager.getSubscription();
      safe(sub ? 'subscribed' : 'unsubscribed', sub ? 'Opdater abonnement' : 'Abonnér');
    } catch {
      safe('unsupported', 'Gem præferencer');
    }
  })();
}

async function ensurePushSubscription({ forcePrompt = false } = {}) {
  await ensureSW();
  await navigator.serviceWorker.ready;

  if (isIOS && !isStandalone()) {
    throw new Error('På iOS virker push først, når appen er føjet til hjemmeskærm.');
  }
  if (!supportsPush()) throw new Error('Push understøttes ikke i denne browser/enhed.');

  const reg = await navigator.serviceWorker.getRegistration();
  if (!reg) throw new Error('Service worker ikke registreret');

  // Hent VAPID public key
  const r = await fetch(abs('vapid-public-key'), { cache: 'no-cache' });
  if (!r.ok) throw new Error('Kan ikke hente /vapid-public-key');
  const { publicKey, valid } = await r.json();
  if (!valid || !publicKey) throw new Error('Ugyldig/manglende VAPID public key');

  // Permission
  let perm = Notification.permission;
  if (perm === 'default' || forcePrompt) perm = await Notification.requestPermission();
  if (perm !== 'granted') throw new Error(`Notifikationer ikke tilladt (permission='${perm}')`);

  // Subscribe hvis nødvendigt
  let sub = await reg.pushManager.getSubscription();
  if (!sub) {
    const toKey = (k) => {
      const p = '='.repeat((4 - k.length % 4) % 4);
      const b = (k + p).replace(/-/g, '+').replace(/_/g, '/');
      const raw = atob(b);
      const out = new Uint8Array(raw.length);
      for (let i = 0; i < raw.length; i++) out[i] = raw.charCodeAt(i);
      return out;
    };
    sub = await reg.pushManager.subscribe({
      userVisibleOnly: true,
      applicationServerKey: toKey(publicKey)
    });
  }

  // Server: gem subscription og ryd opt-out for denne enhed
  const user_id = getOrCreateUserId();
  const device_id = getOrCreateDeviceId();
  const resp = await fetch(abs('api/subscribe'), {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({ ...sub.toJSON(), user_id, device_id })
  });
  if (!resp.ok) throw new Error(`Server afviste api/subscribe (HTTP ${resp.status})`);

  setOptOut(false);
  setServerOptOut(user_id, device_id, false);
  return true;
}

// Komplet, robust afmelding – paste 1:1
async function unsubscribePush() {
  // --- små fallbacks, hvis helpers ikke findes globalt ---
  const getDevId = () => {
    try {
      if (typeof getOrCreateDeviceId === 'function') return getOrCreateDeviceId();
      // Fallback: lav et device-id og gem lokalt
      let id = localStorage.getItem('dofnot-device-id');
      if (!id) {
        id = (crypto?.randomUUID ? crypto.randomUUID()
             : (Date.now() + '-' + Math.random().toString(16).slice(2)));
        localStorage.setItem('dofnot-device-id', id);
      }
      return id;
    } catch {
      return Date.now() + '-' + Math.random().toString(16).slice(2);
    }
  };
  const setLocalOptOut = (v) => {
    try {
      if (typeof setOptOut === 'function') setOptOut(!!v);
      else localStorage.setItem('dofnot-push-optout', v ? '1' : '0');
    } catch {}
  };
  const setServerOptOutSafe = async (user_id, device_id, opted) => {
    try {
      if (typeof setServerOptOut === 'function') {
        await setServerOptOut(user_id, device_id, !!opted);
      } else {
        await fetch(abs('api/push/optout'), {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ user_id, device_id, opted_out: !!opted, ts: Date.now() })
        });
      }
    } catch (e) {
      console.warn('Kunne ikke sync’e opt-out til serveren:', e);
    }
  };

  try {
    await ensureSW?.();
    await navigator.serviceWorker?.ready;

    const reg = await navigator.serviceWorker.getRegistration();
    if (!reg) return false;

    const sub = await reg.pushManager.getSubscription();

    // Hvis der slet ikke er et subscription-objekt: marker stadig opt-out server-side
    if (!sub) {
      const user_id = getOrCreateUserId();
      const device_id = getDevId();
      setLocalOptOut(true);
      await setServerOptOutSafe(user_id, device_id, true);
      return false;
    }

    // 1) Afmeld i browseren (husk endpoint før unsubscribe())
    const endpoint = sub.endpoint;
    const ok = await sub.unsubscribe();

    // 2) Fortæl serveren at endpoint er slettet + sæt opt-out for (user, device)
    const user_id = getOrCreateUserId();
    const device_id = getDevId();

    try {
      await fetch(abs('api/unsubscribe'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ endpoint, user_id, device_id })
      });
    } catch (e) {
      console.warn('Server-unsubscribe fejlede:', e);
      // fortsæt: vi prøver stadig at skrive opt-out
    }

    // 3) Persistér opt-out (lokalt + server)
    setLocalOptOut(true);
    await setServerOptOutSafe(user_id, device_id, true);

    // 4) (valgfrit) opdatér UI-knapper
    try { updateSaveButtonLabel?.(); } catch {}

    return ok;
  } catch (e) {
    console.warn('Unsubscribe fejlede:', e);
    return false;
  }
}

async function onSave() {
  const prefs = {};
  for (const afd of DOF_AFDELINGER) {
    const slug = slugify(afd);
    const sel = document.querySelector(`input[name="pref-${slug}"]:checked`);
    prefs[afd] = sel ? sel.value : 'none';
  }
  // Gem lokalt (SW + localStorage)
  await postToSW({ type: 'SAVE_PREFS', prefs });
  try { localStorage.setItem('dofnot-prefs', JSON.stringify(prefs)); } catch {}

  // Gem centralt pr. bruger
  try {
    const userId = getOrCreateUserId();
    await fetch(abs('api/prefs/user'), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ user_id: userId, prefs, ts: Date.now() })
    });
  } catch (e) {
    console.warn('POST /api/prefs/user fejlede:', e);
  }

  
  try {
    const hasActive = Object.values(prefs || {}).some(v =>
      ['su','sub','alle'].includes(String(v).toLowerCase())
    );
    if (!isOptedOut() && hasActive) {
      await ensurePushSubscription({ forcePrompt: true });
      setDiag('Abonnement + præferencer gemt.', '#2e7d32', 2000);
    } else {
      setDiag('Præferencer gemt (ingen auto-abonnement).', '#607d8b', 2000);
    }
  } catch {
    setDiag('Gemte præferencer (push ikke tilladt/understøttet).', '#607d8b', 2500);
  } finally {
    updateSaveButtonLabel();
  }
}

async function onUnsubscribe() {
  try {
    const ok = await unsubscribePush();
    if (ok) setDiag('Abonnement afmeldt på denne enhed.', '#2e7d32', 2000);
  } catch (e) {
    console.warn('Unsubscribe fejlede:', e);
  } finally {
    updateSaveButtonLabel();
  }
}

async function initPrefsAndPush() {
  // Init/bruger-tilknytning server-side
  await ensureSW();
  const userId = getOrCreateUserId();
  try {
    await fetch(abs('api/user/init'), {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ user_id: userId })
    });
  } catch {}
  postToSW({ type: 'SET_USER', user_id: userId });

  // Hent bruger-prefs (fallback til local)
  let prefs = {};
  try {
    const r = await fetch(abs('api/prefs/user') + '?user_id=' + encodeURIComponent(userId), { cache:'no-cache' });
    if (r.ok) { const data = await r.json(); if (data && data.prefs) prefs = data.prefs; }
  } catch {}
  if (!Object.keys(prefs).length) {
    try { const local = JSON.parse(localStorage.getItem('dofnot-prefs') ?? 'null'); if (local) prefs = local; } catch {}
  }

  // Render og bind knapper
  renderPrefsTable(prefs);
  updateSaveButtonLabel();
  if (elSave) elSave.addEventListener('click', onSave);
  if (elUnsub) elUnsub.addEventListener('click', onUnsubscribe);

  // Hold label frisk
  document.addEventListener('visibilitychange', () => { if (!document.hidden) updateSaveButtonLabel(); });
  if ('serviceWorker' in navigator) {
    navigator.serviceWorker.addEventListener('controllerchange', updateSaveButtonLabel);
    navigator.serviceWorker.addEventListener('message', (e) => {
      if (e.data?.type === 'PUSH_SUB_CHANGED') updateSaveButtonLabel();
    });
  }

  // Valgfrit auto-subscribe
  
  if (supportsPush()) {
    try {
      await navigator.serviceWorker.ready;
      const reg = await navigator.serviceWorker.getRegistration();
      const sub = await reg?.pushManager.getSubscription();
      if (!sub && Notification.permission === 'granted' && !isOptedOut() && hasActive) {
        await ensurePushSubscription({ forcePrompt: false });
        updateSaveButtonLabel();
      }
    } catch (e) {
      console.warn('[initPrefsAndPush] auto-subscribe skipped/failed:', e);
    }
  }
}

// ============================================================================
// 3) OBSERVATIONER (log-filer → liste)
// ============================================================================

/* === Indstillinger === */
const LOG_BASE = './logs';
const CONCURRENCY = 8;
const MAX_ITEMS_PER_REGION = 250;
const TZ = 'Europe/Copenhagen'; // DK‑tid til "i dag"

const $status = $id('obs-status');
const $list   = $id('obs-list');
const $hideZero = $id('toggle-hide-zero');
const $limitSel = $id('limit-select');
function setObsStatus(m) { if ($status) $status.textContent = m; }

/* Gem/indlæs valgt limit (valgfrit – for bedre UX) */
(function initLimitFromStorage(){
  if (!$limitSel) return;
  try {
    const saved = localStorage.getItem('obs-limit');
    if (saved && ['5','10','all'].includes(saved)) {
      $limitSel.value = saved;
    }
  } catch {}
})();
if ($limitSel) {
  $limitSel.addEventListener('change', () => {
    try { localStorage.setItem('obs-limit', $limitSel.value); } catch {}
    renderFromCache(); // undgå gen‑fetch; vi har dataToday i cache
  });
}

/* Afdeling -> regionslug (til filnavne) */
const AFD_TO_REGION = {
  "DOF København": "kobenhavn",
  "DOF Nordsjælland": "nordsjaelland",
  "DOF Vestsjælland": "vestsjaelland",
  "DOF Storstrøm": "storstrom",
  "DOF Bornholm": "bornholm",
  "DOF Fyn": "fyn",
  "DOF Sønderjylland": "sonderjylland",
  "DOF Sydvestjylland": "sydvestjylland",
  "DOF Sydøstjylland": "ostjylland", // (bevidst) samme slug som Østjylland?
  "DOF Vestjylland": "vestjylland",
  "DOF Østjylland": "ostjylland",
  "DOF Nordvestjylland": "nordvestjylland",
  "DOF Nordjylland": "nordjylland",
};

/* Hent prefs fra serverens DB (til observationer) */
async function loadUserPrefsFromServer() {
  const userId = getOrCreateUserId();
  const url = abs('api/prefs/user') + '?user_id=' + encodeURIComponent(userId);
  const r = await fetch(url, { cache: 'no-cache' });
  if (!r.ok) throw new Error('Kunne ikke hente brugerpræferencer');
  const data = await r.json();
  return data?.prefs ?? {};
}

/* Fortolkning af valg: SU → {su}, SUB → {su,sub}, "alle" → {su,sub} */
function expandPrefValueToAllowedSources(val) {
  const v = String(val ?? '').toLowerCase();
  if (v === 'su')   return new Set(['su']);
  if (v === 'sub')  return new Set(['su','sub']);
  if (v === 'alle') return new Set(['su','sub']); // bemærkelsesværdige = SUB
  return new Set();
}

/* Plan for hvilke filer der skal hentes pr. region */
function buildRegionPlan(prefs) {
  const plan = new Map(); // region -> { allow:Set<'su'|'sub'>, files:string[] }
  for (const [afd, sel] of Object.entries(prefs ?? {})) {
    const slug = AFD_TO_REGION[afd];
    if (!slug) continue;
    const allow = expandPrefValueToAllowedSources(sel);
    if (allow.size === 0) continue;
    const entry = plan.get(slug) ?? { allow: new Set(), files: [] };
    if (allow.has('su'))  { entry.allow.add('su');  entry.files.push(`${LOG_BASE}/su-${slug}.log`); }
    if (allow.has('sub')) { entry.allow.add('sub'); entry.files.push(`${LOG_BASE}/sub-${slug}.log`); }
    plan.set(slug, entry);
  }
  return plan;
}

function setSelectionOnList(regions) {
  if (!$list) return;
  const label = regions.length
    ? `Valgte regioner: ${regions.join(', ')}`
    : 'Ingen regioner valgt (i dine præferencer).';
  $list.setAttribute('aria-label', label);
  $list.dataset.selection = regions.join(','); // til scripts der vil læse værdien
}

/* ==== Parsing helpers ==== */
function extractCoordsFromString(s) {
  if (!s) return { lon: null, lat: null };
  // (lon, lat)
  let m = s.match(/\((-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)\)/);
  if (m) {
    const lon = Number(m[1]), lat = Number(m[2]);
    return { lon: Number.isFinite(lon) ? lon : null, lat: Number.isFinite(lat) ? lat : null };
  }
  // uden parenteser: lon, lat
  m = s.match(/(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)/);
  if (m) {
    const lon = Number(m[1]), lat = Number(m[2]);
    return { lon: Number.isFinite(lon) ? lon : null, lat: Number.isFinite(lat) ? lat : null };
  }
  return { lon: null, lat: null };
}
function extractCoordsFromParts(parts) {
  let out = { lon: null, lat: null };
  for (const p of parts) {
    const c = extractCoordsFromString(p);
    if (c.lon != null && c.lat != null) out = c;
  }
  return out;
}
function parseLocalDateFromHeader(header) {
  // Matcher: ...] YYYY-MM-DD HH:MM:SS
  const m = header.match(/\]\s+(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})/);
  return m ? new Date(`${m[1]}T${m[2]}`) : null; // lokal tid
}
function parseLogLine(line) {
  if (!line || !line.trim()) return null;
  const parts = line.split(' · ').map((s) => s.trim());
  if (parts.length < 7) return null;

  const header       = parts[0];
  const sourceBr     = parts[1]; // "[su]" eller "[sub]"
  const countSpecies = parts[2];
  const behavior     = parts[3] ?? '';
  const locality     = parts[4] ?? '';
  const org          = parts[5] ?? '';
  const observer     = parts[6] ?? '';

  const { lon, lat } = extractCoordsFromParts(parts);

  // Udtræk key "[su-<region>]" -> "su-<region>"
  const mTag = header.match(/^\[([^\]]+)\]/);
  const key = mTag ? mTag[1] : '';
  const [srcFromKey, regionFromKey] = key.split('-', 2);

  // Udtræk kilde fra "[su]" eller "[sub]"
  const mSrc = sourceBr.match(/\[(su|sub)\]/i);
  const source = (mSrc?.[1] || srcFromKey || '').toLowerCase();

  // Udlæs antal + artsnavn
  const mCount = countSpecies.match(/^\s*(\d+)\s+(.+)$/);
  const count   = mCount ? Number(mCount[1]) : null;
  const species = mCount ? mCount[2].trim() : countSpecies;

  const date = parseLocalDateFromHeader(header);

  return {
    key, source, region: regionFromKey ?? '',
    date, count, species, behavior, locality, org, observer,
    lon, lat, rawLine: line
  };
}

/* ==== Fetch ==== */
async function fetchText(url) {
  const res = await fetch(url, { cache: 'no-cache' });
  if (!res.ok) throw new Error(res.status + ' ' + url);
  return res.text();
}
async function fetchRegionFiles(entry, hideZero) {
  const items = [];
  for (const url of entry.files) {
    try {
      const text = await fetchText(url);
      const lines = text.split(/\r?\n/);
      for (const line of lines) {
        const obj = parseLogLine(line);
        if (!obj) continue;
        if (!entry.allow.has(obj.source)) continue;
        if (hideZero && typeof obj.count === 'number' && obj.count === 0) continue;
        items.push(obj);
        if (items.length >= MAX_ITEMS_PER_REGION) break;
      }
    } catch {
      // fil kan mangle – det er ok
      continue;
    }
  }
  return items;
}
async function fetchAll(plan, hideZero, limit = CONCURRENCY) {
  const regions = [...plan.keys()];
  const results = [];
  let i = 0;
  const inflight = new Set();
  async function pump() {
    if (i >= regions.length) return;
    const slug = regions[i++];
    const p = fetchRegionFiles(plan.get(slug), hideZero)
      .then((list) => results.push(...list))
      .catch(() => {})
      .finally(() => inflight.delete(p));
    inflight.add(p);
    if (inflight.size >= limit) await Promise.race(inflight);
    return pump();
  }
  await pump();
  await Promise.allSettled(inflight);
  return results;
}

/* ==== Render ==== */
function el(tag, className, text) {
  const e = document.createElement(tag);
  if (className) e.className = className;
  if (text != null) e.textContent = String(text);
  return e;
}
function renderItem(item) {
  const li = el('li', 'obs-item');
  const article = el('article');
  const header = el('header');

  const sp = el(
    'h3',
    'title species sp-name',
    `${Number.isFinite(item.count) ? item.count + ' ' : ''}${item.species}`
  );
  // tilføj klassifikation 'su' | 'sub' | 'alm' til artstitlen (for farver)
  sp.classList.add(classifySpeciesName(item.species));

  header.append(
    el('span', 'badge badge-src', item.source.toUpperCase()),
    el('span', 'badge badge-region', item.region),
    sp
  );

  const meta = el('div', 'meta');
  if (item.behavior) meta.append(el('span', null, item.behavior));
  if (item.locality) meta.append(el('span', null, item.locality));
  if (Number.isFinite(item.lat) && Number.isFinite(item.lon)) {
    const sep = el('span', null, '·');
    const a = document.createElement('a');
    a.href = `https://www.openstreetmap.org/?mlat=${item.lat}&mlon=${item.lon}#map=13/${item.lat}/${item.lon}`;
    a.target = '_blank'; a.rel = 'noopener'; a.textContent = 'kort';
    meta.append(sep, a);
  }

  const byline = el('footer', 'byline');
  if (item.observer) byline.append(el('span', 'observer', item.observer));
  if (item.org)      byline.append(el('span', 'org', item.org));
  const t = document.createElement('time');
  if (item.date instanceof Date && !isNaN(item.date)) {
    t.dateTime = item.date.toISOString();
    t.textContent = item.date.toLocaleString('da-DK', { timeZone: TZ });
  }
  byline.append(t);

  article.append(header, meta, byline);
  li.append(article);
  return li;
}

/* ==== Controller + cache ==== */
function ymdInTZ(d, tz) {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: tz, year:'numeric', month:'2-digit', day:'2-digit'
  }).format(d); // "YYYY-MM-DD"
}
let cache = { today: [], totalToday: 0, selectionRegions: [] };

function renderFromCache() {
  if (!$list) return;
  $list.innerHTML = '';
  const allToday = cache.today ?? [];
  const val = $limitSel ? $limitSel.value : 'all';
  const limit = (val === 'all') ? allToday.length : Number(val);
  const items = allToday.slice(0, limit);
  const frag = document.createDocumentFragment();
  for (const item of items) frag.appendChild(renderItem(item));
  $list.appendChild(frag);
  const shown = items.length;
  const total = allToday.length;
  const regCount = cache.selectionRegions.length;
  setObsStatus(`Viser ${shown} af ${total} observationer fra i dag i ${regCount} region${regCount>1?'er':''}.`);
}

async function loadAndRender() {
  if (!$list) return; // observationssektionen findes ikke på siden
  $list.innerHTML = '';
  setObsStatus('Henter præferencer …');
  let prefs = {};
  try { prefs = await loadUserPrefsFromServer(); }
  catch (e) { setObsStatus('Kunne ikke hente præferencer fra serveren.'); return; }

  const plan = buildRegionPlan(prefs);
  const selected = [...plan.keys()];
  setSelectionOnList(selected);
  cache.selectionRegions = selected;
  if (selected.length === 0) {
    setObsStatus('Ingen regioner har aktive valg (SU/SUB).');
    return;
  }

  setObsStatus('Henter observationer …');
  const data = await fetchAll(plan, !!$hideZero?.checked, CONCURRENCY);
  if (!data.length) { setObsStatus('Ingen relevante observationer fundet.'); return; }

  // Kun i dag (DK-tid)
  const todayYMD = ymdInTZ(new Date(), TZ);
  const dataToday = data.filter((it) =>
    it.date instanceof Date && !isNaN(it.date) && ymdInTZ(it.date, TZ) === todayYMD
  );
  if (!dataToday.length) { setObsStatus('Ingen observationer fra i dag matcher dine præferencer.'); return; }

  // Nyeste først
  dataToday.sort((a,b) => (b.date?.getTime?.() ?? -Infinity) - (a.date?.getTime?.() ?? -Infinity));
  cache.today = dataToday;
  cache.totalToday = dataToday.length;

  await ensureClassificationLoaded();
  renderFromCache();
}

if ($hideZero) $hideZero.addEventListener('change', () => { loadAndRender().catch(console.error); });

// ============================================================================
// 4) INIT (kald begge moduler, men kun hvis deres DOM findes)
// ============================================================================
document.addEventListener('DOMContentLoaded', () => {
  // Preferences/Push modulet initialiseres altid (tåler at grid/save ikke findes)
  initPrefsAndPush().catch(console.error);
  // Observationer initialiseres kun hvis deres DOM-IDs findes
  if ($id('obs-list')) {
    loadAndRender().catch(console.error);
  }
});

// ============================================================================
// 5) AVANCERET FILTRERING (arter) – lokalt UI + sync til SW + (valgfrit) server
// ============================================================================

/** Læs artsliste (semicolon-CSV) – returnerer [{id, navn}, ...] */
async function loadSpeciesList() {
  const tryUrls = [
    './data/arter_filter.csv',
    './data/arter_sammenflettet_sorteret.csv' // fallback (kolonner: artsid;artsnavn)
  ];
  let text = '';
  for (const url of tryUrls) {
    try {
      const r = await fetch(url, { cache: 'no-cache' });
      if (r.ok) { text = await r.text(); break; }
    } catch { /* next */ }
  }
  if (!text) return [];
  const lines = text.split(/\r?\n/).filter(Boolean);
  const out = [];
  // detekter header (forventer artsnavn)
  const header = (lines[0] ?? '').toLowerCase();
  const hasHeader = header.includes('artsnavn');
  const start = hasHeader ? 1 : 0;
  for (let i = start; i < lines.length; i++) {
    const [id, name] = lines[i].split(';');
    const navn = (name ?? '').trim();
    if (!navn) continue;
    out.push({ id: (id ?? '').trim(), navn });
  }
  return out;
}

// Normaliser artsnavn robust (æ/ø/å, diakritik, klammer, bindestreger, whitespace)
function normArtKey(s) {
  let t = String(s ?? '').normalize('NFKD');
  // Dansk special-mapping
  t = t
    .replace(/\u00C6/g,'AE').replace(/\u00E6/g,'ae')
    .replace(/\u00D8/g,'OE').replace(/\u00F8/g,'oe')
    .replace(/\u00C5/g,'AA').replace(/\u00E5/g,'aa');
  // Fjern kombinationstegn efter NFKD (diakritik)
  t = t.replace(/[\u0300-\u036f]/g, '');
  // Fjern/ensret typisk støj
  t = t
    .replace(/[\"'«»„”“’”“\[\]\{\}]/g, ' ') // citater/klammer
    .replace(/[.,;:]/g, ' ')               // punktuation
    .replace(/[\u2010-\u2014\u2212]/g, '-')// bindestreger → '-'
    .replace(/\u00A0/g, ' ')               // NBSP → space
    .replace(/\s+/g, ' ')                  // kollaps whitespace
    .trim()
    .toLowerCase();
  return t;
}

/**
 * Model for overrides (global): { include: Set<key>, exclude: Set<key> }
 * UI: tri-state pr. art:
 * 0=default (ingen override) · 1=include · 2=exclude
 */
function overridesToState(ov) {
  return {
    include: new Set([...(ov?.include ?? [])].map(normArtKey)),
    exclude: new Set([...(ov?.exclude ?? [])].map(normArtKey))
  };
}
function stateToOverrides(state) {
  return {
    include: Array.from(state.include),
    exclude: Array.from(state.exclude),
  };
}

/** Hent/gem til server (valgfrit) – forsøger POST, men tolererer 404/410 */
async function saveSpeciesOverridesToServer(overrides) {
  try {
    const userId = getOrCreateUserId();
    const resp = await fetch(abs('api/prefs/user/species'), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ user_id: userId, overrides, ts: Date.now() })
    });
    if (!resp.ok && resp.status !== 404 && resp.status !== 410) {
      console.warn('POST /api/prefs/user/species svar:', resp.status);
    }
  } catch (e) {
    console.warn('Kunne ikke gemme arts-overrides på serveren:', e);
  }
}

/** Lidt hjælpere til reset-beskyttelse */
function makeChallenge() {
  // Små tal for hurtig læsning – ingen negative resultater.
  const a = Math.floor(Math.random() * 9) + 1; // 1..9
  const b = Math.floor(Math.random() * 9) + 1; // 1..9
  const useMinus = Math.random() < 0.4 && a > b; // ca. 40% minus, undgå negativt
  const op = useMinus ? '-' : '+';
  const res = useMinus ? (a - b) : (a + b);
  return { text: `Sikkerhedstjek: Hvad er ${a} ${op} ${b}?`, answer: res };
}

async function guardedResetOverrides() {
  const { text, answer } = makeChallenge();
  // Trin 1: prompt med regnestykke (brugeren skal trykke OK for at indsende)
  const input = self.prompt(text, '');
  if (input === null) {
    if ($status) { $status.textContent = 'Nulstilling annulleret.'; setTimeout(() => $status.textContent='', 1500); }
    return false;
  }
  const parsed = Number(String(input).trim().replace(',', '.'));
  if (!Number.isFinite(parsed) || parsed !== answer) {
    self.alert('Forkert svar – nulstilling blev afbrudt.');
    return false;
  }
  // Trin 2: ekstra sikkerheds-spørgsmål
  const ok = self.confirm('Er du sikker på, at du vil nulstille alle arts-overrides til Default?');
  if (!ok) {
    if ($status) { $status.textContent = 'Nulstilling annulleret.'; setTimeout(() => $status.textContent='', 1500); }
    return false;
  }
  // Trin 3: udfør nulstilling i UI (samme effekt som før)
  overrides.include.clear();
  overrides.exclude.clear();
  $list.querySelectorAll('.sp-toggle').forEach(btn => {
    btn.dataset.state = '0';
    btn.textContent = 'Default';
    btn.classList.remove('is-include','is-exclude');
  });
  if ($status) { $status.textContent = 'Nulstillet – husk at trykke Gem.'; setTimeout(() => $status.textContent='', 2500); }
  return true;
}

// ============================================================================
// Avanceret filtrering (arter) – komplet erstatning af initAdvancedFilteringPage()
// ============================================================================
async function initAdvancedFilteringPage() {
  // Unikke element-referencer for Advanced-siden
  const $advList = document.getElementById('adv-list');
  if (!$advList) return; // siden er ikke i brug
  const $advSearch = document.getElementById('adv-search');
  const $advSave   = document.getElementById('adv-save');
  const $advClear  = document.getElementById('adv-clear');
  const $advStatus = document.getElementById('adv-status');
  const $advShowActive = document.getElementById('adv-show-active');
  const $advExport = document.getElementById('adv-export');
  const $advImport = document.getElementById('adv-import');
  const $advFile   = document.getElementById('adv-file');

  // --- Hjælpere (lokale) ---
  function dirtyUI() { if ($advStatus) $advStatus.textContent = 'Ikke gemt …'; }

  // Kun positive counts; 0 og ugyldige ignoreres (nulstiller også operator ved rydning)
  function toCountsMap(obj) {
    const m = new Map();
    if (!obj || typeof obj !== 'object') return m;
    for (const [k, v] of Object.entries(obj)) {
      const mode = (v && v.mode === 'eq') ? 'eq' : 'gte';
      const raw = Number(v?.value);
      const num = Number.isFinite(raw) ? Math.floor(raw) : null;
      if (num != null && num > 0) m.set(normArtKey(k), { mode, value: num });
    }
    return m;
  }
  function countsMapToObj(map) {
    const out = {};
    for (const [k, v] of map.entries()) {
      const n = Number(v?.value);
      if (v && Number.isFinite(n) && n > 0) {
        out[k] = { mode: (v.mode === 'eq' ? 'eq' : 'gte'), value: Math.floor(n) };
      }
    }
    return out;
  }

  // 1) Hent artsliste (semicolon-CSV)
  const arts = await loadSpeciesList();
  await ensureClassificationLoaded();
  if (!Array.isArray(arts) || arts.length === 0) {
    if ($advStatus) $advStatus.textContent = 'Kunne ikke indlæse artsliste.';
    return;
  }

  // 2) Hent eksisterende overrides (SW ▶︎ localStorage ▶︎ tom)
  let overrides = await (async () => {
    // SW: GET_SPECIES_OVERRIDES (IndexedDB)
    try {
      await navigator.serviceWorker?.ready;
      const ch = new MessageChannel();
      const req = new Promise(resolve => { ch.port1.onmessage = (e) => resolve(e.data?.overrides ?? null); });
      navigator.serviceWorker?.controller?.postMessage({ type: 'GET_SPECIES_OVERRIDES' }, [ch.port2]);
      const fromSw = await Promise.race([req, new Promise(r => setTimeout(() => r(null), 800))]);
      if (fromSw) {
        return {
          // Two-state: ignorér include; behold exclude + counts (back-compat: countFilter -> ignoreres)
          exclude: new Set([...(fromSw.exclude ?? [])].map(normArtKey)),
          counts : toCountsMap(fromSw.counts ?? fromSw.countFilter)
        };
      }
    } catch { /* ignore */ }

    // localStorage fallback
    try {
      const raw = localStorage.getItem('dofnot-species-overrides');
      if (raw) {
        const parsed = JSON.parse(raw);
        return {
          exclude: new Set([...(parsed.exclude ?? [])].map(normArtKey)),
          counts : toCountsMap(parsed.counts ?? parsed.countFilter)
        };
      }
    } catch { /* ignore */ }

    return { exclude: new Set(), counts: new Map() };
  })();

  // Helper: row har “aktivt filter” hvis exclude eller counts for arten
  function hasRowFilter(key) {
    return overrides.exclude.has(key) || overrides.counts.has(key);
  }

  // 3) Render liste: Two-state knap + per-art antal
  const makeLi = (name) => {
    const key = normArtKey(name);
    const li = document.createElement('li');
    li.className = 'species-row';
    li.dataset.key = normArtKey(name);

    // --- navn ---
    const label = document.createElement('span');
    label.className = 'sp-name';
    label.textContent = name;
    label.classList.add(classifySpeciesName(name));

    // --- per-art ANTAL: operator + input ---
    const tools = document.createElement('div');
    tools.className = 'sp-tools';
    const op = document.createElement('select');
    op.className = 'sp-cnt-op';
    op.innerHTML = '<option value="gte">≥</option><option value="eq">=</option>';
    const val = document.createElement('input');
    val.className = 'sp-cnt-val';
    val.type = 'number';
    val.min = '1';
    val.step = '1';
    val.placeholder = 'Antal';
    val.inputMode = 'numeric';
    val.autocomplete = 'off';
    val.style.width = '6.5rem';

    // --- Two-state knap: Inkl. (grå baseline) ↔ Eksl. ---
    const btn = document.createElement('button');
    btn.className = 'sp-toggle';
    btn.type = 'button';
    btn.setAttribute('aria-label', `Filter for ${name}`);
    const isExcluded = () => overrides.exclude.has(key);
    const setBtnUI = () => {
      const excluded = isExcluded();
      btn.textContent = excluded ? 'Eksl.' : 'Inkl.';
      btn.classList.toggle('is-exclude', excluded);
      btn.classList.toggle('is-muted', !excluded); // “grå” stil til Inkl.
      li.dataset.filtered = (excluded || overrides.counts.has(key)) ? '1' : '0';
    };

    // Init fra state (per-art counts)
    const cf = overrides.counts.get(key);
    if (cf && Number.isFinite(Number(cf.value))) {
      op.value = (cf.mode === 'eq' ? 'eq' : 'gte');
      val.value = String(Math.floor(Number(cf.value)));
    }

    // Persistér antal (>0) eller ryd; opdater “filtreret”-markering
    const persistCount = () => {
      const raw = String(val.value ?? '').trim();
      const n = Number(raw);
      const valid = Number.isFinite(n);
      if (!valid || n <= 0) {
        overrides.counts.delete(key);
        op.value = 'gte';
        if (!isExcluded()) val.value = '';
      } else {
        overrides.counts.set(key, { mode: (op.value === 'eq' ? 'eq' : 'gte'), value: Math.floor(n) });
      }
      li.dataset.filtered = (isExcluded() || overrides.counts.has(key)) ? '1' : '0';
      refreshListVisibility();
      dirtyUI();
    };
    op.addEventListener('change', persistCount);
    val.addEventListener('input', persistCount);

    // Toggle exclude on/off (Two-state)
    btn.addEventListener('click', () => {
      if (isExcluded()) {
        overrides.exclude.delete(key); // -> Inkl. (grå)
      } else {
        overrides.exclude.add(key);    // -> Eksl.
      }
      if (!isExcluded() && (String(val.value ?? '').trim() === '0')) {
        overrides.counts.delete(key);
        val.value = '';
        op.value = 'gte';
      }
      setBtnUI();
      refreshListVisibility();
      dirtyUI();
    });

    setBtnUI();

    tools.append(op, val);
    li.append(label, tools, btn);
    return li;
  };

  // Render alle rækker
  const frag = document.createDocumentFragment();
  for (const a of arts) frag.appendChild(makeLi(a.navn));
  $advList.appendChild(frag);

  // 4) Søg + “Vis kun filtrerede”
  let showActiveOnly = false;

  function refreshListVisibility() {
    // VIGTIGT: brug samme normalisering som li.dataset.key (æ→ae, ø→oe, å→aa, mm.)
    const q = normArtKey(String($advSearch?.value ?? ''));
    $advList.querySelectorAll('.species-row').forEach(li => {
      const hitText = li.dataset.key.includes(q);
      const hitAct  = (!showActiveOnly) || li.dataset.filtered === '1';
      li.style.display = (hitText && hitAct) ? '' : 'none';
    });
  }

  if ($advSearch) {
    $advSearch.addEventListener('input',  refreshListVisibility);
    $advSearch.addEventListener('search', refreshListVisibility); // Android søgeknap/clear
    $advSearch.addEventListener('change', refreshListVisibility);
    // Hjælp mobilen: undgå autokorrektion/autocap
    try {
      $advSearch.setAttribute('autocapitalize', 'none');
      $advSearch.setAttribute('autocomplete',   'off');
      $advSearch.setAttribute('autocorrect',    'off');
      $advSearch.setAttribute('spellcheck',     'false');
      $advSearch.setAttribute('enterkeyhint',   'search');
    } catch {}
  }

  // 5) Nulstil (challenge + confirm) – rydder exclude + counts
  function makeChallengeLocal() {
    const a = Math.floor(Math.random()*9)+1, b = Math.floor(Math.random()*9)+1;
    const useMinus = Math.random() < 0.4 && a > b;
    const op = useMinus ? '-' : '+';
    const res = useMinus ? (a - b) : (a + b);
    return { text: `Sikkerhedstjek: Hvad er ${a} ${op} ${b}?`, answer: res };
  }
  async function guardedResetOverridesLocal() {
    const { text, answer } = makeChallengeLocal();
    const input = self.prompt(text, '');
    if (input === null) { if ($advStatus) { $advStatus.textContent = 'Nulstilling annulleret.'; setTimeout(() => $advStatus.textContent='', 1500); } return false; }
    const parsed = Number(String(input).trim().replace(',', '.'));
    if (!Number.isFinite(parsed) || parsed !== answer) { self.alert('Forkert svar – nulstilling blev afbrudt.'); return false; }
    const ok = self.confirm('Er du sikker på, at du vil nulstille alle artsfiltre? (Eksl. + antal)');
    if (!ok) { if ($advStatus) { $advStatus.textContent = 'Nulstilling annulleret.'; setTimeout(() => $advStatus.textContent='', 1500); } return false; }
    overrides.exclude.clear();
    overrides.counts.clear();
    $advList.querySelectorAll('.species-row').forEach(li => {
      const btn = li.querySelector('.sp-toggle');
      const op = li.querySelector('.sp-cnt-op');
      const val = li.querySelector('.sp-cnt-val');
      if (btn) { btn.textContent = 'Inkl.'; btn.classList.remove('is-exclude'); btn.classList.add('is-muted'); }
      if (op) op.value = 'gte';
      if (val) val.value = '';
      li.dataset.filtered = '0';
    });
    refreshListVisibility();
    if ($advStatus) { $advStatus.textContent = 'Nulstillet – husk at trykke Gem.'; setTimeout(() => $advStatus.textContent='', 2500); }
    return true;
  }
  if ($advClear) $advClear.addEventListener('click', (e) => { e.preventDefault(); guardedResetOverridesLocal().catch(console.error); });

  // 6) Export / Import (back-compat: inkluderer tom include[])
  function downloadJson(filename, obj) {
    const blob = new Blob([JSON.stringify(obj, null, 2)], { type: 'application/json;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a'); a.href = url; a.download = filename; a.click(); URL.revokeObjectURL(url);
  }
  function nowStamp() {
    const d = new Date(); const p = (n) => String(n).padStart(2, '0');
    return `${d.getFullYear()}${p(d.getMonth()+1)}${p(d.getDate())}`;
  }
  if ($advExport) {
    $advExport.addEventListener('click', () => {
      const ov = {
        include: [], // two-state: behold tom for back-compat
        exclude: Array.from(overrides.exclude),
        counts : countsMapToObj(overrides.counts)
      };
      downloadJson(`dofnot-adv-filters-${nowStamp()}.json`, ov);
      if ($advStatus) { $advStatus.textContent = 'Eksporteret.'; setTimeout(() => $advStatus.textContent='', 1500); }
    });
  }
  if ($advImport && $advFile) {
    $advImport.addEventListener('click', () => $advFile.click());
    $advFile.addEventListener('change', async () => {
      const file = $advFile.files?.[0]; if (!file) return;
      try {
        const text = await file.text(); const data = JSON.parse(text ?? '{}');
        const exc = Array.isArray(data.exclude) ? data.exclude.map(normArtKey) : [];
        const countsObj = (data.counts && typeof data.counts === 'object') ? data.counts : {};
        const newCounts = toCountsMap(countsObj);
        const replace = self.confirm('Importér: Erstat nuværende filtre? (OK = erstat · Annuller = flet)');
        if (replace) {
          overrides.exclude = new Set(exc);
          overrides.counts = newCounts;
        } else {
          exc.forEach(k => overrides.exclude.add(k));
          newCounts.forEach((v, k) => { overrides.counts.set(k, v); });
        }
        // Opdater UI efter import
        $advList.querySelectorAll('.species-row').forEach(li => {
          const key = li.dataset.key;
          const btn = li.querySelector('.sp-toggle');
          const op = li.querySelector('.sp-cnt-op');
          const val = li.querySelector('.sp-cnt-val');
          const excluded = overrides.exclude.has(key);
          if (btn) {
            btn.textContent = excluded ? 'Eksl.' : 'Inkl.';
            btn.classList.toggle('is-exclude', excluded);
            btn.classList.toggle('is-muted', !excluded);
          }
          const cf = overrides.counts.get(key);
          if (cf) {
            if (op) op.value = (cf.mode === 'eq' ? 'eq' : 'gte');
            if (val) val.value = String(cf.value);
          } else {
            if (op) op.value = 'gte';
            if (val) val.value = '';
          }
          li.dataset.filtered = hasRowFilter(key) ? '1' : '0';
        });
        refreshListVisibility();
        if ($advStatus) { $advStatus.textContent = 'Importeret – husk at trykke Gem.'; setTimeout(() => $advStatus.textContent='', 2500); }
      } catch {
        alert('Import fejlede: ugyldig JSON.');
      } finally {
        $advFile.value = '';
      }
    });
  }

  // 7) Gem (SW + localStorage + server best-effort)
  if ($advSave) {
    $advSave.addEventListener('click', async () => {
      const ov = {
        include: [], // two-state: ikke brugt (back-compat)
        exclude: Array.from(overrides.exclude),
        counts : countsMapToObj(overrides.counts)
      };
      // SW/IndexedDB
      await postToSW({ type: 'SAVE_SPECIES_OVERRIDES', overrides: ov });
      // localStorage (cache)
      try { localStorage.setItem('dofnot-species-overrides', JSON.stringify(ov)); } catch {}
      // server (best effort – helper håndterer 404/410)
      await saveSpeciesOverridesToServer(ov);
      if ($advStatus) { $advStatus.textContent = 'Gemt ✔'; setTimeout(() => $advStatus.textContent='', 1500); }
    });
  }

  // Første visning – markér rows med aktive filtre og anvend søg/visning
  $advList.querySelectorAll('.species-row').forEach(li => {
    const key = li.dataset.key;
    li.dataset.filtered = hasRowFilter(key) ? '1' : '0';
  });
  refreshListVisibility();
}

// Hook in ved DOMContentLoaded (efter eksisterende init)
document.addEventListener('DOMContentLoaded', () => {
  if (document.getElementById('adv-list')) {
    initAdvancedFilteringPage().catch(console.error);
  }
});
