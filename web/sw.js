/* sw.js – path-agnostisk caching + push + prefs (IndexedDB) */

/* ───────────── Scope/URL helpers ───────────── */
const SCOPE = self.registration ? self.registration.scope : self.location.href;
const toURL = (p) => new URL(p, SCOPE).toString();

/* ───────────── CACHE ───────────── */
// Bump cache + DB-version for at trigge opgradering
const CACHE_NAME = 'dofnot-static-v3';
const PRECACHE = [
  'index.html',
  'app.js',
  'manifest.webmanifest',
  'icons/icon-192.png',
  'icons/icon-512.png',
  'icons/maskable-512.png',
].map(toURL);

self.addEventListener('install', (e) => {
  self.skipWaiting();
  e.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(PRECACHE)).catch(() => {})
  );
});

self.addEventListener('activate', (e) => {
  e.waitUntil((async () => {
    const keys = await caches.keys();
    await Promise.all(keys.filter((k) => k !== CACHE_NAME).map((k) => caches.delete(k)));
    try {
      if (USER_ID) {
        await syncPrefsFromServer(USER_ID);
        await syncSpeciesFromServer(USER_ID); // NYT: hent arts‑filtre
      }
    } catch {}
    await self.clients.claim();
  })());
});

/* ───────────── FETCH (offline + SWR) ───────────── */
self.addEventListener('fetch', (event) => {
  const req = event.request;
  if (req.method !== 'GET') return;

  const url = new URL(req.url);

  // Navigationer: net -> cache(index.html) -> offline fallback
  if (req.mode === 'navigate') {
    event.respondWith((async () => {
      try { return await fetch(req); }
      catch {
        const cache = await caches.open(CACHE_NAME);
        const offlineUrl = toURL('index.html');
        const cached = await cache.match(offlineUrl);
        return cached || new Response('\n## Offline\n', { headers: { 'Content-Type': 'text/html' } });
      }
    })());
    return;
  }

  // Network-first for kode og data (JS/CSS/API/obs-json)
  const isCode = url.pathname.endsWith('.js') || url.pathname.endsWith('.css');
  const isData = url.pathname.startsWith('/api/') || url.pathname.includes('/obs/');
  if (isCode || isData) {
    event.respondWith((async () => {
      try { return await fetch(req, { cache: 'no-store' }); }
      catch {
        const cache = await caches.open(CACHE_NAME);
        return cache.match(req);
      }
    })());
    return;
  }

  // Default: stale-while-revalidate for øvrige assets (billeder m.m.)
  event.respondWith((async () => {
    const cache = await caches.open(CACHE_NAME);
    const cached = await cache.match(req);
    const fetched = fetch(req).then((res) => {
      if (res && res.ok) cache.put(req, res.clone());
      return res;
    }).catch(() => cached);
    return cached || fetched;
  })());
});

/* ───────────── IndexedDB til præferencer ───────────── */
const DB_NAME = 'dofnot-db';
const DB_VER  = 4;                 // bump (tidl. 3)
const STORE_PREFS = 'prefs';
const STORE_SPECIES = 'species';
const STORE_SETTINGS = 'settings';

// Opret alle nødvendige stores
function ensureStores(db) {
  if (!db.objectStoreNames.contains(STORE_PREFS)) {
    db.createObjectStore(STORE_PREFS, { keyPath: 'id' });
  }
  if (!db.objectStoreNames.contains(STORE_SPECIES)) {
    db.createObjectStore(STORE_SPECIES, { keyPath: 'id' });
  }
  if (!db.objectStoreNames.contains(STORE_SETTINGS)) {
    db.createObjectStore(STORE_SETTINGS, { keyPath: 'id' });
  }
}

function idbOpen() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, DB_VER);
    req.onupgradeneeded = () => {
      const db = req.result;
      ensureStores(db);
    };
    req.onsuccess = () => {
      const db = req.result;
      const hasAll =
        db.objectStoreNames.contains(STORE_PREFS) &&
        db.objectStoreNames.contains(STORE_SPECIES) &&
        db.objectStoreNames.contains(STORE_SETTINGS);

      if (hasAll) { resolve(db); return; }

      // Manglende store i eksisterende DB → lav en migrations-opgradering
      const newVer = db.version + 1;
      db.close();
      const req2 = indexedDB.open(DB_NAME, newVer);
      req2.onupgradeneeded = () => ensureStores(req2.result);
      req2.onsuccess = () => resolve(req2.result);
      req2.onerror   = () => reject(req2.error);
    };
    req.onerror = () => reject(req.error);
  });
}

/* Settings (opt-out/DND) */
async function idbPutSettings(settings) {
  const db = await idbOpen();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE_SETTINGS, 'readwrite');
    tx.objectStore(STORE_SETTINGS).put({ id: 'settings', ...settings, ts: Date.now() });
    tx.oncomplete = () => resolve(true);
    tx.onerror = () => reject(tx.error);
  });
}
async function idbGetSettings() {
  const db = await idbOpen();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE_SETTINGS, 'readonly');
    const req = tx.objectStore(STORE_SETTINGS).get('settings');
    req.onsuccess = () => resolve(req.result || null);
    req.onerror   = () => reject(req.error);
  });
}
async function isOptedOutSW() {
  try {
    const s = await idbGetSettings();
    return !!(s && s.opted_out);
  } catch { return false; }
}

async function idbPutPrefs(prefs) {
  const db = await idbOpen();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE_PREFS, 'readwrite');
    tx.objectStore(STORE_PREFS).put({ id: 'prefs', data: prefs, ts: Date.now() });
    tx.oncomplete = () => resolve(true);
    tx.onerror = () => reject(tx.error);
  });
}
async function idbGetPrefs() {
  const db = await idbOpen();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE_PREFS, 'readonly');
    const req = tx.objectStore(STORE_PREFS).get('prefs');
    req.onsuccess = () => resolve((req.result && req.result.data) ?? null);
    req.onerror   = () => reject(req.error);
  });
}

// NYT: species overrides
async function idbPutSpecies(overrides) {
  const db = await idbOpen();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE_SPECIES, 'readwrite');
    tx.objectStore(STORE_SPECIES).put({ id: 'overrides', data: overrides, ts: Date.now() });
    tx.oncomplete = () => resolve(true);
    tx.onerror = () => reject(tx.error);
  });
}
async function idbGetSpecies() {
  const db = await idbOpen();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE_SPECIES, 'readonly');
    const req = tx.objectStore(STORE_SPECIES).get('overrides');
    req.onsuccess = () => resolve((req.result && req.result.data) ?? { include: [], exclude: [] });
    req.onerror   = () => reject(req.error);
  });
}


/* ───────────── User-tilstand + sync fra server ───────────── */
let USER_ID = null;

async function syncPrefsFromServer(userId) {
  if (!userId) return;
  try {
    const url = new URL('/api/prefs/user?user_id=' + encodeURIComponent(userId), SCOPE).toString();
    const r = await fetch(url, { cache: 'no-cache' });
    if (r.ok) {
      const data = await r.json();
      if (data && data.prefs && Object.keys(data.prefs).length) {
        await idbPutPrefs(data.prefs);
      }
    }
  } catch {}
}

// Hent arts‑overrides fra server (best effort; tolerér 404/410/empty)
async function syncSpeciesFromServer(userId) {
  if (!userId) return;
  try {
    const url = new URL('/api/prefs/user/species?user_id=' + encodeURIComponent(userId), SCOPE).toString();
    const r = await fetch(url, { cache: 'no-cache' });
    if (!r.ok) return; // server behøver ikke understøtte GET
    const data = await r.json();
    if (data && typeof data === 'object') {
      // forventer { include:[], exclude:[], counts:{ key:{mode,value} } }
      await idbPutSpecies(data);
    }
  } catch {}
}

self.addEventListener('message', (event) => {
  const msg = event.data ?? {};
  if (msg.type === 'SAVE_PREFS' && msg.prefs) {
    event.waitUntil(idbPutPrefs(msg.prefs));
  } else if (msg.type === 'SET_USER' && msg.user_id) {
    USER_ID = msg.user_id;
    event.waitUntil((async () => {
      await syncPrefsFromServer(USER_ID);
      await syncSpeciesFromServer(USER_ID);
    })());
  } else if (msg.type === 'SAVE_SPECIES_OVERRIDES' && msg.overrides) {
    event.waitUntil(idbPutSpecies(msg.overrides));
  } else if (msg.type === 'GET_SPECIES_OVERRIDES' && event.ports?.[0]) {
    event.waitUntil((async () => {
      const ov = await idbGetSpecies();
      event.ports[0].postMessage({ overrides: ov });
    })());
  } else if (msg.type === 'SET_OPT_OUT') { // NYT
    event.waitUntil(idbPutSettings({ opted_out: !!msg.opted_out }));
  }
});

// ---------- PUSH: filtrér først prefs (region/kategori), derefter arts-overrides ----------

function expandPrefValueToCats(sel) {
  const v = String(sel ?? '').toLowerCase();
  if (v === 'su')   return new Set(['su']);
  if (v === 'sub')  return new Set(['su','sub']);
  if (v === 'alle') return new Set(['su','sub','alm','bemaerk']); // ensartet
  return new Set(); // 'none'
}

function normalizeSpeciesName(s) {
  return String(s || '').trim().toLowerCase();
}

function applyRegionCategoryFilter(items, prefs) {
  if (!items || !items.length) return [];
  const catKey = (v) => String(v ?? '').toLowerCase();
  return items.filter((it) => {
    // Afdeling-felt: prøv flere varianter
    const afd = String(
      it.dof_afdeling ?? it.afdeling ?? it.region_name ?? it.region ?? ''
    );
    // Kategori-felt: prøv flere varianter
    const cat = catKey(it.kategori ?? it.cat ?? it.last_kategori);
    const sel = String(prefs[afd] ?? 'none').toLowerCase();
    const allowed = expandPrefValueToCats(sel);
    return allowed.has(cat);
  });
}

function applySpeciesOverrides(items, overrides) {
  if (!items || !items.length) return [];
  const inc = new Set((overrides?.include || []).map(normArtKey));
  const exc = new Set((overrides?.exclude || []).map(normArtKey));
  return items.filter((it) => {
    const key = normArtKey(it.art);
    if (exc.has(key)) return false;     // udeluk altid
    if (inc.has(key)) return true;      // vis altid (back‑compat)
    return true;                        // ellers behold (styret af forrige filter)
  });
}

function parseAntalToNumber(val) {
  if (val == null) return null;
  const s = String(val).toLowerCase().replace(',', '.');
  const m = s.match(/\d+(?:\.\d+)?/g);
  if (!m || !m.length) return null;
  return Math.max(...m.map(Number));
}

function applyPerSpeciesCount(items, countsObj) {
  if (!items || !items.length) return [];
  const counts = (countsObj && typeof countsObj === 'object') ? countsObj : {};
  return items.filter((it) => {
    const key = normArtKey(it.art);
    const cf  = counts[key];
    if (!cf || cf.value == null) return true;
    const a = parseAntalToNumber(it.antal);
    if (!Number.isFinite(a)) return false;
    return (cf.mode === 'eq') ? (a === cf.value) : (a >= cf.value);
  });
}


/* ───────────── PUSH ───────────── */


// --- PUSH: vis ALTID en synlig notifikation på iOS ---
// (krav i Safari – ellers kan permission blive trukket tilbage)  [Apple doc]
self.addEventListener('push', (event) => {
  event.waitUntil((async () => {
    // DND: vis intet hvis slået fra lokalt
    if (await isOptedOutSW()) return;

    // Fortsæt med eksisterende push-flow
    await handlePush(event);
  })());
});

// Små helpers: læs prefs/overrides
async function getUserPrefsSafe() {
  try { return (await idbGetPrefs()) || {}; } catch { return {}; }
}
async function getSpeciesOverridesSafe() {
  try { return (await idbGetSpecies()) || {}; } catch { return {}; }
}

// Aktive filtre?
function hasAfdPrefs(prefs) {
  if (!prefs || typeof prefs !== 'object') return false;
  return Object.values(prefs).some(v => ['su','sub','alle'].includes(String(v).toLowerCase()));
}
function hasSpeciesFilters(ov) {
  if (!ov || typeof ov !== 'object') return false;
  const exc = Array.isArray(ov.exclude) && ov.exclude.length > 0;
  const cnt = ov.counts && typeof ov.counts === 'object' && Object.keys(ov.counts).length > 0;
  return !!(exc || cnt);
}

// Anvend kun relevante filtre
function applyAllFilters(items, prefs, overrides) {
  let out = Array.isArray(items) ? items.slice() : [];
  try {
    if (hasAfdPrefs(prefs) && typeof applyRegionCategoryFilter === 'function') {
      out = applyRegionCategoryFilter(out, prefs);
    }
    if (hasSpeciesFilters(overrides)) {
      if (typeof applySpeciesOverrides === 'function') {
        out = applySpeciesOverrides(out, overrides || {});
      }
      if (typeof applyPerSpeciesCount === 'function') {
        const counts = (overrides && overrides.counts) || {};
        out = applyPerSpeciesCount(out, counts);
      }
    }
  } catch (_) { /* ignore */ }
  return out;
}

async function handlePush(event) {
  // 1) Parse payload forsigtigt
  let data = {};
  try { if (event.data) data = event.data.json(); } catch (_) {}
  const fbTitle = data.title || 'Ny besked';
  const fbBody  = data.body  || '';
  const fbUrl   = data.url   || '/';

  // 2) Batch: prøv at bygge flere notifikationer – ellers falder vi tilbage
  try {
    if (typeof data.url === 'string' && data.url.startsWith('/batches/')) {
      const res = await fetch(data.url, { cache: 'no-cache' });
      if (res.ok) {
        const batch = await res.json();
        const items = Array.isArray(batch.items) ? batch.items : [];

        if (items.length > 0) {
          const prefs = await getUserPrefsSafe();
          const overrides = await getSpeciesOverridesSafe();
          const wantFilter = hasAfdPrefs(prefs) || hasSpeciesFilters(overrides);

          if (wantFilter) {
            const filtered = applyAllFilters(items, prefs, overrides);
            if (filtered.length === 0) return; // intet matcher → ingen notifikationer
            items.splice(0, items.length, ...filtered);
          }

          // Vis en notifikation pr. observation (begræns evt. til 5)
          const notifPromises = items.slice(0, 5).map((it) => {
            const antal = (it.antal ?? '').toString().trim();
            const art   = (it.art   ?? '').toString().trim();
            const lok   = (it.lok   ?? '').toString().trim();
            const adf   = (it.adf   ?? '').toString().trim();
            const navn  = [it.fornavn, it.efternavn].filter(Boolean).join(' ').trim();

            const title = [[antal, art].filter(Boolean).join(' '), lok].filter(Boolean).join(', ') || fbTitle;
            const body  = [adf, navn].filter(Boolean).join(', ') || fbBody;

            const cat = String(it.kategori ?? it.cat ?? it.last_kategori ?? '').toLowerCase();

            // Foretræk intern tråd for su/sub, men behold DOFbasen for bemærk/andet
            let url;
            if ((cat === 'su' || cat === 'sub') && it.ymd && it.thread_id) {
              // Brug loknr-baseret thread_id fra serveren
              url = new URL(`https://dofnot.chfotofilm.dk/thread.html?date=${encodeURIComponent(it.ymd)}&id=${encodeURIComponent(it.thread_id)}`, SCOPE).toString();
            } else {
              url = it.obsid
                ? `https://dofbasen.dk/popobs.php?obsid=${encodeURIComponent(it.obsid)}&summering=tur&obs=obs`
                : fbUrl;
            }

            const tag = it.thread_id
              ? `thr-${it.thread_id}`
              : (it.obsid ? `obs-${it.obsid}` : `obs-${crypto.randomUUID?.() ?? Math.random().toString(36).slice(2)}`);

            return self.registration.showNotification(title, {
              body,
              tag,
              renotify: true,
              timestamp: Date.now(),
              data: { url }
            });
          });

          await Promise.all(notifPromises);
          return; // ✅ mindst én notifikation vist
        }
      }
    }
  } catch (e) {
    console.warn('[SW] batch fetch/parse failed:', e);
  }

  // 3) Fallback: vis altid mindst én synlig notifikation
  try {
    await self.registration.showNotification(fbTitle, {
      body: fbBody,
      tag: 'single-' + (crypto.randomUUID?.() ?? Math.random().toString(36).slice(2)),
      renotify: false,
      timestamp: Date.now(),
      data: { url: fbUrl }
    });
  } catch (e) {
    console.error('[SW] showNotification failed:', e);
  }
}

// --- ÉN fælles notificationclick-handler (fjern dubletter) ---
self.addEventListener('notificationclick', (event) => {
  event.notification?.close();

  const rawUrl = event.notification?.data?.url || '/';
  const target = new URL(rawUrl, SCOPE).toString();

  event.waitUntil((async () => {
    try {
      const all = await clients.matchAll({ type: 'window', includeUncontrolled: true });

      // 1) Hvis en fane allerede er præcis på target → fokusér den
      for (const c of all) {
        if (c.url === target && 'focus' in c) {
          return c.focus();
        }
      }

      // 2) Fokusér første fane på samme origin og forsøg at navigere
      for (const c of all) {
        try {
          if (new URL(c.url).origin === self.location.origin) {
            await (c.focus?.());
            if ('navigate' in c) {
              try { return await c.navigate(target); } catch {}
            }
            break; // prøv ny fane hvis navigate ikke findes/fejler
          }
        } catch {}
      }

      // 3) Åbn ny fane/vinge
      try {
        return await clients.openWindow(target);
      } catch (e) {
        // 4) Sikkerhedsnet: åbn forsiden
        return clients.openWindow(toURL('/'));
      }
    } catch (e) {
      // sidste sikkerhedsnet
      return clients.openWindow(toURL('/'));
    }
  })());
});




function filterItemsByPrefs(items, prefs) {
  if (!items || !items.length) return [];
  return items.filter((it) => {
    const afd = (it.dof_afdeling ?? '').toString();
    const cat = (it.kategori ?? '').toString().toLowerCase();
    const sel = (prefs[afd] ?? 'none').toLowerCase();
    if (sel === 'none') return false;
    if (sel === 'alle') return true;
    if (sel === 'sub') return cat === 'sub';
    if (sel === 'su') return cat === 'su';
    return false;
  });
}

// Ensartet arts-normalisering (samme som i app.js)
function normArtKey(s) {
  let t = String(s ?? '').normalize('NFKD');
  t = t
    .replace(/\u00C6/g,'AE').replace(/\u00E6/g,'ae')
    .replace(/\u00D8/g,'OE').replace(/\u00F8/g,'oe')
    .replace(/\u00C5/g,'AA').replace(/\u00E5/g,'aa');
  t = t.replace(/[\u0300-\u036f]/g, '')
       .replace(/[\"'«»„”“’”“\[\]\{\}]/g, ' ')
       .replace(/[.,;:]/g, ' ')
       .replace(/[\u2010-\u2014\u2212]/g, '-')
       .replace(/\u00A0/g, ' ')
       .replace(/\s+/g, ' ')
       .trim()
       .toLowerCase();
  return t;
}