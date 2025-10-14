/* sw.js – path-agnostisk caching + push + prefs (IndexedDB) */

/* ───────────── Scope/URL helpers ───────────── */
const SCOPE = self.registration ? self.registration.scope : self.location.href;
const toURL = (p) => new URL(p, SCOPE).toString();

/* ───────────── CACHE ───────────── */
const CACHE_NAME = 'dofnot-static-v2';
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
      if (USER_ID) await syncPrefsFromServer(USER_ID);
    } catch {}
    await self.clients.claim();
  })());
});

/* ───────────── FETCH (offline + SWR) ───────────── */
self.addEventListener('fetch', (event) => {
  const req = event.request;
  if (req.method !== 'GET') return;

  // Navigationer: net -> cache(index.html) -> offline fallback
  if (req.mode === 'navigate') {
    event.respondWith((async () => {
      try {
        return await fetch(req);
      } catch {
        const cache = await caches.open(CACHE_NAME);
        const offlineUrl = toURL('index.html');
        const cached = await cache.match(offlineUrl);
        return cached || new Response('\n## Offline\n', { headers: { 'Content-Type': 'text/html' } });
      }
    })());
    return;
  }

  // Statiske assets: stale-while-revalidate
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
const DB_VER = 2;
const STORE_PREFS = 'prefs';

function idbOpen() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, DB_VER);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains(STORE_PREFS)) {
        db.createObjectStore(STORE_PREFS, { keyPath: 'id' });
      }
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
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
    req.onerror = () => reject(req.error);
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

self.addEventListener('message', (event) => {
  const msg = event.data ?? {};
  if (msg.type === 'SAVE_PREFS' && msg.prefs) {
    event.waitUntil(idbPutPrefs(msg.prefs));
  } else if (msg.type === 'SET_USER' && msg.user_id) {
    USER_ID = msg.user_id;
    event.waitUntil(syncPrefsFromServer(USER_ID));
  }
});

/* ───────────── PUSH ───────────── */
self.addEventListener('push', (event) => {
  event.waitUntil(handlePush(event));
});

async function handlePush(event) {
  let data = {};
  try { if (event.data) data = event.data.json(); } catch {}
  const url = data && data.url;

  let items = [];
  if (url) {
    try {
      const res = await fetch(url, { cache: 'no-cache' });
      if (res.ok) {
        const batch = await res.json();
        items = Array.isArray(batch.items) ? batch.items : [];
      }
    } catch {}
  }

  // Hent lokale prefs (hydreres evt. fra server via SET_USER)
  let prefs = {};
  try { prefs = (await idbGetPrefs()) ?? {}; } catch { prefs = {}; }

  const useFilter = Object.keys(prefs).length > 0;
  const filtered = useFilter ? filterItemsByPrefs(items, prefs) : items;

// Undertryk notifikation hvis der kom en batch-url men intet matcher
  if (url && filtered.length === 0) return;

  // Brug de filtrerede items hvis der er nogen, ellers alle hentede items
  const list = (filtered.length ? filtered : items) ?? [];

  // Intet at vise
  if (!Array.isArray(list) || list.length === 0) return;

  // Én notifikation pr. observation
  const notifPromises = list.map((r) => {
    const obsid = (r.obsid ?? '').toString().trim();
    const antal = (r.antal ?? '').toString().trim();
    const art   = (r.art   ?? '').toString().trim();
    const lok   = (r.lok   ?? '').toString().trim();
    const adf   = (r.adf   ?? '').toString().trim();
    const fornavn   = (r.fornavn   ?? '').toString().trim();
    const efternavn = (r.efternavn ?? '').toString().trim();

    // title: "antal art, lok"
    const titleParts = [ [antal, art].filter(Boolean).join(' ') ]
      .concat(lok ? [`, ${lok}`] : []);
    const title = titleParts.join('').trim();

    // body: "adf, fornavn efternavn"
    const navn = [fornavn, efternavn].filter(Boolean).join(' ').trim();
    const body = [adf, navn].filter(Boolean).join(', ').trim();

    // Destination: obsid indsat i DOFbasen-URL
    const urlToOpen = obsid
      ? `https://dofbasen.dk/popobs.php?obsid=${encodeURIComponent(obsid)}&summering=tur&obs=obs`
      : (data.url ?? '/');

    // Unik tag pr. observation ⇒ ingen erstatning/kollision
    const tag = obsid ? `obs-${obsid}` : `obs-${crypto.randomUUID?.() || Math.random().toString(36).slice(2)}`;

    return self.registration.showNotification(title || 'Ny observation', {
      body,
      tag,              // ← unik pr. obs
      renotify: true,   // ← opfylder dit ønske; har kun effekt ved samme tag
      timestamp: Date.now(),        // Hjælp Android med korrekt sortering (nyeste øverst)
      data: { url: urlToOpen },
    });
  });

  await Promise.all(notifPromises);


  await self.registration.showNotification(title, {
    body,
    tag,
    renotify: true,
    data: { url: urlToOpen },
  });
}

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

/* ───────────── Notification click ───────────── */
self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  const raw = (event.notification.data && event.notification.data.url) || './';
  event.waitUntil((async () => {
    const targetUrl = new URL(raw, SCOPE).href;
    const all = await self.clients.matchAll({ type: 'window', includeUncontrolled: true });
    const client = all.find((c) => c.url === targetUrl);
    if (client) { await client.focus(); return; }
    await self.clients.openWindow(targetUrl);
  })());
});