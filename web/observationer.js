const BASE = document.baseURI ?? location.href;
const abs = (p) => new URL(p, BASE).toString();
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

/* === Indstillinger === */
const LOG_BASE = './logs';
const CONCURRENCY = 8;
const MAX_ITEMS_PER_REGION = 250;
const TZ = 'Europe/Copenhagen'; // DK‑tid til "i dag"

const $status   = document.getElementById('obs-status');
const $list     = document.getElementById('obs-list');
const $hideZero = document.getElementById('toggle-hide-zero');
const $limitSel = document.getElementById('limit-select');

function setStatus(m) { $status.textContent = m; }

/* Gem/indlæs valgt limit (valgfrit – for bedre UX) */
(function initLimitFromStorage(){
  const saved = localStorage.getItem('obs-limit');
  if (saved && ['5','10','all'].includes(saved)) {
    $limitSel.value = saved;
  }
})();
$limitSel.addEventListener('change', () => {
  localStorage.setItem('obs-limit', $limitSel.value);
  renderFromCache(); // undgå gen‑fetch; vi har dataToday i cache
});

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
  "DOF Sydøstjylland": "ostjylland",
  "DOF Vestjylland": "vestjylland",
  "DOF Østjylland": "ostjylland",
  "DOF Nordvestjylland": "nordvestjylland",
  "DOF Nordjylland": "nordjylland",
};

/* Hent prefs fra serverens DB */
async function loadUserPrefsFromServer() {
  const userId = getOrCreateUserId();
  const url = abs('api/prefs/user') + '?user_id=' + encodeURIComponent(userId);
  const r = await fetch(url, { cache: 'no-cache' });
  if (!r.ok) throw new Error('Kunne ikke hente brugerpræferencer');
  const data = await r.json();
  return data?.prefs || {};
}

/* Fortolkning af valg: SU → {su}, SUB → {su,sub}, "alle" (Bemærkelsesværdige) → {su,sub} */
function expandPrefValueToAllowedSources(val) {
  const v = String(val || '').toLowerCase();
  if (v === 'su')  return new Set(['su']);
  if (v === 'sub') return new Set(['su','sub']);
  if (v === 'alle') return new Set(['su','sub']); // bemærkelsesværdige = SUB
  return new Set();
}

/* Plan for hvilke filer der skal hentes pr. region */
function buildRegionPlan(prefs) {
  const plan = new Map(); // region -> { allow:Set<'su'|'sub'>, files:string[] }
  for (const [afd, sel] of Object.entries(prefs || {})) {
    const slug = AFD_TO_REGION[afd];
    if (!slug) continue;
    const allow = expandPrefValueToAllowedSources(sel);
    if (allow.size === 0) continue;
    const entry = plan.get(slug) || { allow: new Set(), files: [] };
    if (allow.has('su'))  { entry.allow.add('su');  entry.files.push(`${LOG_BASE}/su-${slug}.log`); }
    if (allow.has('sub')) { entry.allow.add('sub'); entry.files.push(`${LOG_BASE}/sub-${slug}.log`); }
    plan.set(slug, entry);
  }
  return plan;
}

function setSelectionOnList(regions) {
  const label = regions.length
    ? `Valgte regioner: ${regions.join(', ')}`
    : 'Ingen regioner valgt (i dine præferencer).';
  $list.setAttribute('aria-label', label);
  $list.dataset.selection = regions.join(','); // til scripts der vil læse værdien
}

/* ==== Parsing helpers ==== */
function extractCoordsFromString(s) {
  if (!s) return { lon: null, lat: null };
  const m = s.match(/(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)/);
  if (!m) return { lon: null, lat: null };
  const lon = Number(m[1]), lat = Number(m[2]);
  return { lon: Number.isFinite(lon) ? lon : null, lat: Number.isFinite(lat) ? lat : null };
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
  const m = header.match(/\]\s+(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})/);
  return m ? new Date(`${m[1]}T${m[2]}`) : null; // lokal tid
}
function parseLogLine(line) {
  if (!line || !line.trim()) return null;
  const parts = line.split(' · ').map(s => s.trim());
  if (parts.length < 7) return null;

  const header       = parts[0];
  const sourceBr     = parts[1]; // [su] eller [sub]
  const countSpecies = parts[2];
  const behavior     = parts[3] || '';
  const locality     = parts[4] || '';
  const org          = parts[5] || '';
  const observer     = parts[6] || '';

  const { lon, lat } = extractCoordsFromParts(parts);

  const mTag = header.match(/^\[(?<key>[^\]]+)]/);
  const key  = mTag?.groups?.key ?? '';
  const [srcFromKey, regionFromKey] = key.split('-', 2);
  const mSrc = sourceBr.match(/\[(?<src>su|sub)]/i);
  const source = (mSrc?.groups?.src || srcFromKey || '').toLowerCase();

  const mCount = countSpecies.match(/^\s*(\d+)\s+(.+)$/);
  const count  = mCount ? Number(mCount[1]) : null;
  const species = mCount ? mCount[2].trim() : countSpecies;

  const date = parseLocalDateFromHeader(header);

  return {
    key, source, region: regionFromKey || '',
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
      continue; // fil kan mangle – det er ok
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
      .then(list => results.push(...list))
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
  const li = el('li', 'obs-item'); const article = el('article');

  const header = el('header');
  header.append(
    el('span', 'badge badge-src', item.source.toUpperCase()),
    el('span', 'badge badge-region', item.region),
    el('span', 'species', `${item.count ?? ''} ${item.species}`)
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
    t.textContent = item.date.toLocaleString('da-DK', { timeZone: TZ || undefined });
  }
  byline.append(t);

  article.append(header, meta, byline);
  li.append(article);
  return li;
}

/* ==== Controller + cache ==== */
function ymdInTZ(d, tz) {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: tz || undefined, year:'numeric', month:'2-digit', day:'2-digit'
  }).format(d); // "YYYY-MM-DD"
}

let cache = { today: [], totalToday: 0, selectionRegions: [] };

function renderFromCache() {
  $list.innerHTML = '';

  const allToday = cache.today || [];
  // Vælg limit
  const val = $limitSel.value;
  const limit = (val === 'all') ? allToday.length : Number(val);
  const items = allToday.slice(0, limit);

  const frag = document.createDocumentFragment();
  for (const item of items) frag.appendChild(renderItem(item));
  $list.appendChild(frag);

  const shown = items.length;
  const total = allToday.length;
  const regCount = cache.selectionRegions.length;
  setStatus(`Viser ${shown} af ${total} observationer fra i dag i ${regCount} region${regCount>1?'er':''}.`);
}

async function loadAndRender() {
  $list.innerHTML = '';
  setStatus('Henter præferencer …');

  let prefs = {};
  try { prefs = await loadUserPrefsFromServer(); }
  catch (e) { setStatus('Kunne ikke hente præferencer fra serveren.'); return; }


  const plan = buildRegionPlan(prefs);
  const selected = [...plan.keys()];

  // Sæt attributter i stedet for at vise en separat <div>
  setSelectionOnList(selected);

  if (selected.length === 0) {
    setStatus('Ingen regioner har aktive valg (SU/SUB).');
    return;
  }

  setStatus('Henter observationer …');
  const data = await fetchAll(plan, $hideZero.checked, CONCURRENCY);
  if (!data.length) { setStatus('Ingen relevante observationer fundet.'); return; }

  // Kun i dag (DK-tid)
  const todayYMD = ymdInTZ(new Date(), TZ);
  const dataToday = data.filter(it =>
    it.date instanceof Date && !isNaN(it.date) && ymdInTZ(it.date, TZ) === todayYMD
  );
  if (!dataToday.length) { setStatus('Ingen observationer fra i dag matcher dine præferencer.'); return; }

  // Sortér nyeste først
  dataToday.sort((a,b) => (b.date?.getTime?.() ?? -Infinity) - (a.date?.getTime?.() ?? -Infinity));

  // Læg i cache og render efter valgt limit
  cache.today = dataToday;
  cache.totalToday = dataToday.length;
  renderFromCache();
}

/* ==== Events ==== */
$hideZero.addEventListener('change', () => loadAndRender()); // kræver refetch
document.addEventListener('DOMContentLoaded', () => { loadAndRender().catch(console.error); });