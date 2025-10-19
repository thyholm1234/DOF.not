(function () {
  const $ = (id) => document.getElementById(id);

  const $st = $('thread-status');
  const $panel = $('thread-panel');
  const $title = $('thread-title');
  const $sub = $('thread-sub');
  const $list = $('thread-events');
  const $frontControls = $('front-controls');

  // Forside-kontroller (for trådsammendrag)
  let $frontChkHideZero, $frontSelLimit, $frontSelSort, $frontChkPrefs;
  const SORT_PREFS_KEY = 'dofnot-use-prefs-sort';
  const SORT_MODE_KEY = 'dofnot-sort-mode';
  const frontState = {
    hideZero: true,
    limit: 0,
    // tænd/sluk for brugerpræferencer (default: til)
    usePrefs: (localStorage.getItem(SORT_PREFS_KEY) ?? '1') === '1',
    // kun to manuelle modes
    sortMode: localStorage.getItem(SORT_MODE_KEY) || 'date_desc', // 'date_desc' | 'alpha_asc'
  };

  // State
  let userPrefs = {};
  let threadEvents = [];
  let summaryItems = [];

  // Pref-baserede filtre (grundlæggende + avanceret)
  let allowedCatsByRegion = new Map(); // Map('DOF København' -> Set<'su'|'sub'>)
  let speciesOverrides = null;         // { include:[], exclude:[], counts:{ key:{mode,value} } }

  // Utils
  const isYMD = (s) => /^\d{4}-\d{2}-\d{2}$/.test(s || '');
  function todayYMDLocal() {
    const d = new Date();
    return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
  }
  function yesterdayYMDLocal() {
    const d = new Date();
    d.setDate(d.getDate()-1);
    return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
  }
  function fmtAge(iso) {
    if (!iso) return '';
    const t = new Date(iso).getTime();
    const s = (Date.now() - t)/1000;
    if (s < 60) return 'for få sek. siden';
    const m = Math.floor(s/60); if (m < 60) return `${m} min siden`;
    const h = Math.floor(m/60); return `${h} t siden`;
  }
  function parseRoute() {
    const qs = new URLSearchParams(location.search);
    return { date: qs.get('date') || 'today', id: qs.get('id') || '' };
  }
  async function fetchUserPrefs() {
    // Brug global helper fra app.js, ellers fallback til localStorage
    let userId = '';
    try {
      userId = typeof getOrCreateUserId === 'function'
        ? getOrCreateUserId()
        : (localStorage.getItem('dofnot-user-id') || '');
    } catch {}
    // Server-API med user_id (kræves af server.py)
    if (userId) {
      try {
        const r = await fetch(`./api/prefs/user?user_id=${encodeURIComponent(userId)}`, { cache: 'no-cache' });
        if (r.ok) {
          const data = await r.json();
          // API returnerer { prefs: {...}, ts, source }
          if (data && data.prefs && typeof data.prefs === 'object') return data.prefs;
        }
      } catch {}
    }
    // Fallback til lokalt cachede prefs (samme format som server.prefs)
    try {
      const raw = localStorage.getItem('dofnot-prefs');
      if (raw) return JSON.parse(raw);
    } catch {}
    return {};
  }
  async function fetchSummary(dateParam) {
    try {
      const r = await fetch(`./api/obs/summary?date=${encodeURIComponent(dateParam)}`, { cache: 'no-cache' });
      if (r.status === 204) return [];
      if (!r.ok) throw new Error(String(r.status));
      const arr = await r.json();
      return Array.isArray(arr) ? arr : [];
    } catch { return []; }
  }

  // DOM helpers
  
  function el(tag, cls, text) {
    const x = document.createElement(tag);
    if (cls) x.className = cls;
    if (text != null) x.textContent = text;
    return x;
  }
  function catClass(kat) { return `badge cat-${String(kat||'').toLowerCase()}`; }
  const dofUrl = (obsid) => `https://dofbasen.dk/popobs.php?obsid=${encodeURIComponent(obsid)}&summering=tur&obs=obs`;

  // Sortering
  function evTime(ev) { return ev.ts_thread_display || ev.ts_obs || ev.ts_seen || ''; }
  function evTimeMs(ev) { const t = evTime(ev); return t ? new Date(t).getTime() : 0; }
  function evCount(ev) {
    if (typeof ev.antal_num === 'number') return ev.antal_num;
    const n = parseInt(String(ev.antal_text || ''), 10);
    return Number.isFinite(n) ? n : NaN;
  }
  function sortModeFromPrefs(prefs) {
    const v = (prefs && (prefs.thread_sort || prefs.sort || prefs.event_sort)) || 'date_desc';
    switch (String(v).toLowerCase()) {
      case 'date_asc': return 'date_desc'; // vi understøtter kun faldende
      case 'alpha': case 'alpha_asc': return 'alpha_asc';
      case 'count_desc': return 'date_desc';
      case 'count_asc': return 'date_desc';
      case 'date_desc':
      default: return 'date_desc';
    }
  }
  function sortEvents(arr, mode) {
    const m = mode === 'prefs' ? sortModeFromPrefs(userPrefs) : mode;
    const a = [...arr];
    if (m === 'alpha_asc') {
      a.sort((x,y) => String(x.art||'').localeCompare(String(y.art||''), 'da', {sensitivity:'base'}));
    } else if (m === 'date_asc') {
      a.sort((x,y) => evTimeMs(x) - evTimeMs(y));
    } else if (m === 'count_desc') a.sort((x,y) => (evCount(y) || -Infinity) - (evCount(x) || -Infinity) || (evTimeMs(y) - evTimeMs(x)));
    else if (m === 'count_asc') a.sort((x,y) => (evCount(x) || Infinity) - (evCount(y) || Infinity) || (evTimeMs(y) - evTimeMs(x)));
    else a.sort((x,y) => evTimeMs(y) - evTimeMs(x));
    return a;
  }
  // Tråd-sammendrag sortering
  function sortThreads(arr, mode) {
    const m = mode === 'prefs' ? sortModeFromPrefs(userPrefs) : mode;
    const a = [...arr];
    const lastMs = (s) => s && s.last_ts_obs ? new Date(s.last_ts_obs).getTime() : 0;
    if (m === 'alpha_asc') {
      a.sort((x,y) => String(x.art||'').localeCompare(String(y.art||''), 'da', {sensitivity:'base'}));
    } else {
      // default: nyeste først
      a.sort((x,y) => lastMs(y) - lastMs(x));
    }
    return a;
  }

  // Normalisér noter: fjern linjeskift og ekstra mellemrum
  function normalizeNoteText(txt) {
    return String(txt || '')
      .replace(/\r?\n+/g, ' ')   // linjeskift -> mellemrum
      .replace(/\s{2,}/g, ' ')   // dublerede mellemrum -> enkelt
      .trim();
  }

  // Klassifikation af arter (fra CSV)
  let klassMap = null; // Map(lowercased navn -> 'alm'|'sub'|'su')
  function normName(s) {
    return String(s || '').trim().toLowerCase().replace(/\s+/g,' ');
  }
  function stripBrackets(s) {
    // fjern evt. omsluttende [ ... ]
    const m = String(s || '').trim();
    return m.startsWith('[') && m.endsWith(']') ? m.slice(1, -1).trim() : m;
  }
  async function ensureKlassMap() {
    if (klassMap) return klassMap;
    klassMap = new Map();
    try {
      const r = await fetch('./data/arter_filter_klassificeret.csv', { cache: 'no-cache' });
      if (!r.ok) return klassMap;
      const text = await r.text();
      const lines = text.split(/\r?\n/);
      for (let i = 1; i < lines.length; i++) {
        const line = lines[i];
        if (!line || !line.includes(';')) continue;
        const parts = line.split(';');
        if (parts.length < 3) continue;
        const artsnavn = parts[1]?.trim();
        const klass = parts[2]?.trim();
        if (!artsnavn || !klass) continue;
        const k = klass.toLowerCase(); // 'alm' | 'sub' | 'su'
        const key1 = normName(artsnavn);
        const key2 = normName(stripBrackets(artsnavn));
        if (!klassMap.has(key1)) klassMap.set(key1, k);
        if (!klassMap.has(key2)) klassMap.set(key2, k);
      }
    } catch {
      // ignorer – bruger fallback
    }
    return klassMap;
  }
  function artKategoriFromMap(artName) {
    if (!klassMap) return undefined;
    const k1 = klassMap.get(normName(artName));
    if (k1) return k1;
    const k2 = klassMap.get(normName(stripBrackets(artName)));
    return k2;
  }
  function resolveKategori(artName, fallbackKategori) {
    // Prøv CSV, ellers eksisterende felt, normaliser til 'alm'|'sub'|'su'|'bemaerk'
    const csvK = artKategoriFromMap(artName);
    if (csvK) return csvK;
    const fb = String(fallbackKategori || '').toLowerCase();
    if (fb === 'alm' || fb === 'sub' || fb === 'su' || fb === 'bemaerk') return fb;
    // også hvis fallback er 'almindelig' mv.
    if (fb.startsWith('alm')) return 'alm';
    if (fb.startsWith('sub')) return 'sub';
    if (fb === 'su') return 'su';
    return undefined;
  }

  // Render: event som card
  function renderEvent(ev) {
    const li = document.createElement('li'); li.className = 'obs-item';
    const article = el('article');
    // Toplinje venstre/højre
    const top = el('div','card-top');
    const left = el('div','left');
    const kat = ev.kategori || ev.cat || '';
    if (kat) left.appendChild(el('span', catClass(kat), String(kat).toUpperCase()));
    if (ev.region) left.appendChild(el('span','badge region', ev.region));
    const right = el('div','right');
    const tIso = ev.ts_thread_display || ev.ts_obs || ev.ts_seen || '';
    const t = tIso ? tIso.replace('T',' ').slice(0,16) : '';
    if (t) right.appendChild(el('span','badge region', t));
    top.append(left, right);
    article.appendChild(top);

    // Titel: antal + artsnavn (samme layout som summary)
    const title = el('div','title');
    const katForArt = resolveKategori(ev.art, ev.kategori);
    const artCls = katForArt ? ` cat-${katForArt}` : '';

    // Antal (brug antal_num hvis tilgængelig, ellers forsøg parse antal_text)
    let antalStr = '';
    if (typeof ev.antal_num === 'number' && Number.isFinite(ev.antal_num)) {
      const i = Math.trunc(ev.antal_num);
      antalStr = Math.abs(ev.antal_num - i) < 1e-9 ? String(i) : String(ev.antal_num);
    } else if (ev.antal_text) {
      const n = parseInt(String(ev.antal_text), 10);
      if (Number.isFinite(n)) antalStr = String(n);
    }
    if (antalStr) title.appendChild(el('span', `count${artCls}`, antalStr));

    const artSpan = el('span', `art-name${artCls}`, ev.art || '');
    title.appendChild(artSpan);
    article.appendChild(title);

    // Info
    const info = el('div','info');
    if (ev.adf) info.appendChild(el('span','', ev.adf));
    if (ev.lok) info.appendChild(el('span','', ev.lok));
    if (ev.observer) info.appendChild(el('span','', ev.observer));
    article.appendChild(info);

    // Kommentarspor: Turnote/Obsnote
    const notes = [];
    const tn = ev.turnoter || (ev.raw && (ev.raw.Turnoter || ev.raw.TurNoter));
    const fn = ev.fuglnoter || (ev.raw && (ev.raw.Fuglnoter || ev.raw.Fuglenoter));
    function pushNotes(txt, typeLabel) {
      const one = normalizeNoteText(txt);
      if (!one) return;
      notes.push({ type: typeLabel, text: one });
    }
    pushNotes(tn, 'Turnote');
    pushNotes(fn, 'Obsnote');

    if (notes.length) {
      const hr = document.createElement('hr');
      hr.style.border = '0';
      hr.style.borderTop = '1px solid var(--line)';
      hr.style.margin = '8px 0 10px';
      article.appendChild(hr);

      const thread = document.createElement('div');
      thread.className = 'comments';
      notes.forEach(n => {
        const row = document.createElement('div'); row.className = 'comment'; row.style.display='flex'; row.style.gap='8px';
        const pill = document.createElement('span'); pill.textContent = n.type; pill.className = 'badge';
        pill.style.background = '#eef2ff'; pill.style.color = '#1e3a8a'; pill.style.fontSize = '11px';
        const txt = document.createElement('div'); txt.className = 'comment-text'; txt.textContent = n.text;
        row.appendChild(pill); row.appendChild(txt); thread.appendChild(row);
      });
      article.appendChild(thread);
    }

    // Link
    if (ev.obsid) {
      const a = document.createElement('a');
      a.href = `https://dofbasen.dk/popobs.php?obsid=${encodeURIComponent(ev.obsid)}&summering=tur&obs=obs`;
      a.style.display = 'block';
      a.appendChild(article);
      li.appendChild(a);
    } else {
      li.appendChild(article);
    }

    return li;
  }

  // Render: thread summary item
  function renderThreadSummary(s, fallbackDay) {
    const li = document.createElement('li'); li.className = 'obs-item';
    const article = el('article');

    const top = el('div','card-top');
    const left = el('div','left');
    const katRaw = s.last_kategori || '';
    const katResolvedForBadge = resolveKategori(s.art, katRaw);
    const katForBadge = katResolvedForBadge || katRaw;
    if (katForBadge) left.appendChild(el('span', catClass(katForBadge), String(katForBadge).toUpperCase()));
    if (s.region) left.appendChild(el('span','badge region', s.region));
    const right = el('div','right');
    const last = s.status === 'withdrawn' ? (s.last_active_ts_obs || s.last_ts_obs) : s.last_ts_obs;
    if (last) right.appendChild(el('span','badge region', `${fmtAge(last)}`));
    top.append(left, right);
    article.appendChild(top);

    // Titel-linje: venstre (antal + art) | højre (event_count)
    const title = el('div','title');
    const titleLeft = el('div','title-left');
    const katForArt = resolveKategori(s.art, s.last_kategori);
    const artCls = katForArt ? ` cat-${katForArt}` : '';

    const antal = (s.max_antal_num != null ? String(s.max_antal_num)
                  : (s.last_antal_num != null ? String(s.last_antal_num) : ''));
    if (antal) titleLeft.appendChild(el('span', `count${artCls}`, antal));
    titleLeft.appendChild(el('span', `art-name${artCls}`, s.art || ''));

    const titleRight = el('div','title-right');
    const ec = (typeof s.event_count === 'number') ? s.event_count
              : (typeof s.num_events === 'number') ? s.num_events : 0;
    if (ec > 0) {
      const cls = 'badge event-count' + (ec >= 2 ? ' warn' : '');
      titleRight.appendChild(el('span', cls, `${ec} obs`));
    }

    title.append(titleLeft, titleRight);
    article.appendChild(title);

    const info = el('div','info');
    if (s.last_adf) info.appendChild(el('span','', s.last_adf));
    if (s.lok) info.appendChild(el('span','', s.lok));
    if (s.last_observer) info.appendChild(el('span','', s.last_observer));
    article.appendChild(info);

    // Kommentarspor fra index.json (når kun 1 obs → felter findes)
    const notes = [];
    function pushNotes(txt, label) {
      const one = normalizeNoteText(txt);
      if (!one) return;
      notes.push({ type: label, text: one });
    }
    pushNotes(s.turnoter, 'Turnote');
    pushNotes(s.fuglnoter, 'Obsnote');

    if (notes.length) {
      const hr = document.createElement('hr');
      hr.style.border = '0';
      hr.style.borderTop = '1px solid var(--line)';
      hr.style.margin = '8px 0 10px';
      article.appendChild(hr);

      const thread = document.createElement('div');
      thread.className = 'comments';
      notes.forEach(n => {
        const row = document.createElement('div'); row.className = 'comment'; row.style.display='flex'; row.style.gap='8px';
        const pill = document.createElement('span'); pill.textContent = n.type; pill.className = 'badge';
        pill.style.background = '#eef2ff'; pill.style.color = '#1e3a8a'; pill.style.fontSize = '11px';
        const txt = document.createElement('div'); txt.className = 'comment-text'; txt.textContent = n.text;
        row.appendChild(pill); row.appendChild(txt); thread.appendChild(row);
      });
      article.appendChild(thread);
    }

    const d = s.day || (s.first_ts_obs || s.last_ts_obs || '').slice(0,10) || fallbackDay || todayYMDLocal();
    const fallbackHref = `./thread.html?date=${encodeURIComponent(d)}&id=${encodeURIComponent(s.thread_id)}`;
    const a = document.createElement('a'); a.style.display = 'block';

    // Behold linking-logikken med count kun her (ingen badge)
    const count = Number.isFinite(s.event_count) ? s.event_count
                 : (typeof s.event_count === 'number' ? s.event_count : (s.event_count || 0));
    if (count === 1) {
      if (s.last_obsid) {
        a.href = dofUrl(s.last_obsid);
      } else {
        a.href = fallbackHref;
        a.addEventListener('click', async (e) => {
          try {
            e.preventDefault();
            const r = await fetch(`./api/obs/thread/${encodeURIComponent(d)}/${encodeURIComponent(s.thread_id)}`, { cache:'no-cache' });
            if (r.ok) {
              const data = await r.json();
              const first = (data.events || []).find(ev => ev && ev.obsid);
              if (first && first.obsid) { location.href = dofUrl(first.obsid); return; }
            }
          } catch {}
          location.href = fallbackHref;
        });
      }
    } else {
      a.href = fallbackHref;
    }

    a.appendChild(article);
    li.appendChild(a);
    return li;
  }

  // Forside: kontrolpanel
  function buildFrontControls() {
    if (!$frontControls) return;

    // byg altid for at sikre at alle elementer findes
    $frontControls.innerHTML = '';
    $frontControls.style.display = 'flex';
    $frontControls.style.gap = '12px';
    $frontControls.style.alignItems = 'center';
    $frontControls.style.flexWrap = 'wrap';

    // Brug brugerpræferencer (tænd/sluk)
    const prefsWrap = document.createElement('label');
    prefsWrap.style.display = 'inline-flex'; prefsWrap.style.alignItems = 'center'; prefsWrap.style.gap = '6px';
    $frontChkPrefs = document.createElement('input'); $frontChkPrefs.type = 'checkbox'; $frontChkPrefs.checked = frontState.usePrefs;
    prefsWrap.appendChild($frontChkPrefs); prefsWrap.appendChild(document.createTextNode('Brug brugerpræferencer'));

    // Skjul 0‑fund
    const hideWrap = document.createElement('label');
    hideWrap.style.display = 'inline-flex'; hideWrap.style.alignItems = 'center'; hideWrap.style.gap = '6px';
    $frontChkHideZero = document.createElement('input'); $frontChkHideZero.type = 'checkbox'; $frontChkHideZero.checked = frontState.hideZero;
    hideWrap.appendChild($frontChkHideZero); hideWrap.appendChild(document.createTextNode('Skjul 0‑fund'));

    // Limit
    const limitWrap = document.createElement('label');
    limitWrap.style.display = 'inline-flex'; limitWrap.style.alignItems = 'center'; limitWrap.style.gap = '6px';
    limitWrap.appendChild(document.createTextNode('Vis'));
    $frontSelLimit = document.createElement('select');
    ['10','20','50','100','Alle'].forEach(opt => {
      const o = document.createElement('option'); o.value = opt === 'Alle' ? '0' : opt; o.textContent = opt; $frontSelLimit.appendChild(o);
    });
    $frontSelLimit.value = String(frontState.limit);
    limitWrap.appendChild($frontSelLimit);

    // Sortering – altid aktiv uanset prefs-toggle
    const sortWrap = document.createElement('label');
    sortWrap.style.display = 'inline-flex'; sortWrap.style.alignItems = 'center'; sortWrap.style.gap = '6px';
    sortWrap.appendChild(document.createTextNode('Sortér'));
    $frontSelSort = document.createElement('select');
    [
      { value: 'date_desc', label: 'Nyeste' },
      { value: 'alpha_asc', label: 'Alfabetisk' },
    ].forEach(s => { const o=document.createElement('option'); o.value=s.value; o.textContent=s.label; $frontSelSort.appendChild(o); });
    $frontSelSort.value = frontState.sortMode === 'alpha_asc' ? 'alpha_asc' : 'date_desc';
    sortWrap.appendChild($frontSelSort); // FIX: tilføj select til label

    // Tilføj til panelet
    $frontControls.appendChild(prefsWrap);
    $frontControls.appendChild(hideWrap);
    $frontControls.appendChild(limitWrap);
    $frontControls.appendChild(sortWrap);

    // Events
    $frontChkPrefs.addEventListener('change', () => {
      frontState.usePrefs = $frontChkPrefs.checked;
      try { localStorage.setItem(SORT_PREFS_KEY, frontState.usePrefs ? '1' : '0'); } catch {}
      renderThreadSummaries();
    });
    $frontChkHideZero.addEventListener('change', () => { frontState.hideZero = $frontChkHideZero.checked; renderThreadSummaries(); });
    $frontSelLimit.addEventListener('change', () => { frontState.limit = parseInt($frontSelLimit.value, 10) || 0; renderThreadSummaries(); });
    $frontSelSort.addEventListener('change', () => {
      frontState.sortMode = $frontSelSort.value;
      try { localStorage.setItem(SORT_MODE_KEY, frontState.sortMode); } catch {}
      renderThreadSummaries();
    });
  }

  // Forside: render trådsammendrag
  function renderThreadSummaries() {
    let base = summaryItems.slice();

    // Baseline uden brugerfilter: vis kun SU + SUB
    if (!frontState.usePrefs) {
      base = base.filter(s => {
        const kat = getThreadCategory(s);
        return kat === 'su' || kat === 'sub';
      });
    } else {
      // Grundlæggende brugerfilter: region + kategori
      base = base.filter(matchesBasicPrefs);
      // Avanceret: arts-exclude + antal
      base = applyAdvancedFilters(base);
    }

    // Skjul 0-fund (hvis valgt)
    if (frontState.hideZero) {
      base = base.filter(s => {
        const v = typeof s.max_antal_num === 'number' ? s.max_antal_num
                : (typeof s.last_antal_num === 'number' ? s.last_antal_num : null);
        return v == null ? true : v > 0;
      });
    }

    // Sortér (manuel to-state)
    const sorted = sortThreads(base, frontState.sortMode);

    // Limit
    const arr = frontState.limit > 0 ? sorted.slice(0, frontState.limit) : sorted;

    $st.innerHTML = '';
    if (!arr.length) { $st.textContent = frontState.usePrefs ? 'Ingen tråde matcher dine præferencer.' : 'Ingen tråde at vise.'; return; }

    const tYMD = todayYMDLocal();
    const ul = document.createElement('ul'); ul.className = 'obs-list';
    for (const s of arr) ul.appendChild(renderThreadSummary(s, tYMD));
    $st.appendChild(ul);
  }

  // Helpers til prefs-filtrering
  function normalizeKey(s) { return String(s || '').trim().toLowerCase(); }
  function toSet(v) {
    const arr = Array.isArray(v) ? v : (v != null ? [v] : []);
    const set = new Set(); arr.forEach(x => { const k = normalizeKey(x); if (k) set.add(k); });
    return set;
  }
  function getThreadCount(s) {
    return (typeof s.max_antal_num === 'number') ? s.max_antal_num
         : (typeof s.last_antal_num === 'number') ? s.last_antal_num
         : 0;
  }
  function getThreadCategory(s) {
    const katRaw = s.last_kategori || '';
    const kat = resolveKategori(s.art, katRaw) || katRaw || '';
    return normalizeKey(kat);
  }

  // Udvid prefs-værdi til tilladte kategorier (samme semantik som app.js)
  function expandPrefValueToAllowedCats(val) {
    const v = String(val ?? '').toLowerCase();
    if (v === 'su')   return new Set(['su']);
    if (v === 'sub')  return new Set(['su','sub']);
    if (v === 'alle') return new Set(['su','sub']); // bemærkelsesværdige = SUB
    return new Set(); // 'none'
  }

  // Byg grundlæggende filter pr. lokalafdeling
  function buildAllowedCatsByRegion(prefs) {
    const map = new Map();
    for (const [afd, sel] of Object.entries(prefs || {})) {
      const allow = expandPrefValueToAllowedCats(sel);
      if (allow.size > 0) map.set(String(afd), allow);
    }
    return map;
  }

  // Hent arts-overrides fra SW (fallback: localStorage)
  async function loadSpeciesOverrides() {
    // Forsøg via SW
    try {
      await navigator.serviceWorker?.ready;
      const ch = new MessageChannel();
      const resP = new Promise((resolve) => {
        ch.port1.onmessage = (e) => resolve(e.data?.overrides || null);
      });
      navigator.serviceWorker?.controller?.postMessage({ type: 'GET_SPECIES_OVERRIDES' }, [ch.port2]);
      const swRes = await Promise.race([resP, new Promise(r => setTimeout(() => r(null), 800))]);
      if (swRes) return swRes;
    } catch {}
    // Fallback localStorage
    try {
      const raw = localStorage.getItem('dofnot-species-overrides');
      if (raw) return JSON.parse(raw);
    } catch {}
    return { include: [], exclude: [], counts: {} };
  }

  // Avanceret: arts-exclude + per-art antal
  function normArtKey(s) {
    // samme som i app.js
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
  function applyAdvancedFilters(list) {
    const ov = speciesOverrides || { include: [], exclude: [], counts: {} };
    const exc = new Set((ov.exclude || []).map(normArtKey));
    // include ignoreres (samme som i app.js-laget)
    const counts = (ov.counts && typeof ov.counts === 'object') ? ov.counts : {};

    return list.filter((s) => {
      const key = normArtKey(s.art || '');
      if (exc.has(key)) return false;

      const cf = counts[key];
      if (!cf || cf.value == null) return true;

      const n = (typeof s.max_antal_num === 'number') ? s.max_antal_num
              : (typeof s.last_antal_num === 'number') ? s.last_antal_num
              : null;
      if (!Number.isFinite(n)) return false;
      const mode = (cf.mode === 'eq') ? 'eq' : 'gte';
      return mode === 'eq' ? (n === Math.floor(cf.value)) : (n >= Math.floor(cf.value));
    });
  }

  // Match grundlæggende prefs (region + kategori)
  function matchesBasicPrefs(s) {
    // Når brugerfilter er ON: kræv at region er valgt i prefs
    if (!allowedCatsByRegion || allowedCatsByRegion.size === 0) return false;
    const allow = allowedCatsByRegion.get(String(s.region || ''));
    if (!allow || allow.size === 0) return false;
    const kat = getThreadCategory(s);
    return allow.has(kat);
  }

  // Forside
  async function suggestThreads(pickDate) {
    userPrefs = await fetchUserPrefs();
    await ensureKlassMap();                 // kategoriopslag (SU/SUB/ALM)
    allowedCatsByRegion = buildAllowedCatsByRegion(userPrefs);
    speciesOverrides = await loadSpeciesOverrides();

    const candidates = [];
    if (pickDate) candidates.push(pickDate);
    const tYMD = todayYMDLocal(), yYMD = yesterdayYMDLocal();
    if (!pickDate || pickDate === 'today') candidates.push(tYMD, 'today', yYMD);

    let items = [];
    const seen = new Set();
    for (const d of candidates) {
      const arr = await fetchSummary(d);
      for (const s of arr) { if (seen.has(s.thread_id)) continue; seen.add(s.thread_id); items.push(s); }
      if (items.length) break;
    }
    if (!items.length) { if ($frontControls) $frontControls.style.display = 'none'; $st.innerHTML = `Ingen tråde for ${pickDate || 'today'}.`; return; }

    summaryItems = items;
    buildFrontControls();
    renderThreadSummaries();
  }

  // Trådvisning
   async function loadThread(date, id) {
    if ($frontControls) $frontControls.style.display = 'none';
    $st.textContent = 'Henter tråd…';
    const tryDates = isYMD(date) ? [date] : [date, todayYMDLocal()];
    let data = null, usedDate = date;
    for (const d of tryDates) {
      const r = await fetch(`./api/obs/thread/${encodeURIComponent(d)}/${encodeURIComponent(id)}`, { cache:'no-cache' });
      if (!r.ok) { usedDate = d; continue; }
      data = await r.json(); usedDate = d; break;
    }
    if (!data) { $panel.style.display='none'; await suggestThreads(date); return; }

    try { userPrefs = await fetchUserPrefs(); } catch { userPrefs = {}; }

    const t = data.thread || {};
    const events = Array.isArray(data.events) ? data.events : [];
    threadEvents = events.slice();

    try { localStorage.setItem('last-thread-id', t.thread_id || id); localStorage.setItem('last-thread-date', usedDate); } catch {}
    if (!isYMD(date) && isYMD(usedDate)) {
      const u = new URL(location.href); u.searchParams.set('date', usedDate); history.replaceState(null,'',u.toString());
    }

    $title.textContent = `${t.art || ''} — ${t.lok || ''}`.trim();
    const last = t.status === 'withdrawn' ? (t.last_active_ts_obs || t.last_ts_obs) : t.last_ts_obs;
    const badge = t.last_kategori ? t.last_kategori.toUpperCase() : '';
    $sub.textContent = [t.region || '', badge, t.status === 'withdrawn' ? 'Tilbagekaldt' : '', last ? `sidst set ${fmtAge(last)}` : '']
      .filter(Boolean).join(' • ');

    // Sortér events efter prefs-toggle eller manuel tilstand
    const usePrefs = (localStorage.getItem(SORT_PREFS_KEY) ?? '1') === '1';
    const manualMode = localStorage.getItem(SORT_MODE_KEY) || 'date_desc';
    const sorted = sortEvents(threadEvents, usePrefs ? 'prefs' : manualMode);

    $list.innerHTML = '';
    for (const ev of sorted) {
      if (ev.event_type === 'correction') continue;
      $list.appendChild(renderEvent(ev));
    }

      $panel.style.display = '';
      $st.textContent = '';
    }
  
    // Initialize based on route
    const route = parseRoute();
    if (route.id) {
      loadThread(route.date, route.id);
    } else {
      suggestThreads(route.date === 'today' ? undefined : route.date);
    }
  })();