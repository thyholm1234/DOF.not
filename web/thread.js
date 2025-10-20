(function () {
  const $ = (id) => document.getElementById(id);

  // Lazy DOM refs (så script kan loades før DOM er klar eller på andre sider)
  let $st, $panel, $title, $sub, $list, $frontControls;
  function ensureDomRefs() {
    $st = $st || $('thread-status');
    $panel = $panel || $('thread-panel');
    $title = $title || $('thread-title');
    $sub = $sub || $('thread-sub');
    $list = $list || $('thread-events');
    $frontControls = $frontControls || $('front-controls');
  }

  // Forside-kontroller (for trådsammendrag)
  let $btnPrefs, $btnCat, $btnZero, $btnSort;
  const SORT_PREFS_KEY = 'dofnot-use-prefs-sort';
  const SORT_MODE_KEY = 'dofnot-sort-mode';
  const ONLY_SU_KEY = 'dofnot-only-su';
  const INCLUDE_ZERO_KEY = 'dofnot-include-zero';

  // UI-state m. defaults (grøn = ON)
  const frontState = {
    usePrefs: (localStorage.getItem(SORT_PREFS_KEY) ?? '1') === '1',         // Brugerpræferencer (ON)
    onlySU: (localStorage.getItem(ONLY_SU_KEY) ?? '0') === '1',              // Kun SU (OFF = SU+SUB)
    includeZero: (localStorage.getItem(INCLUDE_ZERO_KEY) ?? '1') === '1',    // Inkl. 0-obs (ON)
    sortMode: localStorage.getItem(SORT_MODE_KEY) || 'date_desc',            // Nyeste
    limit: 0
  };

  // --------- NYT: Global state + helpers der manglede ----------
  let userPrefs = {};
  let summaryItems = [];
  let allowedCatsByRegion = new Map();
  let speciesOverrides = null;
  let threadEvents = [];

  function parseRoute() {
    const q = new URLSearchParams(location.search);
    const date = q.get('date') || 'today';
    const id = q.get('id') || '';
    return { date, id };
  }
  function isYMD(s) {
    return /^\d{4}-\d{2}-\d{2}$/.test(String(s || ''));
  }
  function ymdInTZ(tz, d = new Date()) {
    const parts = new Intl.DateTimeFormat('en-CA', {
      timeZone: tz, year: 'numeric', month: '2-digit', day: '2-digit'
    }).formatToParts(d);
    const get = (t) => parts.find(p => p.type === t)?.value;
    return `${get('year')}-${get('month')}-${get('day')}`;
  }
  function hourInTZ(tz, d = new Date()) {
    return parseInt(new Intl.DateTimeFormat('en-GB', {
      timeZone: tz, hour: '2-digit', hour12: false
    }).format(d), 10);
  }
  function todayYMDLocal() {
    return ymdInTZ('Europe/Copenhagen');
  }
  function yesterdayYMDLocal() {
    const d = new Date(Date.now() - 24*60*60*1000);
    return ymdInTZ('Europe/Copenhagen', d);
  }
  function fmtAge(iso) {
    if (!iso) return '';
    const now = Date.now();
    const t = new Date(iso).getTime();
    const diff = Math.max(0, now - t);
    const min = Math.floor(diff / 60000);
    if (min < 1) return 'nu';
    if (min < 60) return `${min} min`;
    const h = Math.floor(min / 60);
    if (h === 1) return `${h} time`;
    if (h < 24) return `${h} timer`;
    const d = Math.floor(h / 24);
    return `${d} d`;
  }
  async function fetchUserPrefs() {
    try {
      // hvis app.js har en bruger-id helper, brug den
      const uid = typeof window.getOrCreateUserId === 'function'
        ? window.getOrCreateUserId()
        : (localStorage.getItem('dofnot-user-id') || '');
      if (uid) {
        const r = await fetch(`./api/prefs/user?user_id=${encodeURIComponent(uid)}`, { cache: 'no-store' });
        if (r.ok) {
          const data = await r.json();
          return (data && data.prefs) ? data.prefs : (data || {});
        }
      }
    } catch {}
    try {
      const raw = localStorage.getItem('dofnot-prefs');
      if (raw) return JSON.parse(raw);
    } catch {}
    return {};
  }
  async function fetchSummary(dateParam) {
    try {
      const r = await fetch(`./api/obs/summary?date=${encodeURIComponent(dateParam)}`, { cache: 'no-store' });
      if (!r.ok) return [];
      const arr = await r.json();
      return Array.isArray(arr) ? arr : [];
    } catch { return []; }
  }
  // --------- slut: NYT ----------

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

  // Ensret badge-kolonnebredde i et comments‑afsnit (deaktiver lokal override)
  function normalizeCommentLayout(commentsEl) {
    // no-op: vi bruger kun global måling
  }

  // Global måling – fælles start for indholdskolonnen
  function updateGlobalCommentBadgeWidth() {
    try {
      const pills = document.querySelectorAll('.comment > .badge');
      let max = 0;
      pills.forEach(p => { const w = p.offsetWidth || 0; if (w > max) max = w; });
      if (max > 0) document.documentElement.style.setProperty('--comment-badge-w', (Math.ceil(max) + 6) + 'px');
    } catch {}
  }

  // Mål efter layout (næste frame + fallback)
  function scheduleBadgeWidthMeasure() {
    requestAnimationFrame(() => {
      updateGlobalCommentBadgeWidth();
      setTimeout(updateGlobalCommentBadgeWidth, 0);
    });
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
        const row = document.createElement('div'); 
        row.className = 'comment';
        // fjern inline flex-styles – CSS grid styrer layoutet
        const pill = document.createElement('span'); pill.textContent = n.type; pill.className = 'badge';
        pill.style.background = '#eef2ff'; pill.style.color = '#1e3a8a'; pill.style.fontSize = '11px';
        const txt = document.createElement('div'); txt.className = 'comment-text'; txt.textContent = n.text;
        row.appendChild(pill); row.appendChild(txt); thread.appendChild(row);
      });
      article.appendChild(thread);
      // Ensret kolonnebredde
      // normalizeCommentLayout(thread);    // ← fjernet lokal override
      scheduleBadgeWidthMeasure();          // ← globalt mål
    }

    // Billeder under noter (kun hvis obsid findes). Async – tilføjes når de er hentet.
    if (ev.obsid) {
      renderObsImagesSection(article, ev.obsid);
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

    // Kommentarspor fra index.json
    const notes = [];
    function pushNotes(txt, label) {
      const one = normalizeNoteText(txt);
      if (!one) return;
      notes.push({ type: label, text: one });
    }
    pushNotes(s.turnoter, 'Turnote');
    pushNotes(s.fuglnoter, 'Obsnote');

    // Kun vis noter i summary når der er >1 obs i tråden
    if (notes.length && ec > 1) {
      const hr = document.createElement('hr');
      hr.style.border = '0';
      hr.style.borderTop = '1px solid var(--line)';
      hr.style.margin = '8px 0 10px';
      article.appendChild(hr);

      const thread = document.createElement('div');
      thread.className = 'comments';
      notes.forEach(n => {
        const row = document.createElement('div'); 
        row.className = 'comment';
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

    // Åbn altid tråden – også når der kun er 1 obs
    a.href = fallbackHref;

    a.appendChild(article);
    li.appendChild(a);
    return li;
  }

  // Byg to‑state kontrolkort
  function buildFrontControls() {
    ensureDomRefs();
    if (!$frontControls) return;
    $frontControls.innerHTML = '';
    $frontControls.classList.add('controls-card');

    // Brugerpræferencer: ON = “Brugerpræferencer” (grøn), OFF = “Alle obs” (rød)
    $btnPrefs = document.createElement('button');
    $btnPrefs.type = 'button'; $btnPrefs.className = 'twostate';
    $frontControls.appendChild($btnPrefs);

    // Kategori: OFF = “SU+SUB” (grøn), ON = “SU” (rød)
    $btnCat = document.createElement('button');
    $btnCat.type = 'button'; $btnCat.className = 'twostate';
    $frontControls.appendChild($btnCat);

    // 0-obs: ON = “0-obs” (grøn = inkl.), OFF = “0-obs” (rød = skjul)
    $btnZero = document.createElement('button');
    $btnZero.type = 'button'; $btnZero.className = 'twostate';
    $frontControls.appendChild($btnZero);

    // Sortering: date_desc = “Nyeste” (grøn), alpha_asc = “Alfabetisk” (rød)
    $btnSort = document.createElement('button');
    $btnSort.type = 'button'; $btnSort.className = 'twostate';
    $frontControls.appendChild($btnSort);

    // Events
    $btnPrefs.addEventListener('click', () => {
      frontState.usePrefs = !frontState.usePrefs;
      try { localStorage.setItem(SORT_PREFS_KEY, frontState.usePrefs ? '1' : '0'); } catch {}
      updateFrontButtons(); renderThreadSummaries();
    });
    $btnCat.addEventListener('click', () => {
      frontState.onlySU = !frontState.onlySU;
      try { localStorage.setItem(ONLY_SU_KEY, frontState.onlySU ? '1' : '0'); } catch {}
      updateFrontButtons(); renderThreadSummaries();
    });
    $btnZero.addEventListener('click', () => {
      frontState.includeZero = !frontState.includeZero;
      try { localStorage.setItem(INCLUDE_ZERO_KEY, frontState.includeZero ? '1' : '0'); } catch {}
      updateFrontButtons(); renderThreadSummaries();
    });
    $btnSort.addEventListener('click', () => {
      frontState.sortMode = frontState.sortMode === 'alpha_asc' ? 'date_desc' : 'alpha_asc';
      try { localStorage.setItem(SORT_MODE_KEY, frontState.sortMode); } catch {}
      updateFrontButtons(); renderThreadSummaries();
    });

    updateFrontButtons();
  }

  function updateFrontButtons() {
    // Prefs
    if (frontState.usePrefs) {
      $btnPrefs.textContent = 'Bruger';
      $btnPrefs.classList.add('is-on'); $btnPrefs.classList.remove('is-off');
    } else {
      $btnPrefs.textContent = 'Alle';
      $btnPrefs.classList.add('is-off'); $btnPrefs.classList.remove('is-on');
    }
    // Kategori
    if (frontState.onlySU) {
      $btnCat.textContent = 'SU';
      $btnCat.classList.add('is-off'); $btnCat.classList.remove('is-on');
    } else {
      $btnCat.textContent = 'SUB';
      $btnCat.classList.add('is-on'); $btnCat.classList.remove('is-off');
    }
    // 0-obs
    if (frontState.includeZero) {
      $btnZero.textContent = '0-obs';
      $btnZero.classList.add('is-on'); $btnZero.classList.remove('is-off');
    } else {
      $btnZero.textContent = '0-obs';
      $btnZero.classList.add('is-off'); $btnZero.classList.remove('is-on');
    }
    // Sort
    if (frontState.sortMode === 'date_desc') {
      $btnSort.textContent = 'Nyeste';
      $btnSort.classList.add('is-on'); $btnSort.classList.remove('is-off');
    } else {
      $btnSort.textContent = 'Alfabet';
      $btnSort.classList.add('is-off'); $btnSort.classList.remove('is-on');
    }
    if ($frontControls) $frontControls.style.display = 'flex';
  }

  // Render summary med filtrering
  function renderThreadSummaries() {
    ensureDomRefs();
    if (!$list || !$st) return;

    // FIX: brug lokal state, ikke window.summaryItems
    let base = summaryItems.slice();

    if (frontState.usePrefs) {
      const hasBasicPrefs = allowedCatsByRegion && allowedCatsByRegion.size > 0;

      if (hasBasicPrefs) {
        // Grundlæggende: region + kategori fra brugerens prefs
        base = base.filter(matchesBasicPrefs);
        // Kun SU override
        if (frontState.onlySU) base = base.filter(s => getThreadCategory(s) === 'su');
        // Avanceret: arts-exclude + antal
        base = applyAdvancedFilters(base);
      } else {
        // Fallback når der ikke findes gyldige prefs: baseline SU+SUB eller SU-only
        base = base.filter(s => {
          const kat = getThreadCategory(s);
          return frontState.onlySU ? (kat === 'su') : (kat === 'su' || kat === 'sub');
        });
      }
    } else {
      // Uden brugerfilter: baseline SU+SUB, evt. SU-only
      base = base.filter(s => {
        const kat = getThreadCategory(s);
        return frontState.onlySU ? (kat === 'su') : (kat === 'su' || kat === 'sub');
      });
    }

    // 0-obs: skjul når includeZero = false
    if (!frontState.includeZero) {
      base = base.filter(s => {
        const v = typeof s.max_antal_num === 'number' ? s.max_antal_num
                : (typeof s.last_antal_num === 'number' ? s.last_antal_num : null);
        return v == null ? true : v > 0;
      });
    }

    // Sorter
    const sorted = sortThreads(base, frontState.sortMode);
    const arr = frontState.limit > 0 ? sorted.slice(0, frontState.limit) : sorted;

    $st.innerHTML = '';
    if (!arr.length) { $st.textContent = 'Ingen tråde at vise.'; return; }

    const tYMD = (todayYMDLocal() || new Date().toISOString().slice(0,10));
    const ul = document.createElement('ul'); ul.className = 'obs-list';

    // FIX: kald lokal renderer, ikke window.renderThreadSummary
    for (const s of arr) {
      const li = renderThreadSummary(s, tYMD);
      if (li) ul.appendChild(li);
    }
    $st.appendChild(ul);
  }

  window.buildFrontControls = buildFrontControls;
  window.renderThreadSummaries = renderThreadSummaries;

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
    const katRaw = s?.last_kategori || '';
    const kat = resolveKategori(s?.art, katRaw) || katRaw || '';
    return String(kat).trim().toLowerCase();
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
    ensureDomRefs();
    userPrefs = await fetchUserPrefs();
    await ensureKlassMap();
    allowedCatsByRegion = buildAllowedCatsByRegion(userPrefs);
    speciesOverrides = await loadSpeciesOverrides();

    const tYMD = todayYMDLocal();
    const yYMD = yesterdayYMDLocal();
    const hour = hourInTZ('Europe/Copenhagen');
    const isTodayRequested = (!pickDate || pickDate === 'today' || pickDate === tYMD);
    const wantCombined = isTodayRequested && hour < 4;

    let items = [];
    const seen = new Set();

    if (wantCombined) {
      const [arrToday, arrYest] = await Promise.all([ fetchSummary(tYMD), fetchSummary(yYMD) ]);
      const pushAll = (arr, defDay) => {
        for (const s of Array.isArray(arr) ? arr : []) {
          const id = s && s.thread_id;
          if (!id || seen.has(id)) continue;
          seen.add(id);
          const d = s.day || (s.first_ts_obs || s.last_ts_obs || '').slice(0,10) || defDay;
          items.push({ ...s, day: d, _sourceDay: defDay });
        }
      };
      pushAll(arrToday, tYMD);
      pushAll(arrYest, yYMD);
    } else {
      const target = pickDate || tYMD;
      const arr = await fetchSummary(target);
      for (const s of arr) {
        const id = s.thread_id;
        if (!id || seen.has(id)) continue;
        seen.add(id);
        items.push({ ...s, day: s.day || (s.first_ts_obs || s.last_ts_obs || '').slice(0,10) || target });
      }
    }

    if (!items.length) {
      if ($frontControls) $frontControls.style.display = 'none';
      if ($st) $st.innerHTML = `Ingen observationer for ${pickDate || 'today'}.`;
      return;
    }

    summaryItems = items;
    buildFrontControls();
    renderThreadSummaries();
  }

  // Trådvisning
  async function loadThread(date, id) {
    ensureDomRefs(); // ← vigtig
    if ($frontControls) $frontControls.style.display = 'none';
    if ($st) $st.textContent = 'Henter tråd…';

    const tryDates = isYMD(date) ? [date] : [date, todayYMDLocal()];
    let data = null, usedDate = date;
    for (const d of tryDates) {
      const r = await fetch(`./api/obs/thread/${encodeURIComponent(d)}/${encodeURIComponent(id)}`, { cache:'no-cache' });
      if (!r.ok) { usedDate = d; continue; }
      data = await r.json(); usedDate = d; break;
    }
    if (!data) {
      if ($panel) $panel.style.display='none';
      // Vis forsiden for den valgte dag (fx i går), så brugeren stadig får noget at se
      await suggestThreads(date);
      return;
    }

    try { userPrefs = await fetchUserPrefs(); } catch { userPrefs = {}; }

    const t = data.thread || {};
    const events = Array.isArray(data.events) ? data.events : [];
    threadEvents = events.slice();

    try {
      localStorage.setItem('last-thread-id', t.thread_id || id);
      localStorage.setItem('last-thread-date', usedDate);
    } catch {}

    if (!isYMD(date) && isYMD(usedDate)) {
      const u = new URL(location.href); u.searchParams.set('date', usedDate); history.replaceState(null,'',u.toString());
    }

    if ($title) $title.textContent = `${t.art || ''} — ${t.lok || ''}`.trim();
    const last = t.status === 'withdrawn' ? (t.last_active_ts_obs || t.last_ts_obs) : t.last_ts_obs;
    const badge = t.last_kategori ? t.last_kategori.toUpperCase() : '';
    if ($sub) {
      $sub.textContent = [t.region || '', badge, t.status === 'withdrawn' ? 'Tilbagekaldt' : '', last ? `sidst set ${fmtAge(last)}` : '']
        .filter(Boolean).join(' • ');
    }

    // Sortér events efter prefs-toggle eller manuel tilstand
    const usePrefs = (localStorage.getItem(SORT_PREFS_KEY) ?? '1') === '1';
    const manualMode = localStorage.getItem(SORT_MODE_KEY) || 'date_desc';
    const sorted = sortEvents(threadEvents, usePrefs ? 'prefs' : manualMode);

    if ($list) {
      $list.innerHTML = '';
      for (const ev of sorted) {
        if (ev.event_type === 'correction') continue;
        $list.appendChild(renderEvent(ev));
      }
    }

    if ($panel) $panel.style.display = '';
    if ($st) $st.textContent = '';
  }

 // Hent billed-URLs til en observation
 async function fetchObsImages(obsid) {
   if (!obsid) return [];
   // Prøv global hook først (fx fra SW/app.js)
  try {
     if (typeof window.fetchObsImages === 'function') {
       const res = await window.fetchObsImages(obsid);
       if (Array.isArray(res)) return res;
       if (res && Array.isArray(res.urls)) return res.urls;
     }
   } catch {}
   // Fallback: API-endpoint
   try {
     const r = await fetch(`./api/obs/images?obsid=${encodeURIComponent(obsid)}`, { cache: 'no-store' });
     if (!r.ok) return [];
     const j = await r.json();
     const urls = Array.isArray(j) ? j
       : (j && Array.isArray(j.images)) ? j.images
       : (j && Array.isArray(j.urls)) ? j.urls  
       : [];  
     // Dedupliker og filtrér tomme
     const seen = new Set(), out = [];
     for (const u of urls) {
       const s = String(u || '').trim();
       if (!s || seen.has(s)) continue;
       seen.add(s); out.push(s);
     }
     return out;
   } catch {
     return [];
   }
 }

async function renderObsImagesSection(cardEl, obsid) {
  if (!obsid || !cardEl) return;

  const imgs = await fetchObsImages(obsid);
  if (!imgs.length) return;

  // Find eksisterende kommentar-område, eller opret et nyt med HR
  let comments = cardEl.querySelector('.comments');
  if (!comments) {
    const hr = document.createElement('hr');
    hr.style.border = '0';
    hr.style.borderTop = '1px solid var(--line)';
    hr.style.margin = '8px 0 10px';
    cardEl.appendChild(hr);

    comments = document.createElement('div');
    comments.className = 'comments';
    cardEl.appendChild(comments);
  }

  // Tilføj én række pr. billede: badge "Pic#N" + billedet som indhold
  imgs.forEach((full, idx) => {
    const row = document.createElement('div');
    row.className = 'comment'; // CSS grid styrer kolonner

    const pill = document.createElement('span');
    pill.textContent = `Pic#${idx + 1}`;
    pill.className = 'badge';
    pill.style.background = '#eef2ff';
    pill.style.color = '#1e3a8a';
    pill.style.fontSize = '11px';

    const media = document.createElement('div');
    media.className = 'comment-media';

    const box = document.createElement('div');
    box.className = 'img-box';
    box.title = 'Åbn billede';
    box.setAttribute('role', 'button');
    box.tabIndex = 0;

    const img = document.createElement('img');
    img.src = full;
    img.loading = 'lazy';
    img.alt = `Pic#${idx + 1}`;

    const open = (e) => {
      e.preventDefault();
      e.stopPropagation();
      window.open(full, '_blank', 'noopener');
    };
    box.addEventListener('click', open);
    box.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' || e.key === ' ') open(e);
    });

    box.appendChild(img);
    media.appendChild(box);
    row.appendChild(pill);
    row.appendChild(media);
    comments.appendChild(row);
  });

  // Ensret efter billeder er tilføjet
  // normalizeCommentLayout(comments);     // ← fjernet lokal override
  scheduleBadgeWidthMeasure();             // ← globalt mål
}

// Minimal styling til kommentar-layout m. fælles badge-kolonne (global)
const style = document.createElement('style');
style.textContent = `
  .comments { --badge-w: var(--comment-badge-w, 72px); }
  .comment {
    display: grid;
    grid-template-columns: var(--badge-w) 1fr;
    column-gap: 8px;
    align-items: flex-start;
  }
  .comment > .badge {
    justify-self: end;
    white-space: nowrap;
  }
  .comment > .comment-text,
  .comment > .comment-media {
    min-width: 0;
    margin: 0;             /* ← nulstil margin */
    padding: 0;            /* ← nulstil padding */
  }

  .comment-media { display: block; }
  .comment-media .img-box { display: inline-block; position: relative; cursor: zoom-in; margin: 0; padding: 0; }
  .comment-media img { max-height: 110px; border-radius: 4px; display: block; margin: 0; }
`;
document.head.appendChild(style);

// Initialize based on route
    const route = parseRoute();
    if (route.id) {
      loadThread(route.date, route.id);
    } else {
      suggestThreads(route.date === 'today' ? undefined : route.date);
    }
  })();