(function () {
  const $ = (id) => document.getElementById(id);

  const $st = $('thread-status');
  const $panel = $('thread-panel');
  const $title = $('thread-title');
  const $sub = $('thread-sub');
  const $list = $('thread-events');

  function todayYMDLocal() {
    const d = new Date();
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    return `${y}-${m}-${day}`;
  }
  function yesterdayYMDLocal() {
    const d = new Date();
    d.setDate(d.getDate() - 1);
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    return `${y}-${m}-${day}`;
  }
  const isYMD = (s) => /^\d{4}-\d{2}-\d{2}$/.test(s || '');

  function parseRoute() {
    const qs = new URLSearchParams(location.search);
    let date = qs.get('date') || '';
    let id = qs.get('id') || '';
    if (!date) date = 'today';
    return { date, id };
  }

  async function fetchUserPrefs() {
    try {
      const r = await fetch('./api/prefs/user', { cache: 'no-cache' });
      if (!r.ok) return {};
      const p = await r.json();
      return p || {};
    } catch {
      return {};
    }
  }

  function normArr(v, f = (x) => x) {
    if (v == null) return [];
    if (Array.isArray(v)) return v.map((x) => f(String(x)));
    return String(v).split(',').map((s) => f(s.trim())).filter(Boolean);
  }

  function parseCoords(c) {
    if (!c) return null;
    if (Array.isArray(c) && c.length >= 2) return [Number(c[0]), Number(c[1])];
    if (typeof c === 'string') {
      const m = c.trim().split(/[,; ]+/).map(Number).filter((v) => !Number.isNaN(v));
      if (m.length >= 2) {
        const a = Math.abs(m[0]), b = Math.abs(m[1]);
        if (a > 54 && a < 58 && b > 7 && b < 16) return [m[1], m[0]];
        return [m[0], m[1]];
      }
    }
    return null;
  }

  function inBBox(pt, bbox) {
    if (!pt || !bbox || bbox.length < 4) return true;
    const [lon, lat] = pt;
    const [minLon, minLat, maxLon, maxLat] = bbox.map(Number);
    return lon >= minLon && lon <= maxLon && lat >= minLat && lat <= maxLat;
  }

  function applyPrefs(items, prefs) {
    const p = prefs || {};
    const cats = normArr(p.categories, (s) => s.toLowerCase());
    const regs = normArr(p.regions, (s) => s.toLowerCase());
    const species = normArr(p.species, (s) => s.toLowerCase());
    const onlyCoords = Boolean(p.only_with_coords);
    const bbox = Array.isArray(p.bbox) ? p.bbox : null;
    const minCount = (typeof p.min_count === 'number') ? p.min_count : (p.min_count ? Number(p.min_count) : null);

    return items.filter((s) => {
      if (cats.length && !cats.includes(String(s.last_kategori || '').toLowerCase())) return false;
      const rname = String(s.region || '').toLowerCase();
      const rslug = String(s.region_slug || '').toLowerCase();
      if (regs.length && !(regs.includes(rslug) || regs.includes(rname))) return false;
      const art = String(s.art || '').toLowerCase();
      if (species.length && !species.includes(art)) return false;
      const pt = parseCoords(s.coords);
      if (onlyCoords && !pt) return false;
      if (bbox && !inBBox(pt, bbox)) return false;
      const c = (typeof s.max_antal_num === 'number') ? s.max_antal_num
        : (typeof s.last_antal_num === 'number' ? s.last_antal_num : null);
      if (minCount != null && !(c != null && c >= minCount)) return false;
      return true;
    });
  }

  async function fetchSummary(dateParam) {
    try {
      const r = await fetch(`./api/obs/summary?date=${encodeURIComponent(dateParam)}`, { cache: 'no-cache' });
      if (r.status === 204) return [];
      if (!r.ok) throw new Error(String(r.status));
      const arr = await r.json();
      return Array.isArray(arr) ? arr : [];
    } catch {
      return [];
    }
  }

  function fmtAge(iso) {
    if (!iso) return '';
    const t = new Date(iso);
    const diff = (Date.now() - t.getTime()) / 1000;
    if (diff < 60) return 'for få sek. siden';
    const m = Math.floor(diff / 60);
    if (m < 60) return `for ${m} min siden`;
    const h = Math.floor(m / 60);
    return `for ${h} t siden`;
  }

  function renderEvent(ev) {
    const li = document.createElement('li');
    li.className = 'obs-item';
    const header = document.createElement('header');
    const h3 = document.createElement('h3');
    const antal = ev.antal_text || (ev.antal_num != null ? String(ev.antal_num) : '');
    h3.className = 'title species sp-name';
    h3.textContent = `${antal ? antal + ' ' : ''}${ev.art || ''}`.trim();
    header.appendChild(h3);
    const meta = document.createElement('div'); meta.className = 'meta';
    if (ev.adf) { const s = document.createElement('span'); s.textContent = ev.adf; meta.appendChild(s); }
    if (ev.observer) { const s = document.createElement('span'); s.textContent = ev.observer; meta.appendChild(s); }
    const by = document.createElement('footer'); by.className = 'byline';
    const tm = document.createElement('time'); tm.textContent = (ev.ts_obs || ev.ts_seen || '').replace('T', ' ').slice(0, 16);
    by.appendChild(tm);
    const article = document.createElement('article');
    article.append(header, meta, by);
    li.appendChild(article);
    return li;
  }

  async function suggestThreads(pickDate) {
    const prefs = await fetchUserPrefs();

    const candidates = [];
    if (pickDate) candidates.push(pickDate);
    const tYMD = todayYMDLocal();
    const yYMD = yesterdayYMDLocal();
    if (!pickDate || pickDate === 'today') candidates.push(tYMD, 'today', yYMD);

    let items = [];
    const seen = new Set();
    for (const d of candidates) {
      const arr = await fetchSummary(d);
      for (const s of arr) {
        if (seen.has(s.thread_id)) continue;
        seen.add(s.thread_id);
        items.push(s);
      }
      if (items.length) break;
    }

    items = applyPrefs(items, prefs);

    if (!items.length) {
      const label = pickDate || 'today';
      $st.innerHTML = `Ingen tråde matcher dine præferencer for ${label}.`;
      return;
    }

    items.sort((a, b) => (new Date(b.last_ts_obs || 0)) - (new Date(a.last_ts_obs || 0)));

    const ul = document.createElement('ul');
    ul.className = 'obs-list';
    for (const s of items.slice(0, 100)) {
      const li = document.createElement('li');
      li.className = 'obs-item';

      // 1) Link-linje
      const a = document.createElement('a');
      const d = s.day || (s.first_ts_obs || s.last_ts_obs || '').slice(0, 10) || tYMD;
      a.href = `./thread.html?date=${encodeURIComponent(d)}&id=${encodeURIComponent(s.thread_id)}`;
      a.textContent = `${s.art || ''} — ${s.lok || ''}`;
      li.appendChild(a);

      // 2) Kategori-linje (fx SUB)
      const catLine = document.createElement('div');
      catLine.className = 'list-cat-line';
      const cat = (s.last_kategori || '').toUpperCase();
      if (cat) {
        const badge = document.createElement('span');
        badge.className = `badge cat-${(s.last_kategori||'').toLowerCase()}`;
        badge.textContent = cat;
        catLine.appendChild(badge);
      }
      li.appendChild(catLine);

      // 3) Sidst set-linje
      const last = s.status === 'withdrawn' ? (s.last_active_ts_obs || s.last_ts_obs) : s.last_ts_obs;
      const timeLine = document.createElement('div');
      timeLine.className = 'list-time-line';
      if (last) {
        const span = document.createElement('span');
        span.textContent = `sidst set ${fmtAge(last)}`;
        timeLine.appendChild(span);
      }
      li.appendChild(timeLine);

      ul.appendChild(li);
    }
    $st.innerHTML = 'Vælg en tråd:';
    $st.appendChild(ul);
  }

  async function loadThread(date, id) {
    $st.textContent = 'Henter tråd…';
    const tryDates = isYMD(date) ? [date] : [date, todayYMDLocal()];
    let data = null, usedDate = date;
    for (const d of tryDates) {
      const r = await fetch(`./api/obs/thread/${encodeURIComponent(d)}/${encodeURIComponent(id)}`, { cache: 'no-cache' });
      if (!r.ok) { usedDate = d; continue; }
      data = await r.json(); usedDate = d; break;
    }
    if (!data) { $panel.style.display = 'none'; await suggestThreads(date); return; }

    const t = data.thread || {};
    const events = Array.isArray(data.events) ? data.events : [];

    try {
      localStorage.setItem('last-thread-id', t.thread_id || id);
      localStorage.setItem('last-thread-date', usedDate);
    } catch {}

    if (!isYMD(date) && isYMD(usedDate)) {
      const u = new URL(location.href);
      u.searchParams.set('date', usedDate);
      history.replaceState(null, '', u.toString());
    }

    $title.textContent = `${t.art || ''} — ${t.lok || ''}`.trim();
    const last = t.status === 'withdrawn' ? (t.last_active_ts_obs || t.last_ts_obs) : t.last_ts_obs;
    const badge = t.last_kategori ? t.last_kategori.toUpperCase() : '';
    $sub.textContent = [t.region || '', badge, t.status === 'withdrawn' ? 'Tilbagekaldt' : '', last ? `sidst set ${fmtAge(last)}` : '']
      .filter(Boolean).join(' • ');

    $list.innerHTML = '';
    for (const ev of events) {
      if (ev.event_type === 'correction') continue;
      $list.appendChild(renderEvent(ev));
    }

    $panel.style.display = '';
    $st.textContent = '';
  }

  document.addEventListener('DOMContentLoaded', () => {
    const { date, id } = parseRoute();
    if (id) loadThread(date, id).catch(() => { $st.textContent = 'Fejl ved indlæsning.'; });
    else suggestThreads(date).catch(() => { $st.textContent = 'Fejl ved indlæsning.'; });
  });
})();