// app.js – DOF.not (v3.2) · robust abonnements-knap + unsubscribe · uden IIFE

// ─────────────────────────── Konstanter & elementer ──────────────────────────
const BASE = document.baseURI || document.location.href;
const abs  = (p) => new URL(p, BASE).toString();

const DOF_AFDELINGER = [
  "DOF København","DOF Nordsjælland","DOF Vestsjælland","DOF Storstrøm","DOF Bornholm",
  "DOF Fyn","DOF Sønderjylland","DOF Sydvestjylland","DOF Sydøstjylland","DOF Vestjylland",
  "DOF Østjylland","DOF Nordvestjylland","DOF Nordjylland"
];
const CHOICE_ORDER = ['none', 'su', 'sub', 'alle'];

const $ = (id) => document.getElementById(id);
const elGrid = $('grid');
const elSave = $('save');          // hovedknap (Gem/Abonnér/Opdater …)
const elUnsub = $('unsubscribe');  // knap til at afmelde subscription
const elStatus = $('status'); 
const isIOS = /iphone|ipad|ipod/i.test(navigator.userAgent);
const isStandalone = () =>
  window.matchMedia('(display-mode: standalone)').matches || window.navigator.standalone === true;


// ─────────────────────────── Diagnostik (uden IIFE) ──────────────────────────
function setupDiagnostics() {
  const stamp = new Date().toISOString();
  if (elStatus) {
    elStatus.textContent = `app.js loaded @ ${stamp}`;
    elStatus.style.color = '#f2a900';
  }
  window.addEventListener('error', (e) => {
    if (elStatus) elStatus.textContent = `JS-fejl: ${e.message}`;
  });
  window.addEventListener('unhandledrejection', (e) => {
    if (elStatus) elStatus.textContent = `Promise-fejl: ${e?.reason?.message ?? String(e.reason)}`;
  });
}

// ─────────────────────────── User ID ─────────────────────────────────────────
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

// ─────────────────────────── SW & Push ───────────────────────────────────────
async function ensureSW() {
  if ('serviceWorker' in navigator) {
    try {
      // Justér scope hvis din app ligger et andet sted
      await navigator.serviceWorker.register('sw.js', { scope: './' });
      await navigator.serviceWorker.ready;
    } catch (e) {
      console.warn('SW-registrering fejlede:', e);
    }
  }
}
function supportsPush() {
  return 'serviceWorker' in navigator && 'PushManager' in window && 'Notification' in window;
}
function postToSW(msg) {
  return navigator.serviceWorker.getRegistration().then(reg => {
    try { reg?.active?.postMessage(msg); } catch {}
  });
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

  // Hent VAPID public key (samme endpoint som før)
  const r = await fetch(abs('vapid-public-key'), { cache: 'no-cache' });
  if (!r.ok) throw new Error('Kan ikke hente /vapid-public-key');
  const { publicKey, valid } = await r.json();
  if (!valid || !publicKey) throw new Error('Ugyldig/manglende VAPID public key');

  // Permission
  let perm = Notification.permission;
  if (perm === 'default' || forcePrompt) perm = await Notification.requestPermission();
  if (perm !== 'granted') throw new Error(`Notifikationer ikke tilladt (permission='${perm}')`);

  // Subscribe om nødvendigt
  let sub = await reg.pushManager.getSubscription();
  if (!sub) {
    const toKey = (k) => {
      const p = '='.repeat((4 - k.length % 4) % 4);
      const b = (k + p).replace(/-/g, '+').replace(/_/g, '/');
      const raw = atob(b);
      const out = new Uint8Array(raw.length);
      for (let i=0;i<raw.length;i++) out[i] = raw.charCodeAt(i);
      return out;
    };
    sub = await reg.pushManager.subscribe({
      userVisibleOnly: true,
      applicationServerKey: toKey(publicKey)
    });
  }

  // Knyt subscription til user_id (som i din tidligere v3)
  const user_id = getOrCreateUserId();
  const resp = await fetch(abs('api/subscribe'), {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({ ...sub.toJSON(), user_id })
  });
  if (!resp.ok) throw new Error(`Server afviste api/subscribe (HTTP ${resp.status})`);

  return true;
}

async function unsubscribePush() {
  await ensureSW();
  await navigator.serviceWorker.ready;
  const reg = await navigator.serviceWorker.getRegistration();
  const sub = await reg?.pushManager.getSubscription();
  if (sub) {
    try {
      await sub.unsubscribe();
      // (Valgfrit) Hvis du har et server-endpoint til at rydde sub i DB, kan du kalde det her.
      // await fetch(abs('api/unsubscribe'), { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ endpoint: sub.endpoint, user_id: getOrCreateUserId() }) });
      return true;
    } catch (e) {
      console.warn('Unsubscribe fejlede:', e);
    }
  }
  return false;
}

// ─────────────────────────── Prefs & filtrering ─────────────────────────────
const _norm = (s) => String(s ?? '').trim().toLowerCase();

async function loadPrefsForFiltering() {
  // 1) Per-bruger (DB)
  try {
    const userId = getOrCreateUserId();
    const r = await fetch(abs('api/prefs/user') + '?user_id=' + encodeURIComponent(userId), { cache: 'no-cache' });
    if (r.ok) {
      const data = await r.json();
      if (data && data.prefs && Object.keys(data.prefs).length) return data.prefs;
    }
  } catch {}
  // 2) Local fallback
  try {
    const local = JSON.parse(localStorage.getItem('dofnot-prefs') ?? '{}');
    if (local && typeof local === 'object') return local;
  } catch {}
  // 3) Tomt
  return {};
}

function filterItemsByPrefs(items, prefs) {
  if (!items || !items.length) return [];
  if (!prefs || !Object.keys(prefs).length) return items.slice();
  const prefsMap = new Map(Object.entries(prefs).map(([k,v]) => [_norm(k), _norm(v)]));
  return items.filter(it => {
    const afd = _norm(it.dof_afdeling);
    const cat = _norm(it.kategori);
    const sel = prefsMap.get(afd) ?? 'none';
    if (sel === 'none') return false;
    if (sel === 'alle') return true;
    if (sel === 'sub')  return cat === 'sub';
    if (sel === 'su')   return cat === 'su';
    return false;
  });
}

function slugify(s) {
  return String(s ?? '')
    .toLowerCase()
    .replace(/æ/g,'ae').replace(/ø/g,'oe').replace(/å/g,'aa')
    .normalize('NFD').replace(/[\u0300-\u036f]/g,'')
    .replace(/[^a-z0-9]+/g,'-').replace(/(^-|-$)/g,'');
}
function escapeHtml(s) {
  return String(s ?? '')
    .replace(/&/g,'&amp;').replace(/</g,'&lt;')
    .replace(/>/g,'&gt;').replace(/"/g,'&quot;')
    .replace(/'/g,'&#39;');
}

// ─────────────────────────── UI: præference-matrix ───────────────────────────
function renderTable(prefs) {
  if (!elGrid) return;
  let html = `
<table class="prefs-table" aria-label="Abonnementsfiltre pr. lokalafdeling">
  <thead><tr><th>Lokalafdeling</th><th>Ingen</th><th>SU</th><th>SUB</th><th>Bemærkelsesværdig</th></tr></thead>
  <tbody>`;
  for (const afd of DOF_AFDELINGER) {
    const slug = slugify(afd);
    const current = (prefs && prefs[afd]) ? String(prefs[afd]).toLowerCase() : 'none';
    html += `<tr><td class="afd">${escapeHtml(afd)}</td>`;
    for (const v of CHOICE_ORDER) {
      const id = `pref-${slug}-${v}`;
      const label = (v === 'none') ? 'Ingen' : v.toUpperCase();
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
  elGrid.querySelectorAll('.prefs-table td.sel').forEach(td => {
    td.addEventListener('click', () => {
      const input = td.querySelector('input[type=radio]');
      if (input) { input.checked = true; input.dispatchEvent(new Event('change', { bubbles:true })); }
    });
  });
}

// ─────────────────────────── Knap-tilstand (tekst + state/klasser) ───────────
function setSaveBtnState(state, label) {
  const btn = elSave;
  if (!btn) return;

  // state: subscribed | unsubscribed | blocked | unsupported
  btn.dataset.state = state;
  btn.classList.remove('is-subscribed','is-unsubscribed','is-blocked','is-unsupported');
  btn.classList.add(`is-${state}`);

  // Hvis knappen har et indre label-element, brug det – ellers knappen selv
  const labelEl = btn.querySelector('[data-role="label"]');
  if (labelEl) labelEl.textContent = label; else btn.textContent = label;

  btn.setAttribute('aria-label', label);
  btn.disabled = (state === 'blocked' || state === 'unsupported');

  // Unsubscribe-knappen må kun være aktiv når vi har en subscription
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

// ─────────────────────────── Handlinger (gem/subscribe + unsubscribe) ────────
async function onSave() {
  const prefs = {};
  for (const afd of DOF_AFDELINGER) {
    const slug = slugify(afd);
    const sel = document.querySelector(`input[name="pref-${slug}"]:checked`);
    prefs[afd] = sel ? sel.value : 'none';
  }

  // Gem lokalt (SW IndexedDB + localStorage)
  await postToSW({ type: 'SAVE_PREFS', prefs });
  localStorage.setItem('dofnot-prefs', JSON.stringify(prefs));

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

  // Forsøg at sikre subscription (prompt hvis nødvendigt)
  try {
    await ensurePushSubscription({ forcePrompt: true });
    if (elStatus) { elStatus.textContent = 'Abonnement + præferencer gemt.'; setTimeout(()=>elStatus.textContent='', 2000); }
  } catch (e) {
    if (elStatus) { elStatus.textContent = 'Gemte præferencer (push ikke tilladt/understøttet).'; setTimeout(()=>elStatus.textContent='', 2500); }
  } finally {
    updateSaveButtonLabel();
  }
}

async function onUnsubscribe() {
  try {
    const ok = await unsubscribePush();
    if (ok && elStatus) {
      elStatus.textContent = 'Abonnement afmeldt på denne enhed.';
      setTimeout(()=> elStatus.textContent='', 2000);
    }
  } catch (e) {
    console.warn('Unsubscribe fejlede:', e);
  } finally {
    updateSaveButtonLabel();
  }
}

// ─────────────────────────── Init ────────────────────────────────────────────
async function init() {
  setupDiagnostics();
  await ensureSW();

  // Init/bruger-tilknytning på server
  const userId = getOrCreateUserId();
  try {
    await fetch(abs('api/user/init'), {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ user_id: userId })
    });
  } catch {}
  postToSW({ type: 'SET_USER', user_id: userId }); // hydrere prefs i SW (IndexedDB) [2](https://aarhusuniversitet-my.sharepoint.com/personal/au669614_uni_au_dk/Documents/Microsoft%20Copilot%20Chat-filer/index.html)

  // Hent bruger-prefs (fallback til local)
  let prefs = {};
  try {
    const r = await fetch(abs('api/prefs/user') + '?user_id=' + encodeURIComponent(userId), { cache:'no-cache' });
    if (r.ok) { const data = await r.json(); if (data && data.prefs) prefs = data.prefs; }
  } catch {}
  if (!Object.keys(prefs).length) {
    try { const local = JSON.parse(localStorage.getItem('dofnot-prefs') ?? 'null'); if (local) prefs = local; } catch {}
  }

  // Render UI + knapper
  renderTable(prefs);
  updateSaveButtonLabel();

  if (elSave)  elSave.addEventListener('click', onSave);
  if (elUnsub) elUnsub.addEventListener('click', onUnsubscribe);

  // Hold label "frisk"
  document.addEventListener('visibilitychange', () => { if (!document.hidden) updateSaveButtonLabel(); });
  if ('serviceWorker' in navigator) {
    navigator.serviceWorker.addEventListener('controllerchange', updateSaveButtonLabel);
    navigator.serviceWorker.addEventListener('message', (e) => {
      if (e.data?.type === 'PUSH_SUB_CHANGED') updateSaveButtonLabel();
    });
  }

  // (Valgfrit) auto-subscribe: hvis permission=granted men ingen subscription
  if (supportsPush()) {
    try {
      await navigator.serviceWorker.ready;
      const reg = await navigator.serviceWorker.getRegistration();
      const sub = await reg?.pushManager.getSubscription();
      if (!sub && Notification.permission === 'granted') {
        await ensurePushSubscription({ forcePrompt: false });
        updateSaveButtonLabel();
      }
    } catch (e) {
      console.warn('[init] auto-subscribe failed:', e);
    }
  }
}

// ─────────────────────────── DOM ready ───────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => { init().catch(console.error); });

// (Valgfrit) eksponér til DevTools debugging:
// window.updateSaveButtonLabel = updateSaveButtonLabel;
// window.unsubscribePush = unsubscribePush;
// window.ensurePushSubscription = ensurePushSubscription;