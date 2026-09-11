// instagramScraper.js
// Scraper PRÓPRIO de perfil do Instagram (sem RocketAPI/Apify/API de terceiros).
//
// Como funciona: a página pública do perfil (instagram.com/<usuario>/) vem já
// montada, com os dados do perfil embutidos, quando pedida com o identificador de
// robô de pré-visualização de links (o mesmo tipo que monta a prévia de um link no
// WhatsApp/Facebook). Dela saem: seguidores EXATOS, seguindo, privado, verificado,
// nome, bio, foto e IDs. O nº de posts vem do texto de pré-visualização (og:description).
// Posts recentes NÃO vêm sem login.
//
// Limites honestos: não é uma API oficial. O Instagram pode mudar a página ou passar
// a bloquear a qualquer momento — por isso há cache, fila com intervalo mínimo entre
// consultas e "stale-if-error" (devolve o último dado conhecido se o Instagram negar).
'use strict';

const USERNAME_RE = /^[a-z0-9._]{1,30}$/;
const UA = String(process.env.IG_SCRAPER_UA || 'facebookexternalhit/1.1 (+http://www.facebook.com/externalhit_uatext.php)');
const CACHE_MS = Math.max(0, Number(process.env.IG_SCRAPER_CACHE_MIN || 10)) * 60 * 1000;
const MIN_GAP_MS = Math.max(0, Number(process.env.IG_SCRAPER_MIN_GAP_MS || 400));
const MAX_CONCURRENCY = Math.max(1, Number(process.env.IG_SCRAPER_CONCURRENCY || 3));
const TIMEOUT_MS = Math.max(2000, Number(process.env.IG_SCRAPER_TIMEOUT_MS || 12000));
const CACHE_MAX = 5000;

const cache = new Map();      // username -> { at, data }
const inflight = new Map();   // username -> Promise
let active = 0, lastStartAt = 0;
const waiters = [];
let blockedUntil = 0;          // quando o Instagram nega, espera antes de insistir

function normalizeUsername(raw) {
  let s = String(raw == null ? '' : raw).trim();
  const m = s.match(/instagram\.com\/([^/?#\s]+)/i);   // aceita link do perfil
  if (m) s = m[1];
  return s.replace(/^@+/, '').replace(/\/+$/, '').toLowerCase();
}

function decodeEntities(s) {
  return String(s || '')
    .replace(/&#x([0-9a-f]+);/gi, (_, h) => String.fromCodePoint(parseInt(h, 16)))
    .replace(/&#(\d+);/g, (_, d) => String.fromCodePoint(parseInt(d, 10)))
    .replace(/&quot;/g, '"').replace(/&#039;/g, "'").replace(/&apos;/g, "'")
    .replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&');
}

// "8,584" / "8.584" / "1.234.567" → inteiro. "687M" / "1,2 mil" / "12.3K" → aproximado.
function parseCount(txt) {
  const t = String(txt || '').trim().toLowerCase().replace(/\s+/g, ' ');
  const abbr = t.match(/^([\d.,]+)\s*(k|m|mi|mil|b|bi)\b/);
  if (abbr) {
    const n = Number(abbr[1].replace(',', '.'));
    const mult = { k: 1e3, mil: 1e3, m: 1e6, mi: 1e6, b: 1e9, bi: 1e9 }[abbr[2]] || 1;
    return Number.isFinite(n) ? { value: Math.round(n * mult), exact: false } : null;
  }
  const digits = t.replace(/[^\d]/g, '');
  return digits ? { value: Number(digits), exact: true } : null;
}

// og:description em pt ou en:
//   "687M seguidores, seguindo 292, 8,584 posts — …"
//   "281 Followers, 461 Following, 18 Posts - See Instagram photos…"
function parseOgDescription(og) {
  const s = decodeEntities(og);
  const pick = (re) => { const m = s.match(re); return m ? parseCount(m[1]) : null; };
  const NUM = '([\\d.,]+\\s*(?:k|m|mi|mil|b|bi)?)';
  return {
    followers: pick(new RegExp(NUM + '\\s*(?:seguidores|followers)', 'i')),
    // pt: "seguindo 292" ou "292 seguindo"; en: "461 Following"
    following: pick(new RegExp('seguindo\\s*' + NUM, 'i')) || pick(new RegExp(NUM + '\\s*(?:seguindo|following)', 'i')),
    posts: pick(new RegExp(NUM + '\\s*(?:posts|publica[cç][õo]es)', 'i')),
  };
}

// Acha o objeto JSON do usuário (o que tem "follower_count" e o username pedido).
function extractUserObject(html, username) {
  let from = 0;
  for (let guard = 0; guard < 20; guard++) {
    const i = html.indexOf('"follower_count"', from);
    if (i < 0) return null;
    from = i + 16;
    let start = -1, depth = 0;
    for (let k = i; k >= 0 && i - k < 20000; k--) {
      const c = html[k];
      if (c === '}') depth++;
      else if (c === '{') { if (depth === 0) { start = k; break; } depth--; }
    }
    if (start < 0) continue;
    let end = -1, d = 0, inStr = false;
    for (let k = start; k < html.length && k - start < 60000; k++) {
      const c = html[k];
      if (inStr) { if (c === '\\') { k++; continue; } if (c === '"') inStr = false; continue; }
      if (c === '"') inStr = true;
      else if (c === '{') d++;
      else if (c === '}') { d--; if (d === 0) { end = k + 1; break; } }
    }
    if (end < 0) continue;
    try {
      const o = JSON.parse(html.slice(start, end));
      if (o && String(o.username || '').toLowerCase() === username) return o;
    } catch (_) {}
  }
  return null;
}

async function slot() {
  if (active >= MAX_CONCURRENCY) await new Promise((r) => waiters.push(r));
  active++;
  const wait = lastStartAt + MIN_GAP_MS - Date.now();
  if (wait > 0) await new Promise((r) => setTimeout(r, wait));
  lastStartAt = Date.now();
}
function release() { active--; const w = waiters.shift(); if (w) w(); }

async function fetchFromInstagram(username) {
  await slot();
  try {
    const ctrl = new AbortController();
    const to = setTimeout(() => ctrl.abort(), TIMEOUT_MS);
    let resp, html;
    try {
      resp = await fetch(`https://www.instagram.com/${encodeURIComponent(username)}/`, {
        headers: { 'User-Agent': UA, 'Accept': 'text/html', 'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8' },
        redirect: 'manual', signal: ctrl.signal,
      });
      html = await resp.text();
    } finally { clearTimeout(to); }
    const loc = resp.headers.get('location') || '';
    if (resp.status === 429 || (resp.status >= 300 && resp.status < 400 && /login|challenge|accounts/i.test(loc))) {
      blockedUntil = Date.now() + 5 * 60 * 1000;
      return { ok: false, status: 'blocked', httpStatus: resp.status };
    }
    if (resp.status === 404) return { ok: false, status: 'not_found', httpStatus: 404 };
    if (resp.status !== 200) return { ok: false, status: 'error', httpStatus: resp.status };

    const u = extractUserObject(html, username);
    const ogM = html.match(/<meta[^>]+property="og:description"[^>]+content="([^"]*)"/i) || html.match(/<meta[^>]+content="([^"]*)"[^>]+property="og:description"/i);
    const og = ogM ? parseOgDescription(ogM[1]) : null;
    if (!u && !(og && og.followers)) {
      // Página sem dados: perfil não existe (ou foi removido/suspenso).
      const titulo = (html.match(/<title>([^<]*)<\/title>/i) || [])[1] || '';
      if (!ogM || /n[ãa]o encontrad|not found|isn.t available|n[ãa]o est[áa] dispon/i.test(decodeEntities(titulo))) return { ok: false, status: 'not_found', httpStatus: 200 };
      return { ok: false, status: 'error', httpStatus: 200, detail: 'layout_desconhecido' };
    }
    const followers = u && Number.isFinite(Number(u.follower_count)) ? Number(u.follower_count) : (og && og.followers ? og.followers.value : null);
    const profile = {
      username: (u && u.username) || username,
      fullName: u ? String(u.full_name || '') : null,
      biography: u ? String(u.biography || '') : null,
      followers,
      followersExact: !!(u && Number.isFinite(Number(u.follower_count))) || !!(og && og.followers && og.followers.exact),
      following: u && Number.isFinite(Number(u.following_count)) ? Number(u.following_count) : (og && og.following ? og.following.value : null),
      posts: og && og.posts ? og.posts.value : null,
      isPrivate: u ? u.is_private === true : null,
      isVerified: u ? u.is_verified === true : null,
      hasReels: u ? u.has_any_clips === true : null,
      profilePicUrl: u && u.profile_pic_url ? String(u.profile_pic_url) : null,
      instagramId: u && u.pk ? String(u.pk) : null,
      source: 'instagram_html',
      fetchedAt: new Date().toISOString(),
    };
    return { ok: true, status: 'ok', profile };
  } catch (e) {
    return { ok: false, status: 'error', detail: (e && e.name === 'AbortError') ? 'timeout' : String((e && e.message) || e) };
  } finally { release(); }
}

// API do módulo. opts.fresh=true ignora o cache.
async function getInstagramProfile(raw, opts = {}) {
  const username = normalizeUsername(raw);
  if (!USERNAME_RE.test(username)) return { ok: false, status: 'invalid_username' };
  const hit = cache.get(username);
  if (!opts.fresh && hit && (Date.now() - hit.at) < CACHE_MS) return Object.assign({}, hit.data, { cached: true });
  if (Date.now() < blockedUntil) {
    if (hit) return Object.assign({}, hit.data, { cached: true, stale: true });
    return { ok: false, status: 'blocked', retryAfterSec: Math.ceil((blockedUntil - Date.now()) / 1000) };
  }
  if (inflight.has(username)) return inflight.get(username);
  const p = (async () => {
    const r = await fetchFromInstagram(username);
    if (r.ok || r.status === 'not_found') {
      cache.set(username, { at: Date.now(), data: r });
      if (cache.size > CACHE_MAX) cache.delete(cache.keys().next().value);
    } else if (hit) {
      return Object.assign({}, hit.data, { cached: true, stale: true, upstream: r.status });   // stale-if-error
    }
    return Object.assign({}, r, { cached: false });
  })();
  inflight.set(username, p);
  try { return await p; } finally { inflight.delete(username); }
}

module.exports = { getInstagramProfile, normalizeUsername, parseOgDescription, parseCount };
