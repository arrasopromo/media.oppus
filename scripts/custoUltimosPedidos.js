// scripts/custoUltimosPedidos.js
// ---------------------------------------------------------------------------
// RELATÓRIO (read-only) de CUSTO REAL dos últimos N pedidos pagos.
//
// Para cada pedido, junta TODOS os sub-pedidos feitos nos fornecedores
// (principal + bumps de views/curtidas/comentários), consulta o
// `action=status` de cada painel e soma o campo `charge` = custo real.
//
// Também lista os sub-pedidos com orderId "unknown"/faltando (pedidos que
// podem ter sido feitos no fornecedor mas o orderId não ficou registrado).
//
// NÃO grava nada no banco. Só lê e imprime.
//
// USO:
//   node scripts/custoUltimosPedidos.js
//   node scripts/custoUltimosPedidos.js --limit=10
//   node scripts/custoUltimosPedidos.js --limit=20 --json
//
// Requer .env com MONGODB_URI e as API keys dos fornecedores
// (FAMA24H_API_KEY, FORNECEDOR_SOCIAL_API_KEY, TOPFAMA_API_KEY,
//  WORLDSMM_API_KEY, GGRAM_API_KEY).
// ---------------------------------------------------------------------------

const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });

const axios = require('axios');
const { getCollection } = require('../mongodbClient');

// --- endpoints + env key de cada "provider slot" -------------------------
const PROVIDERS = {
  fama24h:                 { url: 'https://fama24h.net/api/v2',        keyEnv: 'FAMA24H_API_KEY',           nome: 'Fama24h' },
  fama24h_views:           { url: 'https://fama24h.net/api/v2',        keyEnv: 'FAMA24H_API_KEY',           nome: 'Fama24h (Bump Views)' },
  fama24h_likes:           { url: 'https://fama24h.net/api/v2',        keyEnv: 'FAMA24H_API_KEY',           nome: 'Fama24h (Bump Curtidas)' },
  fornecedor_social:       { url: 'https://fornecedorsocial.com/api/v2', keyEnv: 'FORNECEDOR_SOCIAL_API_KEY', nome: 'Fornecedor Social' },
  fornecedor_social_likes: { url: 'https://fornecedorsocial.com/api/v2', keyEnv: 'FORNECEDOR_SOCIAL_API_KEY', nome: 'Fornecedor Social (Bump Curtidas)' },
  topfama:                 { url: 'https://topfama.com/api/v2',        keyEnv: 'TOPFAMA_API_KEY',           nome: 'TopFama' },
  topfama_likes:           { url: 'https://topfama.com/api/v2',        keyEnv: 'TOPFAMA_API_KEY',           nome: 'TopFama (Bump Curtidas)' },
  worldsmm_comments:       { url: 'https://worldsmm.com.br/api/v2',    keyEnv: 'WORLDSMM_API_KEY',          nome: 'WorldSMM (Bump Comentários)' },
  ggram:                   { url: 'https://ggram.me/api/v2',           keyEnv: 'GGRAM_API_KEY',             nome: 'GGRAM (Curtidas BR)' },
};

function cliArg(name, def = null) {
  const key = `--${name}`;
  const hit = process.argv.find(a => a === key || a.startsWith(`${key}=`));
  if (!hit) return def;
  if (hit === key) return true;
  return hit.split('=').slice(1).join('=');
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function isUnknownToken(v) {
  const s = String(v == null ? '' : v).toLowerCase().trim();
  return s === '' || s === 'unknown' || s === 'unknow' || s === 'null' || s === 'undefined';
}

function parseCharge(v) {
  const s = String(v == null ? '' : v).trim();
  if (!s) return null;
  const n = Number(s.replace(',', '.').replace(/[^\d.-]/g, ''));
  return Number.isFinite(n) ? n : null;
}

function extractCharge(payload) {
  if (!payload || typeof payload !== 'object') return null;
  const cands = ['charge', 'Charge', 'cost', 'Cost', 'price', 'Price'];
  for (const k of cands) {
    const c = parseCharge(payload[k]);
    if (c != null) return c;
  }
  return null;
}

function statusText(payload) {
  if (!payload || typeof payload !== 'object') return '';
  return String(payload.status || payload.Status || payload.status_text || payload.statusText || '').trim();
}

// --- leitura de additionalInfo / order_bumps -----------------------------
function extractInfoAny(o, key) {
  const k = String(key || '').trim();
  if (!k) return '';
  const sources = [o.additionalInfoMapPaid, o.additionalInfoMap];
  for (const map of sources) {
    if (map && typeof map === 'object' && typeof map[k] !== 'undefined') return String(map[k] ?? '');
  }
  const arrs = [o.additionalInfoPaid, o.additionalInfo];
  for (const arr of arrs) {
    if (Array.isArray(arr)) {
      const it = arr.find(x => x && String(x.key || '').trim() === k);
      if (it && typeof it.value !== 'undefined') return String(it.value ?? '');
    }
  }
  return '';
}

function parseBumps(o) {
  const raw = extractInfoAny(o, 'order_bumps');
  const out = { likes: 0, views: 0, comments: 0 };
  if (!raw) return out;
  String(raw).split(';').forEach(part => {
    const [k, q] = String(part).split(':');
    const n = parseInt(q, 10) || 0;
    if (k === 'likes') out.likes += n;
    else if (k === 'views') out.views += n;
    else if (k === 'comments') out.comments += n;
  });
  return out;
}

function subQty(subdoc, fallback) {
  const q = subdoc && subdoc.requestPayload && subdoc.requestPayload.quantity;
  if (typeof q !== 'undefined' && q !== null && String(q).trim() !== '') return String(q);
  return fallback != null ? String(fallback) : '';
}

function resolveUser(o) {
  return String(
    extractInfoAny(o, 'instagram_username') || o.instauser || o.instagramUsername || ''
  ).replace(/^@+/, '').replace(/\/+$/, '').trim();
}

function resolveServiceLabel(o) {
  const cat = extractInfoAny(o, 'categoria_servico') || o.categoriaServico || '';
  const tipo = extractInfoAny(o, 'tipo_servico') || o.tipoServico || o.tipo || '';
  return [cat, tipo].filter(Boolean).join(' / ') || (o.type || '-');
}

function resolveQty(o) {
  const q = Number(o.quantidade || o.qtd || extractInfoAny(o, 'quantidade') || extractInfoAny(o, 'qtd'));
  return Number.isFinite(q) && q > 0 ? q : '';
}

function orderDateSP(o) {
  const cands = [o.paidAt, o.woovi && o.woovi.paidAt, o.paghiper && o.paghiper.paidAt, o.createdAt];
  for (const c of cands) {
    const t = c ? new Date(c).getTime() : NaN;
    if (Number.isFinite(t) && t) {
      const d = new Date(t - 3 * 3600 * 1000);
      return d.getUTCFullYear() + '-' +
        String(d.getUTCMonth() + 1).padStart(2, '0') + '-' +
        String(d.getUTCDate()).padStart(2, '0') + ' ' +
        String(d.getUTCHours()).padStart(2, '0') + ':' +
        String(d.getUTCMinutes()).padStart(2, '0');
    }
  }
  return '?';
}

// --- monta a lista de sub-pedidos (principal + bumps) de 1 pedido --------
function collectSubOrders(o) {
  const subs = [];
  const bumps = parseBumps(o);
  const qty = resolveQty(o);

  const pushDirect = (slot, subdoc, fallbackQty) => {
    if (!subdoc) return false;
    const oid = subdoc.orderId ?? subdoc.orderID ?? subdoc.order_id;
    subs.push({
      slot,
      provider: slot, // slot == chave de PROVIDERS
      orderId: (oid == null ? '' : String(oid)).trim(),
      qty: subQty(subdoc, fallbackQty),
      status: String(subdoc.status || '').trim(),
      statusPayload: subdoc.statusPayload || null,
    });
    return true;
  };

  const pushMulti = (slot, multi, fallbackQty) => {
    const orders = multi && Array.isArray(multi.orders) ? multi.orders : [];
    if (!orders.length) return false;
    orders.forEach((it, i) => {
      const oid = it && (it.orderId ?? it.id);
      subs.push({
        slot: `${slot}[${i}]`,
        provider: slot,
        orderId: (oid == null ? '' : String(oid)).trim(),
        qty: subQty(it, fallbackQty),
        status: String((it && it.status) || '').trim(),
        statusPayload: (it && it.statusPayload) || null,
      });
    });
    return true;
  };

  // ---- PRINCIPAL ----
  let hasMain = false;
  if (o.fama24h) hasMain = pushDirect('fama24h', o.fama24h, qty) || hasMain;
  else if (o.fama24h_multi) hasMain = pushMulti('fama24h', o.fama24h_multi, qty) || hasMain;
  if (o.fornecedor_social) hasMain = pushDirect('fornecedor_social', o.fornecedor_social, qty) || hasMain;
  else if (o.fornecedor_social_multi) hasMain = pushMulti('fornecedor_social', o.fornecedor_social_multi, qty) || hasMain;
  if (o.topfama) hasMain = pushDirect('topfama', o.topfama, qty) || hasMain;
  if (o.ggram) hasMain = pushDirect('ggram', o.ggram, qty) || hasMain;

  // Principal esperado mas não registrado (unknown)
  if (!hasMain) {
    subs.push({ slot: 'principal', provider: '', orderId: '', qty: String(qty || ''), status: 'unknown', statusPayload: null, missing: true });
  }

  // ---- BUMP VIEWS ----
  if (o.fama24h_views || bumps.views > 0) {
    if (o.fama24h_views) pushDirect('fama24h_views', o.fama24h_views, bumps.views);
    else subs.push({ slot: 'fama24h_views', provider: 'fama24h_views', orderId: '', qty: String(bumps.views || ''), status: 'unknown', statusPayload: null, missing: true });
  }

  // ---- BUMP CURTIDAS ---- (pode ser TopFama, Fornecedor Social ou Fama24h)
  if (o.topfama_likes || o.fama24h_likes || o.fornecedor_social_likes || bumps.likes > 0) {
    if (o.topfama_likes) pushDirect('topfama_likes', o.topfama_likes, bumps.likes);
    else if (o.fornecedor_social_likes) pushDirect('fornecedor_social_likes', o.fornecedor_social_likes, bumps.likes);
    else if (o.fama24h_likes) pushDirect('fama24h_likes', o.fama24h_likes, bumps.likes);
    else subs.push({ slot: 'bump_curtidas', provider: '', orderId: '', qty: String(bumps.likes || ''), status: 'unknown', statusPayload: null, missing: true });
  }

  // ---- BUMP COMENTÁRIOS ----
  if (o.worldsmm_comments || bumps.comments > 0) {
    if (o.worldsmm_comments) pushDirect('worldsmm_comments', o.worldsmm_comments, bumps.comments);
    else subs.push({ slot: 'worldsmm_comments', provider: 'worldsmm_comments', orderId: '', qty: String(bumps.comments || ''), status: 'unknown', statusPayload: null, missing: true });
  }

  return subs;
}

// --- consulta action=status no painel do fornecedor ----------------------
const chargeCache = new Map();
const missingKeyWarned = new Set();

async function fetchStatus(provider, orderId) {
  const cfg = PROVIDERS[provider];
  if (!cfg) return { error: 'provider_desconhecido' };
  const key = String(process.env[cfg.keyEnv] || '').trim();
  if (!key) {
    if (!missingKeyWarned.has(cfg.keyEnv)) { missingKeyWarned.add(cfg.keyEnv); }
    return { error: `sem ${cfg.keyEnv}` };
  }
  const cacheKey = `${provider}:${orderId}`;
  if (chargeCache.has(cacheKey)) return chargeCache.get(cacheKey);
  try {
    const payload = new URLSearchParams({ key, action: 'status', order: String(orderId) });
    const resp = await axios.post(cfg.url, payload.toString(), {
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      timeout: 20000,
    });
    const data = (resp && resp.data && typeof resp.data === 'object') ? resp.data : {};
    const out = { charge: extractCharge(data), status: statusText(data), raw: data };
    chargeCache.set(cacheKey, out);
    await sleep(200); // gentil com a API
    return out;
  } catch (e) {
    const out = { error: (e && e.message) ? e.message.slice(0, 120) : String(e) };
    chargeCache.set(cacheKey, out);
    return out;
  }
}

const money = (n) => (n == null ? '—' : 'R$ ' + Number(n).toFixed(2).replace('.', ','));

async function main() {
  const limit = Math.max(1, Math.min(200, parseInt(cliArg('limit', '10'), 10) || 10));
  const asJson = !!cliArg('json', false);

  const col = await getCollection('checkout_orders');
  const paidOr = [
    { status: 'pago' }, { 'woovi.status': 'pago' },
    { paidAt: { $exists: true, $nin: [null, ''] } },
    { 'woovi.paidAt': { $exists: true, $nin: [null, ''] } },
    { 'paghiper.paidAt': { $exists: true, $nin: [null, ''] } },
  ];

  const orders = await col
    .find({ $or: paidOr })
    .sort({ 'woovi.paidAt': -1, paidAt: -1, createdAt: -1, _id: -1 })
    .limit(limit)
    .toArray();

  if (!orders.length) {
    console.log('Nenhum pedido pago encontrado (verifique MONGODB_URI no .env).');
    process.exit(0);
  }

  const relatorio = [];
  let grandTotal = 0;
  let anyChargeMissing = false;
  const unknownList = [];

  for (const o of orders) {
    const subs = collectSubOrders(o);
    const linhas = [];
    let totalPedido = 0;
    let pedidoIncompleto = false;

    for (const s of subs) {
      const provNome = PROVIDERS[s.provider] ? PROVIDERS[s.provider].nome : (s.slot || s.provider || '?');
      if (s.missing || isUnknownToken(s.orderId) || !s.provider) {
        pedidoIncompleto = true;
        anyChargeMissing = true;
        const item = { fornecedor: provNome, slot: s.slot, orderId: s.orderId || '(vazio)', qty: s.qty, status: s.status || 'unknown', charge: null, obs: 'orderId ausente/unknown' };
        linhas.push(item);
        unknownList.push({ _id: String(o._id), user: resolveUser(o), servico: resolveServiceLabel(o), ...item });
        continue;
      }
      // charge já persistido no doc?
      let charge = extractCharge(s.statusPayload);
      let status = s.status || statusText(s.statusPayload);
      let obs = charge != null ? 'do banco' : '';
      if (charge == null) {
        const r = await fetchStatus(s.provider, s.orderId);
        if (r.error) { obs = r.error; }
        else { charge = r.charge; status = r.status || status; obs = charge != null ? 'action=status' : 'sem charge no retorno'; }
      }
      if (charge != null) totalPedido += charge;
      else { pedidoIncompleto = true; anyChargeMissing = true; }
      linhas.push({ fornecedor: provNome, slot: s.slot, orderId: s.orderId, qty: s.qty, status, charge, obs });
    }

    grandTotal += totalPedido;
    relatorio.push({
      _id: String(o._id),
      data: orderDateSP(o),
      user: resolveUser(o),
      servico: resolveServiceLabel(o),
      qty: resolveQty(o),
      totalPedido,
      incompleto: pedidoIncompleto,
      linhas,
    });
  }

  if (asJson) {
    console.log(JSON.stringify({ limit, grandTotal, anyChargeMissing, pedidos: relatorio, unknowns: unknownList }, null, 2));
    process.exit(0);
  }

  // --- impressão legível ---
  console.log('\n══════════════════════════════════════════════════════════════════');
  console.log(`  CUSTO DOS ÚLTIMOS ${orders.length} PEDIDOS PAGOS`);
  console.log('══════════════════════════════════════════════════════════════════');

  relatorio.forEach((p, idx) => {
    console.log(`\n#${idx + 1}  ${p.data} SP  ·  @${p.user || '-'}  ·  ${p.servico}  (qtd ${p.qty})`);
    console.log(`     _id: ${p._id}`);
    p.linhas.forEach(l => {
      const c = l.charge != null ? money(l.charge) : '   ???  ';
      console.log(
        `     • ${String(l.fornecedor).padEnd(34)} oid=${String(l.orderId).padEnd(12)} qtd=${String(l.qty || '-').padEnd(7)} ${String(l.status || '-').padEnd(11)} ${c.padStart(11)}  ${l.obs || ''}`
      );
    });
    console.log(`     ──> CUSTO DO PEDIDO: ${money(p.totalPedido)}${p.incompleto ? '  ⚠️ (incompleto — algum sub-pedido sem charge/orderId)' : ''}`);
  });

  console.log('\n──────────────────────────────────────────────────────────────────');
  console.log(`  CUSTO TOTAL (${orders.length} pedidos): ${money(grandTotal)}${anyChargeMissing ? '  ⚠️ parcial' : ''}`);
  console.log('──────────────────────────────────────────────────────────────────');

  if (unknownList.length) {
    console.log(`\n⚠️  SUB-PEDIDOS SEM ORDERID (unknown) — ${unknownList.length} ocorrência(s):`);
    console.log('   (pedido pode ter sido feito no fornecedor mas o orderId não ficou salvo)\n');
    unknownList.forEach(u => {
      console.log(`   • @${u.user || '-'}  ${u.servico}  → ${u.fornecedor} (${u.slot})  qtd=${u.qty || '-'}  status=${u.status}  _id=${u._id}`);
    });
  } else {
    console.log('\n✅ Nenhum sub-pedido com orderId unknown nos pedidos analisados.');
  }

  if (missingKeyWarned.size) {
    console.log(`\nℹ️  API keys ausentes no .env (charge desses fornecedores não foi consultado): ${[...missingKeyWarned].join(', ')}`);
  }

  console.log('');
  process.exit(0);
}

main().catch(e => {
  console.error('[custoUltimosPedidos] erro fatal:', e && e.message ? e.message : String(e));
  process.exit(1);
});
