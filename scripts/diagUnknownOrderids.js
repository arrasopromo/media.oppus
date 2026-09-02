// scripts/diagUnknownOrderids.js
// ---------------------------------------------------------------------------
// DIAGNÓSTICO (read-only) da listagem "orderid unknown" do /painel.
//
// Roda a MESMA query que o painel usa (unknownProviderQuery em app.js ~38780)
// e, para cada pedido, verifica no Mongo se o orderId de cada sub-pedido
// (principal + bumps de views/curtidas/comentários) EXISTE de verdade —
// olhando TODOS os campos de fornecedor, inclusive os que o painel ignora
// (topfama_likes, fornecedor_social_likes, ggram, *_multi).
//
// Separa em:
//   • REAL      → sub-pedido esperado SEM orderId em lugar nenhum (precisa cadastrar)
//   • FALSO     → painel marca unknown, mas o orderId JÁ EXISTE no Mongo (bug do painel)
//
// NÃO grava nada. Só lê e imprime.
//
// USO:
//   node scripts/diagUnknownOrderids.js
//   node scripts/diagUnknownOrderids.js --limit=200
// ---------------------------------------------------------------------------

const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });
const { getCollection } = require('../mongodbClient');

function cliArg(name, def = null) {
  const key = `--${name}`;
  const hit = process.argv.find(a => a === key || a.startsWith(`${key}=`));
  if (!hit) return def;
  if (hit === key) return true;
  return hit.split('=').slice(1).join('=');
}

// orderId "de verdade": tem dígito e não é token vazio/unknown
function validOid(v) {
  if (v === null || typeof v === 'undefined') return null;
  const s = String(v).trim();
  if (!s) return null;
  const low = s.toLowerCase();
  if (low === 'unknown' || low === 'unknow' || low === 'null' || low === 'undefined' || low === '0') return null;
  if (!/\d/.test(s)) return null;
  return s;
}

// procura orderId válido num subdoc direto OU no *_multi.orders[]
function oidFromSlot(subdoc) {
  if (!subdoc || typeof subdoc !== 'object') return null;
  const direct = validOid(subdoc.orderId ?? subdoc.orderID ?? subdoc.order_id ?? subdoc.id);
  if (direct) return direct;
  const orders = Array.isArray(subdoc.orders) ? subdoc.orders : null;
  if (orders) {
    for (const it of orders) {
      const oid = validOid(it && (it.orderId ?? it.id));
      if (oid) return oid;
    }
  }
  return null;
}

function extractInfoAny(o, key) {
  if (o.additionalInfoMapPaid && typeof o.additionalInfoMapPaid[key] !== 'undefined') return String(o.additionalInfoMapPaid[key] ?? '');
  if (o.additionalInfoMap && typeof o.additionalInfoMap[key] !== 'undefined') return String(o.additionalInfoMap[key] ?? '');
  const arrPaid = Array.isArray(o.additionalInfoPaid) ? o.additionalInfoPaid : [];
  const ip = arrPaid.find(i => i && i.key === key);
  if (ip && typeof ip.value !== 'undefined') return String(ip.value ?? '');
  const arr = Array.isArray(o.additionalInfo) ? o.additionalInfo : [];
  const it = arr.find(i => i && i.key === key);
  if (it && typeof it.value !== 'undefined') return String(it.value ?? '');
  return '';
}

function parseBumps(o) {
  const raw = extractInfoAny(o, 'order_bumps');
  const out = { views: 0, likes: 0, comments: 0 };
  String(raw || '').split(';').forEach(p => {
    const [k, v] = String(p).split(':');
    const n = parseInt(String(v || '').replace(/[^\d]/g, ''), 10) || 0;
    const key = String(k || '').toLowerCase().trim();
    if (key === 'views') out.views = n;
    else if (key === 'likes') out.likes = n;
    else if (key === 'comments') out.comments = n;
  });
  return out;
}

function resolveUser(o) {
  return String(extractInfoAny(o, 'instagram_username') || o.instauser || o.instagramUsername || '')
    .replace(/^@+/, '').replace(/\/+$/, '').trim();
}

function serviceLabel(o) {
  const cat = extractInfoAny(o, 'categoria_servico');
  const tipo = extractInfoAny(o, 'tipo_servico') || o.tipo || o.tipoServico || '';
  return [cat, tipo].filter(Boolean).join(' / ') || (o.type || '-');
}

function dateSP(o) {
  const c = (o.woovi && o.woovi.paidAt) || o.paidAt || o.createdAt;
  const t = c ? new Date(c).getTime() : NaN;
  if (!Number.isFinite(t)) return '?';
  const d = new Date(t - 3 * 3600 * 1000);
  return `${String(d.getUTCDate()).padStart(2,'0')}/${String(d.getUTCMonth()+1).padStart(2,'0')} ${String(d.getUTCHours()).padStart(2,'0')}:${String(d.getUTCMinutes()).padStart(2,'0')}`;
}

// Avalia cada slot esperado do pedido -> {found, oid, field}
function evalSlots(o) {
  const b = parseBumps(o);
  const slots = [];

  // MAIN — pode estar em fama24h / fornecedor_social / topfama / ggram (+_multi)
  const mainCandidates = [
    ['fama24h', o.fama24h], ['fama24h_multi', o.fama24h_multi],
    ['fornecedor_social', o.fornecedor_social], ['fornecedor_social_multi', o.fornecedor_social_multi],
    ['topfama', o.topfama], ['ggram', o.ggram],
  ];
  let mainOid = null, mainField = '';
  for (const [f, sd] of mainCandidates) {
    const oid = oidFromSlot(sd);
    if (oid) { mainOid = oid; mainField = f; break; }
  }
  const mainExists = mainCandidates.some(([, sd]) => sd && typeof sd === 'object');
  slots.push({ tipo: 'principal', esperado: true, found: !!mainOid, oid: mainOid, field: mainField || (mainExists ? '(existe mas sem oid)' : '(ausente)') });

  // VIEWS bump
  if (b.views > 0 || o.fama24h_views) {
    const oid = oidFromSlot(o.fama24h_views);
    slots.push({ tipo: 'bump_views', esperado: true, found: !!oid, oid, field: oid ? 'fama24h_views' : (o.fama24h_views ? 'fama24h_views (sem oid)' : '(ausente)') });
  }

  // LIKES bump — o painel só olha fama24h_likes; aqui olhamos os 3
  if (b.likes > 0 || o.fama24h_likes || o.fornecedor_social_likes || o.topfama_likes) {
    let oid = null, field = '(ausente)';
    for (const f of ['topfama_likes', 'fornecedor_social_likes', 'fama24h_likes']) {
      const v = oidFromSlot(o[f]);
      if (v) { oid = v; field = f; break; }
    }
    if (!oid) {
      const present = ['topfama_likes','fornecedor_social_likes','fama24h_likes'].find(f => o[f]);
      if (present) field = `${present} (sem oid)`;
    }
    slots.push({ tipo: 'bump_curtidas', esperado: true, found: !!oid, oid, field });
  }

  // COMMENTS bump
  if (b.comments > 0 || o.worldsmm_comments) {
    const oid = oidFromSlot(o.worldsmm_comments);
    slots.push({ tipo: 'bump_comentarios', esperado: true, found: !!oid, oid, field: oid ? 'worldsmm_comments' : (o.worldsmm_comments ? 'worldsmm_comments (sem oid)' : '(ausente)') });
  }

  return slots;
}

async function main() {
  const limit = Math.max(1, Math.min(5000, parseInt(cliArg('limit', '500'), 10) || 500));
  const col = await getCollection('checkout_orders');

  // === QUERY IDÊNTICA À DO PAINEL (/painel/unknown_orderid/export) ===
  const paidQuery = { $or: [{ status: 'pago' }, { 'woovi.status': 'pago' }] };
  const unknownProviderQuery = { $or: [
    { 'fama24h.status': 'unknown' }, { 'fama24h.orderId': { $in: ['unknown', 'unknow'] } },
    { 'fama24h.status': 'error' }, { 'fama24h.error': { $exists: true, $nin: [null, ''] } },
    { 'fornecedor_social.status': 'unknown' }, { 'fornecedor_social.orderId': { $in: ['unknown', 'unknow'] } },
    { 'fornecedor_social.status': 'error' }, { 'fornecedor_social.error': { $exists: true, $nin: [null, ''] } },
    { 'worldsmm_comments.status': 'unknown' }, { 'worldsmm_comments.orderId': { $in: ['unknown', 'unknow'] } },
    { 'worldsmm_comments.status': 'error' }, { 'worldsmm_comments.error': { $regex: 'timeout', $options: 'i' } },
    { $and: [ { worldsmm_comments: { $exists: true } }, { $or: [ { 'worldsmm_comments.orderId': { $exists: false } }, { 'worldsmm_comments.orderId': '' }, { 'worldsmm_comments.orderId': null } ] } ] },
    { $and: [ { $or: [ { additionalInfo: { $elemMatch: { key: 'order_bumps', value: { $regex: 'comments\\s*:\\s*[1-9]', $options: 'i' } } } }, { additionalInfoPaid: { $elemMatch: { key: 'order_bumps', value: { $regex: 'comments\\s*:\\s*[1-9]', $options: 'i' } } } }, { 'additionalInfoMap.order_bumps': { $regex: 'comments\\s*:\\s*[1-9]', $options: 'i' } }, { 'additionalInfoMapPaid.order_bumps': { $regex: 'comments\\s*:\\s*[1-9]', $options: 'i' } } ] }, { $or: [{ worldsmm_comments: { $exists: false } }, { 'worldsmm_comments.orderId': { $exists: false } }] } ] },
    { 'fama24h_views.status': 'unknown' }, { 'fama24h_views.orderId': { $in: ['unknown', 'unknow'] } },
    { 'fama24h_views.status': 'error' }, { 'fama24h_views.error': { $exists: true, $nin: [null, ''] } },
    { $and: [ { fama24h_views: { $exists: true } }, { $or: [ { 'fama24h_views.orderId': { $exists: false } }, { 'fama24h_views.orderId': '' }, { 'fama24h_views.orderId': null } ] } ] },
    { $and: [ { $or: [ { additionalInfo: { $elemMatch: { key: 'order_bumps', value: { $regex: 'views\\s*:\\s*[1-9]', $options: 'i' } } } }, { additionalInfoPaid: { $elemMatch: { key: 'order_bumps', value: { $regex: 'views\\s*:\\s*[1-9]', $options: 'i' } } } }, { 'additionalInfoMap.order_bumps': { $regex: 'views\\s*:\\s*[1-9]', $options: 'i' } }, { 'additionalInfoMapPaid.order_bumps': { $regex: 'views\\s*:\\s*[1-9]', $options: 'i' } } ] }, { $or: [ { fama24h_views: { $exists: false } }, { 'fama24h_views.orderId': { $exists: false } }, { 'fama24h_views.orderId': '' }, { 'fama24h_views.orderId': null } ] } ] },
    { 'fama24h_likes.status': 'unknown' }, { 'fama24h_likes.orderId': { $in: ['unknown', 'unknow'] } },
    { 'fama24h_likes.status': 'error' }, { 'fama24h_likes.error': { $exists: true, $nin: [null, ''] } },
    { $and: [ { fama24h_likes: { $exists: true } }, { $or: [ { 'fama24h_likes.orderId': { $exists: false } }, { 'fama24h_likes.orderId': '' }, { 'fama24h_likes.orderId': null } ] } ] },
    { $and: [ { $or: [ { additionalInfo: { $elemMatch: { key: 'order_bumps', value: { $regex: 'likes\\s*:\\s*[1-9]', $options: 'i' } } } }, { additionalInfoPaid: { $elemMatch: { key: 'order_bumps', value: { $regex: 'likes\\s*:\\s*[1-9]', $options: 'i' } } } }, { 'additionalInfoMap.order_bumps': { $regex: 'likes\\s*:\\s*[1-9]', $options: 'i' } }, { 'additionalInfoMapPaid.order_bumps': { $regex: 'likes\\s*:\\s*[1-9]', $options: 'i' } } ] }, { $or: [ { fama24h_likes: { $exists: false } }, { 'fama24h_likes.orderId': { $exists: false } }, { 'fama24h_likes.orderId': '' }, { 'fama24h_likes.orderId': null } ] } ] },
  ] };

  const orders = await col.find({ $and: [paidQuery, unknownProviderQuery] })
    .sort({ createdAt: -1 }).limit(limit).toArray();

  const real = [];   // pedidos com pelo menos 1 slot esperado SEM orderid
  const falso = [];  // pedidos que o painel marca mas TODOS os slots têm orderid
  const slotCount = {};    // contagem de slots REAL faltando por tipo
  const falseByField = {}; // contagem de falsos positivos por campo que salvou

  for (const o of orders) {
    const slots = evalSlots(o);
    const faltando = slots.filter(s => s.esperado && !s.found);
    const info = { _id: String(o._id), data: dateSP(o), user: resolveUser(o), servico: serviceLabel(o), slots };
    if (faltando.length) {
      real.push(info);
      faltando.forEach(s => { slotCount[s.tipo] = (slotCount[s.tipo] || 0) + 1; });
    } else {
      falso.push(info);
      // qual campo "salvou" (tipicamente likes via topfama/fornecedor social)
      slots.filter(s => s.found).forEach(s => {
        if (/likes/.test(s.field)) falseByField[s.field] = (falseByField[s.field] || 0) + 1;
      });
    }
  }

  console.log('\n════════════════════════════════════════════════════════════════════');
  console.log(`  DIAGNÓSTICO "ORDERID UNKNOWN" — ${orders.length} pedidos que o painel lista`);
  console.log('════════════════════════════════════════════════════════════════════');
  console.log(`  ✅ FALSO positivo (orderid JÁ existe no Mongo): ${falso.length}`);
  console.log(`  ⚠️  REAL (falta cadastrar de verdade):          ${real.length}`);
  if (Object.keys(slotCount).length) {
    console.log('     Slots realmente faltando:', Object.entries(slotCount).map(([k,v]) => `${k}=${v}`).join('  '));
  }
  if (Object.keys(falseByField).length) {
    console.log('     Falsos positivos salvos por:', Object.entries(falseByField).map(([k,v]) => `${k}=${v}`).join('  '));
  }

  console.log('\n────────────────────────────────────────────────────────────────────');
  console.log(`⚠️  PRECISA CADASTRAR DE VERDADE (${real.length}):`);
  console.log('────────────────────────────────────────────────────────────────────');
  if (!real.length) console.log('   (nenhum)');
  real.forEach(p => {
    const falta = p.slots.filter(s => s.esperado && !s.found).map(s => s.tipo).join(', ');
    console.log(`\n   ${p.data} SP · @${p.user || '-'} · ${p.servico}  [_id ${p._id}]`);
    console.log(`      FALTA: ${falta}`);
    p.slots.forEach(s => {
      const mark = s.found ? '✓' : '✗';
      console.log(`        ${mark} ${s.tipo.padEnd(16)} ${s.found ? ('oid=' + s.oid + '  (' + s.field + ')') : ('SEM ORDERID  ' + s.field)}`);
    });
  });

  console.log('\n────────────────────────────────────────────────────────────────────');
  console.log(`✅ FALSO POSITIVO — já tem orderid, é bug do painel (${falso.length}):`);
  console.log('────────────────────────────────────────────────────────────────────');
  if (!falso.length) console.log('   (nenhum)');
  falso.slice(0, 60).forEach(p => {
    const found = p.slots.filter(s => s.found).map(s => `${s.tipo}=${s.oid}(${s.field})`).join(', ');
    console.log(`   ${p.data} · @${p.user || '-'} · ${p.servico} → ${found}  [_id ${p._id}]`);
  });
  if (falso.length > 60) console.log(`   ... e mais ${falso.length - 60} (use --json pra ver todos)`);

  if (cliArg('json', false)) {
    console.log('\n===JSON===');
    console.log(JSON.stringify({ total: orders.length, real, falso, slotCount, falseByField }, null, 2));
  }

  console.log('');
  process.exit(0);
}

main().catch(e => { console.error('[diag] erro:', e && e.message ? e.message : String(e)); process.exit(1); });
