require('dotenv').config();

const axios = require('axios');
const { getCollection } = require('../mongodbClient');
const { ObjectId } = require('mongodb');

const DEFAULT_COST_SETTINGS = {
  seguidores_mistos: 2.70,
  seguidores_brasileiros: 9.60,
  seguidores_organicos: 35.0,
  curtidas_mistos: 0.75,
  curtidas_brasileiras: 2.0,
  curtidas_organicas: 12.0,
  curtidas: 1.65,
  comentarios: 0.34,
  visualizacoes: 0.01
};

function parseCliArg(name) {
  try {
    const key = `--${String(name || '').trim()}`;
    const item = process.argv.find((a) => String(a || '').startsWith(`${key}=`));
    if (!item) return null;
    return String(item.split('=').slice(1).join('=') || '').trim() || null;
  } catch (_) {
    return null;
  }
}

function asBool(v) {
  const s = String(v == null ? '' : v).trim().toLowerCase();
  return s === '1' || s === 'true' || s === 'yes' || s === 'y';
}

function asInt(v, fallback) {
  const n = parseInt(String(v == null ? '' : v), 10);
  return Number.isFinite(n) ? n : fallback;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function parseIso(v) {
  try {
    const s = String(v || '').trim();
    if (!s) return '';
    const t = new Date(s).getTime();
    if (!Number.isFinite(t) || !t) return '';
    return new Date(t).toISOString();
  } catch (_) {
    return '';
  }
}

function parseCharge(v) {
  try {
    const s = String(v == null ? '' : v).trim();
    if (!s) return null;
    const n = Number(s.replace(',', '.').replace(/[^\d.-]/g, ''));
    return Number.isFinite(n) ? n : null;
  } catch (_) {
    return null;
  }
}

function extractChargeFromPayload(p) {
  if (!p || typeof p !== 'object') return null;
  return (
    parseCharge(p.charge) ??
    parseCharge(p.Charge) ??
    parseCharge(p.cost) ??
    parseCharge(p.Cost) ??
    parseCharge(p.price) ??
    parseCharge(p.Price) ??
    null
  );
}

function extractInfoAny(o, key) {
  try {
    const k = String(key || '').trim();
    if (!k) return '';
    const mapPaid = (o && o.additionalInfoMapPaid && typeof o.additionalInfoMapPaid === 'object') ? o.additionalInfoMapPaid : {};
    if (typeof mapPaid[k] !== 'undefined') return String(mapPaid[k] ?? '');
    const arrPaid = Array.isArray(o && o.additionalInfoPaid) ? o.additionalInfoPaid : [];
    const itPaid = arrPaid.find(x => x && String(x.key || '').trim() === k);
    if (itPaid && typeof itPaid.value !== 'undefined') return String(itPaid.value ?? '');
    const map = (o && o.additionalInfoMap && typeof o.additionalInfoMap === 'object') ? o.additionalInfoMap : {};
    if (typeof map[k] !== 'undefined') return String(map[k] ?? '');
    const arr = Array.isArray(o && o.additionalInfo) ? o.additionalInfo : [];
    const it = arr.find(x => x && String(x.key || '').trim() === k);
    if (it && typeof it.value !== 'undefined') return String(it.value ?? '');
  } catch (_) {}
  return '';
}

function toNumber(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function resolveQty(o) {
  const v0 = toNumber(o && (o.quantidade || o.qtd));
  if (v0 > 0) return v0;
  const q1 = toNumber(extractInfoAny(o, 'quantidade') || extractInfoAny(o, 'qtd'));
  return q1 > 0 ? q1 : 0;
}

function resolveTypeAndCategoryForCost(o) {
  let category = String(extractInfoAny(o, 'categoria_servico') || o?.categoriaServico || '').toLowerCase().trim();
  if (category === 'curtidas_brasileiras') category = 'curtidas';
  let type = String(extractInfoAny(o, 'tipo_servico') || o?.tipoServico || o?.tipo || '').toLowerCase().trim();
  if (category === 'curtidas' && type === 'mistos') type = 'curtidas_mistos';
  else if (category === 'curtidas' && (type === 'brasileiros' || type === 'curtidas_brasileiras' || /brasileir/.test(type))) type = 'curtidas_brasileiras';
  else if (category === 'curtidas' && type === 'organicos') type = 'curtidas_organicas';
  else if (category === 'seguidores' && type === 'mistos') type = 'seguidores_mistos';
  else if (category === 'seguidores' && type === 'brasileiros') type = 'seguidores_brasileiros';
  return { category, type };
}

function resolveCostPer1000(costSettings, typeForCost, categoryForCost) {
  const t = String(typeForCost || '').toLowerCase();
  const cat = String(categoryForCost || '').toLowerCase();
  if (t.includes('curtidas') && t.includes('mistos')) return Number(costSettings.curtidas_mistos || 0) || 0;
  if (t === 'curtidas_brasileiras') return Number((typeof costSettings.curtidas_brasileiras !== 'undefined' ? costSettings.curtidas_brasileiras : costSettings.curtidas) || 0) || 0;
  if (cat === 'curtidas' && t.includes('organicos')) return Number((typeof costSettings.curtidas_organicas !== 'undefined' ? costSettings.curtidas_organicas : costSettings.curtidas) || 0) || 0;
  if (t.includes('mistos')) return Number(costSettings.seguidores_mistos || 0) || 0;
  if (t.includes('brasileiros') && !t.includes('curtidas') && !t.includes('comentarios') && !t.includes('visualiza')) return Number(costSettings.seguidores_brasileiros || 0) || 0;
  if (t.includes('organicos')) return Number(costSettings.seguidores_organicos || 0) || 0;
  if (t.includes('curtidas')) return Number(costSettings.curtidas || 0) || 0;
  if (t.includes('comentarios')) return Number(costSettings.comentarios || 0) || 0;
  if (t.includes('visualiza') || t.includes('views')) return Number(costSettings.visualizacoes || 0) || 0;
  return 0;
}

function resolveProviderOrder(o) {
  try {
    const famaMulti = (o && o.fama24h_multi && Array.isArray(o.fama24h_multi.orders)) ? o.fama24h_multi.orders : [];
    const fsMulti = (o && o.fornecedor_social_multi && Array.isArray(o.fornecedor_social_multi.orders)) ? o.fornecedor_social_multi.orders : [];
    const pickMulti = (arr) => {
      for (const it of (arr || [])) {
        const oid = String((it && (it.orderId ?? it.id)) || '').trim();
        if (oid) return { orderId: oid, statusPayload: it && it.statusPayload ? it.statusPayload : null };
      }
      return null;
    };
    const famaOid = String((o && o.fama24h && (o.fama24h.orderId ?? o.fama24h.orderID ?? o.fama24h.order_id)) || '').trim();
    const fsOid = String((o && o.fornecedor_social && (o.fornecedor_social.orderId ?? o.fornecedor_social.orderID ?? o.fornecedor_social.order_id)) || '').trim();
    const fama = famaOid ? { provider: 'fama24h', orderId: famaOid, statusPayload: o.fama24h.statusPayload || null } : null;
    const fs = fsOid ? { provider: 'fornecedor_social', orderId: fsOid, statusPayload: o.fornecedor_social.statusPayload || null } : null;
    if (fs) return fs;
    if (fama) return fama;
    const fsM = pickMulti(fsMulti);
    if (fsM) return { provider: 'fornecedor_social', orderId: fsM.orderId, statusPayload: fsM.statusPayload };
    const famaM = pickMulti(famaMulti);
    if (famaM) return { provider: 'fama24h', orderId: famaM.orderId, statusPayload: famaM.statusPayload };
  } catch (_) {}
  return { provider: '', orderId: '', statusPayload: null };
}

async function fetchStatus(provider, orderId) {
  const key = provider === 'fama24h'
    ? String(process.env.FAMA24H_API_KEY || '').trim()
    : String(process.env.FORNECEDOR_SOCIAL_API_KEY || '').trim();
  if (!key) return null;
  const url = provider === 'fama24h' ? 'https://fama24h.net/api/v2' : 'https://fornecedorsocial.com/api/v2';
  const payload = new URLSearchParams({ key, action: 'status', order: String(orderId) });
  const resp = await axios.post(url, payload.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 15000 });
  const data = resp?.data || {};
  return (data && typeof data === 'object') ? data : null;
}

async function main() {
  const dryRun = asBool(parseCliArg('dry') || '0');
  const batchSize = Math.max(50, Math.min(2000, asInt(parseCliArg('batch') || '800', 800)));
  const concurrency = Math.max(1, Math.min(6, asInt(parseCliArg('concurrency') || '2', 2)));
  const onlyMissing = asBool(parseCliArg('onlyMissing') || '0');
  const maxDocs = Math.max(0, asInt(parseCliArg('limit') || '0', 0));
  const startIso = parseIso(parseCliArg('startIso') || '');
  const endIso = parseIso(parseCliArg('endIso') || '');

  const settingsCol = await getCollection('settings');
  const costSettingsDoc = await settingsCol.findOne({ _id: 'cost_settings' });
  const costSettings = Object.assign({}, DEFAULT_COST_SETTINGS, (costSettingsDoc && costSettingsDoc.values) || {});

  const ordersCol = await getCollection('checkout_orders');

  const filter = (function () {
    const and = [];
    if (startIso) and.push({ $or: [{ $and: [{ createdAt: { $type: 'date' } }, { createdAt: { $gte: new Date(startIso) } }] }, { $and: [{ createdAt: { $type: 'string' } }, { createdAt: { $gte: startIso } }] }] });
    if (endIso) and.push({ $or: [{ $and: [{ createdAt: { $type: 'date' } }, { createdAt: { $lte: new Date(endIso) } }] }, { $and: [{ createdAt: { $type: 'string' } }, { createdAt: { $lte: endIso } }] }] });
    if (onlyMissing) {
      and.push({
        $or: [
          { costs: { $exists: false } },
          { 'costs.recomputedAt': { $exists: false } },
          { 'costs.recomputedAt': null },
          { 'costs.recomputedAt': '' }
        ]
      });
    }
    if (!and.length) return {};
    return and.length === 1 ? and[0] : { $and: and };
  })();

  const projection = {
    _id: 1,
    createdAt: 1,
    quantidade: 1,
    qtd: 1,
    tipo: 1,
    tipoServico: 1,
    categoriaServico: 1,
    additionalInfoMapPaid: 1,
    additionalInfoPaid: 1,
    additionalInfoMap: 1,
    additionalInfo: 1,
    fama24h: 1,
    fama24h_multi: 1,
    fornecedor_social: 1,
    fornecedor_social_multi: 1,
    costs: 1
  };

  const statusCache = new Map();
  const chargeCache = new Map();

  let lastId = null;
  let scanned = 0;
  let updated = 0;
  let providerUpdated = 0;
  let estimatedUpdated = 0;
  let errors = 0;

  console.log('[recalcularCustosPedidos] start', { dryRun, batchSize, concurrency, onlyMissing, maxDocs: maxDocs || null, startIso: startIso || null, endIso: endIso || null });

  while (true) {
    const q = Object.assign({}, filter);
    if (lastId) {
      const and = q.$and ? q.$and.slice() : (Object.keys(q).length ? [q] : []);
      and.push({ _id: { $gt: lastId } });
      q.$and = and;
      if (Object.keys(q).length === 1 && Array.isArray(q.$and) && q.$and.length === 1) {
        Object.assign(q, q.$and[0]);
        delete q.$and;
      }
    }

    const docs = await ordersCol.find(q, { projection }).sort({ _id: 1 }).limit(batchSize).toArray();
    if (!docs.length) break;

    const runBatch = async (items) => {
      const tasks = items.map((o) => async () => {
        scanned++;
        const { provider, orderId, statusPayload } = resolveProviderOrder(o);
        const cacheKey = provider && orderId ? `${provider}:${orderId}` : '';

        let providerCharge = null;
        let providerChargeFrom = '';
        try {
          providerCharge = extractChargeFromPayload(statusPayload);
          if (providerCharge != null) providerChargeFrom = 'statusPayload';
        } catch (_) {}

        if (provider && orderId && providerCharge == null) {
          try {
            if (chargeCache.has(cacheKey)) {
              providerCharge = chargeCache.get(cacheKey);
              providerChargeFrom = 'cache';
            } else {
              let remote = null;
              if (statusCache.has(cacheKey)) remote = statusCache.get(cacheKey);
              else {
                remote = await fetchStatus(provider, orderId);
                statusCache.set(cacheKey, remote);
                await sleep(180);
              }
              providerCharge = extractChargeFromPayload(remote);
              if (providerCharge != null) providerChargeFrom = 'action_status';
              chargeCache.set(cacheKey, providerCharge);
            }
          } catch (_) {
            errors++;
          }
        }

        const qty = resolveQty(o);
        const { category, type } = resolveTypeAndCategoryForCost(o);
        const costPer1000 = resolveCostPer1000(costSettings, type, category);
        const estimatedServiceCost = (qty > 0 && costPer1000 > 0) ? ((qty / 1000) * costPer1000) : 0;

        const sets = {};
        const nowIso = new Date().toISOString();
        sets['costs.recomputedAt'] = nowIso;
        sets['costs.qty'] = qty || null;
        sets['costs.typeForCost'] = type || null;
        sets['costs.categoryForCost'] = category || null;
        sets['costs.costPer1000'] = (Number.isFinite(Number(costPer1000)) ? Number(costPer1000) : null);
        sets['costs.estimatedServiceCost'] = (Number.isFinite(Number(estimatedServiceCost)) ? Number(estimatedServiceCost) : null);
        sets['costs.provider'] = provider || null;
        sets['costs.providerOrderId'] = String(orderId || '').trim() || null;

        if (providerCharge != null) {
          sets['costs.providerCharge'] = providerCharge;
          sets['costs.providerChargeFrom'] = providerChargeFrom || null;
        } else {
          sets['costs.providerCharge'] = null;
          sets['costs.providerChargeFrom'] = null;
        }

        if (dryRun) {
          if (providerCharge != null) providerUpdated++;
          else estimatedUpdated++;
          updated++;
          return;
        }

        try {
          const r = await ordersCol.updateOne({ _id: o._id }, { $set: sets });
          if (r && r.matchedCount) {
            updated++;
            if (providerCharge != null) providerUpdated++;
            else estimatedUpdated++;
          }
        } catch (_) {
          errors++;
        }
      });

      let idx = 0;
      const workers = new Array(concurrency).fill(0).map(async () => {
        while (idx < tasks.length) {
          const cur = tasks[idx];
          idx++;
          await cur();
          if (maxDocs && scanned >= maxDocs) break;
        }
      });
      await Promise.all(workers);
    };

    await runBatch(docs);
    lastId = docs[docs.length - 1]._id;

    if (maxDocs && scanned >= maxDocs) break;
    if (scanned % 5000 === 0) {
      console.log('[recalcularCustosPedidos] progress', { scanned, updated, providerUpdated, estimatedUpdated, errors, lastId: String(lastId) });
    }
  }

  console.log('[recalcularCustosPedidos] done', { scanned, updated, providerUpdated, estimatedUpdated, errors });
}

main()
  .then(() => process.exit(0))
  .catch((e) => {
    console.error('[recalcularCustosPedidos] fatal', e?.message || String(e));
    process.exit(1);
  });
