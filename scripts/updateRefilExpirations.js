require('dotenv').config();

const { getCollection } = require('../mongodbClient');
const { ObjectId } = require('mongodb');

function safeDateMs(iso) {
  try {
    const t = new Date(iso).getTime();
    return Number.isFinite(t) ? t : 0;
  } catch (_) {
    return 0;
  }
}

function buildInfoMap(order) {
  const arrPaid = Array.isArray(order?.additionalInfoPaid) ? order.additionalInfoPaid : [];
  const arrOrig = Array.isArray(order?.additionalInfo) ? order.additionalInfo : [];
  const mapBase = Object.assign(
    {},
    (order?.additionalInfoMapPaid && typeof order.additionalInfoMapPaid === 'object') ? order.additionalInfoMapPaid : {},
    (order?.additionalInfoMap && typeof order.additionalInfoMap === 'object') ? order.additionalInfoMap : {}
  );
  const mapFromArr = (arrPaid.length ? arrPaid : arrOrig).reduce((acc, it) => {
    const k = String(it?.key || '').trim();
    if (!k) return acc;
    acc[k] = String(it?.value || '').trim();
    return acc;
  }, {});
  return Object.assign({}, mapBase, mapFromArr);
}

function escapeRegex(s) {
  return String(s || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function normalizeInstaUser(v) {
  try {
    let s = String(v || '').trim();
    if (!s) return '';
    if (/^https?:\/\//i.test(s)) {
      s = s.replace(/^https?:\/\/(www\.)?instagram\.com\//i, '');
      s = s.split('?')[0].split('#')[0];
      s = s.replace(/\/+$/, '');
      const parts = s.split('/').filter(Boolean);
      s = parts.length ? String(parts[parts.length - 1] || '') : s;
    }
    s = s.trim();
    if (s.startsWith('@')) s = s.slice(1);
    return s.toLowerCase().trim();
  } catch (_) {
    return '';
  }
}

function orderInstaKey(order) {
  const map = buildInfoMap(order || {});
  const raw = map.instagram_username || order?.instauser || order?.instagramUsername || '';
  return normalizeInstaUser(raw);
}

function parseBumpKeys(order) {
  const map = buildInfoMap(order || {});
  const bumpsStr = String(map.order_bumps || '').trim();
  if (!bumpsStr) return [];
  return bumpsStr
    .split(';')
    .map((s) => String(s || '').trim())
    .filter(Boolean)
    .map((part) => {
      const segs = String(part || '').split(':');
      const key = String(segs[0] || '').trim().toLowerCase();
      if (!key) return '';
      const qtyRaw = segs.length > 1 ? String(segs[1] || '').trim() : '';
      const qtyParsed = qtyRaw ? Number(qtyRaw) : 1;
      const qty = Number.isFinite(qtyParsed) ? qtyParsed : 1;
      if (qty <= 0) return '';
      return key;
    })
    .filter(Boolean);
}

function orderBaseMs(order) {
  const pick = (v) => {
    try {
      if (!v) return 0;
      if (typeof v === 'number' && Number.isFinite(v)) return v;
      const t = new Date(v).getTime();
      return Number.isFinite(t) ? t : 0;
    } catch (_) {
      return 0;
    }
  };
  const a = pick(order?.woovi?.paidAt);
  if (a) return a;
  const b = pick(order?.paidAt);
  if (b) return b;
  const c = pick(order?.createdAt);
  if (c) return c;
  return 0;
}

function isPaid(order) {
  const st = String(order?.status || '').toLowerCase();
  const wst = String(order?.woovi?.status || '').toLowerCase();
  if (st === 'pago' || wst === 'pago') return true;
  if (order?.paidAt) return true;
  if (order?.woovi?.paidAt) return true;
  return false;
}

function resolveTipoCategoria(order) {
  const map = buildInfoMap(order || {});
  const tipo = String(map.tipo_servico || order?.tipoServico || order?.tipo || '').trim().toLowerCase();
  const categoria = String(map.categoria_servico || order?.categoriaServico || '').trim().toLowerCase();
  return { tipo, categoria };
}

function isEligibleRefil(order) {
  const { tipo, categoria } = resolveTipoCategoria(order);
  const isSeguidores = !categoria || categoria.includes('seguidores');
  const isOkTipo = tipo.includes('mistos') || tipo.includes('brasileir');
  return isSeguidores && isOkTipo;
}

function warrantyFromBumpKeys(keys) {
  const arr = Array.isArray(keys) ? keys : [];
  if (arr.includes('warranty_lifetime') || arr.includes('warranty_life')) return { isLifetime: true, months: null, mode: 'life', days: null };
  return { isLifetime: false, months: 1, mode: '30', days: 30 };
}

function addMonthsEndOfDayBrtIso(baseMs, monthsToAdd) {
  const brtOffsetMs = 3 * 60 * 60 * 1000;
  const base = new Date(Number(baseMs || 0) - brtOffsetMs);
  const baseY = base.getUTCFullYear();
  const baseM = base.getUTCMonth() + 1;
  const baseD = base.getUTCDate();
  let y = baseY;
  let m = baseM + Number(monthsToAdd || 0);
  while (m > 12) { y += 1; m -= 12; }
  while (m < 1) { y -= 1; m += 12; }
  const maxDay = new Date(Date.UTC(Number(y), Number(m), 0)).getUTCDate();
  const d = Math.min(baseD, maxDay);
  const utcMs = Date.UTC(y, m - 1, d, 23, 59, 59, 999) + brtOffsetMs;
  return new Date(utcMs).toISOString();
}

(async () => {
  const tl = await getCollection('temporary_links');
  const ordersCol = await getCollection('checkout_orders');

  const cursor = tl.find(
    { purpose: 'refil' },
    { projection: { id: 1, orderId: 1, orders: 1, createdAt: 1, expiresAt: 1, warrantyMode: 1, warrantyDays: 1 } }
  );

  const dayMs = 24 * 60 * 60 * 1000;
  const targetDays = 30;

  let scanned = 0;
  let updated = 0;
  let skippedNoOrder = 0;
  let skippedNoEligibleOrder = 0;
  let errors = 0;

  while (await cursor.hasNext()) {
    const link = await cursor.next();
    scanned++;

    const token = String(link?.id || '').trim();
    if (!token || !/^[0-9a-z]+$/i.test(token)) continue;
    const linkedIds = new Set();
    if (link?.orderId && /^[0-9a-fA-F]{24}$/.test(String(link.orderId))) linkedIds.add(String(link.orderId));
    const arr = Array.isArray(link?.orders) ? link.orders : [];
    for (const x of arr) { if (x && /^[0-9a-fA-F]{24}$/.test(String(x))) linkedIds.add(String(x)); }

    const orders = [];
    try {
      const ids = Array.from(linkedIds).map((x) => new ObjectId(String(x)));
      if (ids.length) {
        const found = await ordersCol.find(
          { _id: { $in: ids } },
          { projection: { _id: 1, status: 1, woovi: 1, paidAt: 1, createdAt: 1, instauser: 1, instagramUsername: 1, customer: 1, additionalInfoPaid: 1, additionalInfo: 1, additionalInfoMapPaid: 1, additionalInfoMap: 1, tipoServico: 1, tipo: 1, categoriaServico: 1 } }
        ).toArray();
        for (const o of (found || [])) orders.push(o);
      }
    } catch (_) {}

    const phoneDigits = String(link?.phone || '').replace(/\D/g, '');
    const instaKey = normalizeInstaUser(link?.instauser || '');
    if (orders.length === 0 && (phoneDigits || instaKey)) {
      const paidFilter = { $or: [ { status: 'pago' }, { 'woovi.status': 'pago' }, { paidAt: { $exists: true, $ne: null } }, { 'woovi.paidAt': { $exists: true, $ne: null } } ] };
      const conds = [];
      if (phoneDigits) conds.push({ 'customer.phone': `+55${phoneDigits}` });
      if (instaKey) {
        const re = new RegExp(`^@?${escapeRegex(instaKey)}$`, 'i');
        conds.push({ instauser: { $regex: re } });
        conds.push({ instagramUsername: { $regex: re } });
        conds.push({ 'additionalInfoMapPaid.instagram_username': { $regex: re } });
        conds.push({ 'additionalInfoMap.instagram_username': { $regex: re } });
      }
      if (conds.length) {
        try {
          const recent = await ordersCol.find({ $and: [ paidFilter, { $or: conds } ] })
            .sort({ 'woovi.paidAt': -1, paidAt: -1, createdAt: -1, _id: -1 })
            .limit(20)
            .toArray();
          for (const o of (recent || [])) orders.push(o);
        } catch (_) {}
      }
    }

    const eligibleOrders = orders.filter((o) => {
      if (!(isPaid(o) && isEligibleRefil(o))) return false;
      if (instaKey) return orderInstaKey(o) === instaKey;
      return true;
    });
    if (!eligibleOrders.length) {
      if (linkedIds.size === 0) skippedNoOrder++;
      else skippedNoEligibleOrder++;
      continue;
    }

    eligibleOrders.sort((a, b) => Number(orderBaseMs(b) || 0) - Number(orderBaseMs(a) || 0));
    const lastOrder = eligibleOrders[0];
    const anyLifetime = eligibleOrders.some((o) => {
      const bumpKeys = parseBumpKeys(o);
      return bumpKeys.includes('warranty_lifetime') || bumpKeys.includes('warranty_life');
    });
    const warranty = anyLifetime ? { isLifetime: true, months: null, mode: 'life', days: null } : warrantyFromBumpKeys(parseBumpKeys(lastOrder));
    const baseMs = orderBaseMs(lastOrder);
    if (!baseMs) {
      skippedNoEligibleOrder++;
      continue;
    }
    const desiredExpiresAt = warranty.isLifetime
      ? new Date('2099-12-31T23:59:59.999Z').toISOString()
      : addMonthsEndOfDayBrtIso(baseMs, Number(warranty.months || 1));

    const currentExpMs = safeDateMs(link?.expiresAt);
    const desiredExpMs = safeDateMs(desiredExpiresAt);

    const shouldUpdate = !currentExpMs || (desiredExpMs && Math.abs(desiredExpMs - currentExpMs) > (60 * 1000));
    if (!shouldUpdate) continue;

    const sets = {
      expiresAt: desiredExpiresAt,
      warrantyMode: warranty.mode,
      warrantyDays: warranty.days,
      createdAt: new Date(baseMs).toISOString()
    };
    if (lastOrder && lastOrder._id) sets.orderId = String(lastOrder._id);

    try {
      const addToSet = {};
      if (lastOrder && lastOrder._id) addToSet.orders = String(lastOrder._id);
      await tl.updateOne({ id: token }, { $set: sets, ...(Object.keys(addToSet).length ? { $addToSet: addToSet } : {}) });
      updated++;
    } catch (e) {
      errors++;
      try { console.error('update_failed', token, e?.message || String(e)); } catch (_) {}
    }
  }

  console.log(JSON.stringify({ ok: errors === 0, scanned, updated, skippedNoOrder, skippedNoEligibleOrder, errors }));
  process.exit(errors ? 1 : 0);
})().catch((e) => {
  try { console.error(e?.message || String(e)); } catch (_) {}
  process.exit(1);
});
