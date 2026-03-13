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

function parseBumpKeys(order) {
  const map = buildInfoMap(order || {});
  const bumpsStr = String(map.order_bumps || '').trim();
  if (!bumpsStr) return [];
  return bumpsStr
    .split(';')
    .map((s) => String(s || '').trim())
    .filter(Boolean)
    .map((s) => String(s.split(':')[0] || '').trim().toLowerCase())
    .filter(Boolean);
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
  let skippedNoCreatedAt = 0;
  let errors = 0;

  while (await cursor.hasNext()) {
    const link = await cursor.next();
    scanned++;

    const token = String(link?.id || '').trim();
    const oidStr = String(link?.orderId || (Array.isArray(link?.orders) ? (link.orders[0] || '') : '') || '').trim();
    if (!token || !/^[0-9a-z]+$/i.test(token)) continue;
    if (!/^[0-9a-fA-F]{24}$/.test(oidStr)) {
      skippedNoOrder++;
      continue;
    }

    const createdMs = safeDateMs(link?.createdAt);
    if (!createdMs) {
      skippedNoCreatedAt++;
      continue;
    }

    let order = null;
    try {
      order = await ordersCol.findOne(
        { _id: new ObjectId(oidStr) },
        { projection: { _id: 1, additionalInfoPaid: 1, additionalInfo: 1, additionalInfoMapPaid: 1, additionalInfoMap: 1 } }
      );
    } catch (_) {}

    const bumpKeys = parseBumpKeys(order);
    const hasLifetime = bumpKeys.includes('warranty_lifetime');

    const desiredExpiresAt = hasLifetime
      ? new Date('2099-12-31T23:59:59.999Z').toISOString()
      : new Date(createdMs + (targetDays * dayMs)).toISOString();

    const currentExpMs = safeDateMs(link?.expiresAt);
    const desiredExpMs = safeDateMs(desiredExpiresAt);

    const shouldUpdate = !currentExpMs || (desiredExpMs && desiredExpMs > currentExpMs + (60 * 1000));
    if (!shouldUpdate) continue;

    const sets = {
      expiresAt: desiredExpiresAt,
      warrantyMode: hasLifetime ? 'life' : '30',
      warrantyDays: hasLifetime ? null : 30
    };

    try {
      await tl.updateOne({ id: token }, { $set: sets });
      updated++;
    } catch (e) {
      errors++;
      try { console.error('update_failed', token, e?.message || String(e)); } catch (_) {}
    }
  }

  console.log(JSON.stringify({ ok: errors === 0, scanned, updated, skippedNoOrder, skippedNoCreatedAt, errors }));
  process.exit(errors ? 1 : 0);
})().catch((e) => {
  try { console.error(e?.message || String(e)); } catch (_) {}
  process.exit(1);
});

