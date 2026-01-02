/**
 * Unify temporary refil links by phone and extend expiration based on latest purchase.
 * Usage:
 *   node scripts/unifyTemporaryLinksByPhone.js [days]
 * Default days window: 7
 */

const { getCollection } = require('../mongodbClient');

function toISO(d) {
  try { return new Date(d).toISOString(); } catch (_) { return null; }
}

async function run(daysArg) {
  const days = Math.max(1, Number(daysArg || 7) || 7);
  const msWindow = days * 24 * 60 * 60 * 1000;
  const tl = await getCollection('temporary_links');
  const ordersCol = await getCollection('checkout_orders');

  const refils = await tl.find({ purpose: 'refil' }).toArray();
  const groups = refils.reduce((acc, d) => {
    const phone = String(d.phone || '').replace(/\D/g, '');
    if (!phone) return acc;
    (acc[phone] = acc[phone] || []).push(d);
    return acc;
  }, {});

  let phones = 0, dups = 0, ordersUpdated = 0, linksDeleted = 0, linksUpdated = 0, expirationsExtended = 0;

  for (const [phone, arr] of Object.entries(groups)) {
    if (arr.length <= 1) {
      // Even single link: ensure orders array and expiration extension if newer order exists
      const only = arr[0];
      const orderIds = [String(only.orderId)].filter(Boolean).concat(Array.isArray(only.orders) ? only.orders.map(String) : []);
      const uniq = Array.from(new Set(orderIds.filter(Boolean)));
      if (uniq.length) {
        await tl.updateOne({ id: only.id }, { $set: { orders: uniq, phone } });
        linksUpdated++;
      }
      // Extend expiration based on latest order creation
      const ordersDocs = uniq.length ? await ordersCol.find({ _id: { $in: uniq.map(id => new (require('mongodb').ObjectId)(id)) } }).project({ criado: 1 }).toArray() : [];
      const createdTimes = ordersDocs.map(d => new Date(d.criado || 0).getTime()).filter(n => isFinite(n));
      if (createdTimes.length) {
        const lastCreated = Math.max.apply(null, createdTimes);
        const targetExp = new Date(lastCreated + msWindow).toISOString();
        if (!only.expiresAt || new Date(only.expiresAt).getTime() < new Date(targetExp).getTime()) {
          await tl.updateOne({ id: only.id }, { $set: { expiresAt: targetExp } });
          expirationsExtended++;
        }
      }
      continue;
    }
    phones++;
    // Choose canonical: oldest createdAt
    arr.sort((a,b)=> new Date(a.createdAt||0).getTime() - new Date(b.createdAt||0).getTime());
    const canonical = arr[0];
    const canonicalId = canonical.id;
    const setOrders = new Set();
    arr.forEach(x => {
      if (x.orderId) setOrders.add(String(x.orderId));
      (Array.isArray(x.orders) ? x.orders : []).forEach(o => setOrders.add(String(o)));
    });
    const allOrderIds = Array.from(setOrders).filter(Boolean);
    if (allOrderIds.length) {
      await tl.updateOne({ id: canonicalId }, { $set: { orders: allOrderIds, phone } });
      linksUpdated++;
    }
    // Repoint orders to canonical
    for (let i = 1; i < arr.length; i++) {
      const dup = arr[i];
      dups++;
      const updRes = await ordersCol.updateMany({ refilLinkId: dup.id }, { $set: { refilLinkId: canonicalId } });
      ordersUpdated += updRes.modifiedCount || 0;
      // Merge orderId from dup
      if (dup.orderId) {
        await tl.updateOne({ id: canonicalId }, { $addToSet: { orders: String(dup.orderId) } });
      }
      const delRes = await tl.deleteOne({ id: dup.id });
      linksDeleted += delRes.deletedCount || 0;
    }
    // Extend expiration based on latest order creation time
    const ordersDocs = allOrderIds.length ? await ordersCol.find({ _id: { $in: allOrderIds.map(id => new (require('mongodb').ObjectId)(id)) } }).project({ criado: 1 }).toArray() : [];
    const createdTimes = ordersDocs.map(d => new Date(d.criado || 0).getTime()).filter(n => isFinite(n));
    if (createdTimes.length) {
      const lastCreated = Math.max.apply(null, createdTimes);
      const targetExp = new Date(lastCreated + msWindow).toISOString();
      if (!canonical.expiresAt || new Date(canonical.expiresAt).getTime() < new Date(targetExp).getTime()) {
        await tl.updateOne({ id: canonicalId }, { $set: { expiresAt: targetExp } });
        expirationsExtended++;
      }
    }
  }

  return { ok: true, phonesProcessed: phones, duplicatesResolved: dups, ordersRepointed: ordersUpdated, linksDeleted, linksUpdated, expirationsExtended, daysWindow: days };
}

(async () => {
  try {
    const daysArg = process.argv[2] || process.env.UNIFY_DAYS;
    const result = await run(daysArg);
    console.log(JSON.stringify(result, null, 2));
    process.exit(0);
  } catch (e) {
    console.error('unify-by-phone_error:', e?.message || String(e));
    process.exit(1);
  }
})();