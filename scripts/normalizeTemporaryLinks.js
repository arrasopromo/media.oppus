require('dotenv').config();
const { getCollection } = require('../mongodbClient');

(async () => {
  try {
    const daysArg = process.argv[2];
    const days = Math.max(1, Number(daysArg || 7) || 7);
    const ms = days * 24 * 60 * 60 * 1000;
    const tl = await getCollection('temporary_links');
    const cursor = tl.find({}, { projection: { id: 1, createdAt: 1, expiresAt: 1 } });
    const docs = await cursor.toArray();
    const ops = [];
    let updated = 0;
    for (const d of docs) {
      try {
        const cr = d && d.createdAt;
        let createdMs = null;
        if (cr instanceof Date) createdMs = cr.getTime();
        else if (typeof cr === 'string') createdMs = Date.parse(cr);
        else if (typeof cr === 'number') createdMs = cr;
        if (!createdMs || Number.isNaN(createdMs)) continue;
        const targetISO = new Date(createdMs + ms).toISOString();
        ops.push({ updateOne: { filter: { id: d.id }, update: { $set: { expiresAt: targetISO } } } });
        updated++;
      } catch (_) {}
    }
    if (ops.length) await tl.bulkWrite(ops, { ordered: false });
    console.log(JSON.stringify({ ok: true, total: docs.length, updated, days }));
    process.exit(0);
  } catch (e) {
    console.error(JSON.stringify({ ok: false, error: e && e.message ? e.message : String(e) }));
    process.exit(1);
  }
})();