const { MongoClient } = require('mongodb');

let client;
let db;

async function connect() {
  const uri = process.env.MONGODB_URI;
  const dbName = process.env.MONGODB_DB_NAME || 'site-whatsapp';
  if (!uri) throw new Error('MONGODB_URI não definido no .env');
  if (db) return db;
  client = new MongoClient(uri, {
    serverSelectionTimeoutMS: 5000,
    // tls desabilitado conforme URI
  });
  await client.connect();
  db = client.db(dbName);
  try {
    console.log(`✅ MongoDB conectado ao database '${dbName}'`);
    try {
      const col = db.collection('validated_insta_users');
      await col.createIndexes([
        { key: { username: 1, checkedAt: -1 }, name: 'username_checkedAt_idx' },
        { key: { linkId: 1 }, name: 'linkId_idx' },
        { key: { ip: 1, userAgent: 1 }, name: 'ip_userAgent_idx' }
      ]);
      const validet = db.collection('validet');
      await validet.createIndexes([
        { key: { username: 1, checkedAt: -1 }, name: 'username_checkedAt_idx' },
        { key: { linkId: 1 }, name: 'linkId_idx' },
        { key: { ip: 1, userAgent: 1 }, name: 'ip_userAgent_idx' }
      ]);
    } catch (_) {}
  } catch (_) {}
  return db;
}

async function getCollection(name) {
  const database = await connect();
  return database.collection(name);
}

module.exports = { connect, getCollection };