const { MongoClient } = require('mongodb');

let client;
let db;

async function connect() {
  const uri = process.env.MONGODB_URI;
  const dbName = process.env.MONGODB_DB_NAME || 'site-whatsapp';
  
  if (!uri) {
    console.warn('⚠️ MONGODB_URI não definido no .env. MongoDB desabilitado.');
    return null;
  }

  if (db) return db;
  
  try {
    client = new MongoClient(uri, {
      serverSelectionTimeoutMS: 5000,
    });
    await client.connect();
    db = client.db(dbName);
  } catch (err) {
    console.warn('⚠️ Falha ao conectar ao MongoDB:', err.message);
    return null;
  }

  try {
    try {
      const host = (() => { try { const m = String(uri).match(/^mongodb(?:\+srv)?:\/\/[^@]*@([^\/]+)/i) || String(uri).match(/^mongodb(?:\+srv)?:\/\/([^\/]+)/i); return m && m[1] ? m[1] : 'unknown'; } catch(_) { return 'unknown'; } })();
      console.log(`✅ MongoDB conectado ao database '${dbName}' em host '${host}'`);
    } catch(_) { console.log(`✅ MongoDB conectado ao database '${dbName}'`); }
    try {
      const col = db.collection('validated_insta_users');
      await col.createIndexes([
        { key: { username: 1, checkedAt: -1 }, name: 'username_checkedAt_idx' },
        { key: { linkId: 1 }, name: 'linkId_idx' },
        { key: { ip: 1, userAgent: 1 }, name: 'ip_userAgent_idx' }
      ]);
      try { await col.createIndex({ username: 1 }, { name: 'username_unique', unique: true }); } catch(_) {}
    } catch (_) {}
    try { await db.collection('validet').drop(); } catch(_) {}
    try { await db.collection('validated-insta-users').drop(); } catch(_) {}
  } catch (_) {}
  return db;
}

async function getCollection(name) {
  const database = await connect();
  if (!database) {
    // Retorna coleção mockada para evitar crashes
    return {
      find: () => ({ sort: () => ({ limit: () => ({ toArray: async () => [] }) }) }),
      findOne: async () => null,
      updateOne: async () => ({ modifiedCount: 0, upsertedCount: 0 }),
      insertOne: async () => ({ insertedId: 'mock_id' }),
      countDocuments: async () => 0,
      createIndex: async () => {},
      createIndexes: async () => {},
      drop: async () => {}
    };
  }
  return database.collection(name);
}

module.exports = { connect, getCollection };