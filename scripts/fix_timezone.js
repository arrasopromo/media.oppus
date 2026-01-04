require('dotenv').config();
const { MongoClient } = require('mongodb');

(async () => {
  const uri = process.env.MONGODB_URI;
  const dbName = process.env.MONGODB_DB_NAME || 'site-whatsapp';
  
  if (!uri) {
    console.error('❌ MONGODB_URI não definido no .env');
    process.exit(1);
  }
  
  console.log(`🔌 Conectando ao MongoDB: ${dbName}...`);
  const client = new MongoClient(uri);
  
  try {
    await client.connect();
    const db = client.db(dbName);
    
    const collections = await db.listCollections().toArray();
    console.log(`📂 Encontradas ${collections.length} coleções.`);
    
    for (const colInfo of collections) {
      const colName = colInfo.name;
      if (colName.startsWith('system.')) continue;
      
      console.log(`🔄 Processando coleção: ${colName}`);
      const col = db.collection(colName);
      const cursor = col.find({});
      
      let updatedCount = 0;
      let processedCount = 0;
      
      const bulkOps = [];
      
      while (await cursor.hasNext()) {
        const doc = await cursor.next();
        processedCount++;
        
        let modified = false;
        
        // Função recursiva para encontrar e ajustar datas
        const adjustDates = (obj) => {
          if (!obj || typeof obj !== 'object') return;
          
          for (const key in obj) {
            const val = obj[key];
            
            // Check for Date object
            if (val instanceof Date) {
              obj[key] = new Date(val.getTime() - (3 * 60 * 60 * 1000));
              modified = true;
            } 
            // Check for ISO Date String (e.g. "2026-01-03T16:07:49.020Z")
            else if (typeof val === 'string' && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/.test(val)) {
              try {
                const d = new Date(val);
                if (!isNaN(d.getTime())) {
                  const newDate = new Date(d.getTime() - (3 * 60 * 60 * 1000));
                  obj[key] = newDate.toISOString();
                  modified = true;
                }
              } catch (_) {}
            }
            else if (Array.isArray(val)) {
              val.forEach(item => adjustDates(item));
            } else if (typeof val === 'object') {
              adjustDates(val);
            }
          }
        };
        
        // Clone doc to check modifications (not strictly necessary with replaceOne but good for safety)
        const originalDoc = JSON.parse(JSON.stringify(doc)); // JSON serialization loses Date types, so this check is tricky.
        // Instead, we trust the 'modified' flag set by adjustDates which operates in-place on 'doc'
        
        adjustDates(doc);
        
        if (modified) {
          bulkOps.push({
            replaceOne: {
              filter: { _id: doc._id },
              replacement: doc
            }
          });
          updatedCount++;
        }
        
        // Executar em lotes de 500
        if (bulkOps.length >= 500) {
          await col.bulkWrite(bulkOps);
          bulkOps.length = 0;
        }
      }
      
      // Processar restantes
      if (bulkOps.length > 0) {
        await col.bulkWrite(bulkOps);
      }
      
      console.log(`   ✅ ${updatedCount} documentos atualizados em ${colName} (de ${processedCount} processados).`);
    }
    
    console.log('🏁 Correção de timezone concluída com sucesso!');
    
  } catch (err) {
    console.error('❌ Erro durante a execução:', err);
  } finally {
    await client.close();
  }
})();
