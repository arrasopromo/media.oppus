require('dotenv').config({ path: require('path').resolve(__dirname, '../.env') });
const { MongoClient } = require('mongodb');
const axios = require('axios');

const uri = process.env.MONGODB_URI || 'mongodb://localhost:27017';
const dbName = process.env.MONGODB_DB || 'site-whatsapp';

async function geoLookupIp(ipRaw) {
  try {
    const ip = String(ipRaw || '').split(',')[0].replace('::ffff:', '').trim();
    if (!ip || ip === 'unknown') return null;
    // Use ip-api.com (free, 45 req/min)
    const url = `http://ip-api.com/json/${encodeURIComponent(ip)}`;
    
    // axios request
    const resp = await axios.get(url, { timeout: 10000 });
    const data = resp.data;
    
    if (data.status === 'fail') throw new Error(data.message || 'API returned fail');
    
    const city = String(data.city || '').trim();
    const region = String(data.regionName || data.region || '').trim();
    const country = String(data.country || '').trim();
    
    // Only return if we got at least a country or city
    if (!city && !country) return null;
    
    return { city, region, country, source: 'ip-api.com' };
  } catch (e) { 
    console.error('Lookup failed for', ipRaw, e.message);
    if (e.response) {
        console.error('Response status:', e.response.status);
        console.error('Response data:', e.response.data);
    }
    return null; 
  }
}

async function run() {
  const client = new MongoClient(uri);
  try {
    await client.connect();
    console.log('Connected to MongoDB');
    const db = client.db(dbName);
    const col = db.collection('checkout_orders');

    // Find documents with IP but without geolocation.city
    const query = {
      ip: { $exists: true, $ne: 'unknown' },
      'geolocation.city': { $exists: false }
    };
    
    const total = await col.countDocuments(query);
    console.log(`Found ${total} documents to process`);

    const cursor = col.find(query);
    let count = 0;
    let success = 0;

    while (await cursor.hasNext()) {
      const doc = await cursor.next();
      count++;
      
      const ip = doc.ip;
      console.log(`[${count}/${total}] Processing IP: ${ip} (ID: ${doc._id})`);
      
      const geo = await geoLookupIp(ip);
      
      if (geo) {
        await col.updateOne(
          { _id: doc._id },
          { $set: { geolocation: geo } }
        );
        console.log(`  -> Updated: ${geo.city}, ${geo.region}, ${geo.country}`);
        success++;
      } else {
        console.log(`  -> Failed or no data found.`);
        // Mark as processed with failed status to avoid reprocessing loop if re-run
        // or just leave it to retry later.
        // For now, let's just log.
      }

      // Add a small delay to be nice to the API (e.g. 1.5 second)
      await new Promise(r => setTimeout(r, 1500));
    }

    console.log(`Finished. Processed: ${count}, Updated: ${success}`);

  } catch (err) {
    console.error('Fatal error:', err);
  } finally {
    await client.close();
  }
}

run();
