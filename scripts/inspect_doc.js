require('dotenv').config();
const { getCollection } = require('../mongodbClient');

(async () => {
  try {
    const col = await getCollection('checkout_orders');
    const doc = await col.findOne({}, { sort: { $natural: -1 } });
    console.log('SAMPLE_DOC:', JSON.stringify(doc, null, 2));
  } catch (e) {
    console.error(e);
  }
  process.exit(0);
})();
