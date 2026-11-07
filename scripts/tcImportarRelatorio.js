// Importa a atribuição do relatório "Vendas" do painel do TrackCombo para a coleção tc_atribuicao.
// A API (MCP) do TC esconde a UTM; o relatório do painel mostra a UTM completa de cada venda.
// O painel /painel/meta-ads usa tc_atribuicao para a coluna TrackCombo por anúncio.
//
// Uso: node scripts/tcImportarRelatorio.js <arquivo>
// Arquivo: uma venda por linha (ou separadas por ";"), campos separados por "|":
//   transacao|dd/mm/aaaa|valor|utm_content|utm_medium
//   ex.: 11XRDT9HKRWB2U26|06/11/2026|42.9|120253361812850763|paid
const path = require('path');
const fs = require('fs');
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });
const { MongoClient } = require('mongodb');

(async () => {
  const arq = process.argv[2];
  if (!arq || !fs.existsSync(arq)) { console.error('arquivo não encontrado:', arq); process.exit(1); }
  const linhas = fs.readFileSync(arq, 'utf8').split(/[;\r\n]+/).map((s) => s.trim()).filter(Boolean);
  const vendas = new Map();
  for (const l of linhas) {
    const [tx, data, valor, content, medium] = l.split('|').map((x) => (x || '').trim());
    const m = /^(\d{2})\/(\d{2})\/(\d{4})$/.exec(data || '');
    if (!tx || !m) { console.warn('linha ignorada:', l.slice(0, 80)); continue; }
    vendas.set(tx, { tx, dia: `${m[3]}-${m[2]}-${m[1]}`, valor: Number(valor) || 0, content: /^\d{10,}$/.test(content) ? content : '', pago: medium === 'paid' });
  }
  if (!vendas.size) { console.log('nada para importar'); process.exit(0); }
  const c = new MongoClient(process.env.MONGODB_URI, { serverSelectionTimeoutMS: 20000 });
  await c.connect();
  const col = c.db(process.env.MONGODB_DB_NAME || 'site-whatsapp').collection('tc_atribuicao');
  const agora = new Date();
  const r = await col.bulkWrite([...vendas.values()].map((v) => ({
    updateOne: { filter: { _id: v.tx }, update: { $set: { content: v.content, pago: v.pago, valor: v.valor, dia: v.dia, fonte: 'painel_tc', importadoEm: agora } }, upsert: true },
  })), { ordered: false });
  const comAnuncio = [...vendas.values()].filter((v) => v.content).length;
  console.log(`importadas ${vendas.size} vendas (${comAnuncio} com anúncio): ${r.upsertedCount} novas, ${r.modifiedCount} atualizadas`);
  await c.close();
})().catch((e) => { console.error('erro:', e.message); process.exit(1); });
