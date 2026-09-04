// scripts/marcarProvedorNosBlocos.js
// ---------------------------------------------------------------------------
// Grava o PROVEDOR REAL dentro de cada bloco de fornecedor do pedido:
//   fama24h.provider = 'nuvra' | 'fama24h'   (idem _views, _likes e _multi.orders)
//
// Por que não renomear o campo `fama24h` para `nuvra`: dos ~17.800 pedidos com
// orderId nesses blocos, só ~717 são da Nuvra — os outros são fama24h.net de
// verdade. Renomear só os 717 deixaria DOIS nomes para o mesmo slot, e as 597
// referências a `fama24h*` no app.js teriam que ler os dois para sempre (custo,
// refil, status, painéis, sweeper). Gravar o provedor dentro do bloco dá a mesma
// informação sem esse risco — e o resolveRefillProviderApi JÁ prefere blk.provider.
//
// Também elimina a dependência da heurística "orderId < 1.000.000 = nuvra", que
// quebra sozinha quando os IDs da Nuvra (hoje ~19 mil) passarem de 1 milhão.
//
// USO:
//   node scripts/marcarProvedorNosBlocos.js          (simula)
//   node scripts/marcarProvedorNosBlocos.js --apply  (grava)
// ---------------------------------------------------------------------------

const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });
const { getCollection } = require('../mongodbClient');

const APPLY = process.argv.includes('--apply');
const SLOTS = ['fama24h', 'fama24h_views', 'fama24h_likes'];
const LIMITE_NUVRA = 1000000;

// Só decide por ID quando ele é puramente numérico. IDs digitados à mão no painel
// às vezes vêm com texto ("4418878 (worldsmm)", "Contatar Suporte") — esses ficam
// sem provider, para não gravar um palpite errado.
function provedorDoOrderId(v) {
  const raw = String(v == null ? '' : v).trim();
  if (!/^[0-9]+$/.test(raw)) return null;
  return Number(raw) < LIMITE_NUVRA ? 'nuvra' : 'fama24h';
}

(async () => {
  const col = await getCollection('checkout_orders');
  const proj = { 'fama24h_multi.orders': 1 };
  for (const s of SLOTS) { proj[s + '.orderId'] = 1; proj[s + '.provider'] = 1; }

  const filtro = { $or: [...SLOTS.map(s => ({ [s + '.orderId']: { $exists: true, $nin: [null, ''] } })), { 'fama24h_multi.orders.0': { $exists: true } }] };
  const docs = await col.find(filtro).project(proj).limit(400000).toArray();

  const cont = { nuvra: 0, fama24h: 0, indefinido: 0, jaMarcado: 0 };
  const ops = [];

  for (const o of docs) {
    const sets = {};
    for (const s of SLOTS) {
      const blk = o[s];
      if (!blk || blk.orderId == null || blk.orderId === '') continue;
      // NUNCA sobrescreve provider já gravado: o app grava no despacho e sabe para
      // onde mandou de verdade. A heurística por tamanho de ID é só para preencher
      // o que ficou vazio. (Existe 1 bloco de 10/08 com id 633.655 marcado fama24h —
      // fora da série da Nuvra, que foi de 4.942 a 20.760; sobrescrever seria errado.)
      if (typeof blk.provider === 'string' && blk.provider.trim()) { cont.jaMarcado++; continue; }
      const p = provedorDoOrderId(blk.orderId);
      if (!p) { cont.indefinido++; continue; }
      sets[`${s}.provider`] = p;
      cont[p]++;
    }
    // _multi: provider por sub-pedido, preservando o resto do item
    const multi = o.fama24h_multi && Array.isArray(o.fama24h_multi.orders) ? o.fama24h_multi.orders : null;
    if (multi && multi.length) {
      let mudou = false;
      const novos = multi.map((it) => {
        if (it && typeof it.provider === 'string' && it.provider.trim()) { cont.jaMarcado++; return it; }
        const p = provedorDoOrderId(it && (it.orderId ?? it.id));
        if (!p) { cont.indefinido++; return it; }
        cont[p]++; mudou = true;
        return Object.assign({}, it, { provider: p });
      });
      if (mudou) sets['fama24h_multi.orders'] = novos;
    }
    if (Object.keys(sets).length) ops.push({ updateOne: { filter: { _id: o._id }, update: { $set: sets } } });
  }

  console.log('');
  console.log('  pedidos analisados ..... ' + docs.length);
  console.log('  blocos -> nuvra ........ ' + cont.nuvra);
  console.log('  blocos -> fama24h ...... ' + cont.fama24h);
  console.log('  ja marcados ............ ' + cont.jaMarcado);
  console.log('  indefinidos (id sujo) .. ' + cont.indefinido);
  console.log('  pedidos a atualizar .... ' + ops.length);

  if (!APPLY) { console.log('\n  (simulacao — nada gravado)\n'); process.exit(0); }

  let gravados = 0;
  for (let i = 0; i < ops.length; i += 500) {
    const lote = ops.slice(i, i + 500);
    const r = await col.bulkWrite(lote, { ordered: false });
    gravados += (r.modifiedCount || 0);
  }
  console.log('\n  gravados: ' + gravados + ' pedido(s)\n');
  process.exit(0);
})().catch((e) => { console.error('erro:', e && e.message ? e.message : String(e)); process.exit(1); });
