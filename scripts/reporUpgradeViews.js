// scripts/reporUpgradeViews.js
// ---------------------------------------------------------------------------
// Repõe as VIEWS do upgrade que o cliente pagou e não recebeu.
//
// Causa (corrigida no app.js): o despacho do webhook lia `order_bumps` só do
// additionalInfoMap. Quando o campo estava apenas nos arrays, hasUpgrade saía
// false e ia a quantidade BASE pro fornecedor — o cálculo correto chegava 1 a 15s
// depois e pulava o envio por já estar despachado.
//
// Só repõe pedido que: é de visualizações, tem upgrade nos bumps, foi despachado
// com quantidade menor que o alvo, e ainda NÃO tem reposição registrada.
// Marca `upgradeRepo` no pedido para nunca repetir.
//
// USO:
//   node scripts/reporUpgradeViews.js                 (simula)
//   node scripts/reporUpgradeViews.js --dias=30       (só os últimos 30 dias)
//   node scripts/reporUpgradeViews.js --apply         (envia)
// ---------------------------------------------------------------------------

const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });
const axios = require('axios');
const { getCollection } = require('../mongodbClient');

const APPLY = process.argv.includes('--apply');
const DIAS = (() => {
  const a = process.argv.find(x => x.startsWith('--dias='));
  const n = a ? parseInt(a.split('=')[1], 10) : 0;
  return Number.isFinite(n) && n > 0 ? n : 0;
})();

// Alvos do upgrade de views — mesmos do pricing.js e dos blocos de despacho.
const ALVO = { 1000: 2500, 5000: 10000, 25000: 50000, 100000: 150000, 200000: 250000, 500000: 1000000 };

// --via-nuvra: manda TUDO pela NuvraSMM com o serviço de views atual, ignorando o
// provedor do despacho original. Necessário para os pedidos antigos: eles foram
// despachados no fama24h.net, e aquela conta está zerada (saldo negativo). Como a
// operação migrou para a Nuvra, repor lá entrega o mesmo produto (views de reels).
const VIA_NUVRA = process.argv.includes('--via-nuvra');
const SERVICE_VIEWS_NUVRA = (() => {
  const a = process.argv.find(x => x.startsWith('--service='));
  const n = a ? parseInt(a.split('=')[1], 10) : 7;
  return Number.isFinite(n) && n > 0 ? n : 7;
})();

const API = {
  nuvra: { url: 'https://nuvrasmm.com/api/v2', keyEnv: 'NUVRASMM_API_KEY' },
  fama24h: { url: 'https://fama24h.net/api/v2', keyEnv: 'FAMA24H_API_KEY' },
};
// Provedor do bloco: usa o gravado; senão heurística pelo tamanho do orderId.
function apiDoBloco(blk) {
  const p = String((blk && blk.provider) || '').trim().toLowerCase();
  if (p === 'nuvra' || p === 'nuvrasmm') return API.nuvra;
  if (p === 'fama24h') return API.fama24h;
  const n = Number(String((blk && blk.orderId) || '').trim());
  return (Number.isFinite(n) && n < 1000000) ? API.nuvra : API.fama24h;
}

const arrGet = (o, k) => {
  for (const a of [o.additionalInfoPaid, o.additionalInfo]) {
    if (!Array.isArray(a)) continue;
    const i = a.find(x => x && x.key === k);
    if (i) return String(i.value ?? '');
  }
  return '';
};

(async () => {
  const col = await getCollection('checkout_orders');
  const pagos = await col.find({ $or: [{ status: 'pago' }, { paidAt: { $exists: true, $nin: [null, ''] } }] }).limit(300000).toArray();
  const corte = DIAS ? new Date(Date.now() - DIAS * 864e5).toISOString().slice(0, 10) : '';

  const fila = [];
  for (const o of pagos) {
    if (o.upgradeRepo && o.upgradeRepo.at) continue;                       // já reposto
    const map = o.additionalInfoMapPaid || o.additionalInfoMap || {};
    const cat = map['categoria_servico'] || arrGet(o, 'categoria_servico') || '';
    if (!/visualiza/i.test(cat)) continue;
    const bumps = map['order_bumps'] || arrGet(o, 'order_bumps') || '';
    if (!/(^|;)upgrade:/i.test(bumps)) continue;

    const qb = Number(o.quantidade || o.qtd || 0);
    const alvo = ALVO[qb] || 0;
    if (!alvo) continue;                                                   // quantidade sem upgrade definido
    const blk = (o.fama24h && o.fama24h.requestPayload) ? o.fama24h : (o.fama24h_views || null);
    const rp = blk && blk.requestPayload;
    const env = Number((rp && rp.quantity) || 0);
    if (!env || env >= alvo) continue;                                     // entregue certo
    const link = String((rp && rp.link) || '').trim();
    const service = rp && rp.service;
    if (!link || !service) continue;                                       // sem como repor
    const dt = String(o.paidAt || o.createdAt || '').slice(0, 10);
    if (corte && dt < corte) continue;

    const api = VIA_NUVRA ? API.nuvra : apiDoBloco(blk);
    const svc = VIA_NUVRA ? SERVICE_VIEWS_NUVRA : service;
    fila.push({ _id: o._id, u: o.instauser || '-', dt, qb, env, alvo, falta: alvo - env, link, service: svc, api });
  }

  fila.sort((a, b) => b.dt.localeCompare(a.dt));
  const totalFalta = fila.reduce((s, x) => s + x.falta, 0);
  console.log('');
  console.log('  pedidos a repor .... ' + fila.length + (DIAS ? ('  (últimos ' + DIAS + ' dias)') : ''));
  console.log('  views a enviar ..... ' + totalFalta.toLocaleString('pt-BR'));
  console.log('  custo estimado ..... R$ ' + ((totalFalta / 1000) * 0.01).toFixed(2) + '  (a R$ 0,01/1000)');
  console.log('');

  if (!APPLY) {
    for (const f of fila.slice(0, 15)) console.log('   ' + f.dt + '  @' + String(f.u).slice(0, 22).padEnd(23) + ' +' + String(f.falta).padStart(7) + '  svc ' + String(f.service).padEnd(5) + ' ' + f.link.slice(0, 44));
    if (fila.length > 15) console.log('   … e mais ' + (fila.length - 15));
    console.log('\n  (simulação — nada enviado)\n');
    process.exit(0);
  }

  let ok = 0, erro = 0;
  for (const f of fila) {
    const key = String(process.env[f.api.keyEnv] || '').trim();
    if (!key) { erro++; console.log('   ✗ @' + f.u + ' — sem ' + f.api.keyEnv); continue; }
    try {
      const payload = new URLSearchParams({ key, action: 'add', service: String(f.service), link: f.link, quantity: String(f.falta) });
      const r = await axios.post(f.api.url, payload.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 25000, validateStatus: () => true });
      const d = (r && r.data) || {};
      const oid = d.order ?? d.orderId ?? null;
      if (oid) {
        ok++;
        await col.updateOne({ _id: f._id }, { $set: { upgradeRepo: { orderId: String(oid), quantity: f.falta, service: f.service, link: f.link, provider: (f.api === API.nuvra ? 'nuvra' : 'fama24h'), at: new Date().toISOString(), motivo: 'upgrade de views nao aplicado no despacho do webhook' } } });
        console.log('   ✓ @' + String(f.u).padEnd(23) + ' +' + String(f.falta).padStart(7) + '  order ' + oid);
      } else {
        erro++;
        console.log('   ✗ @' + String(f.u).padEnd(23) + ' +' + String(f.falta).padStart(7) + '  ' + JSON.stringify(d).slice(0, 90));
      }
    } catch (e) { erro++; console.log('   ✗ @' + f.u + ' — ' + (e && e.message ? e.message : String(e))); }
    await new Promise(s => setTimeout(s, 300));
  }
  console.log('\n  enviados: ' + ok + '   falharam: ' + erro + '\n');
  process.exit(0);
})().catch(e => { console.error('erro:', e && e.message ? e.message : String(e)); process.exit(1); });
