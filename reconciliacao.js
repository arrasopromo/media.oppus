// ══════════════════════════════════════════════════════════════════════════
// RECONCILIAÇÃO GATEWAY ↔ BANCO + WATCHDOG DE PEDIDOS
//
// Contingência para incidentes tipo "Mongo caiu / app sem gravar": o gateway é a
// FONTE DA VERDADE do pagamento. Este módulo, a cada X minutos:
//   1) CARTÃO (pagar.me): lista os pedidos PAGOS das últimas N horas e confere se
//      cada um tem checkout_order PAGO no banco. Faltando → alerta no WhatsApp.
//   2) PIX (PagHiper): pega nossos pendentes com transactionId e checa o status
//      real; se o PagHiper diz "pago" mas o banco está pendente → marca pago.
//   3) WATCHDOG: se 0 pedidos criados nos últimos N min em horário comercial →
//      alerta (sinal de app travado / sem gravar).
// Dedup de alertas na coleção `reconciliacao_alertas`.
// Liga/desliga por RECONCILIACAO_ENABLED (default on).
// ══════════════════════════════════════════════════════════════════════════
const axios = require('axios');
const { getCollection } = require('./mongodbClient');

const ENABLED = String(process.env.RECONCILIACAO_ENABLED || 'true').toLowerCase() !== 'false';
const INTERVAL_MIN = Math.max(2, Number(process.env.RECONCILIACAO_INTERVAL_MIN || 10) || 10);
const CARTAO_LOOKBACK_H = Math.max(1, Number(process.env.RECONCILIACAO_CARTAO_HORAS || 12) || 12);
const WATCHDOG_MIN = Math.max(5, Number(process.env.WATCHDOG_PEDIDOS_MIN || 20) || 20);
const WATCHDOG_HORA_INI = Number(process.env.WATCHDOG_HORA_INI || 8) || 8;   // BRT
const WATCHDOG_HORA_FIM = Number(process.env.WATCHDOG_HORA_FIM || 23) || 23; // BRT
const ALERT_PHONE = String(process.env.RECONCILIACAO_ALERT_PHONE || process.env.PANEL_BALANCE_ALERT_PHONE || '5531975938916').replace(/\D/g, '');

const PAGARME_SECRET = String(process.env.PAGARME_SECRET || process.env.PAGARME_SECRET_KEY || '').trim();
const PAGHIPER_TOKEN = String(process.env.PAGHIPER_TOKEN || '').trim();
const PAGHIPER_API_KEY = String(process.env.PAGHIPER_API_KEY || '').trim();

const brt = (d) => new Date((d ? new Date(d) : new Date()).getTime() - 3 * 3600e3);

// ── alerta no WhatsApp (Evolution), com dedup ──────────────────────────────
async function sendEvolution(text) {
  try {
    const baseUrl = String(process.env.EVOLUTION_API_URL || '').replace(/\/+$/, '');
    const apiKey = String(process.env.EVOLUTION_API_KEY || '').trim();
    const instance = String(process.env.EVOLUTION_INSTANCE_NAME || 'oppus').trim();
    const instanceToken = String(process.env.EVOLUTION_INSTANCE_TOKEN || '').trim();
    if (!baseUrl || !apiKey || !instance || !ALERT_PHONE) return false;
    const num = ALERT_PHONE.startsWith('55') ? ALERT_PHONE : '55' + ALERT_PHONE;
    const headers = Object.assign({ 'Content-Type': 'application/json', apikey: apiKey }, instanceToken ? { token: instanceToken } : {});
    const r = await axios.post(`${baseUrl}/message/sendText/${encodeURIComponent(instance)}`, { number: num, text }, { timeout: 15000, headers, validateStatus: () => true });
    return r.status >= 200 && r.status < 300;
  } catch (_) { return false; }
}
async function alertaUmaVez(chave, text) {
  try {
    const col = await getCollection('reconciliacao_alertas');
    const ja = await col.findOne({ _id: chave });
    if (ja) return false;
    await col.insertOne({ _id: chave, at: new Date().toISOString(), text: String(text).slice(0, 500) });
    await sendEvolution(text);
    return true;
  } catch (_) { return false; }
}

// ── 1) CARTÃO (pagar.me): pagos sem pedido no banco ────────────────────────
async function listaPagarmePagos(sinceIso, untilIso) {
  if (!PAGARME_SECRET) return [];
  const auth = 'Basic ' + Buffer.from(PAGARME_SECRET + ':').toString('base64');
  const out = [];
  for (let page = 1; page <= 20; page++) {
    const r = await axios.get('https://api.pagar.me/core/v5/orders', { params: { created_since: sinceIso, created_until: untilIso, page, size: 100 }, headers: { Authorization: auth }, timeout: 30000, validateStatus: () => true });
    if (r.status !== 200) break;
    const data = (r.data && r.data.data) || [];
    out.push(...data);
    if (data.length < 100) break;
  }
  return out.filter((o) => String(o.status || '').toLowerCase() === 'paid');
}
async function reconciliarCartao() {
  const col = await getCollection('checkout_orders');
  const until = new Date(), since = new Date(Date.now() - CARTAO_LOOKBACK_H * 3600e3);
  let pagos = [];
  try { pagos = await listaPagarmePagos(since.toISOString(), until.toISOString()); } catch (_) { return { perdidos: [], erro: true }; }
  const perdidos = [];
  for (const o of pagos) {
    const code = o.code || '', orderId = o.id || '';
    const found = await col.findOne({
      status: 'pago',
      $or: [{ correlationID: code }, { correlationId: code }, { 'pagarme.order_id': orderId }, { identifier: orderId }, { 'pagamentoReal.correlationID': code }],
    });
    if (!found) perdidos.push({ code, orderId, cliente: (o.customer && o.customer.name) || '?', valor: (o.amount || 0) / 100, em: o.created_at });
  }
  return { perdidos };
}

// ── 2) PIX (PagHiper): nossos pendentes que o PagHiper diz pago → marca pago ─
async function statusPaghiper(txId) {
  try {
    const r = await axios.post('https://pix.paghiper.com/invoice/status/', { token: PAGHIPER_TOKEN, apiKey: PAGHIPER_API_KEY, transaction_id: txId }, { headers: { 'Content-Type': 'application/json', Accept: 'application/json' }, timeout: 15000, validateStatus: () => true });
    const sr = r.data && r.data.status_request;
    return sr ? String(sr.status || '') : '';
  } catch (_) { return ''; }
}
async function reconciliarPixPendentes() {
  if (!PAGHIPER_TOKEN) return { corrigidos: [] };
  const col = await getCollection('checkout_orders');
  const since = new Date(Date.now() - CARTAO_LOOKBACK_H * 3600e3).toISOString();
  const docs = await col.find({
    status: { $nin: ['pago', 'cancelado', 'estornado', 'refunded', 'canceled'] },
    'paghiper.transactionId': { $exists: true, $nin: [null, ''] },
    $or: [{ criado: { $gte: since } }, { createdAt: { $gte: since } }],
  }, { projection: { identifier: 1, valueCents: 1, 'paghiper.transactionId': 1, instauser: 1 } }).limit(500).toArray();
  const corrigidos = [];
  for (const o of docs) {
    const st = await statusPaghiper(o.paghiper.transactionId);
    if (/paid|completed|pago/i.test(st)) {
      await col.updateOne({ _id: o._id }, { $set: { status: 'pago', paidAt: new Date().toISOString(), paidBy: 'reconciliacao' } });
      corrigidos.push({ identifier: o.identifier, instauser: o.instauser, valor: (o.valueCents || 0) / 100 });
    }
  }
  return { corrigidos };
}

// ── 3) WATCHDOG: parou de criar pedido ─────────────────────────────────────
async function watchdogPedidos() {
  const h = brt().getUTCHours();
  if (h < WATCHDOG_HORA_INI || h >= WATCHDOG_HORA_FIM) return { ok: true, foraHorario: true };
  const col = await getCollection('checkout_orders');
  const since = new Date(Date.now() - WATCHDOG_MIN * 60e3).toISOString();
  const n = await col.countDocuments({ recovered: { $ne: true }, $or: [{ criado: { $gte: since } }, { createdAt: { $gte: since } }] });
  return { ok: n > 0, criadosUltimos: n, janelaMin: WATCHDOG_MIN };
}

// ── runner ─────────────────────────────────────────────────────────────────
let __rodando = false;
async function runReconciliacao() {
  if (!ENABLED || __rodando) return { skipped: true };
  __rodando = true;
  const res = { cartaoPerdidos: 0, pixCorrigidos: 0, watchdog: null };
  try {
    const cartao = await reconciliarCartao();
    for (const p of (cartao.perdidos || [])) {
      res.cartaoPerdidos++;
      await alertaUmaVez('cartao:' + (p.code || p.orderId), `🔴 PAGAMENTO SEM PEDIDO (cartão)\nCliente: ${p.cliente}\nValor: R$ ${p.valor.toFixed(2)}\nCod: ${p.code}\nPagar.me: ${p.orderId}\nCriar e despachar manualmente.`);
    }
    const pix = await reconciliarPixPendentes();
    for (const c of (pix.corrigidos || [])) {
      res.pixCorrigidos++;
      await sendEvolution(`✅ Pix reconciliado: pedido ${c.identifier} (@${c.instauser || '-'}) R$ ${c.valor.toFixed(2)} estava pendente mas já foi pago — marcado pago.`);
    }
    const wd = await watchdogPedidos();
    res.watchdog = wd;
    if (!wd.ok && !wd.foraHorario) {
      // alerta a cada ciclo enquanto estiver parado (chave por janela de 20min)
      const slot = Math.floor(Date.now() / (WATCHDOG_MIN * 60e3));
      await alertaUmaVez('watchdog:' + slot, `⚠️ ALERTA: 0 pedidos criados nos últimos ${wd.janelaMin} min. O app pode estar sem gravar no Mongo. Verifique/reinicie o app.`);
    }
    try { console.log('🔁 [reconciliacao]', JSON.stringify(res)); } catch (_) {}
  } catch (e) { try { console.warn('⚠️ [reconciliacao] erro:', e && e.message); } catch (_) {} }
  finally { __rodando = false; }
  return res;
}
function startReconciliacaoLoop() {
  if (!ENABLED) { try { console.log('⏸️ [reconciliacao] desligada (RECONCILIACAO_ENABLED=false)'); } catch (_) {} return; }
  setTimeout(() => { runReconciliacao().catch(() => {}); }, 90 * 1000); // 1º ciclo após 90s
  const t = setInterval(() => { runReconciliacao().catch(() => {}); }, INTERVAL_MIN * 60 * 1000);
  try { t.unref && t.unref(); } catch (_) {}
  try { console.log(`🔁 [reconciliacao] agendada a cada ${INTERVAL_MIN} min | watchdog ${WATCHDOG_MIN}min (${WATCHDOG_HORA_INI}-${WATCHDOG_HORA_FIM}h BRT) | alerta → ${ALERT_PHONE}`); } catch (_) {}
}

module.exports = { runReconciliacao, reconciliarCartao, reconciliarPixPendentes, watchdogPedidos, startReconciliacaoLoop };
