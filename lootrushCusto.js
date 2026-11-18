// lootrushCusto.js — Custo REAL do dólar da ca3 pelas recargas da LootRush.
//
// A ca3 (conta Meta em USD) é paga pelo cartão LootRush, que é abastecido com dólares (USDC/USDT)
// comprados via Pix. O custo em reais de cada dólar gasto = CUSTO MÉDIO PONDERADO do estoque
// (carteira + cartão): cada compra via Pix recalcula a média ponderando o que já havia em estoque;
// gastos do cartão e envios para fora só baixam o estoque (não mudam a média).
//
// Grava a média vigente no fim de cada dia (horário de SP) em `fx_lootrush_custo` ({ _id: 'YYYY-MM-DD',
// rate, estoqueUsd }) e o resumo em settings `_id:'fx_lootrush_custo'` (média atual, 1º dia com cobrança
// da Meta no cartão, conferência de saldo). O app.js usa essa média para converter o gasto da ca3
// a partir do 1º dia em que o cartão pagou a Meta; antes disso vale a cotação do dia.
'use strict';

const { getCollection } = require('./mongodbClient');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const diaSP = (ms) => new Date(Number(ms) - 3 * 3600e3).toISOString().slice(0, 10);
const tsUtc = (s) => { const x = String(s || ''); return Date.parse(x.includes('T') ? x : x.replace(' ', 'T') + 'Z'); };
const META_ID = 'fx_lootrush_custo';

let __cache = { atMs: 0, meta: null, dias: null };

async function baixarHistorico(call) {
  const carteira = [];
  for (let off = 0; off < 5000; off += 100) {
    const h = await call('getAccountHistory', { startDate: '2026-01-01', limit: 100, offset: off });
    const lst = (h && h.transactions) || [];
    carteira.push(...lst);
    if (lst.length < 100) break;
    await sleep(1100);
  }
  await sleep(1100);
  const cartao = [];
  for (let pg = 1; pg < 60; pg++) {
    const r = await call('getUserCardBalanceMovements', { startDate: '2026-01-01', page: pg, pageSize: 100 });
    const lst = [];
    for (const g of ((r && r.balanceMovements) || [])) lst.push(...((g && g.movements) || []));
    cartao.push(...lst);
    if (lst.length < 100) break;
    await sleep(1100);
  }
  return { carteira, cartao };
}

// Linha do tempo → custo médio por dia. Exportada para teste.
function calcular({ carteira, cartao }, hojeMs = Date.now()) {
  const ev = [];
  for (const t of carteira) {
    const at = tsUtc(t.executedAtDate || t.transactionDate);
    if (!Number.isFinite(at) || String(t.status || '').toLowerCase() === 'failed') continue;
    const desc = String(t.description || '');
    if (String(t.assetSent || '').toUpperCase() === 'BRL' && t.direction === 'in') {
      const usd = Number(t.amountReceived), brl = Number(t.amountSent);
      if (usd > 0 && brl > 0) ev.push({ at, tipo: 'compra', usd, brl });
    } else if (t.direction === 'out' && /credit card/i.test(desc)) continue;      // carteira → cartão (interno)
    else if (String(t.transactionType || '') === 'Swap') continue;                // USDC ↔ USDT (interno)
    else if (t.direction === 'out') {
      // netChange já vem com a taxa de rede embutida (ex.: envio de 150 = 149,25 + 0,75 de taxa).
      const nc = Number(t.netChange);
      ev.push({ at, tipo: 'saida', usd: Number.isFinite(nc) && nc !== 0 ? Math.abs(nc) : (Number(t.amountSent) || 0) + (Number(t.fee) || 0) });
    }
    else if (t.direction === 'in') ev.push({ at, tipo: 'entrada', usd: Number(t.netChange || t.amountReceived || 0) }); // sem custo conhecido
  }
  let primeiroMeta = null;
  for (const m of cartao) {
    const acao = String(m.action || '').toLowerCase();
    if (/refund|reversal|chargeback|credit/.test(acao) && String(m.status || '').toLowerCase() === 'completed') {
      const atR = Date.parse(m.createdAt);
      if (Number.isFinite(atR)) ev.push({ at: atR, tipo: 'entrada', usd: Number(m.amount) || 0 }); // estorno: dólar volta ao estoque pela média
      continue;
    }
    if (acao !== 'spend' || !(m.status === 'completed' || m.status === 'pending')) continue;
    const at = Date.parse(m.createdAt);
    if (!Number.isFinite(at)) continue;
    ev.push({ at, tipo: 'gasto', usd: Number(m.amount) || 0 });
    if (/facebk|facebook|meta/i.test(String(m.description || '')) && (primeiroMeta == null || at < primeiroMeta)) primeiroMeta = at;
  }
  ev.sort((a, b) => a.at - b.at);
  let estoque = 0, media = null;
  const fimDoDia = {}; // dia -> { rate, estoqueUsd }
  const compras = [];
  for (const e of ev) {
    if (e.tipo === 'compra') {
      const taxa = e.brl / e.usd;
      const base = Math.max(estoque, 0);
      media = (media == null || base <= 0) ? taxa : (base * media + e.usd * taxa) / (base + e.usd);
      estoque = base + e.usd;
      compras.push({ dia: diaSP(e.at), at: new Date(e.at).toISOString(), brl: e.brl, usd: e.usd, taxa: Math.round(taxa * 10000) / 10000, mediaDepois: Math.round(media * 10000) / 10000 });
    } else if (e.tipo === 'entrada') {
      estoque += e.usd; // entrada sem custo em reais (ex.: cripto recebida): entra pela média atual
    } else {
      estoque -= e.usd;
    }
    if (media != null) fimDoDia[diaSP(e.at)] = { rate: media, estoqueUsd: estoque };
  }
  // Preenche os dias sem evento com a média vigente (do 1º dia com compra até hoje).
  const dias = {};
  const ks = Object.keys(fimDoDia).sort();
  if (ks.length) {
    let cur = null;
    for (let t = Date.parse(ks[0] + 'T12:00:00Z'); diaSP(t + 3 * 3600e3) <= diaSP(hojeMs); t += 864e5) {
      const d = new Date(t).toISOString().slice(0, 10);
      if (fimDoDia[d]) cur = fimDoDia[d];
      if (cur) dias[d] = { rate: Math.round(cur.rate * 10000) / 10000, estoqueUsd: Math.round(cur.estoqueUsd * 100) / 100 };
    }
  }
  return { dias, media, estoque, primeiroDiaMeta: primeiroMeta ? diaSP(primeiroMeta) : null, compras };
}

async function sincronizar() {
  const key = String(process.env.LOOTRUSH_API_KEY || '').trim();
  if (!key) return { ok: false, error: 'sem_chave' };
  const { mcpSession } = require('./lootrushMonitor.js');
  const call = await mcpSession(key);
  const hist = await baixarHistorico(call);
  const r = calcular(hist);
  if (r.media == null) return { ok: false, error: 'sem_compras' };
  // Conferência: estoque calculado x saldo real (carteira + cartão).
  let saldoReal = null;
  try {
    await sleep(1100);
    const ab = await call('getAccountBalance', {});
    await sleep(1100);
    const uc = await call('getUserCards', {});
    const carteira = Number(ab && ab.availableStableUsdValue);
    const cartao = Number(uc && uc.cardBalance && uc.cardBalance.total);
    if (Number.isFinite(carteira) && Number.isFinite(cartao)) saldoReal = Math.round((carteira + cartao) * 100) / 100;
  } catch (_) {}
  const col = await getCollection('fx_lootrush_custo');
  const ops = Object.entries(r.dias).map(([d, v]) => ({ updateOne: { filter: { _id: d }, update: { $set: { rate: v.rate, estoqueUsd: v.estoqueUsd, atIso: new Date().toISOString() } }, upsert: true } }));
  if (ops.length) await col.bulkWrite(ops, { ordered: false });
  const meta = {
    mediaAtual: Math.round(r.media * 10000) / 10000, estoqueCalculadoUsd: Math.round(r.estoque * 100) / 100, saldoRealUsd: saldoReal,
    primeiroDiaMeta: r.primeiroDiaMeta, ultimasCompras: r.compras.slice(-10), syncAt: new Date().toISOString(),
  };
  const st = await getCollection('settings');
  await st.updateOne({ _id: META_ID }, { $set: meta }, { upsert: true });
  __cache = { atMs: 0, meta: null, dias: null };
  return Object.assign({ ok: true, dias: ops.length }, meta);
}

// { 'YYYY-MM-DD': rate } com o custo médio LootRush para os dias pedidos que caem a partir do
// 1º dia em que o cartão pagou a Meta. Dias fora disso não vêm (o app.js usa a cotação do dia).
async function custoPorDia(dias) {
  const out = {};
  try {
    if (!__cache.meta || (Date.now() - __cache.atMs) > 5 * 60 * 1000) {
      const st = await getCollection('settings');
      const meta = await st.findOne({ _id: META_ID });
      __cache = { atMs: Date.now(), meta: meta || {}, dias: new Map() };
    }
    const meta = __cache.meta || {};
    if (!meta.primeiroDiaMeta) return out;
    const hoje = diaSP(Date.now());
    const pedir = [];
    for (const d of dias || []) {
      if (d < meta.primeiroDiaMeta) continue;
      if (d >= hoje && meta.mediaAtual) { out[d] = Number(meta.mediaAtual); continue; }
      if (__cache.dias.has(d)) out[d] = __cache.dias.get(d); else pedir.push(d);
    }
    if (pedir.length) {
      const col = await getCollection('fx_lootrush_custo');
      const docs = await col.find({ _id: { $in: pedir } }).toArray();
      for (const x of docs) { const r = Number(x.rate); if (r > 1 && r < 20) { out[x._id] = r; __cache.dias.set(x._id, r); } }
    }
  } catch (_) {}
  return out;
}

let __timer = null;
function iniciar() {
  if (__timer || !String(process.env.LOOTRUSH_API_KEY || '').trim()) return;
  const roda = () => sincronizar().then((r) => { if (r && r.ok) console.log('💵 [lootrush-custo] média R$ ' + r.mediaAtual + ' | estoque US$ ' + r.estoqueCalculadoUsd + ' (real ' + r.saldoRealUsd + ')'); }).catch((e) => console.log('⚠️ [lootrush-custo]', (e && e.message) || e));
  setTimeout(roda, 45000);
  __timer = setInterval(roda, 60 * 60 * 1000); // 1x por hora
}

module.exports = { sincronizar, custoPorDia, iniciar, _calcular: calcular };
