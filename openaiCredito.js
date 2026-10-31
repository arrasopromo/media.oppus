// openaiCredito.js — Crédito da OpenAI no painel. A API não expõe o saldo pré-pago (o endpoint de
// billing só aceita sessão do navegador), então o painel mostra duas coisas:
//   • situação REAL: uma chamada mínima ao modelo diz se ainda há crédito;
//   • saldo ESTIMADO: valor informado no painel menos o gasto dos tokens registrados daí em diante.
// A transcrição de áudio não entra na estimativa (a API não devolve o consumo dela).
'use strict';

const { getCollection } = require('./mongodbClient');
let axios = require('axios'); if (axios && axios.default) axios = axios.default;

const DOC_ID = 'openai_credit';
const PROBE_TTL_MS = 5 * 60 * 1000;
// US$ por 1 milhão de tokens. O mais específico vem primeiro (gpt-4.1-mini antes de gpt-4.1).
const PRECOS = [
  ['gpt-4.1-mini', { entrada: 0.40, cache: 0.10, saida: 1.60 }],
  ['gpt-4.1', { entrada: 2.00, cache: 0.50, saida: 8.00 }],
  ['gpt-4o-mini', { entrada: 0.15, cache: 0.075, saida: 0.60 }],
];

function custoUsd(model, usage) {
  if (!usage) return 0;
  const m = String(model || '');
  const preco = (PRECOS.find(([nome]) => m.startsWith(nome)) || PRECOS[1])[1];
  const total = Number(usage.prompt_tokens) || 0;
  const cache = Number(usage.prompt_tokens_details && usage.prompt_tokens_details.cached_tokens) || 0;
  const saida = Number(usage.completion_tokens) || 0;
  return (((total - cache) * preco.entrada) + (cache * preco.cache) + (saida * preco.saida)) / 1e6;
}

// Soma o gasto de uma resposta. Nunca atrapalha a conversa: erro aqui é ignorado.
async function registrarUso(model, usage) {
  try {
    const usd = custoUsd(model, usage);
    if (!(usd > 0)) return;
    const col = await getCollection('settings');
    await col.updateOne({ _id: DOC_ID }, { $inc: { gastoUsd: usd }, $set: { ultimoUsoEm: new Date().toISOString() } }, { upsert: true });
  } catch (_) {}
}

// Novo ponto de partida: o saldo que o painel da OpenAI mostra agora.
async function definirSaldo(valor) {
  const col = await getCollection('settings');
  await col.updateOne({ _id: DOC_ID }, { $set: { saldoBaseUsd: valor, gastoUsd: 0, baseEm: new Date().toISOString() } }, { upsert: true });
}

let __probe = { atMs: 0, valor: null };
async function testarCredito({ force = false } = {}) {
  if (!force && __probe.valor && (Date.now() - __probe.atMs) < PROBE_TTL_MS) return __probe.valor;
  const key = String(process.env.OPENAI_API_KEY || '').trim();
  if (!key) return { semCredito: false, erro: 'missing_key', checadoEm: new Date().toISOString() };
  let valor;
  try {
    const r = await axios.post('https://api.openai.com/v1/chat/completions',
      { model: String(process.env.OPENAI_MODEL || 'gpt-4o-mini').trim(), messages: [{ role: 'user', content: 'ok' }], max_tokens: 1 },
      { headers: { Authorization: 'Bearer ' + key, 'Content-Type': 'application/json' }, timeout: 20000, validateStatus: () => true });
    const e = (r.data && r.data.error) || {};
    const semCredito = r.status === 429 && /credit_balance_exhausted|insufficient_quota/.test(String(e.code || '') + ' ' + String(e.type || ''));
    valor = { semCredito, erro: (r.status === 200 || semCredito) ? '' : ('http_' + r.status), checadoEm: new Date().toISOString() };
  } catch (e) {
    valor = { semCredito: false, erro: (e && (e.code || e.message)) || 'network_error', checadoEm: new Date().toISOString() };
  }
  __probe = { atMs: Date.now(), valor };
  return valor;
}

async function status({ force = false } = {}) {
  const teste = await testarCredito({ force });
  let doc = null;
  try { const col = await getCollection('settings'); doc = await col.findOne({ _id: DOC_ID }); } catch (_) {}
  const base = doc && Number.isFinite(Number(doc.saldoBaseUsd)) ? Number(doc.saldoBaseUsd) : null;
  const gasto = (doc && Number(doc.gastoUsd)) || 0;
  const estimado = base == null ? null : Math.max(0, Math.round((base - gasto) * 100) / 100);
  return {
    semCredito: teste.semCredito, erro: teste.erro, checadoEm: teste.checadoEm,
    saldoUsd: teste.semCredito ? 0 : estimado,
    saldoBaseUsd: base, gastoUsd: Math.round(gasto * 100) / 100, baseEm: (doc && doc.baseEm) || null,
  };
}

module.exports = { custoUsd, registrarUso, definirSaldo, status };
