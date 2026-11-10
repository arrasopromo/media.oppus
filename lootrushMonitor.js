// lootrushMonitor.js — Vigia o saldo do cartão LootRush que paga a conta Meta ca3 (USD)
// e avisa no WhatsApp (Evolution, número do PANEL_BALANCE_ALERT_PHONE) ANTES da
// cobrança da Meta ser recusada. A Meta cobra a ca3 em parcelas fixas (limite de
// cobrança, ex.: US$ 196) sempre que o gasto não cobrado chega nesse valor; se o
// cartão não tiver saldo, a cobrança volta recusada e a conta pode ser bloqueada.
//
// Conta: horas até a cobrança que o cartão NÃO cobre = (limite − não cobrado + k·limite) / gasto por hora,
// onde k = quantas cobranças cheias o saldo do cartão paga. Alerta em degraus (12h, 6h, 3h, 1h),
// um por degrau a cada "ciclo" (o ciclo zera quando o saldo volta a cobrir mais de 12h), e na hora
// em que aparecer uma cobrança RECUSADA no cartão (repete a cada 45 min enquanto não resolver).
// Textos variam (vários modelos por nível + saudação/fecho sorteados) pra não repetir mensagem igual.
'use strict';

const { getCollection } = require('./mongodbClient');
let axios = require('axios'); if (axios && axios.default) axios = axios.default;

const MCP_URL = 'https://mcp.lootrush.com/mcp';
const GRAPH = 'https://graph.facebook.com/v21.0/';
const DEGRAUS = [12, 6, 3, 1]; // horas antes da recusa prevista
const STATE_ID = 'lootrush_monitor';

const usd = (v) => 'US$ ' + Number(v || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const sorteia = (arr) => arr[Math.floor(Math.random() * arr.length)];
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ── LootRush MCP (JSON-RPC sobre Streamable HTTP). Só ferramentas de leitura. ──
async function mcpSession(key) {
  let sid = ''; let id = 1;
  const parse = (d) => {
    if (d && typeof d === 'object') return d;
    const s = String(d || '');
    const linhas = s.split('\n').filter((x) => x.startsWith('data:')).map((x) => x.slice(5).trim());
    return JSON.parse(linhas.length ? linhas[linhas.length - 1] : s);
  };
  const rpc = async (method, params, notif) => {
    const headers = { Authorization: 'Bearer ' + key, 'Content-Type': 'application/json', Accept: 'application/json, text/event-stream' };
    if (sid) headers['Mcp-Session-Id'] = sid;
    const body = notif ? { jsonrpc: '2.0', method, params } : { jsonrpc: '2.0', id: id++, method, params };
    const r = await axios.post(MCP_URL, body, { headers, timeout: 45000, validateStatus: () => true, responseType: 'text', transformResponse: (x) => x });
    if (r.headers && r.headers['mcp-session-id']) sid = r.headers['mcp-session-id'];
    if (notif) return null;
    if (r.status === 401 || r.status === 403) throw new Error('lootrush_auth_' + r.status);
    if (r.status === 429) throw new Error('lootrush_rate_limit');
    const d = parse(r.data);
    if (d && d.error) throw new Error('lootrush_' + (d.error.message || d.error.code));
    return d && d.result;
  };
  await rpc('initialize', { protocolVersion: '2025-03-26', capabilities: {}, clientInfo: { name: 'oppus-monitor-ca3', version: '1.0' } });
  await rpc('notifications/initialized', {}, true);
  return async (name, args) => {
    const res = await rpc('tools/call', { name, arguments: args || {} });
    const txt = (res && Array.isArray(res.content) ? res.content.map((c) => c.text || '').join('') : '');
    if (res && res.isError) throw new Error('lootrush_tool_' + name + ': ' + txt.slice(0, 160));
    return JSON.parse(txt || '{}');
  };
}

// ── Leitura do cartão (saldo + cobranças da Meta) ──
async function lerLootrush(key) {
  const call = await mcpSession(key);
  const cards = await call('getUserCards', {});
  await sleep(1100);
  const desde = new Date(Date.now() - 4 * 24 * 3600e3).toISOString();
  const tx = await call('getUserCardTransactions', { pageSize: 100, startDate: desde, polarity: 'debit' });
  const cb = cards.cardBalance || {};
  const lista = (cards.cards && cards.cards.userCards) || [];
  const cartao = lista.find((c) => String(c.lastFourDigits) === String(process.env.LOOTRUSH_CARD_LAST4 || '5955')) || lista[0] || {};
  const fb = ((tx && tx.transactions) || []).filter((t) => /facebk|facebook|meta/i.test(String(t.merchantName || '')) && (!cartao.id || t.userCardId === cartao.id));
  return {
    saldo: Number(cb.total || 0),
    depositoPendente: Number(cb.pendingDeposit || 0),
    limiteDiarioDisponivel: cartao.availableLimit != null ? Number(cartao.availableLimit) : null,
    cartao: { apelido: cartao.nickname || cartao.name || '', final: cartao.lastFourDigits || '', status: cartao.status || '' },
    cobrancasFb: fb.map((t) => ({ id: t.id, valor: Number(t.amount || 0), status: String(t.status || ''), motivo: t.statusReason || '', quando: t.authorizedAt || t.createdAt })),
  };
}

// ── Leitura da ca3 na Meta (não cobrado, gasto de hoje e orçamento ativo) ──
async function lerMeta() {
  const tok = String(process.env.FB_ADS_TOKEN || '').trim();
  const act = 'act_' + String(process.env.FB_AD_ACCOUNT_ID_USD || '').trim().replace(/^act_/, '');
  if (!tok || act === 'act_') throw new Error('meta_nao_configurada');
  const g = async (path, params) => {
    const r = await axios.get(GRAPH + path, { params: Object.assign({}, params, { access_token: tok }), timeout: 30000, validateStatus: () => true });
    if (!r.data || r.data.error) throw new Error('meta_' + ((r.data && r.data.error && r.data.error.message) || r.status));
    return r.data;
  };
  const conta = await g(act, { fields: 'account_status,balance,amount_spent,timezone_name' });
  const hoje = await g(act + '/insights', { fields: 'spend', date_preset: 'today' });
  const ativo = JSON.stringify(['ACTIVE']);
  const camps = await g(act + '/campaigns', { fields: 'daily_budget', effective_status: ativo, limit: 200 });
  const sets = await g(act + '/adsets', { fields: 'daily_budget', effective_status: ativo, limit: 200 });
  const orc = [...(camps.data || []), ...(sets.data || [])].reduce((s, x) => s + (Number(x.daily_budget) || 0), 0) / 100;
  return {
    status: Number(conta.account_status),
    naoCobrado: Number(conta.balance || 0) / 100,
    gastoHoje: Number((hoje.data && hoje.data[0] && hoje.data[0].spend) || 0),
    orcamentoDiario: orc,
  };
}

// Horas já passadas hoje no fuso de São Paulo (fuso da ca3).
function horasHojeSP() {
  const d = new Date(Date.now() - 3 * 3600e3);
  return d.getUTCHours() + d.getUTCMinutes() / 60;
}

function calcula(lr, meta) {
  const okFb = lr.cobrancasFb.filter((c) => c.status !== 'declined' && c.status !== 'reversed');
  // Limite de cobrança = maior cobrança recente aprovada (as parciais/avulsas são menores).
  const limite = Math.max(0, ...okFb.slice(0, 6).map((c) => c.valor)) || Number(process.env.LOOTRUSH_LIMITE_PADRAO || 196);
  const h = horasHojeSP();
  const porHoraHoje = h >= 2 ? meta.gastoHoje / h : 0;
  const porHora = Math.max(porHoraHoje, meta.orcamentoDiario / 24);
  const saldo = lr.saldo + lr.depositoPendente;
  const cobre = Math.floor(saldo / limite); // cobranças cheias que o saldo paga
  const faltaProxima = Math.max(0, limite - meta.naoCobrado);
  const horasAteProxima = porHora > 0 ? faltaProxima / porHora : null;
  const horasAteRecusa = porHora > 0 ? (faltaProxima + cobre * limite) / porHora : null;
  // Recarga para cobrir as próximas 24h de gasto (com 1 cobrança de folga).
  const precisa24h = porHora > 0 ? (Math.ceil((meta.naoCobrado + porHora * 24) / limite) + 1) * limite - meta.naoCobrado : limite;
  const recarga = Math.max(0, Math.ceil(precisa24h - saldo));
  const recargaMinima = Math.max(0, Math.ceil(limite * (cobre + 1) - saldo));
  return { limite, porHora, porDia: porHora * 24, saldo, cobre, horasAteProxima, horasAteRecusa, recarga, recargaMinima };
}

function horaPrevista(h) {
  if (h == null) return '';
  const d = new Date(Date.now() + h * 3600e3 - 3 * 3600e3);
  return String(d.getUTCHours()).padStart(2, '0') + ':' + String(d.getUTCMinutes()).padStart(2, '0');
}

// ── Textos: formato curto e fixo; variam só o título e a linha de recarga (não repetir mensagem igual) ──
const LINK = () => String(process.env.LOOTRUSH_LINK || 'https://www.lootrush.com').trim();
function prazoTxt(h) {
  if (h == null) return 'sem previsão';
  if (h < 1) return 'menos de 1 hora';
  return 'até ' + Math.max(1, Math.round(h)) + ' hora' + (Math.round(h) > 1 ? 's' : '');
}
const TITULOS = {
  12: ['💳 *Cartão ca3*', '💳 *ca3 — saldo do cartão*', '💳 *Aviso de saldo ca3*', '💳 *ca3 · cartão LootRush*'],
  6: ['⚠️ *Cartão ca3*', '⚠️ *ca3 — saldo curto*', '⚠️ *Saldo do cartão ca3*', '⚠️ *ca3 · cartão LootRush*'],
  3: ['⚠️ *ca3 — recarregar logo*', '⚠️ *Cartão ca3 quase sem saldo*', '⚠️ *ca3 — saldo acabando*', '⚠️ *Atenção: cartão ca3*'],
  1: ['🚨 *ca3 — recusa em menos de 1h*', '🚨 *Cartão ca3 sem saldo*', '🚨 *ca3 — recarregar agora*', '🚨 *Urgente: cartão ca3*'],
};
function linhaRecarregue(c) {
  if (!(c.recargaMinima > 0)) return sorteia(['Recarregue', 'Recarregue o cartão', 'Recarregar pela LootRush']);
  return sorteia([
    `Recarregue - mín. ${usd(c.recargaMinima)} · ideal ${usd(c.recarga)}`,
    `Recarregue: no mínimo ${usd(c.recargaMinima)} (24h: ${usd(c.recarga)})`,
    `Recarregar - mínimo ${usd(c.recargaMinima)}, ideal ${usd(c.recarga)}`,
  ]);
}
function blocoNumeros(c, meta) {
  return `Saldo atual cartão - ${usd(c.saldo)}\nGasto atual Meta - ${usd(meta.gastoHoje)}\nPendente - ${usd(meta.naoCobrado)}`;
}

function msgAntecipada(nivel, c, lr, meta) {
  const prazo = sorteia([
    `Prazo de ${prazoTxt(c.horasAteRecusa)} pelo ritmo para cobrança`,
    `Prazo de ${prazoTxt(c.horasAteRecusa)} pelo ritmo para a cobrança`,
    `Pelo ritmo, prazo de ${prazoTxt(c.horasAteRecusa)} para cobrança`,
  ]) + (c.horasAteRecusa != null ? ` (~${horaPrevista(c.horasAteRecusa)})` : '');
  return `${sorteia(TITULOS[nivel] || TITULOS[12])}\n\n${blocoNumeros(c, meta)}\n\n${prazo}\n${linhaRecarregue(c)}\n${LINK()}`;
}

function msgRecusa(rec, c, lr, meta, n) {
  const hora = (() => { const d = new Date(new Date(rec.quando).getTime() - 3 * 3600e3); return String(d.getUTCHours()).padStart(2, '0') + ':' + String(d.getUTCMinutes()).padStart(2, '0'); })();
  const titulo = sorteia(['🛑 *Cobrança recusada na ca3*', '🛑 *ca3 — pagamento recusado*', '🛑 *Cartão ca3 recusou a Meta*']) + (n > 1 ? ` (${n}º aviso)` : '');
  const status = meta.status !== 1 ? `\nConta na Meta com status ${meta.status} (não ativa)` : '';
  return `${titulo}\n\n${blocoNumeros(c, meta)}\n\nRecusada ${usd(rec.valor)} às ${hora}${status}\n${linhaRecarregue(c)}\n${LINK()}`;
}

// ── Ciclo principal ──
let __running = false;
async function verificar({ force = false, enviar } = {}) {
  if (__running) return { skipped: 'running' };
  __running = true;
  const out = { at: new Date().toISOString() };
  try {
    const key = String(process.env.LOOTRUSH_API_KEY || '').trim();
    if (!key) { out.erro = 'LOOTRUSH_API_KEY ausente'; return out; }
    const [lr, meta] = await Promise.all([lerLootrush(key), lerMeta()]);
    const c = calcula(lr, meta);
    Object.assign(out, { cartao: lr.cartao, saldo: c.saldo, limiteDiarioDisponivel: lr.limiteDiarioDisponivel, metaStatus: meta.status, naoCobrado: meta.naoCobrado, gastoHoje: meta.gastoHoje, orcamentoDiario: meta.orcamentoDiario, limiteCobranca: c.limite, porDia: c.porDia, cobre: c.cobre, horasAteProxima: c.horasAteProxima, horasAteRecusa: c.horasAteRecusa, recarga: c.recarga, recargaMinima: c.recargaMinima });

    const col = await getCollection('settings');
    const st = (await col.findOne({ _id: STATE_ID })) || {};
    const phone = String(process.env.LOOTRUSH_ALERT_PHONE || process.env.PANEL_BALANCE_ALERT_PHONE || '5531975938916').replace(/\D/g, '');
    const ligado = String(process.env.LOOTRUSH_ALERT_ENABLED || 'true').toLowerCase() !== 'false';
    const set = { ultimo: out, updatedAt: new Date() };
    const envios = [];

    // 1) Cobrança recusada recente (últimas 6h) que ainda não foi resolvida por uma aprovada depois dela.
    const recentes = lr.cobrancasFb.filter((x) => Date.now() - new Date(x.quando).getTime() < 6 * 3600e3);
    const recusa = recentes.find((x) => x.status === 'declined');
    const aprovadaDepois = recusa && recentes.some((x) => x.status !== 'declined' && new Date(x.quando) > new Date(recusa.quando));
    if (recusa && !aprovadaDepois && c.saldo < c.limite) {
      const mesma = st.recusa && st.recusa.id === recusa.id;
      const n = mesma ? (st.recusa.n || 1) + 1 : 1;
      const ultimoMs = mesma && st.recusa.at ? new Date(st.recusa.at).getTime() : 0;
      if (force || !ultimoMs || Date.now() - ultimoMs >= 45 * 60e3) {
        envios.push({ tipo: 'recusa', texto: msgRecusa(recusa, c, lr, meta, n) });
        set.recusa = { id: recusa.id, n, at: new Date() };
      }
    } else if (st.recusa) set.recusa = null;

    // 2) Aviso antecipado em degraus. O ciclo zera quando o saldo volta a cobrir > 12h.
    let degrausEnviados = Array.isArray(st.degraus) ? st.degraus : [];
    const h = c.horasAteRecusa;
    if (h == null || h > DEGRAUS[0]) degrausEnviados = [];
    else if (meta.status === 1) {
      const nivel = DEGRAUS.filter((d) => h <= d).pop(); // menor degrau atingido
      if (nivel && (force || !degrausEnviados.includes(nivel)) && !envios.length) {
        envios.push({ tipo: 'antecipado_' + nivel, texto: msgAntecipada(nivel, c, lr, meta) });
        degrausEnviados = [...new Set([...degrausEnviados, ...DEGRAUS.filter((d) => d >= nivel)])];
      }
    }
    set.degraus = degrausEnviados;

    // Envia (nunca o mesmo texto do último envio).
    out.enviados = [];
    for (const e of envios) {
      if (!ligado || !phone || typeof enviar !== 'function') {
        out.enviados.push({ tipo: e.tipo, enviado: false, motivo: 'alerta desligado/sem envio', texto: e.texto });
        set.degraus = Array.isArray(st.degraus) ? st.degraus : []; set.recusa = st.recusa || null; // simulação não consome o degrau
        continue;
      }
      let texto = e.texto;
      if (st.ultimoTexto && st.ultimoTexto === texto) texto = texto + '\n\n' + sorteia(['—', '·', '(atualizado)']);
      const r = await enviar(phone, texto);
      out.enviados.push({ tipo: e.tipo, enviado: !!(r && r.ok), erro: r && r.error });
      if (r && r.ok) { set.ultimoTexto = texto; set.ultimoEnvioAt = new Date(); }
      else if (e.tipo.startsWith('antecipado')) set.degraus = Array.isArray(st.degraus) ? st.degraus : []; // falhou: tenta de novo no próximo ciclo
      else if (e.tipo === 'recusa') set.recusa = st.recusa || null;
    }
    await col.updateOne({ _id: STATE_ID }, { $set: set }, { upsert: true });
    return out;
  } catch (e) {
    out.erro = (e && e.message) || String(e);
    try { const col = await getCollection('settings'); await col.updateOne({ _id: STATE_ID }, { $set: { ultimoErro: { at: new Date(), erro: out.erro } } }, { upsert: true }); } catch (_) {}
    return out;
  } finally { __running = false; }
}

let __timer = null;
function iniciar({ enviar } = {}) {
  if (__timer) return;
  if (!String(process.env.LOOTRUSH_API_KEY || '').trim()) { try { console.log('💳 [lootrush] sem LOOTRUSH_API_KEY — monitor da ca3 desligado'); } catch (_) {} return; }
  const min = Math.max(5, Number(process.env.LOOTRUSH_CHECK_MINUTES || 15) || 15);
  const roda = () => verificar({ enviar }).then((r) => {
    try { console.log('💳 [lootrush] saldo', r.saldo, '| recusa em', r.horasAteRecusa != null ? r.horasAteRecusa.toFixed(1) + 'h' : '-', '| envios', JSON.stringify(r.enviados || []), r.erro ? '| erro ' + r.erro : ''); } catch (_) {}
  }).catch(() => {});
  setTimeout(roda, 60 * 1000);
  __timer = setInterval(roda, min * 60 * 1000);
  try { __timer.unref && __timer.unref(); } catch (_) {}
}

async function status() {
  const col = await getCollection('settings');
  return (await col.findOne({ _id: STATE_ID }, { projection: { ultimo: 1, ultimoErro: 1, degraus: 1, recusa: 1, ultimoEnvioAt: 1 } })) || {};
}

module.exports = { verificar, iniciar, status, _calcula: calcula, _msgAntecipada: msgAntecipada, _msgRecusa: msgRecusa };
