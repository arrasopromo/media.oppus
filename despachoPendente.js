// ═══════════════════════════════════════════════════════════════════════════
//  DESPACHO PENDENTE — pedido pago que ficou sem pedido no fornecedor
// ═══════════════════════════════════════════════════════════════════════════
//  Duas rotinas, a cada DESPACHO_PENDENTE_LOOP_MIN (padrão 5 min):
//
//  1) PAGO SEM ENVIO: pedido pago há mais de DESPACHO_PENDENTE_MIN (padrão 10 min)
//     e menos de DESPACHO_PENDENTE_JANELA_H (padrão 48h), de perfil público, sem
//     nenhum orderId de fornecedor. Acontece quando o servidor reinicia no meio do
//     despacho (deploy) ou quando o fornecedor demora e a chamada estoura o timeout.
//       • nunca chegou a chamar o fornecedor, ou o fornecedor RECUSOU com erro claro:
//         despacha de novo pelo fluxo normal (processOrderFulfillment);
//       • timeout / queda de rede / 'processing' parado: o pedido PODE ter entrado no
//         fornecedor sem o número voltar. Antes de reenviar, varre no fornecedor os
//         orderIds vizinhos (action=status em lotes de 100; os IDs são sequenciais e só
//         os nossos respondem). Só reenvia se não houver pedido nosso desconhecido com a
//         mesma quantidade ou o mesmo custo. Se não der para conferir, avisa no WhatsApp
//         e não reenvia.
//     No máximo DESPACHO_PENDENTE_MAX_TENTATIVAS (padrão 3) por pedido, 20 min entre elas.
//
//  2) PRIVADO QUE ABRIU: pedido pago de perfil privado (o despacho fica adiado), dos
//     últimos PRIVADOS_RECHECK_DIAS (padrão 7). A cada PRIVADOS_RECHECK_MIN (padrão 30)
//     confere o perfil AO VIVO; se abriu, despacha. Arquivados em /painel/privados e
//     marcados como enviados ficam de fora (decisão manual).
//
//  Desliga o envio: DESPACHO_PENDENTE_ENABLED=false. Privados vem DESLIGADO (gasta RocketAPI); liga com PRIVADOS_RECHECK_ENABLED=true.
// ═══════════════════════════════════════════════════════════════════════════

const _ax = require('axios'); const axios = (_ax && typeof _ax.post === 'function') ? _ax : (_ax && _ax.default) || _ax;

const espera = (ms) => new Promise((ok) => setTimeout(ok, ms));
const num = (v, d) =>{ const n = Number(v); return Number.isFinite(n) && n > 0 ? n : d; };
const temId = (v) => v != null && /^[0-9]+$/.test(String(v).trim());
const CHAVES_NAO_SLOT = /^(paghiper|woovi|pagarme|xpag|nowpayments|customer|costs|utms|utmPrimeiroToque|geolocation|emails|notaFiscal|profilePrivacy|fulfillmentCalc|fulfillmentDispatch|additionalInfoMap(Paid)?|upsell|followersQueue|tracking|trackcombo|manualSale|manualApproval|discount|mismatchDetails|autoDespacho|alertaSemEnvio|refil.*|recovery.*|parcialHistorico)$/i;
const ERRO_AMBIGUO = /timeout|timed out|ETIMEDOUT|ECONNRESET|ECONNABORTED|ECONNREFUSED|EAI_AGAIN|ENOTFOUND|socket hang up|network|\b50[234]\b|bad gateway|gateway time|service unavailable|missing_order_id/i;

function temEnvioNoFornecedor(o) {
  for (const [k, v] of Object.entries(o || {})) {
    if (!v || typeof v !== 'object' || Array.isArray(v) || CHAVES_NAO_SLOT.test(k)) continue;
    if (temId(v.orderId) || temId(v.id)) return true;
    if (Array.isArray(v.orders) && v.orders.some((x) => x && (temId(x.orderId) || temId(x.id)))) return true;
  }
  return false;
}
// Slots que já tentaram falar com o fornecedor (têm requestPayload/status/attemptedAt).
function slotsTentados(o) {
  const out = [];
  for (const [k, v] of Object.entries(o || {})) {
    if (!v || typeof v !== 'object' || Array.isArray(v) || CHAVES_NAO_SLOT.test(k)) continue;
    if (v.requestPayload || v.attemptedAt || v.requestedAt || v.status) out.push([k, v]);
  }
  return out;
}
const msDe = (v) => { if (!v) return 0; const t = new Date(v).getTime(); return Number.isFinite(t) ? t : 0; };
function pagoEmMs(o) {
  return msDe(o.paidAt) || msDe(o.paghiper && o.paghiper.paidAt) || msDe(o.woovi && o.woovi.paidAt)
    || msDe(o.xpag && o.xpag.status === 'confirmed' && o.xpag.updatedAt) || msDe(o.createdAt);
}
function mapaInfo(o) {
  const m = {};
  for (const a of [o.additionalInfo, o.additionalInfoPaid]) if (Array.isArray(a)) for (const it of a) if (it && it.key) m[String(it.key)] = String(it.value == null ? '' : it.value);
  return Object.assign(m, o.additionalInfoMap || {}, o.additionalInfoMapPaid || {});
}
function ehPrivado(o) { return o.isPrivate === true || (o.profilePrivacy && o.profilePrivacy.isPrivate === true) || String((o.additionalInfoMap || {}).isPrivate) === 'true'; }
function foraDoEscopo(o) {
  const m = mapaInfo(o);
  const tipo = String(m.tipo_servico || o.tipoServico || o.tipo || '').toLowerCase();
  const cat = String(m.categoria_servico || o.categoriaServico || '').toLowerCase();
  if (/refil|extens/.test(tipo) || /refil|extens/.test(cat)) return 'refil';
  if (o.upsell && o.upsell.isUpsell === true) return 'upsell';           // o upsell-poller cuida
  if (o.manualSale && o.manualSale.skipProvider) return 'skip_provider';
  if (o.followersQueue && o.followersQueue.status === 'waiting_previous') return 'fila';   // a fila cuida
  if (o.fulfillmentDispatch && o.fulfillmentDispatch.blocked) return 'bloqueado';
  if (o.orderSent === true) return 'marcado_enviado';
  return '';
}

module.exports = function iniciarDespachoPendente(deps) {
  const {
    getCollection, processOrderFulfillment, verifyInstagramProfile, smmProviders, normalizeSmmProvider,
    sendEvolutionText, backgroundJobsEnabled, site,
  } = deps;
  const ligadoEnvio = String(process.env.DESPACHO_PENDENTE_ENABLED || 'true').toLowerCase() !== 'false';
  // Privados: DESLIGADO por padrão (cada checagem gasta request da RocketAPI). Só liga com PRIVADOS_RECHECK_ENABLED=true.
  const ligadoPriv = String(process.env.PRIVADOS_RECHECK_ENABLED || 'false').toLowerCase() === 'true';
  if (!ligadoEnvio && !ligadoPriv) { try { console.log('⏸️ [despacho-pendente] DESLIGADO'); } catch (_) {} return; }

  const LOOP_MIN = num(process.env.DESPACHO_PENDENTE_LOOP_MIN, 5);
  const ESPERA_MIN = num(process.env.DESPACHO_PENDENTE_MIN, 10);
  const JANELA_H = num(process.env.DESPACHO_PENDENTE_JANELA_H, 48);
  const MAX_TENT = num(process.env.DESPACHO_PENDENTE_MAX_TENTATIVAS, 3);
  const PRIV_DIAS = num(process.env.PRIVADOS_RECHECK_DIAS, 7);
  const PRIV_MIN = num(process.env.PRIVADOS_RECHECK_MIN, 30);
  const MAX_IDS_VARRER = num(process.env.DESPACHO_PENDENTE_MAX_IDS, 30000);
  const fone = String(process.env.DESPACHO_PENDENTE_ALERTA_PHONE || process.env.PANEL_BALANCE_ALERT_PHONE || '5531975938916').replace(/\D/g, '');
  const log = (...a) => { try { console.log('🚚 [despacho-pendente]', ...a); } catch (_) {} };

  const prov = (p) => { const k = normalizeSmmProvider(p); return smmProviders[k] ? { id: k, url: smmProviders[k].url, key: String(process.env[smmProviders[k].keyEnv] || '').trim() } : null; };
  const slotProvider = (nome, slot) => prov(slot.provider || (/^fornecedor_social/.test(nome) ? 'fornecedor_social' : /^topfama/.test(nome) ? 'topfama' : /^nuvra/.test(nome) ? 'nuvra' : /^worldsmm/.test(nome) ? 'worldsmm' : ''));
  const chamar = async (p, params, timeout) => {
    const r = await axios.post(p.url, new URLSearchParams(Object.assign({ key: p.key }, params)).toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: timeout || 30000, validateStatus: () => true });
    return r && r.data;
  };

  async function avisar(o, texto) {
    try {
      const col = await getCollection('checkout_orders');
      const ult = msDe(o.autoDespacho && o.autoDespacho.avisadoEm);
      if (ult && Date.now() - ult < 6 * 3600e3) return;
      await col.updateOne({ _id: o._id }, { $set: { 'autoDespacho.avisadoEm': new Date().toISOString() } });
      if (fone && typeof sendEvolutionText === 'function') await sendEvolutionText(fone, `⚠️ ${site}: ${texto}`);
    } catch (_) {}
  }
  const quem = (o) => '@' + String(o.instagramUsername || o.instauser || mapaInfo(o).instagram_username || '?') + ' ' + (o.identifier || o._id);

  // orderIds de um fornecedor gravados no banco (para achar a faixa a varrer e descartar os conhecidos).
  // Oppus e You Agency usam as MESMAS contas de fornecedor (ex.: JAP) e o mesmo servidor Mongo, em bancos
  // diferentes. Um pedido da outra loja não é "desconhecido": lê os dois bancos (pedidos e testes).
  // Se o outro banco não puder ser lido, os pedidos dele aparecem como suspeitos e nada é reenviado (lado seguro).
  const OUTROS_DBS = String(process.env.DESPACHO_PENDENTE_OUTROS_DBS || (/you agency/i.test(String(site || '')) ? 'site-whatsapp' : 'you-agency'))
    .split(',').map((x) => x.trim()).filter((x) => x && x !== String(process.env.MONGODB_DB_NAME || ''));
  let cliOutros = null;
  async function colecoesOutras(nome) {
    if (!OUTROS_DBS.length || !process.env.MONGODB_URI) return [];
    try {
      if (!cliOutros) { const { MongoClient } = require('mongodb'); cliOutros = await MongoClient.connect(process.env.MONGODB_URI); }
      return OUTROS_DBS.map((n) => cliOutros.db(n).collection(nome));
    } catch (e) { cliOutros = null; console.warn('[despacho-pendente] não abriu o banco da outra loja:', e && e.message); return []; }
  }

  async function idsConhecidos(col, provId, desdeMs) {
    const pares = []; const todos = new Set();
    const desdeIso = new Date(desdeMs).toISOString();
    const proj = { paghiper: 0, woovi: 0, xpag: 0, additionalInfo: 0, additionalInfoPaid: 0, customer: 0, utms: 0, tracking: 0 };
    for (const c of [col].concat(await colecoesOutras('checkout_orders'))) {
      let docs = [];
      try { docs = await c.find({ createdAt: { $gte: desdeIso } }).project(proj).toArray(); } catch (e) { if (c === col) throw e; continue; }
      for (const d of docs) for (const [k, v] of Object.entries(d)) {
        if (!v || typeof v !== 'object' || Array.isArray(v) || CHAVES_NAO_SLOT.test(k)) continue;
        const p = slotProvider(k, v); if (!p || p.id !== provId) continue;
        const lista = [v].concat(Array.isArray(v.orders) ? v.orders : []);
        for (const x of lista) { const id = x && (x.orderId || x.id); if (!temId(id)) continue; todos.add(String(id)); const t = msDe(x.requestedAt || v.requestedAt || x.at); if (t) pares.push([t, Number(id)]); }
        if (Array.isArray(v.reorders)) for (const r of v.reorders) if (r && temId(r.orderId)) todos.add(String(r.orderId));
        if (Array.isArray(v.parcialHistorico)) for (const r of v.parcialHistorico) if (r && temId(r.orderId)) todos.add(String(r.orderId));
      }
    }
    // Pedidos da tela de Testes de Serviços também são nossos: servem de referência e não são "desconhecidos".
    let colTestes = []; try { colTestes = [await getCollection('service_tests')]; } catch (_) {}
    for (const tc of colTestes.concat(await colecoesOutras('service_tests'))) {
      try {
        const testes = await tc.find({ providerOrderId: { $exists: true, $nin: [null, ''] }, createdAt: { $gte: desdeIso } }).project({ providerOrderId: 1, fornecedor: 1, createdAt: 1, dataPedidoMs: 1 }).toArray();
        for (const t of testes) {
          const host = String(t.fornecedor || '').replace(/^https?:\/\//, '').replace(/^www\./, '').split(/[./]/)[0];
          const p = prov(host); if (!p || p.id !== provId || !temId(t.providerOrderId)) continue;
          todos.add(String(t.providerOrderId)); const ms = Number(t.dataPedidoMs) || msDe(t.createdAt); if (ms) pares.push([ms, Number(t.providerOrderId)]);
        }
      } catch (_) {}
    }
    pares.sort((a, b) => a[0] - b[0]);
    return { pares, todos };
  }

  // Confere no fornecedor se o add que estourou o tempo criou pedido. { seguro, motivo }.
  async function conferirNoFornecedor(col, o, nome, slot) {
    const p = slotProvider(nome, slot);
    if (!p || !p.key) return { seguro: false, motivo: 'fornecedor desconhecido no registro do pedido' };
    const fimMs = msDe(slot.requestedAt) || msDe(slot.attemptedAt);
    if (!fimMs) return { seguro: false, motivo: 'sem horário da tentativa' };
    const iniMs = (msDe(slot.attemptedAt) || fimMs - 25e3) - 60e3;
    const { pares, todos } = await idsConhecidos(col, p.id, iniMs - 72 * 3600e3);
    const antes = pares.filter(([t]) => t < iniMs); const depois = pares.filter(([t]) => t > fimMs + 120e3);
    if (!antes.length) return { seguro: false, motivo: `sem pedido anterior no ${p.id} para servir de referência` };
    const baixo = antes[antes.length - 1][1] + 1;
    let alto = depois.length ? depois[0][1] - 1 : 0;
    if (!alto) {
      // Sem pedido nosso depois: estima o ritmo de IDs do fornecedor pelo histórico e dá folga de 50%.
      const ref = antes.length >= 2 ? antes[0] : null; const ult = antes[antes.length - 1];
      if (!ref || ult[0] <= ref[0]) return { seguro: false, motivo: 'sem histórico para estimar a faixa de IDs' };
      const ritmo = (ult[1] - ref[1]) / (ult[0] - ref[0]);
      alto = Math.ceil(ult[1] + ritmo * (fimMs + 120e3 - ult[0]) * 1.5);
    }
    if (alto < baixo) return { seguro: true, motivo: 'faixa vazia' };
    if (alto - baixo > MAX_IDS_VARRER) return { seguro: false, motivo: `faixa de IDs grande demais para conferir (${alto - baixo})` };
    const qtd = Number(slot.requestPayload && slot.requestPayload.quantity) || 0;
    const svc = String(slot.requestPayload && slot.requestPayload.service || '');
    let custoEsperado = 0;
    try { const sv = await chamar(p, { action: 'services' }, 30000); const s = Array.isArray(sv) && sv.find((x) => String(x.service) === svc); if (s && qtd) custoEsperado = Number(s.rate) * qtd / 1000; } catch (_) {}
    const suspeitos = [];
    for (let ini = baixo; ini <= alto; ini += 100) {
      const ids = []; for (let i = ini; i < Math.min(ini + 100, alto + 1); i++) ids.push(i);
      let d = null, falha = '';
      for (let t = 0; t < 4; t++) {
        try { d = await chamar(p, { action: 'status', orders: ids.join(',') }, 30000); } catch (e) { d = null; falha = (e && (e.code || e.message)) || 'erro'; }
        if (d && typeof d === 'object' && !Array.isArray(d) && !d.error) break;
        if (d) falha = typeof d === 'string' ? d.slice(0, 80) : JSON.stringify(d).slice(0, 80);
        d = null; await espera(1500 * (t + 1));   // limite de requisições: espera e tenta de novo
      }
      if (!d) return { seguro: false, motivo: `fornecedor ${p.id} não respondeu à conferência (IDs ${ini}..., ${falha})` };
      await espera(120);
      for (const [id, v] of Object.entries(d)) {
        if (!v || v.error || todos.has(String(id))) continue;
        const remains = Number(v.remains); const charge = Number(v.charge);
        const bateQtd = qtd && remains === qtd; const bateCusto = custoEsperado && charge && Math.abs(charge - custoEsperado) <= custoEsperado * 0.02;
        if (bateQtd || bateCusto || !custoEsperado) suspeitos.push(id + ' (' + (v.status || '?') + ', ' + (v.remains || '?') + ')');
      }
    }
    if (suspeitos.length) return { seguro: false, motivo: `pedido nosso não registrado no ${p.id}: ${suspeitos.slice(0, 3).join(', ')}` };
    return { seguro: true, motivo: `conferido no ${p.id}: nada criado entre ${baixo} e ${alto}`, faixa: [baixo, alto] };
  }

  async function tratarPagoSemEnvio(col, o) {
    const ad = o.autoDespacho || {};
    if (ad.desistiu) return;
    if ((ad.tentativas || 0) >= MAX_TENT) {
      await col.updateOne({ _id: o._id }, { $set: { 'autoDespacho.desistiu': true, 'autoDespacho.desistiuEm': new Date().toISOString() } });
      await avisar(o, `pedido pago SEM ENVIO depois de ${MAX_TENT} tentativas automáticas: ${quem(o)}. Último erro: ${ad.ultimoErro || '?'}. Reenvie pelo painel.`);
      return;
    }
    if (ad.ultimaEm && Date.now() - msDe(ad.ultimaEm) < 20 * 60e3) return;
    const tentados = slotsTentados(o);
    // Lock em andamento (despacho rodando agora): não mexe.
    if (tentados.some(([, v]) => v.status === 'processing' && Date.now() - (msDe(v.attemptedAt) || msDe(v.requestedAt)) < 20 * 60e3)) return;
    const ambiguos = tentados.filter(([, v]) => v.status === 'processing' || v.status === 'unknown' || ERRO_AMBIGUO.test(typeof v.error === 'string' ? v.error : JSON.stringify(v.error || '')));
    let conferencia = null;
    for (const [nome, slot] of ambiguos) {
      conferencia = await conferirNoFornecedor(col, o, nome, slot);
      if (!conferencia.seguro) {
        await col.updateOne({ _id: o._id }, { $set: { 'autoDespacho.ultimaConferencia': { em: new Date().toISOString(), slot: nome, ...conferencia } } });
        await avisar(o, `pedido pago SEM ENVIO e não reenviei sozinho: ${quem(o)}. Motivo: ${conferencia.motivo}.`);
        log('não reenviado', o.identifier, conferencia.motivo);
        return;
      }
    }
    // Destrava 'processing' parado (o lock impediria o reenvio) depois de conferido.
    const setDestrava = {};
    for (const [nome, slot] of ambiguos) if (slot.status === 'processing') setDestrava[nome + '.status'] = 'error';
    const agora = new Date().toISOString();
    await col.updateOne({ _id: o._id }, {
      $set: Object.assign(setDestrava, { 'autoDespacho.ultimaEm': agora, 'autoDespacho.motivo': ambiguos.length ? 'timeout_conferido' : (tentados.length ? 'erro_fornecedor' : 'nunca_despachado'), ...(conferencia ? { 'autoDespacho.ultimaConferencia': { em: agora, ...conferencia } } : {}) }),
      $inc: { 'autoDespacho.tentativas': 1 },
    });
    const fresh = await col.findOne({ _id: o._id });
    log('reenviando', o.identifier, quem(o), ambiguos.length ? '(timeout conferido)' : tentados.length ? '(erro do fornecedor)' : '(nunca despachado)');
    try { await processOrderFulfillment(fresh, col, null); } catch (e) { log('erro no despacho', o.identifier, e && e.message); }
    const depois = await col.findOne({ _id: o._id });
    if (temEnvioNoFornecedor(depois)) {
      await col.updateOne({ _id: o._id }, { $set: { 'autoDespacho.enviadoEm': new Date().toISOString() } });
      log('enviado', o.identifier);
    } else {
      const err = slotsTentados(depois).map(([k, v]) => k + ': ' + (typeof v.error === 'string' ? v.error : JSON.stringify(v.error || v.status || ''))).join(' | ').slice(0, 300);
      await col.updateOne({ _id: o._id }, { $set: { 'autoDespacho.ultimoErro': err || 'sem orderId' } });
    }
  }

  async function tratarPrivado(col, o) {
    const ad = o.autoDespacho || {};
    if (ad.privCheckEm && Date.now() - msDe(ad.privCheckEm) < PRIV_MIN * 60e3) return;
    const user = String(o.instagramUsername || o.instauser || mapaInfo(o).instagram_username || '').replace(/^@+/, '').trim();
    if (!user) return;
    await col.updateOne({ _id: o._id }, { $set: { 'autoDespacho.privCheckEm': new Date().toISOString() } });
    let check = null;
    try { check = await verifyInstagramProfile(user, 'ProcessFulfillment-Check', '127.0.0.1', { session: {} }, null, true); } catch (_) { return; }
    const publico = !!(check && check.success && check.profile && check.profile.isPrivate === false);
    if (!publico) return;
    const agora = new Date().toISOString();
    await col.updateOne({ _id: o._id }, { $set: { isPrivate: false, 'profilePrivacy.isPrivate': false, 'profilePrivacy.updatedAt': agora, 'autoDespacho.privadoAbriuEm': agora } });
    const fresh = await col.findOne({ _id: o._id });
    log('perfil abriu, despachando', o.identifier, '@' + user);
    const req = { bypassCache: true, body: { bypassCache: true }, headers: {}, session: {}, ip: '127.0.0.1' };
    try { await processOrderFulfillment(fresh, col, req); } catch (e) { log('erro no despacho do privado', o.identifier, e && e.message); }
    const depois = await col.findOne({ _id: o._id });
    if (temEnvioNoFornecedor(depois)) await col.updateOne({ _id: o._id }, { $set: { 'autoDespacho.enviadoEm': new Date().toISOString(), 'autoDespacho.motivo': 'privado_abriu' } });
  }

  let rodando = false;
  async function rodada() {
    if (rodando) return; if (typeof backgroundJobsEnabled === 'function' && !backgroundJobsEnabled()) return;
    rodando = true;
    try {
      const col = await getCollection('checkout_orders');
      const desdeMs = Date.now() - Math.max(JANELA_H * 3600e3, PRIV_DIAS * 864e5);
      const desdeIso = new Date(desdeMs).toISOString();
      const pedidos = await col.find({ status: { $in: ['pago', 'paid'] }, $or: [{ createdAt: { $gte: desdeIso } }, { createdAt: { $gte: new Date(desdeMs) } }] }).toArray();
      const agora = Date.now();
      for (const o of pedidos) {
        try {
          if (temEnvioNoFornecedor(o) || foraDoEscopo(o)) continue;
          const pago = pagoEmMs(o); if (!pago) continue;
          if (ehPrivado(o)) {
            if (ligadoPriv && !o.archived && agora - pago <= PRIV_DIAS * 864e5) await tratarPrivado(col, o);
            continue;
          }
          if (ligadoEnvio && agora - pago >= ESPERA_MIN * 60e3 && agora - pago <= JANELA_H * 3600e3) await tratarPagoSemEnvio(col, o);
        } catch (e) { log('erro no pedido', o && o.identifier, e && e.message); }
      }
    } catch (e) { log('erro geral', e && e.message); }
    finally { rodando = false; }
  }

  setTimeout(() => { rodada().catch(() => {}); }, 3 * 60e3);
  setInterval(() => { rodada().catch(() => {}); }, LOOP_MIN * 60e3);
  log(`ligado: a cada ${LOOP_MIN}min · pago sem envio ${ligadoEnvio ? `após ${ESPERA_MIN}min (até ${JANELA_H}h, máx ${MAX_TENT} tentativas)` : 'OFF'} · privados ${ligadoPriv ? `a cada ${PRIV_MIN}min por ${PRIV_DIAS} dias` : 'OFF'}`);
  return { rodada, temEnvioNoFornecedor, conferirNoFornecedor };
};
