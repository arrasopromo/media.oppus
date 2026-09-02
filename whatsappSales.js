// whatsappSales.js — Catálogo, preços, validação de perfil, criação de pedido + Pix
// (PagHiper), geração de CPF e flag de suporte para a IA de vendas do WhatsApp.
// Reusa os MESMOS preços (pricing.js) e endpoints internos do site — banco: site-whatsapp.
'use strict';

const { getCollection } = require('./mongodbClient');
const { tabelaSeguidores, tabelaCurtidas, tabelaVisualizacoes, parsePrecoToCents } = require('./pricing.js');
let axios = require('axios'); if (axios && axios.default) axios = axios.default;

const INTERNAL_BASE = String(process.env.INTERNAL_BASE || ('http://localhost:' + (process.env.PORT || '3000'))).replace(/\/+$/, '');

const brl = (cents) => 'R$ ' + (Number(cents || 0) / 100).toFixed(2).replace('.', ',');

// ── Catálogo: só os serviços que ESTE projeto fulfila standalone ──
//   (comentários/compartilhamentos/stories só existem como order-bump aqui, ficam de fora.)
const CATALOG = {
  seguidores:    { key: 'seguidores',    categoria: 'seguidores',    unit: 'seguidores',   needsPost: false, tipos: true,  table: (t) => tabelaSeguidores[t] },
  curtidas:      { key: 'curtidas',      categoria: 'curtidas',      unit: 'curtidas',     needsPost: true,  tipos: true,  table: (t) => tabelaCurtidas[t] },
  visualizacoes: { key: 'visualizacoes', categoria: 'visualizacoes', unit: 'visualizações', needsPost: true, tipos: false, tipoFixo: 'visualizacoes_reels', table: () => tabelaVisualizacoes.visualizacoes_reels },
};

function normalizeServico(s) {
  const t = String(s || '').trim().toLowerCase();
  if (/seguidor|follow/.test(t)) return 'seguidores';
  if (/curtida|like/.test(t)) return 'curtidas';
  if (/visual|view|reel/.test(t)) return 'visualizacoes';
  return '';
}

// Rede de segurança de tipo (espelha o backend do site): "reais"/"de verdade"/"orgânico" → organicos.
function normalizeTipo(tipo) {
  const t = String(tipo || '').trim().toLowerCase();
  if (!t) return 'mistos';
  if (/organic|orgânic|reais|real|de verdade|verdade/.test(t)) return 'organicos';
  if (/brasileir/.test(t)) return 'brasileiros';
  if (/misto/.test(t)) return 'mistos';
  return 'mistos';
}

function tableFor(svc, tipo) {
  const t = svc.tipos ? (svc.table(normalizeTipo(tipo)) || svc.table('mistos')) : svc.table();
  // A IA NUNCA oferta pacote abaixo do mínimo da tabela (remove o "pacote de teste"
  // de 100 seguidores R$3 e afins). Só a tabela oficial, a partir de 150.
  return (Array.isArray(t) ? t : []).filter((x) => Number(x.q) >= 150);
}

// Cotação de UM pacote. Retorna preço oficial (server-side) ou a lista de quantidades disponíveis.
async function quote({ servico, tipo, quantidade }) {
  const key = normalizeServico(servico);
  const svc = CATALOG[key];
  if (!svc) return { ok: false, error: 'servico_invalido', message: 'Serviço inválido. Vendo seguidores, curtidas e visualizações.' };
  const q = parseInt(quantidade, 10);
  const table = tableFor(svc, tipo);
  if (!(q > 0)) return { ok: false, error: 'quantidade_invalida', available: table.map((x) => x.q) };
  const item = table.find((x) => x.q === q);
  if (!item) return { ok: false, error: 'quantidade_indisponivel', available: table.map((x) => x.q), message: 'Essa quantidade não está na tabela.' };
  const priceCents = parsePrecoToCents(item.p);
  return {
    ok: true, servico: svc.key, categoria: svc.categoria,
    tipo: svc.tipos ? normalizeTipo(tipo) : svc.tipoFixo,
    quantidade: q, priceCents, priceLabel: brl(priceCents),
    unit: svc.unit, needsPost: svc.needsPost,
  };
}

// Tabela inteira (quantidades + preços) de um serviço/tipo.
function priceTable({ servico, tipo }) {
  const key = normalizeServico(servico);
  const svc = CATALOG[key];
  if (!svc) return { ok: false, error: 'servico_invalido' };
  const table = tableFor(svc, tipo);
  return {
    ok: true, servico: svc.key, tipo: svc.tipos ? normalizeTipo(tipo) : svc.tipoFixo,
    unit: svc.unit, needsPost: svc.needsPost,
    itens: table.map((x) => ({ quantidade: x.q, preco: x.p, precoCents: parsePrecoToCents(x.p) })),
  };
}

// Aceita @user, link instagram.com/user, ou texto cru; junta espaços ("cris tiano" → cristiano).
function parseIgUsername(raw) {
  let s = String(raw || '').trim();
  if (!s) return '';
  const m = s.match(/instagram\.com\/([A-Za-z0-9_.]+)/i);
  if (m) return m[1].toLowerCase();
  s = s.replace(/^@+/, '').replace(/\s+/g, '').replace(/\/+$/g, '');
  return s.toLowerCase();
}

// Valida o @ via o MESMO endpoint do site (RocketAPI). Retorna nome + seguidores.
async function validateProfile(usuario) {
  const username = parseIgUsername(usuario);
  if (!username) return { ok: false, error: 'usuario_invalido' };
  try {
    // Endpoint INTERNO (sem trava de sessão) — o /api/check-instagram-profile exige
    // token de sessão e retornava 403 para a IA, deixando TODO perfil "não encontrado".
    const headers = {};
    const sec = String(process.env.INTERNAL_API_SECRET || '').trim();
    if (sec) headers['x-internal-secret'] = sec;
    const resp = await axios.post(INTERNAL_BASE + '/api/internal/ia-check-profile', { username }, { timeout: 25000, validateStatus: () => true, headers });
    const d = resp && resp.data;
    const p = (d && d.profile) ? d.profile : null;
    if (!d || d.success === false || !p) return { ok: false, error: 'nao_encontrado', username };
    return {
      ok: true, username: p.username || username,
      fullName: p.fullName || '', followers: Number(p.followersCount != null ? p.followersCount : p.followers) || 0,
      isPrivate: !!p.isPrivate, postsCount: Number(p.postsCount) || 0,
    };
  } catch (e) { return { ok: false, error: 'erro_validacao', message: (e && e.message) || 'erro' }; }
}

// CPF válido gerado automaticamente (a PagHiper exige um CPF no pagador). Mesma ideia do checkout do site.
function generateValidCPF() {
  const rnd = (n) => Math.floor(Math.random() * n);
  const n = Array.from({ length: 9 }, () => rnd(10));
  const calc = (arr) => {
    let f = arr.length + 1, sum = 0;
    for (const d of arr) sum += d * f--;
    const r = (sum * 10) % 11;
    return r === 10 ? 0 : r;
  };
  const d1 = calc(n); const d2 = calc([...n, d1]);
  return [...n, d1, d2].join('');
}

// Cria o pedido + Pix chamando o MESMO endpoint do site (/api/paghiper/charge).
// A fulfillment (webhook do Pix pago) despacha o serviço igual a um pedido do site.
async function createPixOrder({ servico, quantidade, tipo, usuario, nome, email, phone, post_links }) {
  // Seguidores/curtidas: o PREÇO depende do tipo. Nunca feche sem tipo explícito —
  // senão cai no default "mistos" e cobra/entrega o produto errado (ex.: "brasileiros
  // reais" R$49,90 viram "brasileiros" R$24,90). Força a IA a especificar o tipo.
  const _svcDef = CATALOG[normalizeServico(servico)];
  if (_svcDef && _svcDef.tipos && !String(tipo || '').trim()) {
    return { ok: false, error: 'tipo_obrigatorio', message: 'Antes de gerar o Pix, confirme o tipo: mistos, brasileiros ou brasileiros reais (orgânicos).' };
  }
  const cot = await quote({ servico, tipo, quantidade });
  if (!cot.ok) return { ok: false, error: cot.error || 'cotacao_falhou', available: cot.available };
  const username = parseIgUsername(usuario);
  if (!username) return { ok: false, error: 'usuario_invalido' };
  if (cot.needsPost) {
    const links = Array.isArray(post_links) ? post_links.filter(Boolean) : String(post_links || '').split(',').map((s) => s.trim()).filter(Boolean);
    if (!links.length) return { ok: false, error: 'faltou_post', message: 'Esse serviço precisa do link do post.' };
    post_links = links;
  }
  const cpf = generateValidCPF();
  const correlationID = 'WppAgent_' + Date.now() + '_' + Math.random().toString(36).slice(2, 9);
  const additionalInfo = [
    { key: 'tipo_servico', value: cot.tipo },
    { key: 'categoria_servico', value: cot.categoria },
    { key: 'quantidade', value: String(cot.quantidade) },
    { key: 'pacote', value: `${cot.quantidade} ${cot.unit} - ${cot.priceLabel}` },
    { key: 'instagram_username', value: username },
    { key: 'phone', value: String(phone || '') },
    { key: 'customer_name', value: String(nome || '') },
    { key: 'payment_method', value: 'pix' },
    { key: 'source', value: 'wpp_agent' },
  ];
  if (cot.needsPost && post_links && post_links.length) {
    additionalInfo.push({ key: 'post_link', value: post_links[0] });
    additionalInfo.push({ key: 'post_links', value: post_links.join(',') });
  }
  const body = {
    correlationID,
    value: cot.priceCents,
    comment: 'Pedido via IA WhatsApp',
    customer: { name: String(nome || 'Cliente').trim() || 'Cliente', email: String(email || '').trim(), phone: String(phone || '').trim(), cpf },
    additionalInfo,
    profile_is_private: false,
    source: 'wpp_agent',
  };
  try {
    const resp = await axios.post(INTERNAL_BASE + '/api/paghiper/charge', body, { timeout: 45000, validateStatus: () => true });
    const d = resp && resp.data;
    if (!d || resp.status < 200 || resp.status >= 300 || d.error) {
      return { ok: false, error: 'charge_failed', message: String((d && (d.message || d.error)) || ('HTTP ' + (resp && resp.status))).slice(0, 200) };
    }
    // Extrai o copia-e-cola do Pix. O /api/paghiper/charge retorna o EMV em
    // charge.brCode (e charge.paymentMethods.pix.brCode) — os campos antigos
    // (pix_code/emv/...) NUNCA batiam, então o código vinha vazio e não era enviado.
    const ch = (d && d.charge) ? d.charge : {};
    const pixPix = (ch.paymentMethods && ch.paymentMethods.pix) ? ch.paymentMethods.pix : {};
    const pixCopia = String(
      ch.brCode || pixPix.brCode || d.brCode
      || d.pix_code || d.emv || d.qrcode || d.pixCopiaECola
      || (d.pix && (d.pix.emv || d.pix.qrcode)) || (d.paghiper && d.paghiper.pix_code) || ''
    ).trim();
    const qrImg = String(ch.qrCodeImage || pixPix.qrCodeImage || d.qrCodeImage || '').trim();
    const ident = String(ch.identifier || ch.id || d.identifier || d.transaction_id || d.id || '').trim();
    return {
      ok: true, correlationID, valorCents: cot.priceCents, valorLabel: cot.priceLabel,
      pixCopiaECola: pixCopia, qrCodeImage: qrImg, identifier: ident,
      resumo: `${cot.quantidade} ${cot.unit}${cot.tipo ? ' (' + cot.tipo + ')' : ''} para @${username} — ${cot.priceLabel}`,
    };
  } catch (e) { return { ok: false, error: 'charge_error', message: (e && e.message) || 'erro' }; }
}

// Getter que varre as várias formas de additionalInfo do pedido.
function orderGetAny(o, key) {
  for (const m of [o.additionalInfoMapPaid, o.additionalInfoMap]) { if (m && m[key] != null && m[key] !== '') return m[key]; }
  for (const a of [o.additionalInfoPaid, o.additionalInfo]) { if (Array.isArray(a)) { const it = a.find((x) => x && x.key === key); if (it && it.value != null && it.value !== '') return it.value; } }
  return '';
}
function _digits(v) { let d = String(v || '').replace(/\D/g, ''); if (d.length > 11 && d.startsWith('55')) d = d.slice(2); return d; }
function _orderPaidMs(o) { for (const c of [o.paidAt, o.woovi && o.woovi.paidAt, o.paghiper && o.paghiper.paidAt, o.createdAt]) { const t = c ? new Date(c).getTime() : NaN; if (Number.isFinite(t)) return t; } return 0; }

// Consulta o status AO VIVO de UMA ordem no fornecedor (action=status). Retorna
// { status, remains, startCount } ou null. Providers: fama24h / fornecedor_social / topfama.
async function fetchProviderOrderStatus(provider, orderId) {
  const M = {
    fama24h: { url: 'https://fama24h.net/api/v2', key: process.env.FAMA24H_API_KEY },
    fornecedor_social: { url: 'https://fornecedorsocial.com/api/v2', key: process.env.FORNECEDOR_SOCIAL_API_KEY },
    topfama: { url: 'https://topfama.com/api/v2', key: process.env.TOPFAMA_API_KEY },
  };
  const cfg = M[provider];
  if (!cfg || !cfg.key || !orderId) return null;
  try {
    const payload = new URLSearchParams({ key: String(cfg.key), action: 'status', order: String(orderId) });
    const resp = await axios.post(cfg.url, payload.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 15000, validateStatus: () => true });
    const d = (resp && resp.data && typeof resp.data === 'object') ? resp.data : {};
    const num = (v) => { const n = Number(String(v == null ? '' : v).replace(',', '.').replace(/[^\d.-]/g, '')); return Number.isFinite(n) ? n : null; };
    const status = String(d.status || d.Status || '').trim();
    if (!status && (d.error || d.Error)) return { error: String(d.error || d.Error) };
    return { status, remains: num(d.remains ?? d.Remains), startCount: num(d.start_count ?? d.startCount) };
  } catch (_) { return null; }
}

// Descobre o provedor + orderId da ordem-BASE do pedido (single ou multi).
function baseProviderOrder(o) {
  const singles = [['fornecedor_social', o.fornecedor_social], ['fama24h', o.fama24h], ['topfama', o.topfama]];
  for (const [prov, sub] of singles) { if (sub && sub.orderId && /^[0-9]+$/.test(String(sub.orderId))) return { provider: prov, orderId: String(sub.orderId) }; }
  const multis = [['fornecedor_social', o.fornecedor_social_multi], ['fama24h', o.fama24h_multi]];
  for (const [prov, m] of multis) { if (m && Array.isArray(m.orders)) { const it = m.orders.find((x) => x && (x.orderId || x.id) && /^[0-9]+$/.test(String(x.orderId || x.id))); if (it) return { provider: prov, orderId: String(it.orderId || it.id) }; } }
  return null;
}

// Consulta o pedido de um cliente por TELEFONE (o número que contatou) ou por @.
// Retorna diagnóstico p/ a IA explicar "paguei e não recebi": tipo do serviço,
// tempo desde o pagamento, se é orgânico (prazo 48h), status no fornecedor e se o
// perfil está PRIVADO (bloqueia entrega — checado ao vivo via RocketAPI).
async function consultarPedido({ telefone, usuario } = {}) {
  const col = await getCollection('checkout_orders');
  const paidOr = [{ status: 'pago' }, { 'woovi.paidAt': { $exists: true, $nin: [null, ''] } }, { 'paghiper.paidAt': { $exists: true, $nin: [null, ''] } }, { paidAt: { $exists: true, $nin: [null, ''] } }];
  const uname = parseIgUsername(usuario);
  const last8 = _digits(telefone).slice(-8);
  let match = null;
  if (uname) {
    const esc = uname.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const rx = new RegExp('^' + esc + '$', 'i');
    match = { $or: [{ instagramUsername: rx }, { instauser: rx }, { 'additionalInfoMap.instagram_username': rx }, { 'additionalInfoMapPaid.instagram_username': rx }] };
  } else if (last8.length >= 8) {
    const rx = new RegExp(last8 + '$');
    match = { $or: [{ 'customer.phone': { $regex: rx } }, { 'customer.telefone': { $regex: rx } }, { 'additionalInfoMap.phone': { $regex: rx } }, { additionalInfo: { $elemMatch: { key: 'phone', value: { $regex: rx } } } }, { additionalInfoPaid: { $elemMatch: { key: 'phone', value: { $regex: rx } } } }] };
  } else {
    return { ok: true, encontrado: false, motivo: 'sem_identificador' };
  }
  let orders = [];
  try { orders = await col.find({ $and: [{ $or: paidOr }, match] }).limit(20).toArray(); } catch (_) { orders = []; }
  if (!orders.length) return { ok: true, encontrado: false, buscaPor: uname ? 'usuario' : 'telefone' };
  orders.sort((a, b) => _orderPaidMs(b) - _orderPaidMs(a));
  const o = orders[0];

  const categoria = String(orderGetAny(o, 'categoria_servico') || '').toLowerCase().trim();
  const tipo = String(orderGetAny(o, 'tipo_servico') || o.tipoServico || o.tipo || '').toLowerCase().trim();
  const quantidade = Number(orderGetAny(o, 'quantidade') || o.quantidade || o.qtd || 0) || 0;
  const usernameOrder = String(o.instagramUsername || o.instauser || orderGetAny(o, 'instagram_username') || uname || '').replace(/^@+/, '');
  const paidMs = _orderPaidMs(o);
  const horasDesdePagamento = paidMs ? Math.round((Date.now() - paidMs) / 3600000) : null;
  const ehBrasileirosReais = /organic|orgânic|real|reais/.test(tipo);

  // Status no fornecedor: consulta AO VIVO via action=status usando o orderId da base.
  // Se a consulta ao vivo falhar, cai no status salvo no pedido.
  let provStatusRaw = '';
  let remains = null, entregues = null;
  const bp = baseProviderOrder(o);
  if (bp) {
    const live = await fetchProviderOrderStatus(bp.provider, bp.orderId);
    if (live && !live.error && live.status) {
      provStatusRaw = live.status;
      if (live.remains != null) { remains = Math.max(0, Math.round(live.remains)); if (quantidade > 0) entregues = Math.max(0, quantidade - remains); }
    }
  }
  if (!provStatusRaw) {
    const cands = [o.fornecedor_social, o.fama24h, o.topfama, o.fornecedor_social_multi, o.fama24h_multi];
    for (const p of cands) { if (p && (p.status || p.orderId || (Array.isArray(p.orders) && p.orders.length))) { provStatusRaw = String(p.status || 'processando'); break; } }
    if (!provStatusRaw) provStatusRaw = 'desconhecido';
  }
  const statusMap = { completed: 'concluído', concluido: 'concluído', complete: 'concluído', created: 'em processamento', pending: 'em processamento', processing: 'em processamento', in_progress: 'em processamento', 'in progress': 'em processamento', 'em andamento': 'em processamento', partial: 'entrega parcial', canceled: 'cancelado', cancelled: 'cancelado', refunded: 'reembolsado', error: 'erro no fornecedor' };
  const statusFornecedor = statusMap[String(provStatusRaw).toLowerCase().trim()] || String(provStatusRaw);

  // Perfil PRIVADO? checa ao vivo (perfil privado bloqueia a entrega).
  let perfilPrivado = null;
  try { const vp = await validateProfile(usernameOrder); if (vp.ok) perfilPrivado = !!vp.isPrivate; } catch (_) {}

  // O cliente só pode ver "em andamento" ou "concluído". Qualquer outro status real
  // (entrega parcial, erro, cancelado, desconhecido) é tratado INTERNAMENTE (reposição
  // com novos pedidos) → para o cliente vira "em andamento". Não expõe o status cru
  // nem quanto falta/entrou (evita revelar entrega parcial).
  const statusCliente = (statusFornecedor === 'concluído') ? 'concluído' : 'em andamento';

  // Link de REPOSIÇÃO (refil) do cliente — token = refilLinkId do pedido.
  // NÃO devolve p/ brasileiros reais (orgânicos): esse tipo é estável e o fluxo é
  // "verificar + informar estabilidade", NÃO mandar link (o cliente nem vê o link então).
  const refilId = String(o.refilLinkId || (orders.find((x) => x && x.refilLinkId) || {}).refilLinkId || '').trim();
  const refilLink = (!ehBrasileirosReais && refilId) ? ('https://agenciaoppus.site/refil?token=' + encodeURIComponent(refilId)) : null;

  return {
    ok: true, encontrado: true, usuario: usernameOrder,
    servico: categoria || 'seguidores', tipo: tipo || '(não informado)', quantidade,
    horasDesdePagamento, dentroPrazo48h: horasDesdePagamento != null ? (horasDesdePagamento <= 48) : null,
    ehBrasileirosReais, perfilPrivado, status: statusCliente, refilLink,
  };
}

// Envia uma notificação push via ntfy (ntfy.sh ou self-hosted). Config por env:
//   NTFY_URL (url completa do tópico) OU NTFY_TOPIC (+ NTFY_SERVER, padrão ntfy.sh).
//   NTFY_TOKEN opcional (ntfy autenticado). Não lança; retorna { ok }.
async function sendNtfy({ title, message, priority, tags, click } = {}) {
  try {
    const explicit = String(process.env.NTFY_URL || '').trim();
    const server = String(process.env.NTFY_SERVER || 'https://ntfy.sh').replace(/\/+$/, '');
    const topic = String(process.env.NTFY_TOPIC || '').trim();
    const url = explicit || (topic ? (server + '/' + topic) : '');
    if (!url) return { ok: false, error: 'ntfy_nao_configurado' };
    const headers = { 'Content-Type': 'text/plain; charset=utf-8' };
    if (title) headers['Title'] = String(title);           // ASCII (sem acento) p/ compatibilidade
    if (priority) headers['Priority'] = String(priority);   // 1..5 ou min/low/default/high/urgent
    if (tags) headers['Tags'] = Array.isArray(tags) ? tags.join(',') : String(tags);
    if (click) headers['Click'] = String(click);
    const token = String(process.env.NTFY_TOKEN || '').trim();
    if (token) headers['Authorization'] = 'Bearer ' + token;
    const resp = await axios.post(url, String(message || ''), { headers, timeout: 12000, validateStatus: () => true });
    return { ok: resp.status >= 200 && resp.status < 300, status: resp.status };
  } catch (e) { return { ok: false, error: (e && e.message) || 'erro' }; }
}

// Aciona o suporte humano: PAUSA a IA nessa conversa (botPaused) + marca a flag no
// whatsapp_contacts (aparece no CRM) + notifica o atendente via ntfy no celular.
async function flagSupport(phone, motivo) {
  try {
    const col = await getCollection('whatsapp_contacts');
    await col.updateOne(
      { _id: String(phone) },
      { $set: { flag: 'suporte', botPaused: true, supportReason: String(motivo || '').slice(0, 300), supportAt: new Date().toISOString() } },
      { upsert: true }
    );
    // Notificação push (ntfy) — não bloqueia o fluxo se falhar/não configurado.
    // Formato compacto: "+5511... - suporte humanizado" (título) + resumo curto (corpo).
    try {
      const base = String(process.env.PUBLIC_BASE_URL || process.env.INTERNAL_BASE || '').replace(/\/+$/, '');
      const phoneFmt = '+' + String(phone).replace(/\D/g, '');
      const resumo = String(motivo || '').replace(/\s+/g, ' ').trim().slice(0, 140) || 'pediu atendimento humano';
      await sendNtfy({
        title: phoneFmt + ' - suporte humanizado',
        message: resumo,
        priority: 'high',
        tags: 'rotating_light',
        click: base ? (base + '/painel/ia-crm') : undefined,
      });
    } catch (_) {}
    return { ok: true };
  } catch (e) { return { ok: false, error: (e && e.message) || 'erro' }; }
}

module.exports = {
  CATALOG, normalizeServico, normalizeTipo, parseIgUsername,
  quote, priceTable, validateProfile, createPixOrder, generateValidCPF, flagSupport, consultarPedido, brl, sendNtfy,
};
