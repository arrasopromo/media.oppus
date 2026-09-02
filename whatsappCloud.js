// whatsappCloud.js — Infra da IA de vendas: webhook Meta Cloud API SEPARADO (número/token
// próprios, isolado do WhatsApp atual do projeto), debounce, envio de texto/mídia, checagem
// de pausa. Registra as rotas via registerWhatsappIa(app).
'use strict';

const { getCollection } = require('./mongodbClient');
const agent = require('./whatsappAgent.js');
let axios = require('axios'); if (axios && axios.default) axios = axios.default;

const CFG = () => ({
  phoneId: String(process.env.WHATSAPP_IA_PHONE_NUMBER_ID || '').trim(),
  token: String(process.env.WHATSAPP_IA_TOKEN || '').trim(),
  verifyToken: String(process.env.WHATSAPP_IA_VERIFY_TOKEN || '').trim(),
  version: String(process.env.WHATSAPP_IA_API_VERSION || process.env.WHATSAPP_API_VERSION || 'v21.0').trim(),
  webhookPath: String(process.env.WHATSAPP_IA_WEBHOOK_PATH || '/api/whatsapp-ia/webhook').trim(),
  autoReply: String(process.env.WHATSAPP_AUTO_REPLY || 'true').toLowerCase() !== 'false',
  debounceMs: Math.max(0, Number(process.env.WHATSAPP_DEBOUNCE_MS || 8000) || 8000),
});
function configured() { const c = CFG(); return !!(c.phoneId && c.token); }

// ── Log de mensagens da IA (inbox/CRM). Coleção wa_ia_messages. Best-effort. ──
let __iaMsgIndexed = false;
async function logIaMessage(d) {
  try {
    const phone = String((d && d.phone) || '').trim();
    const phoneKey = phone.replace(/\D/g, '');
    if (!phoneKey) return;
    const col = await getCollection('wa_ia_messages');
    if (!__iaMsgIndexed) { __iaMsgIndexed = true; try { await col.createIndex({ phone: 1, createdAt: 1 }); await col.createIndex({ createdAt: -1 }); await col.createIndex({ wamid: 1 }, { sparse: true }); } catch (_) {} }
    const wamid = String((d && d.wamid) || '').trim();
    const doc = {
      phone, phoneKey,
      direction: (d.direction === 'out') ? 'out' : 'in',
      type: String(d.type || 'text').slice(0, 20),
      text: String(d.text || '').slice(0, 4000),
      mediaId: String(d.mediaId || '').slice(0, 200),
      mime: String(d.mime || '').slice(0, 100),
      filename: String(d.filename || '').slice(0, 200),
      wamid, name: String(d.name || '').slice(0, 120),
      agent: !!d.agent, // true = enviado por atendente humano no CRM; false = bot
      createdAt: new Date(),
    };
    if (wamid) { try { const r = await col.updateOne({ wamid }, { $setOnInsert: doc }, { upsert: true }); if (!r.upsertedCount && !r.modifiedCount && r.matchedCount) { /* já existia */ } } catch (_) { try { await col.insertOne(doc); } catch (_) {} } }
    else { await col.insertOne(doc); }
    // Atualiza o contato (nome + last message) p/ a lista de conversas do CRM.
    try {
      const cc = await getCollection('whatsapp_contacts');
      const set = { lastMessageAt: new Date(), lastMessageText: doc.text.slice(0, 140), lastMessageDir: doc.direction };
      if (doc.name) set.name = doc.name;
      if (doc.direction === 'in') set.unread = true;
      await cc.updateOne({ _id: phone }, { $set: set, $setOnInsert: { _id: phone, createdAt: new Date() } }, { upsert: true });
    } catch (_) {}
  } catch (_) {}
}

async function sendWhatsAppText(to, text, opts = {}) {
  const c = CFG();
  if (!c.phoneId || !c.token) return { ok: false, error: 'not_configured' };
  const url = `https://graph.facebook.com/${c.version}/${c.phoneId}/messages`;
  const payload = { messaging_product: 'whatsapp', to: String(to), type: 'text', text: { preview_url: false, body: String(text || '').slice(0, 4096) } };
  try {
    const resp = await axios.post(url, payload, { headers: { Authorization: 'Bearer ' + c.token, 'Content-Type': 'application/json' }, timeout: 20000, validateStatus: () => true });
    const ok = resp.status >= 200 && resp.status < 300;
    if (ok && opts.log !== false) {
      const wamid = (resp.data && Array.isArray(resp.data.messages) && resp.data.messages[0]) ? resp.data.messages[0].id : '';
      logIaMessage({ phone: String(to), direction: 'out', type: 'text', text, wamid, agent: !!opts.agent }).catch(() => {});
    }
    return { ok, status: resp.status, data: resp.data };
  } catch (e) { return { ok: false, error: (e && e.message) || 'erro' }; }
}

// kind: image|video|document. source: URL pública ou media_id.
async function sendWhatsAppMedia(to, kind, source, caption) {
  const c = CFG();
  if (!c.phoneId || !c.token) return { ok: false, error: 'not_configured' };
  const url = `https://graph.facebook.com/${c.version}/${c.phoneId}/messages`;
  const isId = source && !/^https?:\/\//i.test(String(source));
  const media = isId ? { id: String(source) } : { link: String(source) };
  if (caption && (kind === 'image' || kind === 'video' || kind === 'document')) media.caption = String(caption).slice(0, 1024);
  const payload = { messaging_product: 'whatsapp', to: String(to), type: kind, [kind]: media };
  try {
    const resp = await axios.post(url, payload, { headers: { Authorization: 'Bearer ' + c.token, 'Content-Type': 'application/json' }, timeout: 30000, validateStatus: () => true });
    return { ok: resp.status >= 200 && resp.status < 300, status: resp.status, data: resp.data };
  } catch (e) { return { ok: false, error: (e && e.message) || 'erro' }; }
}

// ── Menu de boas-vindas (gatilho) + botões interativos ──
const MENU_BUTTONS = [
  { id: 'ja_comprei', title: 'Acabei de comprar' },
  { id: 'quero_comprar', title: 'Quero comprar' },
  { id: 'preciso_suporte', title: 'Preciso de suporte' },
];
const WELCOME_TEXT = 'Olá, tudo bem? 👋\nSeja bem-vindo ao atendimento da *Agência Oppus!*\n\nAqui é o contato oficial para: suporte, novas compras e dúvidas.\n\nMe diz, o que você procura hoje 👇';
const INSTRUCAO_JA_COMPREI = 'Perfeito! 🎉 Acompanhar seu pedido é rapidinho:\n\n1️⃣ Acesse a *Área do Cliente*: https://agenciaoppus.site/cliente\n2️⃣ Informe o *e-mail* que você usou na hora da compra\n3️⃣ No primeiro acesso, *crie uma senha* (nas próximas vezes é só entrar com ela)\n\nProntinho! Lá dentro você vê:\n✅ O *status* e o andamento da entrega\n✅ O *detalhamento* do pedido (serviço, quantidade e perfil)\n✅ Seu *histórico* de compras\n\nSe tiver qualquer dificuldade pra acessar ou dúvida sobre a entrega, é só me chamar aqui que eu te ajudo. 😉';
const PERGUNTA_SUPORTE = 'Claro! Me conta qual é a sua dúvida ou o que aconteceu, que eu te ajudo por aqui.';

// ── Gatilhos EXATOS (as mensagens pré-preenchidas do site/anúncios) ──
//   Comparação exata (só normaliza espaços e caixa) pra não disparar por engano.
const _normTrig = (t) => String(t || '').trim().replace(/\s+/g, ' ').toLowerCase();
const TRIGGER_MENU = _normTrig('Olá! Estou no site e preciso de ajuda.');
const TRIGGERS_SALES = [
  _normTrig('Olá! Tenho interesse e queria mais informações, por favor.'),
  _normTrig('Olá! Posso ter mais informações sobre isso?'),
];
// Gatilho do MENU de boas-vindas (com botões).
function isTriggerMessage(text) { return _normTrig(text) === TRIGGER_MENU; }
// Gatilhos que iniciam direto o processo de VENDA.
function isSalesTrigger(text) { return TRIGGERS_SALES.includes(_normTrig(text)); }

// Envia UMA mensagem interativa com até 3 botões de resposta rápida.
async function sendWhatsAppButtons(to, bodyText, buttons) {
  const c = CFG();
  if (!c.phoneId || !c.token) return { ok: false, error: 'not_configured' };
  const url = `https://graph.facebook.com/${c.version}/${c.phoneId}/messages`;
  const payload = {
    messaging_product: 'whatsapp', to: String(to), type: 'interactive',
    interactive: {
      type: 'button',
      body: { text: String(bodyText || '').slice(0, 1024) },
      action: { buttons: (buttons || []).slice(0, 3).map((b) => ({ type: 'reply', reply: { id: String(b.id).slice(0, 256), title: String(b.title).slice(0, 20) } })) },
    },
  };
  try {
    const resp = await axios.post(url, payload, { headers: { Authorization: 'Bearer ' + c.token, 'Content-Type': 'application/json' }, timeout: 20000, validateStatus: () => true });
    const ok = resp.status >= 200 && resp.status < 300;
    if (ok) { const wamid = (resp.data && Array.isArray(resp.data.messages) && resp.data.messages[0]) ? resp.data.messages[0].id : ''; logIaMessage({ phone: String(to), direction: 'out', type: 'button', text: bodyText, wamid, agent: false }).catch(() => {}); }
    return { ok, status: resp.status, data: resp.data };
  } catch (e) { return { ok: false, error: (e && e.message) || 'erro' }; }
}

async function isBotPaused(phone) {
  try { const col = await getCollection('whatsapp_contacts'); const d = await col.findOne({ _id: String(phone) }, { projection: { botPaused: 1 } }); return !!(d && d.botPaused); } catch (_) { return false; }
}

// ── Debounce: junta mensagens picadas do mesmo telefone antes de responder ──
const buffers = new Map(); // phone -> { parts:[], timer }
function scheduleFlush(phone) {
  const c = CFG();
  const b = buffers.get(phone); if (!b) return;
  if (b.timer) clearTimeout(b.timer);
  b.timer = setTimeout(() => { flush(phone).catch(() => {}); }, c.debounceMs);
}
async function flush(phone) {
  const b = buffers.get(phone); if (!b) return;
  buffers.delete(phone);
  const text = b.parts.join('\n').trim();
  if (!text) return;
  await runAgent(phone, text);
}

// ── "Cutucão" de confirmação do @: se a IA pediu pra confirmar o perfil e o cliente
// sumir por ~2min, manda um retorno. Cancelado assim que o cliente responde. ──
const CONFIRM_NUDGE_MS = Math.max(30000, Number(process.env.WHATSAPP_IA_CONFIRM_NUDGE_MS || 120000) || 120000);
const confirmNudges = new Map(); // phone -> timeout
function cancelNudge(phone) {
  const p = String(phone || '');
  const t = confirmNudges.get(p);
  if (t) { try { clearTimeout(t); } catch (_) {} confirmNudges.delete(p); try { console.log('🔕 [cutucão] cancelado (cliente respondeu) ' + p); } catch (_) {} }
}
function buildConfirmNudgeText(username) {
  const u = username ? ('*@' + String(username).replace(/^@+/, '') + '*') : 'seu perfil';
  return 'Oi, ainda por aí? Me confirma se ' + u + ' está certinho que eu já sigo com o seu pedido.';
}
function scheduleConfirmNudge(phone, username) {
  const p = String(phone || '');
  cancelNudge(p);
  const txt = buildConfirmNudgeText(username);
  try { console.log('🔔 [cutucão] agendado ' + p + ' em ' + Math.round(CONFIRM_NUDGE_MS / 1000) + 's (@' + String(username || '?') + ')'); } catch (_) {}
  const t = setTimeout(async () => {
    confirmNudges.delete(p);
    try { if (await isBotPaused(p)) { console.log('🔕 [cutucão] pulado (IA pausada) ' + p); return; } } catch (_) {} // humano assumiu → não cutuca
    try { await sendWhatsAppText(p, txt); console.log('🔔 [cutucão] DISPARADO ' + p); } catch (e) { try { console.error('🔔 [cutucão] falha ao enviar ' + p + ':', e && e.message); } catch (_) {} }
  }, CONFIRM_NUDGE_MS);
  try { if (t && t.unref) t.unref(); } catch (_) {}
  confirmNudges.set(p, t);
}

// Chama o agente e, se ele acabou pedindo a confirmação do @, agenda o cutucão.
async function runAgent(phone, text) {
  let res = null;
  try { res = await agent.handleAgentMessage({ phone, text }, sendWhatsAppText); }
  catch (e) { try { console.error('❌ IA erro:', e && e.message); } catch (_) {} }
  try { console.log('🤖 [IA] turno ' + phone + ' → awaitingConfirm=' + !!(res && res.awaitingConfirm)); } catch (_) {}
  try { if (res && res.awaitingConfirm) scheduleConfirmNudge(phone, res.username); } catch (_) {}
  return res;
}

// dedup de wamids reentregues pela Meta
const seenWamids = new Set();

async function handleInboundMessage(m, contactName) {
  const c = CFG();
  const from = String(m && m.from || '').trim();
  if (!from) return;
  const wamid = String(m && m.id || '');
  if (wamid) { if (seenWamids.has(wamid)) return; seenWamids.add(wamid); if (seenWamids.size > 5000) seenWamids.clear(); }

  // Cliente respondeu (qualquer tipo de mensagem) → cancela o cutucão pendente.
  cancelNudge(from);

  // Texto normal OU clique de botão interativo (button_reply / list_reply).
  const bodyRaw = (m.type === 'text' && m.text && m.text.body) ? String(m.text.body).trim() : '';
  const btn = (m.type === 'interactive' && m.interactive) ? (m.interactive.button_reply || m.interactive.list_reply) : null;
  const btnId = btn ? String(btn.id || '') : '';
  const btnTitle = btn ? String(btn.title || '') : '';
  const lc = bodyRaw.toLowerCase();

  // MÍDIA recebida (imagem/áudio/vídeo/documento/figurinha): registra no inbox e NÃO
  // passa pro agente (que é texto-only) — o atendente vê e responde pelo CRM.
  const MEDIA_TYPES = ['image', 'audio', 'voice', 'video', 'document', 'sticker'];
  if (MEDIA_TYPES.includes(m.type)) {
    const md = m[m.type] || {};
    const kind = (m.type === 'voice') ? 'audio' : m.type;
    const label = kind === 'image' ? '[imagem]' : kind === 'audio' ? '[áudio]' : kind === 'video' ? '[vídeo]' : kind === 'document' ? ('[documento] ' + (md.filename || '')) : kind === 'sticker' ? '[figurinha]' : '[mídia]';
    logIaMessage({ phone: from, direction: 'in', type: kind, text: (md.caption ? (label + ' ' + md.caption) : label), mediaId: String(md.id || ''), mime: String(md.mime_type || ''), filename: String(md.filename || ''), wamid: String(m.id || ''), name: contactName || '' }).catch(() => {});
    return;
  }

  // Registra a mensagem RECEBIDA no inbox (mesmo se o bot estiver pausado / for comando).
  const inboundText = bodyRaw || (btnTitle ? ('🔘 ' + btnTitle) : '');
  if (inboundText && !/^\/(pause|pausar|start|iniciar|começar|comecar|resume|retomar)$/.test(lc)) {
    logIaMessage({ phone: from, direction: 'in', type: btn ? 'button' : (m.type || 'text'), text: inboundText, wamid: String(m.id || ''), name: contactName || '' }).catch(() => {});
  }

  // Comandos de controle (texto): /pause e /start (para operar o número da IA).
  if (/^\/(pause|pausar)$/.test(lc)) { try { const col = await getCollection('whatsapp_contacts'); await col.updateOne({ _id: from }, { $set: { botPaused: true } }, { upsert: true }); } catch (_) {} return; }
  if (/^\/(start|iniciar|começar|comecar|resume|retomar)$/.test(lc)) { try { const col = await getCollection('whatsapp_contacts'); await col.updateOne({ _id: from }, { $set: { botPaused: false } }, { upsert: true }); } catch (_) {} try { await agent.clearHistory(from); } catch (_) {} return; }

  if (!c.autoReply) return;
  if (await isBotPaused(from)) return; // humano assumiu

  // ── Clique num botão do menu ──
  if (btnId) {
    if (btnId === 'ja_comprei') { await sendWhatsAppText(from, INSTRUCAO_JA_COMPREI); return; }
    if (btnId === 'preciso_suporte') { await sendWhatsAppText(from, PERGUNTA_SUPORTE); return; }
    if (btnId === 'quero_comprar') { await runAgent(from, 'Quero comprar'); return; }
    if (btnTitle) { await runAgent(from, btnTitle); return; }
    return;
  }

  // ── Gatilho do MENU (frase exata) → boas-vindas com botões ──
  if (bodyRaw && isTriggerMessage(bodyRaw)) { await sendWhatsAppButtons(from, WELCOME_TEXT, MENU_BUTTONS); return; }
  // ── Gatilhos de VENDA (frases exatas) → inicia a venda na hora (sem debounce) ──
  if (bodyRaw && isSalesTrigger(bodyRaw)) { await runAgent(from, bodyRaw); return; }

  // Só texto entra no funil de venda (mídia/áudio: ignora por ora).
  if (m.type !== 'text' || !bodyRaw) return;

  let b = buffers.get(from);
  if (!b) { b = { parts: [], timer: null }; buffers.set(from, b); }
  b.parts.push(bodyRaw);
  if (c.debounceMs <= 0) { await flush(from); } else { scheduleFlush(from); }
}

function registerWhatsappIa(app) {
  const c = CFG();
  const path = c.webhookPath || '/api/whatsapp-ia/webhook';

  // Verificação do webhook (Meta chama com hub.challenge)
  app.get(path, (req, res) => {
    const mode = req.query['hub.mode'];
    const token = req.query['hub.verify_token'];
    const challenge = req.query['hub.challenge'];
    const cfg = CFG();
    if (mode === 'subscribe' && token && cfg.verifyToken && token === cfg.verifyToken) return res.status(200).send(challenge);
    return res.sendStatus(403);
  });

  // Recebe mensagens
  app.post(path, async (req, res) => {
    res.sendStatus(200); // responde rápido; processa async
    try {
      const body = req.body || {};
      if (body.object !== 'whatsapp_business_account') return;
      for (const entry of (body.entry || [])) {
        for (const change of (entry.changes || [])) {
          const value = change.value || {};
          const contacts = value.contacts || [];
          const nameByWa = {};
          for (const ct of contacts) { if (ct && ct.wa_id) nameByWa[String(ct.wa_id)] = (ct.profile && ct.profile.name) || ''; }
          for (const msg of (value.messages || [])) {
            handleInboundMessage(msg, nameByWa[String(msg.from)] || '').catch(() => {});
          }
        }
      }
    } catch (e) { try { console.error('❌ IA webhook erro:', e && e.message); } catch (_) {} }
  });

  try { console.log(`🤖 IA WhatsApp: webhook em ${path} | ${configured() ? 'configurado' : 'SEM credenciais (WHATSAPP_IA_*)'} | OpenAI ${agent.isEnabled() ? 'on' : 'off (fallback regras)'}`); } catch (_) {}
}

module.exports = { registerWhatsappIa, sendWhatsAppText, sendWhatsAppMedia, sendWhatsAppButtons, isBotPaused, handleInboundMessage, configured, isTriggerMessage, isSalesTrigger, buildConfirmNudgeText, CONFIRM_NUDGE_MS, MENU_BUTTONS, WELCOME_TEXT, logIaMessage, getIaConfig: CFG };
