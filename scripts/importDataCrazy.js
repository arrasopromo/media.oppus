// scripts/importDataCrazy.js
// Importa o HISTÓRICO de conversas da DataCrazy direto da API para o CRM IA
// WhatsApp (wa_ia_messages + whatsapp_contacts).
//
// REGRA: conversa importada é ANTIGA -> nasce com a IA PAUSADA (botPaused:true)
// e marcada legacy:true, que o painel mostra com a tag laranja "antiga".
// A IA só volta a responder nela se você ligar manualmente no /painel/ia-crm.
// Números NOVOS (que nunca falaram) continuam com a IA ativa normalmente.
//
// USO:
//   node scripts/importDataCrazy.js                          (dry-run, mostra o que faria)
//   node scripts/importDataCrazy.js --limit 5 --apply         (testa com 5 conversas)
//   node scripts/importDataCrazy.js --apply                   (importa tudo)
//   node scripts/importDataCrazy.js --since 2026-07-01 --apply
//
// Retomável: cada mensagem usa o id da DataCrazy como wamid, então rodar de
// novo não duplica e pula o que já entrou. Respeita o rate limit da API.
//
// Requer DC_TOKEN no .env (o token NÃO é impresso em lugar nenhum).
'use strict';

require('dotenv').config();
const { getCollection } = require('../mongodbClient');

const BASE = 'https://api.g1.datacrazy.io/api/v1';
const TOKEN = String(process.env.DC_TOKEN || '').trim();
const arg = (n, d) => { const i = process.argv.indexOf('--' + n); return (i !== -1 && process.argv[i + 1]) ? process.argv[i + 1] : d; };
const APPLY = process.argv.includes('--apply');
const SINCE = arg('since', '2026-07-01');
// Número da IA no WhatsApp Cloud (instância "Oppus" na DataCrazy).
const PHONE_NUMBER_ID = arg('phone-number-id', process.env.WHATSAPP_IA_PHONE_NUMBER_ID || '984467304742989');
const LIMIT = Number(arg('limit', '0')) || 0;
// --refresh: reconsulta conversas já importadas, para capturar mensagens NOVAS de
// threads existentes. A dedupe por wamid garante que nada duplica.
const REFRESH = process.argv.includes('--refresh');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Espera o reset quando a cota acaba (a API devolve x-ratelimit-*).
let rateRemaining = null, rateResetAt = 0;
async function dc(path, tries = 0) {
  if (rateRemaining !== null && rateRemaining <= 1) {
    const wait = Math.max(0, rateResetAt - Date.now()) + 500;
    if (wait > 0) await sleep(wait);
    rateRemaining = null;
  }
  const res = await fetch(BASE + path, { headers: { Authorization: 'Bearer ' + TOKEN, Accept: 'application/json' } });
  const rem = Number(res.headers.get('x-ratelimit-remaining'));
  const rst = Number(res.headers.get('x-ratelimit-reset'));
  if (Number.isFinite(rem)) { rateRemaining = rem; rateResetAt = Date.now() + (Number.isFinite(rst) ? rst * 1000 : 60000); }
  if (res.status === 429) {
    if (tries > 6) throw new Error('429 persistente');
    const ra = Number(res.headers.get('retry-after')) || rst || 10;
    await sleep((ra + 1) * 1000);
    return dc(path, tries + 1);
  }
  if (!res.ok) throw new Error('HTTP ' + res.status + ' ' + (await res.text()).slice(0, 160));
  return res.json();
}
// A API devolve array puro, {data:[]} ou {messages:[]} conforme a rota.
const asArray = (p) => Array.isArray(p) ? p : (Array.isArray(p?.data) ? p.data : (Array.isArray(p?.messages) ? p.messages : []));
const onlyDigits = (v) => String(v == null ? '' : v).replace(/\D/g, '');

(async () => {
  if (!TOKEN) { console.error('DC_TOKEN ausente no .env'); process.exit(1); }

  console.log('Carregando lista de conversas da DataCrazy...');
  const convs = []; const seen = new Set();
  for (let page = 0; page < 40; page++) {
    const batch = asArray(await dc(`/conversations?skip=${page * 1000}&take=1000`));
    for (const c of batch) if (c?.id && !seen.has(c.id)) { seen.add(c.id); convs.push(c); }
    process.stdout.write('\r  ' + convs.length + ' conversas...');
    if (batch.length < 1000) break;
  }
  console.log('');

  const alvo = convs.filter((c) =>
    String(c.instance?.config?.phoneNumberId || '') === String(PHONE_NUMBER_ID) &&
    String(c.lastMessageDate || c.createdAt || '').slice(0, 10) >= SINCE &&
    onlyDigits(c.contact?.phoneNumber || c.contact?.contactId)
  ).sort((a, b) => String(a.lastMessageDate || '').localeCompare(String(b.lastMessageDate || '')));

  console.log('\nnúmero da IA (phoneNumberId) : ' + PHONE_NUMBER_ID);
  console.log('período                      : desde ' + SINCE);
  console.log('conversas a importar         : ' + alvo.length + (LIMIT ? ('  (limitado a ' + LIMIT + ')') : ''));
  const lista = LIMIT ? alvo.slice(0, LIMIT) : alvo;
  console.log('tempo estimado               : ~' + Math.ceil(lista.length / 30) + ' min (limite ~30 req/min)');
  if (!APPLY) {
    console.log('\nprimeiras 5:');
    for (const c of lista.slice(0, 5)) console.log('  ' + onlyDigits(c.contact?.phoneNumber) + '  "' + String(c.contact?.name || c.name || '').slice(0, 30) + '"  última msg ' + String(c.lastMessageDate || '').slice(0, 10));
    console.log('\n(DRY-RUN) nada gravado. Rode com --apply para importar.');
    process.exit(0);
  }

  const col = await getCollection('wa_ia_messages');
  const cc = await getCollection('whatsapp_contacts');
  try { await col.createIndex({ wamid: 1 }, { sparse: true }); await col.createIndex({ dcConversationId: 1 }, { sparse: true }); } catch (_) {}

  const agora = new Date();
  let convOk = 0, convPuladas = 0, convErro = 0, msgNovas = 0, msgJa = 0, contatosNovos = 0, contatosExistentes = 0;
  for (let i = 0; i < lista.length; i++) {
    const c = lista[i];
    const phone = onlyDigits(c.contact?.phoneNumber || c.contact?.contactId);
    const nome = String(c.contact?.name || c.name || '').slice(0, 120);
    // retomável: se já importamos essa conversa, pula sem gastar requisição.
    // Com --refresh, reconsulta mesmo assim (útil pra pegar mensagens NOVAS de threads
    // já importadas). A dedupe por wamid garante que nada duplica.
    if (!REFRESH) {
      const ja = await col.findOne({ dcConversationId: c.id }, { projection: { _id: 1 } });
      if (ja) { convPuladas++; process.stdout.write('\r[' + (i + 1) + '/' + lista.length + '] ok=' + convOk + ' pulados=' + convPuladas + ' erros=' + convErro + ' msgs=' + msgNovas + '   '); continue; }
    }
    try {
      const msgs = asArray(await dc('/conversations/' + encodeURIComponent(c.id) + '/messages'))
        .filter((m) => m && !m.deleted && !m.isInternal)
        .sort((a, b) => new Date(a.createdAt || 0) - new Date(b.createdAt || 0));
      for (const m of msgs) {
        const body = String(m.body || '').trim();
        const nAnexos = Array.isArray(m.attachments) ? m.attachments.length : 0;
        const texto = body || (nAnexos ? ('[' + nAnexos + ' anexo(s)]') : '');
        if (!texto) continue;
        const wamid = 'dc_' + String(m.id);
        const doc = {
          phone, phoneKey: phone,
          direction: m.received ? 'in' : 'out',
          type: 'text',
          text: texto.slice(0, 4000),
          mediaId: '', mime: '', filename: '',
          wamid, name: nome,
          agent: !m.received,               // saída no histórico = humano/atendente, não o bot
          importedFrom: 'datacrazy',
          dcConversationId: c.id,
          createdAt: m.createdAt ? new Date(m.createdAt) : agora,
        };
        const r = await col.updateOne({ wamid }, { $setOnInsert: doc }, { upsert: true });
        if (r.upsertedCount) msgNovas++; else msgJa++;
      }
      const ultima = msgs[msgs.length - 1];
      const existente = await cc.findOne({ _id: phone }, { projection: { _id: 1 } });
      if (existente) contatosExistentes++; else contatosNovos++;
      await cc.updateOne({ _id: phone }, {
        // Só define o estado da IA em contato NOVO: se o número já está no CRM,
        // é conversa em andamento e importar histórico não pode pausá-la.
        $setOnInsert: { _id: phone, createdAt: agora, botPaused: true, legacy: true, importedFrom: 'datacrazy', importedAt: agora.toISOString() },
        $set: {
          ...(nome ? { name: nome } : {}),
          ...(ultima ? { lastMessageAt: new Date(ultima.createdAt || agora), lastMessageText: String(ultima.body || '').slice(0, 140), lastMessageDir: ultima.received ? 'in' : 'out' } : {}),
        },
      }, { upsert: true });
      convOk++;
    } catch (e) {
      convErro++;
      if (convErro <= 5) console.log('\n  erro em ' + phone + ': ' + String(e.message).slice(0, 90));
    }
    process.stdout.write('\r[' + (i + 1) + '/' + lista.length + '] ok=' + convOk + ' pulados=' + convPuladas + ' erros=' + convErro + ' msgs=' + msgNovas + '   ');
  }
  console.log('\n\n=== IMPORTADO ===');
  console.log('conversas importadas : ' + convOk + ' | já existiam: ' + convPuladas + ' | erros: ' + convErro);
  console.log('mensagens novas      : ' + msgNovas + (msgJa ? (' | ' + msgJa + ' já existiam') : ''));
  console.log('contatos NOVOS       : ' + contatosNovos + '  -> IA PAUSADA + tag "antiga"');
  console.log('contatos existentes  : ' + contatosExistentes + '  -> estado da IA preservado');
  console.log('\nAbra /painel/ia-crm — as antigas aparecem com a tag laranja "antiga".');
  process.exit(0);
})().catch((e) => { console.error('\nERRO:', e && e.message); process.exit(1); });
