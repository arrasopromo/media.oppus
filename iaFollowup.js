'use strict';
// ═══════════════════════════════════════════════════════════════════════════
//  FOLLOW-UP DIÁRIO DO BOT (IA WhatsApp)
//  No fim do dia lê todas as conversas do bot, cruza com os pedidos de cada
//  telefone (e o status AO VIVO no fornecedor), marca as pendências concretas
//  e pede à IA um parecer por conversa + os pontos de melhoria que se repetem.
//  Resultado: coleção ia_followup_reports (1 doc por dia BRT), tela
//  /painel/ia-followup e notificação ntfy.
// ═══════════════════════════════════════════════════════════════════════════
const axios = require('axios');
const { getCollection } = require('./mongodbClient');

const BRT = 3 * 3600e3;
const DIA = 86400e3;
const MODELO = () => String(process.env.IA_FOLLOWUP_MODEL || process.env.OPENAI_MODEL || 'gpt-4o-mini').trim();

const PROVEDORES = {
  fama24h: { url: 'https://fama24h.net/api/v2', keyEnv: 'FAMA24H_API_KEY', label: 'Fama24h' },
  nuvra: { url: 'https://nuvrasmm.com/api/v2', keyEnv: 'NUVRASMM_API_KEY', label: 'Nuvra' },
  fornecedor_social: { url: 'https://fornecedorsocial.com/api/v2', keyEnv: 'FORNECEDOR_SOCIAL_API_KEY', label: 'Fornecedor Social' },
  topfama: { url: 'https://topfama.com/api/v2', keyEnv: 'TOPFAMA_API_KEY', label: 'TopFama' },
  worldsmm: { url: 'https://worldsmm.com.br/api/v2', keyEnv: 'WORLDSMM_API_KEY', label: 'WorldSMM' },
};

const diaBRT = (ms) => new Date(ms - BRT).toISOString().slice(0, 10);
const janela = (dia) => { const ini = new Date(dia + 'T03:00:00.000Z'); return { ini, fim: new Date(ini.getTime() + DIA) }; };
const fim8 = (p) => String(p || '').replace(/\D/g, '').slice(-8);
const hhmm = (d) => new Date(new Date(d).getTime() - BRT).toISOString().slice(11, 16);
const ai = (o, k) => {
  for (const mp of [o.additionalInfoMapPaid, o.additionalInfoMap]) if (mp && mp[k] != null && mp[k] !== '') return String(mp[k]);
  for (const arr of [o.additionalInfoPaid, o.additionalInfo]) if (Array.isArray(arr)) { const x = arr.find((i) => i && i.key === k); if (x && x.value != null && x.value !== '') return String(x.value); }
  return '';
};
const pago = (o) => ['pago', 'paid'].includes(String(o.status || '').toLowerCase()) || !!o.paidAt || !!(o.paghiper && o.paghiper.paidAt);

// Pedidos no fornecedor gravados no pedido (principal, bumps, multi-post).
function enviosDoPedido(o) {
  const out = [];
  const fama = (id) => (Number(id) >= 1000000 ? 'fama24h' : 'nuvra');
  for (const k of ['fama24h', 'fama24h_views', 'fama24h_likes']) { const v = o[k]; if (v && v.orderId) out.push({ slot: k, provider: fama(v.orderId), orderId: String(v.orderId) }); else if (v && v.status === 'error') out.push({ slot: k, erro: JSON.stringify(v.error || v.response || 'erro').slice(0, 120) }); }
  if (o.fama24h_multi && Array.isArray(o.fama24h_multi.orders)) for (const x of o.fama24h_multi.orders) { if (x && x.orderId) out.push({ slot: 'fama24h_multi', provider: x.provider || fama(x.orderId), orderId: String(x.orderId) }); else if (x) out.push({ slot: 'fama24h_multi', erro: String(x.status || 'erro') }); }
  for (const k of ['fornecedor_social', 'fornecedor_social_likes']) { const v = o[k]; if (v && v.orderId) out.push({ slot: k, provider: 'fornecedor_social', orderId: String(v.orderId) }); }
  for (const k of ['topfama', 'topfama_likes']) { const v = o[k]; if (v && v.orderId) out.push({ slot: k, provider: 'topfama', orderId: String(v.orderId) }); }
  if (o.worldsmm_comments && o.worldsmm_comments.orderId) out.push({ slot: 'worldsmm_comments', provider: 'worldsmm', orderId: String(o.worldsmm_comments.orderId) });
  return out;
}

// Qual item do pedido cada campo representa (o principal ou um adicional).
const EH_ADICIONAL = (slot) => /likes|views|comments/.test(String(slot || ''));
const ITEM_DO_SLOT = (slot) => (/likes/.test(slot) ? 'as curtidas do adicional' : /views/.test(slot) ? 'as visualizações do adicional' : /comments/.test(slot) ? 'os comentários do adicional' : 'o pedido principal');

// Status ao vivo em lote (action=status&orders=...), com cache por execução.
async function statusAoVivo(itens) {
  const porProv = {};
  for (const e of itens) if (e.provider && e.orderId) (porProv[e.provider] = porProv[e.provider] || new Set()).add(e.orderId);
  const res = {};
  for (const [prov, ids] of Object.entries(porProv)) {
    const p = PROVEDORES[prov]; const key = p && String(process.env[p.keyEnv] || '').trim();
    if (!p || !key) continue;
    const lista = [...ids];
    for (let i = 0; i < lista.length; i += 90) {
      const lote = lista.slice(i, i + 90);
      try {
        const r = await axios.post(p.url, new URLSearchParams({ key, action: 'status', orders: lote.join(',') }).toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 30000, validateStatus: () => true });
        const d = r && r.data;
        for (const id of lote) { const s = d && d[id]; if (s && (s.status || s.error)) res[prov + ':' + id] = { status: String(s.status || s.error), remains: s.remains }; }
      } catch (_) {}
    }
  }
  return res;
}

const RECLAMACAO = /(n[aã]o (chegou|recebi|entrou|caiu nada)|cad[eê]|reembols|devolv|estorn|golpe|engana|palha[cç]|nenhum[ao]? (seguidor|curtida)|n[aã]o mudou nada|cancel|procon|reclame aqui)/i;

async function avaliarComIA(transcricao, fatos) {
  const key = String(process.env.OPENAI_API_KEY || '').trim();
  if (!key) return null;
  const sistema = [
    'Você audita conversas do atendente virtual de vendas (bot) de uma empresa que vende seguidores, curtidas e visualizações para Instagram pelo WhatsApp.',
    'Leia a transcrição e os FATOS (pedidos e status reais no fornecedor, conferidos pelo sistema). Os FATOS mandam: se o bot disse algo que contradiz os fatos, é erro do bot.',
    'POLÍTICA DA EMPRESA (não é erro do bot seguir isso): ao cliente o bot NUNCA revela "parcial", "cancelado", "reembolsado", "erro" nem quantos seguidores faltam/entraram — pedido com problema é sempre tratado internamente e comunicado como "em andamento". Portanto, dizer "em andamento" para um pedido que na verdade está parcial ou cancelado é o comportamento CORRETO e NÃO deve ser marcado como "informacao_falsa". Só marque erro de status quando o pedido já foi ENTREGUE por completo e mesmo assim o bot disse que está em andamento, ou quando o bot inventou um dado que prejudica o cliente.',
    'Responda SOMENTE em JSON, em português, com as chaves:',
    '"resumo" (1–2 frases: o que o cliente queria e como terminou),',
    '"resultado" (uma de: "venda", "sem_venda", "pos_venda", "suporte", "outro"),',
    '"satisfacao" (uma de: "positiva", "neutra", "negativa"),',
    '"pendencias" (lista de ações que a EQUIPE ainda precisa fazer por esse cliente; vazia se nada),',
    '"erros_do_bot" (lista de {"tipo","descricao"}; tipos: "preco_errado", "tipo_trocado", "informacao_falsa", "pix_indevido", "nao_respondeu", "link_perdido", "promessa_indevida", "repetitivo", "outro"; vazia se nada),',
    '"melhorias" (lista curta de sugestões concretas para o bot, ex.: regra de prompt; vazia se nada).',
    'Seja objetivo. Não invente problema: se a conversa foi boa, deixe as listas vazias.',
  ].join('\n');
  try {
    const r = await axios.post('https://api.openai.com/v1/chat/completions', {
      model: MODELO(), temperature: 0.2, response_format: { type: 'json_object' },
      messages: [{ role: 'system', content: sistema }, { role: 'user', content: 'FATOS:\n' + JSON.stringify(fatos) + '\n\nTRANSCRIÇÃO:\n' + transcricao }],
    }, { headers: { Authorization: 'Bearer ' + key, 'Content-Type': 'application/json' }, timeout: 90000, validateStatus: () => true });
    const txt = r && r.data && r.data.choices && r.data.choices[0] && r.data.choices[0].message && r.data.choices[0].message.content;
    return txt ? JSON.parse(txt) : null;
  } catch (_) { return null; }
}

async function consolidarMelhorias(conversas) {
  const key = String(process.env.OPENAI_API_KEY || '').trim();
  const itens = [];
  for (const c of conversas) {
    const a = c.ia || {};
    for (const e of (a.erros_do_bot || [])) itens.push('ERRO ' + (e.tipo || '') + ': ' + (e.descricao || ''));
    for (const m of (a.melhorias || [])) itens.push('MELHORIA: ' + m);
  }
  if (!key || !itens.length) return [];
  try {
    const r = await axios.post('https://api.openai.com/v1/chat/completions', {
      model: MODELO(), temperature: 0.2, response_format: { type: 'json_object' },
      messages: [
        { role: 'system', content: 'Você recebe erros e sugestões de várias conversas de um bot de vendas no WhatsApp. Agrupe o que se repete e devolva SOMENTE JSON {"pontos":[{"titulo","ocorrencias","o_que_acontece","como_corrigir"}]} com no máximo 6 pontos, do mais frequente/grave para o menos. "como_corrigir" deve ser uma ação concreta (regra de prompt, trava no sistema ou ajuste de processo). Português.' },
        { role: 'user', content: itens.slice(0, 400).join('\n') },
      ],
    }, { headers: { Authorization: 'Bearer ' + key, 'Content-Type': 'application/json' }, timeout: 90000, validateStatus: () => true });
    const txt = r && r.data && r.data.choices && r.data.choices[0] && r.data.choices[0].message && r.data.choices[0].message.content;
    const j = txt ? JSON.parse(txt) : null;
    return (j && Array.isArray(j.pontos)) ? j.pontos : [];
  } catch (_) { return []; }
}

async function gerarRelatorio(dia, { comIA = true } = {}) {
  const { ini, fim } = janela(dia);
  const agora = Date.now();
  const msgsCol = await getCollection('wa_ia_messages');
  const pedidosCol = await getCollection('checkout_orders');
  const contatosCol = await getCollection('whatsapp_contacts');

  const msgs = await msgsCol.find({ createdAt: { $gte: ini, $lt: fim } }, { projection: { phone: 1, phoneKey: 1, direction: 1, type: 1, text: 1, agent: 1, name: 1, imagemDescricao: 1, createdAt: 1 } }).sort({ createdAt: 1 }).toArray();
  // O mesmo cliente às vezes aparece com e sem o 55 na frente (ex.: 553184835308 e 3184835308).
  // Junta pelos 8 últimos dígitos para não virar duas conversas no relatório.
  const grupos = new Map();
  const chavesPorFim8 = new Map();
  for (const m of msgs) {
    const k = String(m.phoneKey || m.phone || '').replace(/\D/g, '');
    if (!k) continue;
    const f = fim8(k);
    if (!f) continue;
    if (!chavesPorFim8.has(f)) chavesPorFim8.set(f, new Set());
    chavesPorFim8.get(f).add(k);
    const g = grupos.get(f) || { chave: k, msgs: [] };
    if (k.length > g.chave.length) g.chave = k;
    g.msgs.push(m);
    grupos.set(f, g);
  }
  const porTel = new Map();
  for (const g of grupos.values()) { g.msgs.sort((a, b) => new Date(a.createdAt) - new Date(b.createdAt)); porTel.set(g.chave, g.msgs); }

  // pedidos dos últimos 15 dias ligados a esses telefones (bot e site)
  const alvos = new Set([...porTel.keys()].map(fim8));
  const desde = new Date(ini.getTime() - 15 * DIA).toISOString();
  const pedidos = await pedidosCol.find({ createdAt: { $gte: desde, $lt: fim.toISOString() } }, { projection: { 'paghiper.response': 0, 'paghiper.statusPayload': 0, notaFiscal: 0, emails: 0, geolocation: 0 } }).toArray();
  const pedidosPorFim8 = new Map();
  for (const o of pedidos) {
    const tels = [o.customer && o.customer.phone, o.telefone, ai(o, 'phone')].map(fim8).filter((x) => x.length === 8);
    for (const t of new Set(tels)) if (alvos.has(t)) { if (!pedidosPorFim8.has(t)) pedidosPorFim8.set(t, []); pedidosPorFim8.get(t).push(o); }
  }
  const envios = [];
  for (const lista of pedidosPorFim8.values()) for (const o of lista) if (pago(o)) envios.push(...enviosDoPedido(o));
  const vivo = await statusAoVivo(envios);

  const contatos = new Map();
  const todasChaves = [...chavesPorFim8.values()].flatMap((s) => [...s]);
  const contatosBrutos = new Map();
  for (const c of await contatosCol.find({ _id: { $in: todasChaves } }).toArray()) contatosBrutos.set(String(c._id), c);
  // o contato pode estar gravado em qualquer uma das variações do número: fica com a mais recente
  for (const [f, chaves] of chavesPorFim8) {
    const docs = [...chaves].map((k) => contatosBrutos.get(k)).filter(Boolean);
    if (!docs.length) continue;
    docs.sort((a, b) => new Date(b.supportAt || b.updatedAt || 0) - new Date(a.supportAt || a.updatedAt || 0));
    contatos.set(f, docs[0]);
  }

  const conversas = [];
  for (const [tel, lista] of porTel) {
    const nome = (lista.find((m) => m.name) || {}).name || '';
    const ultima = lista[lista.length - 1];
    const ultimaIn = [...lista].reverse().find((m) => m.direction === 'in');
    const pendencias = [];
    const contato = contatos.get(fim8(tel)) || {};
    // Bot pausado nesse contato = um humano assumiu. O follow-up é sobre o BOT, então tudo que
    // depende dele (sem resposta, parecer da IA, erros do bot) NÃO conta quando ele está pausado.
    const botPausado = !!contato.botPaused;
    // 1) cliente sem resposta (só quando o BOT está ativo; ignora fechamento tipo "ok", "obrigado", "👍")
    const FECHAMENTO = /^\s*(ok+|okay|blz|beleza|valeu+|vlw|obrigad[oa]s?( ?mesmo)?|obg|grat[ao]|show|top|perfeito|certo|t[aá] ?bom|tudo bem|amém|am[eé]m|combinado|entendi|entendido|sim|👍+|🙏+|❤️+|😊+|🥰+|🙌+|[\p{Emoji}\s]+)[\s!.]*$/iu;
    const clienteEsperando = ultima.direction === 'in' && !FECHAMENTO.test(String(ultima.text || '')) && (Math.min(agora, fim.getTime()) - new Date(ultima.createdAt).getTime()) > 15 * 60e3;
    if (clienteEsperando) {
      if (!botPausado) {
        // bot ativo e cliente sem resposta = o bot deveria ter respondido
        pendencias.push({ tipo: 'sem_resposta', detalhe: `Última mensagem do cliente às ${hhmm(ultima.createdAt)} sem resposta: "${String(ultima.text || ultima.type).slice(0, 120)}"` });
      } else {
        // atendimento humano em andamento, mas o cliente escreveu por último e segue esperando:
        // fica pendente do RETORNO da equipe (se um humano já tivesse respondido por último, não cai aqui).
        pendencias.push({ tipo: 'aguardando_retorno', detalhe: `Atendimento humano — cliente aguardando retorno desde ${hhmm(ultima.createdAt)}: "${String(ultima.text || ultima.type).slice(0, 120)}"` });
      }
    }
    // 2) suporte acionado NESTE dia (a marca de suporte não é limpa sozinha; antigas não contam)
    const suporteEm = contato.supportAt ? new Date(contato.supportAt).getTime() : 0;
    if ((contato.flag === 'suporte' || contato.botPaused) && suporteEm >= ini.getTime() && suporteEm < fim.getTime()) {
      pendencias.push({ tipo: 'suporte', detalhe: `Suporte humano acionado às ${hhmm(contato.supportAt)}` + (contato.supportReason ? `: ${String(contato.supportReason).slice(0, 140)}` : '') + (contato.botPaused ? ' (bot pausado)' : '') });
    }
    // 3) reclamação
    const recl = lista.filter((m) => m.direction === 'in' && RECLAMACAO.test(String(m.text || '')));
    if (recl.length) pendencias.push({ tipo: 'reclamacao', detalhe: `Cliente reclamou: "${String(recl[recl.length - 1].text).slice(0, 140)}"` });

    // 4) pedidos
    const pedidosDoTel = (pedidosPorFim8.get(fim8(tel)) || []).sort((a, b) => String(a.createdAt).localeCompare(String(b.createdAt)));
    const resumoPedidos = [];
    for (const o of pedidosDoTel) {
      const doBot = /^WppAgent_/.test(String(o.correlationID || ''));
      const criadoHoje = String(o.createdAt) >= ini.toISOString();
      const item = { identifier: o.identifier, origem: doBot ? 'bot' : 'site', criado: o.createdAt, pago: pago(o), valor: (Number(o.valueCents) || 0) / 100, pacote: ai(o, 'pacote') || `${ai(o, 'quantidade')} ${ai(o, 'categoria_servico')}`, tipo: ai(o, 'tipo_servico'), usuario: o.instagramUsername || o.instauser || ai(o, 'instagram_username'), status: o.status };
      if (!item.pago) {
        if (doBot && criadoHoje && String(o.status || '').toLowerCase() !== 'estornado') item.situacao = 'pix_nao_pago';
        else continue; // pendente antigo: não interessa ao follow-up
      } else if (String(o.status || '').toLowerCase() === 'estornado') {
        item.situacao = 'estornado';
      } else {
        const ev = enviosDoPedido(o);
        const comId = ev.filter((e) => e.orderId);
        if (!comId.length) {
          const privado = o.isPrivate === true || (o.profilePrivacy && o.profilePrivacy.isPrivate === true);
          const upsellEsperando = o.upsell && o.upsell.isUpsell && /wait/.test(String(o.upsell.dispatchStatus || ''));
          // o sistema segura o envio enquanto outro pedido do mesmo perfil está rodando: isso é normal
          const fila = o.followersQueue && /wait/.test(String(o.followersQueue.status || '')) ? o.followersQueue : null;
          const presoHa = fila ? agora - new Date(fila.heldAt || o.paidAt || o.createdAt).getTime() : 0;
          item.situacao = privado ? 'privado_segurando' : (upsellEsperando ? 'upsell_aguardando_pai' : (fila ? 'na_fila' : 'pago_sem_envio'));
          const erro = ev.find((e) => e.erro);
          if (erro) item.erroEnvio = erro.erro;
          if (fila) {
            item.fila = { desde: fila.heldAt, atrasDe: fila.blockedBy || '' };
            // só vira pendência se a fila não andou em 24h
            if (presoHa > DIA) pendencias.push({ tipo: 'na_fila', identifier: o.identifier, detalhe: `${item.pacote} está na fila há ${Math.floor(presoHa / 3600e3)}h, esperando terminar o pedido ${fila.blockedBy || 'anterior'} do mesmo perfil` });
          } else if (!upsellEsperando) {
            pendencias.push({ tipo: item.situacao, identifier: o.identifier, detalhe: `${item.pacote} (R$ ${item.valor.toFixed(2)}) pago e NÃO enviado ao fornecedor${privado ? ' — perfil marcado como privado' : ''}${item.erroEnvio ? ' — erro: ' + item.erroEnvio : ''}` });
          }
        } else {
          item.fornecedor = comId.map((e) => { const s = vivo[e.provider + ':' + e.orderId]; return { provider: e.provider, orderId: e.orderId, slot: e.slot, status: s ? s.status : '?', remains: s ? s.remains : null }; });
          const sts = item.fornecedor.map((f) => String(f.status).toLowerCase());
          if (sts.some((s) => /cancel|refund/.test(s))) {
            const cancelados = item.fornecedor.filter((f) => /cancel|refund/i.test(f.status));
            const soAdicional = cancelados.every((f) => EH_ADICIONAL(f.slot));
            item.situacao = soAdicional ? 'adicional_cancelado' : 'cancelado_fornecedor';
            const oQue = cancelados.map((f) => ITEM_DO_SLOT(f.slot)).filter((v, i, a) => a.indexOf(v) === i).join(' e ');
            const quais = cancelados.map((f) => (PROVEDORES[f.provider] ? PROVEDORES[f.provider].label + ' ' + f.orderId : f.orderId)).join(', ');
            pendencias.push({ tipo: item.situacao, identifier: o.identifier, detalhe: soAdicional
              ? `${item.pacote}: o fornecedor CANCELOU ${oQue} (${quais}) — o resto do pedido segue normal`
              : `${item.pacote} pago, mas o fornecedor CANCELOU ${oQue} (${quais})` });
          } else if (sts.every((s) => s === 'completed')) item.situacao = 'entregue';
          else if (sts.some((s) => s === 'partial')) { item.situacao = 'parcial'; pendencias.push({ tipo: 'parcial', identifier: o.identifier, detalhe: `${item.pacote}: entrega PARCIAL no fornecedor` }); }
          else item.situacao = 'em_andamento';
        }
      }
      resumoPedidos.push(item);
    }

    const conv = {
      telefone: tel, nome, mensagens: lista.length, botPausado,
      doCliente: lista.filter((m) => m.direction === 'in').length,
      doBot: lista.filter((m) => m.direction === 'out' && !m.agent).length,
      doAtendente: lista.filter((m) => m.direction === 'out' && m.agent).length,
      primeira: lista[0].createdAt, ultima: ultima.createdAt, ultimaDoCliente: ultimaIn ? ultimaIn.createdAt : null,
      pedidos: resumoPedidos, pendencias,
    };

    if (comIA && !botPausado) {
      const linhas = lista.slice(-80).map((m) => `[${hhmm(m.createdAt)}] ${m.direction === 'in' ? 'CLIENTE' : (m.agent ? 'ATENDENTE' : 'BOT')}: ${m.imagemDescricao ? '[imagem] ' + m.imagemDescricao : String(m.text || '[' + m.type + ']')}`.replace(/\s+/g, ' ').slice(0, 600));
      let transcricao = linhas.join('\n');
      if (transcricao.length > 12000) transcricao = transcricao.slice(-12000);
      Object.defineProperty(conv, '_transcricao', { value: transcricao, enumerable: false });
    }
    conversas.push(conv);
  }

  // Parecer da IA: só nas conversas com o BOT ATIVO (as pausadas são atendimento humano — não
  // fazem parte da avaliação do bot). 6 em paralelo (sequencial levaria minutos com ~70 conversas).
  if (comIA) {
    const paraIA = conversas.filter((c) => !c.botPausado && c._transcricao);
    for (let i = 0; i < paraIA.length; i += 6) {
      await Promise.all(paraIA.slice(i, i + 6).map(async (conv) => {
        conv.ia = await avaliarComIA(conv._transcricao, { pedidos: conv.pedidos.map((p) => ({ pacote: p.pacote, tipo: p.tipo, valor: p.valor, pago: p.pago, situacao: p.situacao, usuario: p.usuario })), pendencias_detectadas: conv.pendencias.map((p) => p.tipo) });
        // até 2 pendências apontadas pela IA que o sistema não detectou sozinho
        if (conv.ia && Array.isArray(conv.ia.pendencias)) for (const p of conv.ia.pendencias.slice(0, 2)) if (p) conv.pendencias.push({ tipo: 'ia', detalhe: String(p).slice(0, 200) });
      }));
    }
  }

  conversas.sort((a, b) => b.pendencias.length - a.pendencias.length || String(b.ultima).localeCompare(String(a.ultima)));
  const todosPedidos = conversas.flatMap((c) => c.pedidos);
  const conta = (f) => conversas.reduce((n, c) => n + c.pendencias.filter(f).length, 0);
  const vendeu = (p) => p.origem === 'bot' && p.pago && String(p.criado) >= ini.toISOString();
  const conversasComVenda = conversas.filter((c) => c.pedidos.some(vendeu)).length;
  const botAtivas = conversas.filter((c) => !c.botPausado).length;
  const totais = {
    conversas: conversas.length,
    botAtivas,                        // conversas com o bot atendendo (não pausado)
    botPausadas: conversas.length - botAtivas,
    mensagens: msgs.length,
    vendas: todosPedidos.filter(vendeu).length,
    conversasComVenda,
    conversaoPct: conversas.length ? Math.round((conversasComVenda / conversas.length) * 1000) / 10 : 0,
    receita: Math.round(todosPedidos.filter(vendeu).reduce((a, p) => a + p.valor, 0) * 100) / 100,
    pixNaoPago: todosPedidos.filter((p) => p.situacao === 'pix_nao_pago').length,
    conversasComPendencia: conversas.filter((c) => c.pendencias.length).length,
    semResposta: conta((p) => p.tipo === 'sem_resposta'),
    aguardandoRetorno: conta((p) => p.tipo === 'aguardando_retorno'),
    pagoSemEnvio: conta((p) => p.tipo === 'pago_sem_envio'),
    privadoSegurando: conta((p) => p.tipo === 'privado_segurando'),
    canceladoFornecedor: conta((p) => p.tipo === 'cancelado_fornecedor'),
    adicionalCancelado: conta((p) => p.tipo === 'adicional_cancelado'),
    naFila: conta((p) => p.tipo === 'na_fila'),
    parcial: conta((p) => p.tipo === 'parcial'),
    suporte: conta((p) => p.tipo === 'suporte'),
    reclamacoes: conta((p) => p.tipo === 'reclamacao'),
    // satisfação e erros do bot: só valem onde o bot estava ativo (a IA nem roda nas pausadas)
    satisfacaoNegativa: conversas.filter((c) => !c.botPausado && c.ia && c.ia.satisfacao === 'negativa').length,
    errosDoBot: conversas.reduce((n, c) => n + ((!c.botPausado && c.ia && c.ia.erros_do_bot) || []).length, 0),
  };
  const melhorias = comIA ? await consolidarMelhorias(conversas) : [];
  return { _id: dia, dia, geradoEm: new Date().toISOString(), modelo: comIA ? MODELO() : null, totais, melhorias, conversas };
}

async function rodarESalvar(dia, { sendNtfy, publico, comIA = true } = {}) {
  const col = await getCollection('ia_followup_reports');
  // trava simples: não roda duas vezes o mesmo dia ao mesmo tempo
  const trava = await col.updateOne({ _id: dia, $or: [{ gerando: { $ne: true } }, { gerandoDesde: { $lt: new Date(Date.now() - 20 * 60e3).toISOString() } }] }, { $set: { gerando: true, gerandoDesde: new Date().toISOString() } }, { upsert: true }).catch((e) => (e && e.code === 11000 ? { matchedCount: 0, upsertedCount: 0 } : Promise.reject(e)));
  if (!trava.modifiedCount && !trava.upsertedCount) return { ok: false, error: 'ja_gerando' };
  try {
    const rel = await gerarRelatorio(dia, { comIA });
    await col.replaceOne({ _id: dia }, Object.assign(rel, { gerando: false }), { upsert: true });
    try { console.log(`📋 [ia-followup] ${dia}: ${rel.totais.conversas} conversas, ${rel.totais.conversasComPendencia} com pendência, ${rel.totais.vendas} vendas`); } catch (_) {}
    if (typeof sendNtfy === 'function') {
      const t = rel.totais;
      const dd = dia.slice(8, 10) + '/' + dia.slice(5, 7);
      const partes = [`${t.conversas} conversas · ${t.vendas} vendas (R$ ${t.receita.toFixed(2).replace('.', ',')})`];
      const pend = [];
      if (t.semResposta) pend.push(`${t.semResposta} sem resposta`);
      if (t.pagoSemEnvio) pend.push(`${t.pagoSemEnvio} pago sem envio`);
      if (t.privadoSegurando) pend.push(`${t.privadoSegurando} privado`);
      if (t.canceladoFornecedor) pend.push(`${t.canceladoFornecedor} cancelado no fornecedor`);
      if (t.suporte) pend.push(`${t.suporte} suporte`);
      if (t.reclamacoes) pend.push(`${t.reclamacoes} reclamação`);
      partes.push(pend.length ? 'Pendências: ' + pend.join(', ') : 'Sem pendências');
      if (rel.melhorias && rel.melhorias[0]) partes.push('Principal melhoria: ' + String(rel.melhorias[0].titulo || '').slice(0, 80));
      try { await sendNtfy({ title: `Follow-up do bot ${dd}`, message: partes.join('\n'), priority: (t.pagoSemEnvio || t.canceladoFornecedor || t.semResposta) ? 'high' : 'default', tags: 'clipboard', click: publico ? publico + '/painel/ia-followup?dia=' + dia : undefined }); } catch (_) {}
    }
    return { ok: true, totais: rel.totais };
  } catch (e) {
    await col.updateOne({ _id: dia }, { $set: { gerando: false, erro: String((e && e.message) || e).slice(0, 300) } }).catch(() => {});
    return { ok: false, error: (e && e.message) || 'erro' };
  }
}

function registerIaFollowup(app, { requireAdmin, sendNtfy } = {}) {
  const publico = () => String(process.env.PUBLIC_BASE_URL || '').replace(/\/+$/, '');
  app.get('/painel/ia-followup', requireAdmin, async (req, res) => {
    try {
      const col = await getCollection('ia_followup_reports');
      const dias = await col.find({}, { projection: { dia: 1, totais: 1, geradoEm: 1, gerando: 1, erro: 1 } }).sort({ _id: -1 }).limit(45).toArray();
      const pedido = /^\d{4}-\d{2}-\d{2}$/.test(String(req.query.dia || '')) ? String(req.query.dia) : (dias[0] && dias[0]._id);
      const rel = pedido ? await col.findOne({ _id: pedido }) : null;
      return res.render('painel_ia_followup', { page: 'ia-followup', dias, rel, diaSelecionado: pedido || '', hoje: diaBRT(Date.now()) });
    } catch (e) { return res.status(500).send(String((e && e.message) || e)); }
  });
  app.post('/api/painel/ia-followup/gerar', requireAdmin, async (req, res) => {
    const dia = /^\d{4}-\d{2}-\d{2}$/.test(String((req.body && req.body.dia) || '')) ? String(req.body.dia) : diaBRT(Date.now());
    const notificar = String((req.body && req.body.notificar) || '') === '1';
    // roda em segundo plano: pode levar 1–2 min (IA por conversa)
    rodarESalvar(dia, { sendNtfy: notificar ? sendNtfy : null, publico: publico() }).catch(() => {});
    return res.json({ ok: true, dia, iniciado: true });
  });
  app.get('/api/painel/ia-followup/status', requireAdmin, async (req, res) => {
    try {
      const col = await getCollection('ia_followup_reports');
      const d = await col.findOne({ _id: String(req.query.dia || '') }, { projection: { gerando: 1, geradoEm: 1, erro: 1 } });
      return res.json({ ok: true, gerando: !!(d && d.gerando), geradoEm: d && d.geradoEm, erro: d && d.erro });
    } catch (e) { return res.status(500).json({ ok: false }); }
  });
}

// Agenda para todo dia no horário IA_FOLLOWUP_HORA (BRT, padrão 23:30).
function startIaFollowupLoop({ backgroundJobsEnabled, sendNtfy } = {}) {
  if (String(process.env.IA_FOLLOWUP_ENABLED || 'true').toLowerCase() === 'false') { try { console.log('⏸️ [ia-followup] desligado (IA_FOLLOWUP_ENABLED=false)'); } catch (_) {} return; }
  const [h, m] = String(process.env.IA_FOLLOWUP_HORA || '23:30').split(':').map((x) => parseInt(x, 10));
  const agenda = () => {
    const agora = Date.now();
    const brt = new Date(agora - BRT);
    let alvo = Date.UTC(brt.getUTCFullYear(), brt.getUTCMonth(), brt.getUTCDate(), Number.isFinite(h) ? h : 23, Number.isFinite(m) ? m : 30, 0) + BRT;
    if (alvo <= agora) alvo += DIA;
    const t = setTimeout(async () => {
      try {
        if (typeof backgroundJobsEnabled !== 'function' || backgroundJobsEnabled()) {
          const r = await rodarESalvar(diaBRT(Date.now()), { sendNtfy, publico: String(process.env.PUBLIC_BASE_URL || '').replace(/\/+$/, '') });
          try { console.log('📋 [ia-followup] execução diária:', JSON.stringify(r)); } catch (_) {}
        }
      } catch (_) {}
      agenda();
    }, Math.max(1000, alvo - agora));
    try { t.unref && t.unref(); } catch (_) {}
  };
  agenda();
  try { console.log(`📋 [ia-followup] agendado todo dia às ${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')} (BRT)`); } catch (_) {}
}

module.exports = { registerIaFollowup, startIaFollowupLoop, gerarRelatorio, rodarESalvar };
