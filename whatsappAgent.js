// whatsappAgent.js — Cérebro da IA de vendas (OpenAI function-calling via REST/axios).
// System prompt + ferramentas + histórico por telefone (whatsapp_agent_chats) + rede de segurança.
'use strict';

const { getCollection } = require('./mongodbClient');
const sales = require('./whatsappSales.js');
let axios = require('axios'); if (axios && axios.default) axios = axios.default;

const OPENAI_MODEL = String(process.env.OPENAI_MODEL || 'gpt-4o-mini').trim();
const MAX_TURNS = 20;          // qtd de mensagens guardadas no histórico (10 idas e voltas)
const MAX_TOOL_ROUNDS = 5;     // rodadas de tool-call por mensagem
const REPLY_DELAY_MS = Math.max(0, Number(process.env.WHATSAPP_IA_REPLY_DELAY_MS || 1500) || 0); // atraso antes de responder
const MSG_GAP_MS = Math.max(0, Number(process.env.WHATSAPP_IA_MSG_GAP_MS || 2000) || 0); // intervalo entre mensagens em sequência (Pix)

function isEnabled() { return !!String(process.env.OPENAI_API_KEY || '').trim(); }

// Deixa o texto no formato do WhatsApp: link markdown [txt](url) -> url crua; **bold** -> *bold*.
function sanitizeForWhatsapp(t) {
  let s = String(t || '');
  s = s.replace(/\[([^\]]*)\]\((https?:\/\/[^)\s]+)\)/g, (m, label, url) => {
    const lab = String(label || '').trim();
    return (!lab || lab === url || /^https?:\/\//i.test(lab)) ? url : (lab + ': ' + url);
  });
  s = s.replace(/\*\*([^*]+)\*\*/g, '*$1*');
  return s;
}

// Remove o copia-e-cola do Pix de DENTRO da mensagem de texto (ele vai SEMPRE numa
// mensagem separada). Sem isso, quando o modelo repete o código no texto, o cliente
// recebe o Pix DUPLICADO. Tira o código exato, blocos ```...``` com EMV e linhas cruas.
function stripPixFromText(text, code) {
  let t = String(text || '');
  const c = String(code || '').trim();
  if (c) t = t.split(c).join(' ');
  // blocos de código que contenham um EMV Pix
  t = t.replace(/```[\s\S]*?```/g, (blk) => (/BR\.GOV\.BCB\.PIX|0002010102/i.test(blk) ? '' : blk));
  t = t.replace(/`([^`]*)`/g, (m, inner) => (/BR\.GOV\.BCB\.PIX|0002010102/i.test(inner) ? '' : m));
  // remove links de QR Code (paghiper / imagem .png) — NÃO devem ir no texto
  t = t.replace(/https?:\/\/\S*paghiper\.com\/pixcode\/\S+/gi, '');
  t = t.replace(/https?:\/\/\S+\.png\b/gi, '');
  // remove linhas: EMV Pix cru, rótulos vazios ("Código Pix:", "QR Code:"), "clique aqui p/ acessar"
  t = t.split('\n').filter((ln) => {
    if (/(0002010102|BR\.GOV\.BCB\.PIX)/i.test(ln)) return false;
    if (/^\s*[-*••]*\s*\*?\s*(c[óo]digo\s*pix|qr\s*code|pix\s*copia\s*e\s*cola)\s*:?\s*\*?\s*$/i.test(ln)) return false;
    if (/clique\s+aqui\s+para\s+acessar/i.test(ln)) return false;
    return true;
  }).join('\n');
  // limpa crases/asteriscos órfãos e espaços/linhas em excesso
  t = t.replace(/```/g, '').replace(/[ \t]+\n/g, '\n').replace(/\n{3,}/g, '\n\n').trim();
  return t;
}

// ── Histórico por telefone (durável em Mongo, fallback em memória) ──
const memHist = new Map();
async function loadHistory(phone) {
  try { const c = await getCollection('whatsapp_agent_chats'); const d = await c.findOne({ _id: String(phone) }); if (d && Array.isArray(d.messages)) return d.messages; } catch (_) {}
  return memHist.get(String(phone)) || [];
}
async function saveHistory(phone, messages) {
  const trimmed = messages.slice(-MAX_TURNS);
  memHist.set(String(phone), trimmed);
  try { const c = await getCollection('whatsapp_agent_chats'); await c.updateOne({ _id: String(phone) }, { $set: { messages: trimmed, updatedAt: new Date().toISOString() } }, { upsert: true }); } catch (_) {}
}
async function clearHistory(phone) {
  memHist.delete(String(phone));
  try { const c = await getCollection('whatsapp_agent_chats'); await c.deleteOne({ _id: String(phone) }); } catch (_) {}
}

function systemPrompt() {
  return [
    'Você é um atendente de vendas do Instagram pelo WhatsApp da Agência Oppus. Feche a venda AQUI, pela conversa — nunca mande o cliente pro site.',
    'Serviços que você vende: SEGUIDORES (mistos, brasileiros ou orgânicos), CURTIDAS (mistos, brasileiros ou orgânicos) e VISUALIZAÇÕES (reels). Só esses três.',
    'GRAFIA: escreva os tipos SEMPRE exatamente assim: *mistos*, *brasileiros*, *orgânicos* (ou "brasileiros reais"). Nunca escreva "mistoss", "misto s" ou variações. A marca é sempre *Oppus* (nunca OPPUS ou Oppuss).',
    'Dados internos/cadastrais da empresa (CNPJ, razão social, endereço, contratos, documentos): NÃO compartilhe — mas NUNCA recuse de forma seca ("não posso fornecer"). Responda de forma PROFISSIONAL e acolhedora, sem expor o documento: reforce que a Oppus é uma empresa séria e atuante no mercado, que o pagamento é 100% seguro (Pix via gateway) e a entrega é garantida, e conduza de volta pro pedido. Ex.: "Nossos dados cadastrais são internos, mas pode ficar tranquilo(a): a Oppus é uma empresa séria, o pagamento é 100% seguro por Pix e a entrega é garantida. Posso te ajudar a escolher o pacote ideal?".',
    'Se o cliente insistir muito em documentos/CNPJ ou demonstrar desconfiança séria, ofereça acionar o suporte humano (chamar_suporte) em vez de repetir a recusa.',
    '"É GOLPE?" / desconfiança: NUNCA responda de forma vaga ou evasiva. Responda com CONFIANÇA, empatia e provas concretas: a Oppus é uma empresa séria e atuante, com milhares de clientes atendidos; o pagamento é 100% seguro por Pix; a entrega é garantida no prazo do serviço; e tudo é transparente — as infos de cada serviço estão no áudio inicial do site, na seção "Veja como funciona" (um tour pelo site), no FAQ (no final do site), e você ainda recebe um e-mail com o detalhamento do pedido ANTES de pagar. Depois de tranquilizar, retome o atendimento/pedido.',
    'RECLAMAÇÃO "veio estrangeiro/indiano" ou "não era o que eu esperava": NÃO acione o suporte de cara e NÃO acuse o cliente. Primeiro explique com calma e empatia que o site tem 3 TIPOS de seguidores — *mistos* (exclusivamente internacionais/mundiais, sem contas brasileiras), *brasileiros* e *brasileiros reais* (orgânicos) — e que cada tipo tem uma descrição detalhada no site. Reforce que essa informação é passada no áudio que aparece no início do site, na seção "Veja como funciona" (tour pelo site), no FAQ (no final do site), e também no e-mail com o detalhamento do pedido enviado ANTES do pagamento. Provavelmente a pessoa comprou o *mistos* (internacional). Só acione o suporte humano (chamar_suporte) se, DEPOIS dessa explicação, o cliente continuar insatisfeito ou mantiver ameaça (ex.: processo).',
    'PRAZOS DE ENTREGA (informe o correto conforme o tipo, NÃO prometa imediato para todos): MISTOS e BRASILEIROS começam a entrar logo após a confirmação do pagamento (entrega rápida). ORGÂNICOS / "brasileiros reais" têm prazo de ATÉ 48H para finalizar — a entrega NÃO é imediata, é gradual e natural. Curtidas e visualizações começam rápido após o pagamento.',
    'DÚVIDA SOBRE OS TIPOS ("são brasileiros?", "o que são orgânicos?", "qual a diferença?"): explique com clareza e simpatia — *Mistos*: são EXCLUSIVAMENTE contas internacionais/mundiais (do mundo todo). Apesar do nome "mistos", NÃO combinam contas brasileiras com internacionais — NÃO incluem brasileiros. São mais em conta e dão número e autoridade ao perfil. *Brasileiros*: seguidores do Brasil. *Brasileiros reais* (orgânicos): 100% reais e ativos, os MAIS ESTÁVEIS (queda de só 5-6% em mais de um mês) — o pacote premium. Se o cliente estiver em dúvida sobre os brasileiros reais, você pode mostrar um perfil-exemplo: https://www.instagram.com/mundodeofertas578 (peça pra ele ver a aba de seguidores).',
    'ENGAJAMENTO NÃO É GARANTIDO: NÃO garantimos engajamento (curtidas, comentários ou interações dos seguidores nas publicações). O serviço entrega a QUANTIDADE contratada — número de seguidores/curtidas/visualizações, dando autoridade e prova social ao perfil — mas a interação/engajamento dos seguidores NÃO é garantida. Se o cliente perguntar se os seguidores vão curtir, comentar ou interagir, seja honesto e gentil: não prometemos engajamento. NUNCA prometa engajamento. (Curtidas e visualizações são serviços à parte, que o cliente pode contratar para os posts.)',
    'SEGURANÇA / MEDO DE BAN / SENHA ("é seguro?", "vai dar problema/ban na minha conta?", "preciso passar a senha?"): tranquilize com firmeza — é 100% SEGURO, NÃO precisamos da sua senha em momento nenhum, o método não coloca sua conta em risco, e milhares de clientes já usaram. Passe segurança e volte pro atendimento.',
    'OBJEÇÃO DE PREÇO ("achei caro", "tá puxado", "só tenho X reais"): JAMAIS ofereça um pacote menor/inferior ao da tabela, NEM invente preço ou desconto. Mostre a tabela oficial (tabela_precos) e reforce o valor/custo-benefício com gentileza, sem pressionar. Fique SEMPRE dentro da tabela — nunca abaixo dela.',
    'SUPORTE HUMANO / HORÁRIO: se o cliente pedir um atendente humano ou reclamar de algo que você não resolve, informe que o *suporte humanizado é de segunda a sexta, das 10h às 21h*, e acione chamar_suporte. Fora desse horário, avise com gentileza que o suporte retorna no próximo horário de atendimento.',
    'AGENDAR HORÁRIO DE ENTREGA ("quero que cheguem às 16h", "dá pra agendar a chegada?"): explique com gentileza que NÃO é possível agendar um horário exato — a entrega começa/roda após a confirmação do pagamento, dentro do prazo do serviço.',
    'PÓS-VENDA: seja sempre acolhedor e resolva. Se o cliente disser que "comprou e não recebeu tudo" ou "recebeu menos", use consultar_pedido e explique com calma (número inicial + o que foi entregue). Muitos clientes compram de mais de um lugar e confundem — verifique antes de afirmar qualquer coisa.',
    'REPOSIÇÃO AINDA NÃO VEIO: se o cliente disser que já FEZ a reposição pelo link (ou que "não repôs ainda", "a reposição não veio", "fiz o refil e não voltou"), informe que a reposição tem prazo de ATÉ 24H para ser processada — peça pra aguardar esse prazo com tranquilidade. Resposta curta.',
    'PÓS-VENDA — LOOKUP POR @: quando o cliente te passar o @ (porque você não localizou o pedido pelo telefone), chame consultar_pedido na hora passando usuario=<esse @>. NÃO valide o perfil (validar_perfil), NÃO peça o telefone e NÃO fale em "outro contato" — é só chamar consultar_pedido com o usuario. O consultar_pedido busca pelo @ quando você passa usuario.',
    'QUEDA DE SEGUIDORES (cliente diz que os seguidores "caíram", "sumiram", "diminuíram", "perdi seguidores"): chame consultar_pedido IMEDIATAMENTE, SEM argumentos (ela já busca pelo telefone deste contato) — NÃO peça o @ antes de chamar. Só peça o @ e chame de novo (com usuario=<@>) se o resultado vier encontrado:false. Com o resultado: (a) se ehBrasileirosReais=true: NÃO envie link de reposição — diga que você VAI VERIFICAR o pedido dele e informe que os *brasileiros reais* são o serviço MAIS ESTÁVEL, com taxa de queda de apenas 5-6% em período superior a um mês; (b) se NÃO for orgânico: responda CURTÍSSIMO — SÓ 1 frase + o link, MAIS NADA. É QUEDA, então o caminho é SEMPRE o refil, INDEPENDENTE do status (mesmo "concluído"). PROIBIDO: dizer "entrega finalizada/concluída", explicar por que caiu, falar que "mistos são internacionais" ou que "a taxa de queda é maior", ou qualquer justificativa/ressalva. PROIBIDO fechar com "se precisar de mais alguma coisa". Responda EXATAMENTE assim e pare: "Claro! Você pode repor os seguidores que caíram por aqui: <refilLink>". Se não vier refilLink no resultado, diga que vai verificar e encaminhe pro suporte (chamar_suporte).',
    '"PAGUEI E NÃO RECEBI" / "meu pedido não chegou": chame consultar_pedido IMEDIATAMENTE, SEM argumentos (ela busca pelo telefone deste contato) — NÃO peça o @ antes de chamar. Só peça o @ e chame de novo com "usuario" se o resultado vier encontrado:false. SEMPRE confira o resultado antes de responder, não invente. O campo "status" só vem como "em andamento" ou "concluído" — comunique SOMENTE esses dois. NUNCA fale "parcial", "incompleto", "erro", "cancelado", nem cite quantos faltam/entraram. Com o resultado: (a) se status="em andamento" e ehBrasileirosReais=true: explique com calma que os *brasileiros reais* têm prazo de ATÉ 48H e a entrega é gradual/natural — tranquilize que está dentro do prazo e vai completar; (b) se status="em andamento" e NÃO for orgânico: dê uma resposta tranquilizadora e genérica — "seu pedido está *em andamento* e sendo processado, pode ficar tranquilo(a) que vai completar"; (c) se status="concluído": diga que a entrega já foi finalizada (peça pra ele conferir/atualizar o app); (d) se perfilPrivado=true: avise com gentileza que a entrega NÃO acontece com o perfil privado e peça pra deixar o perfil PÚBLICO até finalizar. Se NÃO encontrar o pedido nem por telefone nem por @, acione o suporte (chamar_suporte).',
    'Estilo WhatsApp: sem emojis. Negrito com UM asterisco (*assim*), nunca com dois. Mensagens curtas e diretas. Links/URLs: mande a URL CRUA (ex.: https://agenciaoppus.site/refil?token=abc), NUNCA em markdown [texto](url) — o WhatsApp não renderiza markdown.',
    'PREÇO: use SEMPRE o que as ferramentas retornam, na MESMA mensagem — nunca invente nem calcule preço, e nunca diga só "vou verificar" e pare. Se o cliente pedir "o mais barato"/menor pacote, mostre a MENOR quantidade da tabela.',
    'Curtidas e visualizações precisam do LINK do post — peça o link (não liste posts). Depois pergunte se quer dividir a quantidade em mais de um post (split, divide igualmente).',
    'Ao validar o perfil, confirme com o @ e o NÚMERO REAL de seguidores que a ferramenta validar_perfil retornou. NUNCA escreva "NOME" ou "N" literalmente — use os valores reais. Ex.: "Achei seu perfil! @fulano, 12.345 seguidores. Confere?". Se o nome vier vazio, é só omitir o nome.',
    '"SÃO FALSOS/FAKE/ROBÔS?" (sobre mistos ou internacionais): responda CURTO e GENÉRICO, SEM detalhamento e SEM dizer que são falsos/fake. NÃO faça comparação longa entre os tipos e NÃO empurre os orgânicos. NUNCA afirme que algum tipo é falso, fake ou robô. Ex. de tom: "São contas reais que dão número e autoridade ao seu perfil. Quer que eu te mostre a tabela?". Não volunteie ressalvas nem defeitos.',
    'Pedido múltiplo (vários serviços numa mensagem): avise que faz um de cada vez, organizado, e comece pelo primeiro.',
    'Fluxo de venda: 1) descubra serviço + tipo; 2) mostre a tabela e pergunte a quantidade; 3) cote o valor exato; 4) peça o @ e valide (confirme nome+seguidores); 5) se tem post, peça o link (+ split); 6) colete SÓ nome e e-mail; 7) confirme o resumo e gere o Pix.',
    'GERAR O PIX — REGRA CRÍTICA: assim que tiver serviço, quantidade, @ (validado), nome e e-mail, chame a ferramenta *gerar_pix* IMEDIATAMENTE, no MESMO turno. NUNCA diga "vou gerar o Pix", "aguarde um momento", "estou processando" e pare sem chamar a ferramenta — isso deixa o cliente esperando pra sempre e o pedido NÃO é criado. Não anuncie a intenção: execute (chame gerar_pix) e só então confirme. Se faltar algum dado, peça só o que falta.',
    'Pix: ao gerar (a ferramenta gerar_pix rodou com ok), NUNCA escreva no texto o código copia-e-cola, o QR Code, nenhum link/URL, nem rótulos tipo "Código Pix:" ou "QR Code:". O sistema envia AUTOMATICAMENTE, em mensagens separadas, a instrução de como copiar e o código copia-e-cola — você NÃO precisa (nem deve) colocar nada disso. Sua mensagem deve ter só: a confirmação do valor/resumo do pedido e o fechamento "Fico no aguardo da confirmação do seu pagamento para liberar seu pedido." NÃO use "se precisar de mais alguma coisa, estou à disposição" nesse momento. Se gerar_pix retornar erro, NÃO diga que deu certo — peça pra tentar de novo ou acione o suporte. Nunca peça CPF (é gerado automaticamente).',
    'TIPO no fechamento (CRÍTICO p/ o preço): ao chamar gerar_pix, use EXATAMENTE o mesmo tipo que você cotou e o cliente confirmou. "brasileiros reais"/"reais"/"de verdade"/"orgânico" = tipo *organicos* — NUNCA mande como "brasileiros" (isso troca o produto e cobra o valor errado). "brasileiros" (sem "reais") = brasileiros simples. Na dúvida do tipo, confirme com o cliente antes de gerar. O valor do Pix TEM que ser o mesmo que você cotou.',
    'E-MAIL: para pedir o e-mail, pergunte de forma simples e direta (ex.: "Pra finalizar, me passa seu melhor e-mail?"). Como normalmente é a PRIMEIRA compra, você NÃO tem o e-mail do cliente — então NUNCA diga "parece que não recebi seu e-mail", "faltou seu e-mail" ou algo que sugira que ele já enviou. Só peça.',
    'Se o cliente pedir atendente humano ou fizer reclamação séria, use chamar_suporte e pare de vender.',
  ].join('\n');
}

const TOOLS = [
  { type: 'function', function: { name: 'cotar_preco', description: 'Preço oficial de UM pacote de um serviço.', parameters: { type: 'object', properties: { servico: { type: 'string', enum: ['seguidores', 'curtidas', 'visualizacoes'] }, tipo: { type: 'string', description: 'mistos | brasileiros | organicos (só p/ seguidores/curtidas)' }, quantidade: { type: 'integer' } }, required: ['servico', 'quantidade'] } } },
  { type: 'function', function: { name: 'tabela_precos', description: 'Tabela inteira de quantidades e preços de um serviço para o cliente escolher.', parameters: { type: 'object', properties: { servico: { type: 'string', enum: ['seguidores', 'curtidas', 'visualizacoes'] }, tipo: { type: 'string' } }, required: ['servico'] } } },
  { type: 'function', function: { name: 'validar_perfil', description: 'Valida o @ do Instagram e retorna nome + nº de seguidores para confirmar.', parameters: { type: 'object', properties: { usuario: { type: 'string' } }, required: ['usuario'] } } },
  { type: 'function', function: { name: 'consultar_pedido', description: 'Consulta o pedido do cliente que diz "paguei e não recebi". Sem argumentos, busca pelo telefone do contato; passe "usuario" (@) só se pedir depois de não achar pelo telefone. Retorna tipo do serviço, tempo desde o pagamento, se é orgânico (prazo 48h), status no fornecedor e se o perfil está privado.', parameters: { type: 'object', properties: { usuario: { type: 'string', description: '@ ou link do perfil — só quando não achar pelo telefone' } }, required: [] } } },
  { type: 'function', function: { name: 'chamar_suporte', description: 'Aciona o suporte humano e para de vender.', parameters: { type: 'object', properties: { motivo: { type: 'string', description: 'Resumo CURTO (1 frase, poucas palavras) do que o cliente pediu/precisa — vira a notificação do atendente.' } }, required: ['motivo'] } } },
  { type: 'function', function: { name: 'gerar_pix', description: 'Cria o pedido e gera o Pix. Não pede CPF. O preço é definido pelo tipo — passe SEMPRE o tipo exato que o cliente escolheu.', parameters: { type: 'object', properties: { servico: { type: 'string', enum: ['seguidores', 'curtidas', 'visualizacoes'] }, tipo: { type: 'string', enum: ['mistos', 'brasileiros', 'organicos'], description: 'OBRIGATÓRIO para seguidores/curtidas. "brasileiros reais", "reais", "de verdade", "orgânico" = organicos (NÃO brasileiros). "brasileiros" = brasileiros simples. Use EXATAMENTE o tipo cotado com o cliente.' }, quantidade: { type: 'integer' }, usuario: { type: 'string' }, nome: { type: 'string' }, email: { type: 'string' }, post_links: { type: 'array', items: { type: 'string' } } }, required: ['servico', 'quantidade', 'usuario', 'nome', 'email'] } } },
];

async function runTool(name, args, ctx) {
  try {
    if (name === 'cotar_preco') return await sales.quote(args || {});
    if (name === 'tabela_precos') return sales.priceTable(args || {});
    if (name === 'validar_perfil') {
      const vr = await sales.validateProfile((args && args.usuario) || '');
      if (vr && vr.ok) { ctx.validated = true; ctx.validatedUsername = vr.username || String((args && args.usuario) || '').replace(/^@+/, ''); }
      return vr;
    }
    if (name === 'consultar_pedido') {
      let usuario = (args && args.usuario) || '';
      // Fallback robusto: se o modelo esqueceu de passar o @, mas a mensagem do cliente
      // É basicamente um @/handle (ou link do IG), usa ela como usuário.
      if (!usuario) {
        const t = String(ctx.text || '').trim();
        if (/instagram\.com\//i.test(t) || /^@?[a-zA-Z0-9._]{2,30}$/.test(t)) usuario = t;
      }
      const r = await sales.consultarPedido({ telefone: ctx.phone, usuario });
      ctx.lastConsulta = r;
      return r;
    }
    if (name === 'chamar_suporte') { ctx.support = true; return await sales.flagSupport(ctx.phone, (args && args.motivo) || ''); }
    if (name === 'gerar_pix') {
      const r = await sales.createPixOrder(Object.assign({ phone: ctx.phone }, args || {}));
      if (r.ok && r.pixCopiaECola) ctx.pixToSend = r.pixCopiaECola; // enviado em mensagem separada
      // NÃO exponha o copia-e-cola nem o link do QR ao modelo — senão ele cola no texto.
      // O sistema envia o código em mensagem separada + a instrução de como copiar.
      if (r.ok) return { ok: true, valor: r.valorLabel, resumo: r.resumo, codigo_e_instrucao_enviados_automaticamente: true, aviso: 'NÃO escreva o código Pix, QR Code ou qualquer link no texto — o sistema já envia o copia-e-cola e a instrução em mensagens separadas.' };
      return r;
    }
    return { ok: false, error: 'ferramenta_desconhecida' };
  } catch (e) { return { ok: false, error: (e && e.message) || 'erro_ferramenta' }; }
}

async function openaiChat(messages, useTools) {
  const key = String(process.env.OPENAI_API_KEY || '').trim();
  const body = { model: OPENAI_MODEL, messages, temperature: 0.3 };
  if (useTools) { body.tools = TOOLS; body.tool_choice = 'auto'; } else { body.tool_choice = 'none'; }
  const resp = await axios.post('https://api.openai.com/v1/chat/completions', body, { headers: { Authorization: 'Bearer ' + key, 'Content-Type': 'application/json' }, timeout: 60000, validateStatus: () => true });
  if (resp.status < 200 || resp.status >= 300) throw new Error('openai_http_' + resp.status + ': ' + JSON.stringify(resp.data && resp.data.error || resp.data).slice(0, 200));
  return resp.data && resp.data.choices && resp.data.choices[0] && resp.data.choices[0].message;
}

// Entrada principal. msg: { phone, text }. sendText(to, texto) envia a resposta.
async function handleAgentMessage(msg, sendText) {
  const phone = String(msg && (msg.phone || msg.from) || '').trim();
  const text = String(msg && msg.text || '').trim();
  if (!phone || !text) return;

  if (!isEnabled()) { return handleSalesMessage(msg, sendText); } // fallback determinístico

  const ctx = { phone, support: false, pixToSend: '', text };
  // Histórico DURÁVEL guarda só turnos de texto (user/assistant). O "scaffolding" das
  // ferramentas (tool_calls + resultados) fica só na variável de trabalho `work` desta
  // execução — se fosse pro histórico, o corte em MAX_TURNS podia quebrar o par
  // tool_call↔resultado e a OpenAI rejeitava (erro 400 → "instabilidade").
  const hist = await loadHistory(phone);
  const work = [{ role: 'system', content: systemPrompt() }, ...hist, { role: 'user', content: text }];
  let finalText = '';

  try {
    for (let round = 0; round < MAX_TOOL_ROUNDS; round++) {
      const m = await openaiChat(work, true);
      if (!m) break;
      work.push(m);
      const calls = Array.isArray(m.tool_calls) ? m.tool_calls : [];
      if (!calls.length) { finalText = String(m.content || '').trim(); break; }
      for (const call of calls) {
        let args = {}; try { args = JSON.parse(call.function.arguments || '{}'); } catch (_) {}
        const result = await runTool(call.function.name, args, ctx);
        work.push({ role: 'tool', tool_call_id: call.id, content: JSON.stringify(result) });
      }
    }
    // Rede de segurança 1: o modelo PROMETEU gerar o Pix ("vou gerar", "aguarde",
    // "processando…") mas NÃO chamou a ferramenta gerar_pix → o pedido nunca é criado
    // e o cliente fica esperando. Força uma rodada mandando chamar gerar_pix com o
    // contexto já coletado. Só dispara quando não geramos Pix e não é caso de suporte.
    if (finalText && !ctx.pixToSend && !ctx.support && /(vou\s+gerar|gerar(?:\s+o|\s+seu)?\s+(?:pix|pedido|c[óo]digo)|gerando|aguarde|um\s+momento|processand|estou\s+(?:gerando|criando|preparando)|j[áa]\s+vou\s+(?:gerar|criar))/i.test(finalText)) {
      try {
        work.push({ role: 'system', content: 'Você prometeu gerar o Pix mas NÃO chamou a ferramenta. O cliente já forneceu os dados na conversa. Chame AGORA a ferramenta gerar_pix com serviço, tipo (se houver), quantidade, usuario (@), nome e e-mail já coletados. NÃO peça informação nova e NÃO peça para aguardar.' });
        for (let r2 = 0; r2 < 2; r2++) {
          const mf = await openaiChat(work, true);
          if (!mf) break;
          work.push(mf);
          const calls2 = Array.isArray(mf.tool_calls) ? mf.tool_calls : [];
          if (!calls2.length) { const t = String(mf.content || '').trim(); if (t) finalText = t; break; }
          for (const call of calls2) {
            let args2 = {}; try { args2 = JSON.parse(call.function.arguments || '{}'); } catch (_) {}
            const result2 = await runTool(call.function.name, args2, ctx);
            work.push({ role: 'tool', tool_call_id: call.id, content: JSON.stringify(result2) });
          }
        }
        // Gerou o Pix mas o texto final ainda é a promessa antiga → fecha com confirmação.
        if (ctx.pixToSend && /(aguarde|um\s+momento|vou\s+gerar|processand)/i.test(finalText)) {
          const mc = await openaiChat(work, false);
          const t = String((mc && mc.content) || '').trim();
          finalText = t || 'Prontinho! Gerei seu Pix — o código copia-e-cola vem na próxima mensagem. Fico no aguardo da confirmação do seu pagamento para liberar seu pedido.';
        }
      } catch (_) {}
    }
    // Rede de segurança 2: esgotou só chamando ferramentas sem texto → força UMA resposta final.
    if (!finalText) {
      const m2 = await openaiChat(work, false);
      finalText = String((m2 && m2.content) || '').trim();
    }
  } catch (e) {
    finalText = 'Desculpa, não peguei sua última mensagem — pode repetir, por favor?';
    try { console.error('❌ whatsappAgent erro:', e && e.message); } catch (_) {}
  }

  // FORÇA a resposta de QUEDA (não-orgânico): SÓ o link, sem status/entrega/justificativa.
  // O modelo tende a "explicar demais" quando o pedido está concluído — aqui garantimos
  // o tom seco pedido, independente do que ele escreveu. Só quando: intenção de queda
  // (nesta msg ou nas últimas do cliente) + pedido encontrado, NÃO orgânico e com refil.
  try {
    const quedaRe = /(ca[íi]ram|ca[íi]u|sumir|sumiram|sumiu|diminu[íi]|perdi[^.]*seguidor|queda|despenc|baixaram|baixou)/i;
    const userMsgs = [text].concat((hist || []).filter((h) => h && h.role === 'user').slice(-2).map((h) => String(h.content || '')));
    const isQueda = userMsgs.some((t) => quedaRe.test(t));
    const cp = ctx.lastConsulta;
    if (isQueda && cp && cp.encontrado && !ctx.support && !ctx.pixToSend) {
      if (cp.ehBrasileirosReais) {
        // Orgânico: NÃO manda link — verifica + estabilidade (5-6%). Seco, sem status.
        finalText = 'Pode ficar tranquilo(a)! Vou verificar seu pedido, mas fica despreocupado(a): os *brasileiros reais* são o nosso serviço mais estável, com taxa de queda de apenas 5-6% em período superior a um mês.';
        ctx.quedaResolved = true;
        // Queda em brasileiros reais é caso pra HUMANO: trava a IA + notifica no ntfy.
        try {
          const c = await getCollection('whatsapp_contacts');
          await c.updateOne({ _id: String(phone) }, { $set: { flag: 'queda_reais', botPaused: true, supportReason: 'Queda em brasileiros reais', supportAt: new Date().toISOString() } }, { upsert: true });
        } catch (_) {}
        try {
          const base = String(process.env.PUBLIC_BASE_URL || process.env.INTERNAL_BASE || '').replace(/\/+$/, '');
          await sales.sendNtfy({
            title: '+' + String(phone).replace(/\D/g, '') + ' - queda brasileiros reais',
            message: 'Cliente reclamou de queda e o último pedido é *brasileiros reais*. A IA foi pausada — verifique pelo CRM.',
            priority: 'high',
            tags: 'chart_with_downwards_trend',
            click: base ? (base + '/painel/ia-crm') : undefined,
          });
        } catch (_) {}
      } else if (cp.refilLink) {
        // Não-orgânico: só o link, sem status/entrega/justificativa.
        finalText = 'Claro! Você pode repor os seguidores que caíram por aqui: ' + cp.refilLink;
        ctx.quedaResolved = true;
      }
    }
  } catch (_) {}

  // Salva SÓ o turno de texto (user + resposta final) — histórico sempre válido.
  const newHist = hist.concat([{ role: 'user', content: text }]);
  if (finalText) newHist.push({ role: 'assistant', content: finalText });
  await saveHistory(phone, newHist);

  // Pequeno atraso antes de responder (parece mais humano; não "atropela" o cliente).
  if (REPLY_DELAY_MS > 0) { try { await new Promise((r) => setTimeout(r, REPLY_DELAY_MS)); } catch (_) {} }

  // Se vamos mandar o Pix separado, tira qualquer cópia do código de dentro do texto
  // (evita o cliente receber o copia-e-cola DUPLICADO).
  if (ctx.pixToSend) finalText = stripPixFromText(finalText, ctx.pixToSend);
  finalText = sanitizeForWhatsapp(finalText);
  if (finalText) { try { await sendText(phone, finalText); } catch (_) {} }
  // Entrega do Pix em 3 mensagens SEPARADAS, com 2s entre cada (não despeja tudo de
  // uma vez): 1) instrução de como copiar; 2) o copia-e-cola SOZINHO num balão, pra o
  // cliente copiar tocando/segurando na mensagem (não em link — link não valida).
  if (ctx.pixToSend) {
    const gap = () => new Promise((r) => setTimeout(r, MSG_GAP_MS));
    const instrucao = 'Para pagar, copie a *mensagem abaixo* e cole na opção *Pix Copia e Cola* do seu banco.\n\nImportante: copie tocando e *segurando no balão* da mensagem abaixo e toque em *Copiar* — não clique como se fosse um link, senão o pagamento não valida.';
    await gap();
    try { await sendText(phone, instrucao); } catch (_) {}
    await gap();
    try { await sendText(phone, ctx.pixToSend); } catch (_) {}
  }

  // Sinaliza p/ a camada do WhatsApp que a IA acabou de PEDIR a confirmação do @ (para
  // agendar um "cutucão" se o cliente sumir). Só quando validou e não fechou/parou.
  return { awaitingConfirm: !!(ctx.validated && !ctx.pixToSend && !ctx.support && !ctx.quedaResolved), username: ctx.validatedUsername || '' };
}

// ── Fallback determinístico (sem OPENAI_API_KEY): guia mínimo, não trava o atendimento ──
async function handleSalesMessage(msg, sendText) {
  const phone = String(msg && (msg.phone || msg.from) || '').trim();
  const text = String(msg && msg.text || '').toLowerCase();
  let reply = 'Oi! Vendo *seguidores*, *curtidas* e *visualizações* pra Instagram. Qual você quer?';
  const key = sales.normalizeServico(text);
  if (key) {
    const t = sales.priceTable({ servico: key, tipo: /brasileir/.test(text) ? 'brasileiros' : (/organic|orgânic|real|reais/.test(text) ? 'organicos' : 'mistos') });
    if (t.ok) {
      const linhas = t.itens.slice(0, 8).map((i) => `${i.quantidade} ${t.unit} - ${i.preco}`).join('\n');
      reply = `Tabela de *${t.servico}*${t.tipo ? ' (' + t.tipo + ')' : ''}:\n${linhas}\n\nQual quantidade você quer?`;
    }
  }
  try { await sendText(phone, reply); } catch (_) {}
}

module.exports = { isEnabled, handleAgentMessage, handleSalesMessage, clearHistory, systemPrompt };
