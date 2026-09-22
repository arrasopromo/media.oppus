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

// ── Travas de conteúdo da resposta ────────────────────────────────────────────
// O modelo às vezes INVENTA preço (1.000 visualizações por R$ 39,90; um pacote de 2.500
// que não existe). Toda menção "N <serviço> ... R$ X" é conferida contra as tabelas oficiais.
const { tabelaSeguidores, tabelaCurtidas, tabelaVisualizacoes, parsePrecoToCents } = require('./pricing.js');
function _catDe(unidade) {
  const u = String(unidade || '').toLowerCase();
  if (/seguidor/.test(u)) return 'seguidores';
  if (/curtida/.test(u)) return 'curtidas';
  return 'visualizacoes';
}
function _tabelasDe(cat) {
  if (cat === 'seguidores') return tabelaSeguidores;
  if (cat === 'curtidas') return tabelaCurtidas;
  return tabelaVisualizacoes;
}
function acharPrecosErrados(texto) {
  const erros = [];
  // O trecho entre a quantidade e o preço não pode ter OUTRA quantidade nem "total"/"soma":
  // senão "1000 seguidores e 150 curtidas, total R$ 84,80" viraria preço errado.
  const re = /(\d{1,3}(?:[.\s]\d{3})+|\d+)\s*(seguidor(?:es)?|curtidas?|visualiza[çc][õo]es|views)\b((?:(?!\d{2,}\s*(?:seguidor|curtida|visualiza|views)|total|soma|ao todo)[^\n]){0,45}?)R\$\s*(\d{1,3}(?:\.\d{3})*,\d{2})/gi;
  let m;
  while ((m = re.exec(String(texto || '')))) {
    const qtd = Number(String(m[1]).replace(/[.\s]/g, ''));
    const cents = Number(String(m[4]).replace(/\./g, '').replace(',', ''));
    const cat = _catDe(m[2]);
    if (!(qtd > 0) || !(cents > 0)) continue;
    // Tipo citado na própria frase → confere SÓ na tabela desse tipo. Antes valia qualquer
    // tabela: "500 seguidores brasileiros reais por R$ 39,90" passava porque 39,90 é o
    // preço de 500 *brasileiros*.
    const trechoTipo = String(m[3] || '');
    const chaves = /reais|real\b|org[âa]nic/i.test(trechoTipo) ? ['organicos', 'curtidas_reais']
      : /brasileir/i.test(trechoTipo) ? ['brasileiros', 'curtidas_brasileiras']
      : /mist|mundia|internac/i.test(trechoTipo) ? ['mistos'] : null;
    const tabelas = _tabelasDe(cat);
    const usar = chaves ? chaves.filter((k) => tabelas[k]).map((k) => tabelas[k]) : Object.values(tabelas);
    const validos = new Set();
    for (const tab of (usar.length ? usar : Object.values(tabelas))) for (const it of (tab || [])) if (Number(it.q) === qtd) validos.add(parsePrecoToCents(it.p));
    if (!validos.has(cents)) erros.push({ cat, qtd, preco: m[4], trecho: m[0].slice(0, 80) });
  }
  return erros;
}
function tabelaOficialTexto(cat) {
  const nomes = cat === 'curtidas'
    ? { mistos: '*Curtidas mistas*', brasileiros: '*Curtidas brasileiras*', organicos: '*Curtidas brasileiras reais* (orgânicas)' }
    : { mistos: '*Mistos*', brasileiros: '*Brasileiros*', organicos: '*Brasileiros reais* (orgânicos)', visualizacoes_reels: '*Visualizações (reels)*' };
  const partes = [];
  const vistas = new Set();
  for (const [tipo, tab] of Object.entries(_tabelasDe(cat))) {
    // Só os tipos vendidos (curtidas_brasileiras/curtidas_reais são apelidos internos
    // das mesmas tabelas — antes saíam duplicadas e com o nome cru pro cliente).
    if (!nomes[tipo]) continue;
    const itens = (tab || []).filter((x) => Number(x.q) >= 150).map((x) => '- ' + Number(x.q).toLocaleString('pt-BR') + ': ' + x.p);
    const assinatura = itens.join('|');
    if (!itens.length || vistas.has(assinatura)) continue;
    vistas.add(assinatura);
    partes.push(nomes[tipo] + '\n' + itens.join('\n'));
  }
  return partes.join('\n\n');
}
// Cliente pediu "brasileiros": a resposta TEM que trazer as 2 opções (brasileiros e
// brasileiros reais). Se o modelo mandou só a tabela de brasileiros, anexa a de reais.
function garantirBrasileirosReais(texto) {
  const t = String(texto || '');
  if (/reais\b|org[âa]nic/i.test(t.replace(/R\$\s*[\d.,]+/g, '').replace(/\bs[óo] tenho \d+ reais/gi, ''))) return t;
  const re = /(\d{1,3}(?:[.\s]\d{3})+|\d+)\s*(seguidor(?:es)?|curtidas?)\b[^\n]{0,20}?R\$\s*(\d{1,3}(?:\.\d{3})*,\d{2})/gi;
  const hits = { seguidores: 0, curtidas: 0 };
  let m;
  while ((m = re.exec(t))) {
    const cat = _catDe(m[2]);
    const tab = (_tabelasDe(cat) || {}).brasileiros || [];
    const qtd = Number(String(m[1]).replace(/[.\s]/g, ''));
    const it = tab.find((x) => Number(x.q) === qtd);
    if (it && parsePrecoToCents(it.p) === Number(String(m[3]).replace(/\./g, '').replace(',', ''))) hits[cat]++;
  }
  const cat = hits.seguidores >= 3 ? 'seguidores' : (hits.curtidas >= 3 ? 'curtidas' : '');
  if (!cat) return t;
  const itens = ((_tabelasDe(cat) || {}).organicos || []).filter((x) => Number(x.q) >= 150).map((x) => '- ' + Number(x.q).toLocaleString('pt-BR') + ' ' + cat + ': ' + x.p);
  if (!itens.length) return t;
  const bloco = 'Também temos os *Brasileiros reais* (orgânicos) — perfis reais e ativos, o serviço mais estável:\n' + itens.join('\n');
  // Mantém a pergunta final ("Qual quantidade…?") no fim da mensagem.
  const idx = t.search(/\n[^\n]*\?\s*$/);
  return idx > 0 ? (t.slice(0, idx).trimEnd() + '\n\n' + bloco + '\n\n' + t.slice(idx).trim()) : (t.trimEnd() + '\n\n' + bloco);
}
// Divide em frases mantendo as quebras de linha.
function _frases(linha) { return String(linha).split(/(?<=[.!?])\s+/); }
// "Queda de 5-6%" é SÓ dos brasileiros reais. O modelo atribuía isso a mistos/brasileiros.
function corrigirTaxaQueda(texto) {
  return String(texto || '').split('\n').map((linha) => _frases(linha).filter((f) => !(/5\s*(?:-|–|a)\s*6\s*%/.test(f) && !/reais|org[âa]nic|organic/i.test(f))).join(' ')).join('\n').replace(/\n{3,}/g, '\n\n').trim();
}
// Link de reposição só quando o assunto é queda/reposição — não para quem espera a entrega.
function tirarLinkReposicao(texto) {
  return String(texto || '').split('\n').map((linha) => /refil\?token=/i.test(linha) ? _frases(linha).filter((f) => !/refil\?token=/i.test(f)).join(' ') : linha).join('\n').replace(/\n{3,}/g, '\n\n').trim();
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

function systemPrompt(dispTipos) {
  const _nomeT = { mistos: 'mistos', brasileiros: 'brasileiros', organicos: 'orgânicos (brasileiros reais)' };
  const _d = dispTipos || { seguidores: ['mistos', 'brasileiros', 'organicos'], curtidas: ['mistos', 'brasileiros', 'organicos'] };
  const _seg = (_d.seguidores || []).map((t) => _nomeT[t] || t).join(', ') || 'nenhum tipo';
  const _cur = (_d.curtidas || []).map((t) => _nomeT[t] || t).join(', ') || 'nenhum tipo';
  return [
    `DISPONIBILIDADE ATUAL (REGRA ACIMA DE TUDO): em SEGUIDORES só trabalhamos com ${_seg}; em CURTIDAS só com ${_cur}. NUNCA ofereça, cite tabela ou cote um tipo que NÃO esteja nessa lista — as ferramentas recusam tipo indisponível. Se o cliente pedir um tipo indisponível, diga com gentileza que no momento só temos os tipos acima e ofereça esses. VISUALIZAÇÕES (reels) seguem normais.`,
    'Você é um atendente de vendas do Instagram pelo WhatsApp da Agência Oppus. Feche a venda AQUI, pela conversa — nunca mande o cliente pro site.',
    'Serviços que você vende: SEGUIDORES, CURTIDAS e VISUALIZAÇÕES (reels). Os TIPOS disponíveis de seguidores/curtidas são SEMPRE os listados na regra de DISPONIBILIDADE ATUAL acima — nunca ofereça um tipo fora dela.',
    'GRAFIA: escreva os tipos SEMPRE exatamente assim: *mistos*, *brasileiros*, *orgânicos* (ou "brasileiros reais"). Nunca escreva "mistoss", "misto s" ou variações. A marca é sempre *Oppus* (nunca OPPUS ou Oppuss).',
    'Dados internos/cadastrais da empresa (CNPJ, razão social, endereço, contratos, documentos): NÃO compartilhe — mas NUNCA recuse de forma seca ("não posso fornecer"). Responda de forma PROFISSIONAL e acolhedora, sem expor o documento: reforce que a Oppus é uma empresa séria e atuante no mercado, que o pagamento é 100% seguro (Pix via gateway) e a entrega é garantida, e conduza de volta pro pedido. Ex.: "Nossos dados cadastrais são internos, mas pode ficar tranquilo(a): a Oppus é uma empresa séria, o pagamento é 100% seguro por Pix e a entrega é garantida. Posso te ajudar a escolher o pacote ideal?".',
    'Se o cliente insistir muito em documentos/CNPJ ou demonstrar desconfiança séria, ofereça acionar o suporte humano (chamar_suporte) em vez de repetir a recusa.',
    '"É GOLPE?" / desconfiança: NUNCA responda de forma vaga ou evasiva. Responda com CONFIANÇA, empatia e provas concretas: a Oppus é uma empresa séria e atuante, com milhares de clientes atendidos; o pagamento é 100% seguro por Pix; a entrega é garantida no prazo do serviço; e tudo é transparente — as infos de cada serviço estão no áudio inicial do site, na seção "Veja como funciona" (um tour pelo site), no FAQ (no final do site), e você ainda recebe um e-mail com o detalhamento do pedido ANTES de pagar. Depois de tranquilizar, retome o atendimento/pedido.',
    'RECLAMAÇÃO "veio estrangeiro/indiano" ou "não era o que eu esperava": NÃO acione o suporte de cara e NÃO acuse o cliente. Primeiro explique com calma e empatia que o site tem 3 TIPOS de seguidores — *mistos* (exclusivamente internacionais/mundiais, sem contas brasileiras), *brasileiros* e *brasileiros reais* (orgânicos) — e que cada tipo tem uma descrição detalhada no site. Reforce que essa informação é passada no áudio que aparece no início do site, na seção "Veja como funciona" (tour pelo site), no FAQ (no final do site), e também no e-mail com o detalhamento do pedido enviado ANTES do pagamento. Provavelmente a pessoa comprou o *mistos* (internacional). Só acione o suporte humano (chamar_suporte) se, DEPOIS dessa explicação, o cliente continuar insatisfeito ou mantiver ameaça (ex.: processo).',
    'PRAZOS DE ENTREGA (informe o correto conforme o tipo, NÃO prometa imediato para todos): MISTOS e BRASILEIROS começam a entrar logo após a confirmação do pagamento (entrega rápida). ORGÂNICOS / "brasileiros reais" têm prazo de ATÉ 48H para finalizar — a entrega NÃO é imediata, é gradual e natural. Curtidas e visualizações começam rápido após o pagamento.',
    'DÚVIDA SOBRE OS TIPOS ("são brasileiros?", "o que são orgânicos?", "qual a diferença?"): explique com clareza e simpatia — *Mistos*: são EXCLUSIVAMENTE contas internacionais/mundiais (do mundo todo). Apesar do nome "mistos", NÃO combinam contas brasileiras com internacionais — NÃO incluem brasileiros. São mais em conta e dão número e autoridade ao perfil. *Brasileiros*: seguidores do Brasil. *Brasileiros reais* (orgânicos): 100% reais e ativos, os MAIS ESTÁVEIS (queda de só 5-6% em mais de um mês) — o pacote premium. NUNCA cite perfis de exemplo/terceiros para provar a qualidade: explique pelas características do serviço.',
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
    'Curtidas e visualizações precisam do LINK do post — peça o link (não liste posts). DIVISÃO EM VÁRIOS POSTS (CRÍTICO): ANTES de gerar o Pix, pergunte se ele quer dividir a quantidade em mais de um post e, se quiser, peça pra mandar TODOS os links. Só chame gerar_pix quando ele disser que terminou (ou que é um post só). Clientes mandam vários links seguidos: se ele acabou de mandar um link e pode mandar mais, pergunte "É só esse post ou quer dividir em mais?" em vez de gerar o Pix na hora. Em gerar_pix, passe em post_links TODOS os links de post que o cliente mandou para este pedido na conversa, não só o último.',
    'LINK DEPOIS DO PIX: se o cliente mandar mais links depois que você gerou o Pix e ele AINDA NÃO pagou, chame gerar_pix de novo com TODOS os links (o sistema acrescenta no MESMO pedido, sem cobrar de novo) e diga quantos posts ficaram. Se ele JÁ PAGOU e mandou links novos, NÃO gere Pix: diga que vai encaminhar os posts pra equipe distribuir e chame chamar_suporte.',
    'Ao validar o perfil, confirme com o @ e o NÚMERO REAL de seguidores que a ferramenta validar_perfil retornou. NUNCA escreva "NOME" ou "N" literalmente — use os valores reais. Ex.: "Achei seu perfil! @fulano, 12.345 seguidores. Confere?". Se o nome vier vazio, é só omitir o nome.',
    '"SÃO FALSOS/FAKE/ROBÔS?" (sobre mistos ou internacionais): responda CURTO e GENÉRICO, SEM detalhamento e SEM dizer que são falsos/fake. NÃO faça comparação longa entre os tipos e NÃO empurre os orgânicos. NUNCA afirme que algum tipo é falso, fake ou robô. Ex. de tom: "São contas reais que dão número e autoridade ao seu perfil. Quer que eu te mostre a tabela?". Não volunteie ressalvas nem defeitos.',
    'Pedido múltiplo (vários serviços numa mensagem): avise que faz um de cada vez, organizado, e comece pelo primeiro.',
    'TIPO ANTES DO PREÇO (CRÍTICO): NUNCA mostre tabela/valor de seguidores ou curtidas sem deixar claro DE QUAL TIPO são. Se o cliente ainda não escolheu, PERGUNTE primeiro (*mistos*, *brasileiros* ou *brasileiros reais*). Se as ferramentas retornarem tipoAssumido=true, os preços são de *mistos* (internacionais) — então ou você escreve isso explicitamente na mensagem, ou pergunta o tipo antes de cotar. Mostrar preço de *mistos* como se fosse "o preço" engana o cliente e gera reclamação depois.',
    '@ OBRIGATÓRIO EM TODO PEDIDO (inclusive curtidas e visualizações, mesmo já tendo o link do post): peça o @ do Instagram e valide com validar_perfil ANTES de gerar o Pix. NUNCA use o e-mail (ex.: fulano@hotmail.com) nem o nome do cliente como @.',
    'IMAGEM DO CLIENTE: mensagens "[O cliente enviou uma IMAGEM… Conteúdo da imagem: …]" trazem o que está na foto. Use isso: se aparecer o @ do Instagram, use esse @ (ex.: consultar_pedido com usuario); se for print da página de reposição com erro, trate como ERRO NA REPOSIÇÃO; se for comprovante, trate como pagamento enviado. Não diga que não consegue ver imagens.',
    'LINK DE REPOSIÇÃO: só envie o refilLink que a ferramenta consultar_pedido devolveu. NUNCA monte ou invente link (ex.: token=abc). Se não achar o pedido pelo telefone, peça o nome de usuário (@) do Instagram e consulte de novo.',
    'O @ DO CLIENTE É LITERAL: ao chamar validar_perfil ou gerar_pix, copie o @ EXATAMENTE como o cliente escreveu, caractere por caractere. NUNCA corrija, complete, abrevie ou redigite o handle — trocar uma letra faz um perfil válido parecer inexistente.',
    'Fluxo de venda: 1) descubra serviço + tipo; 2) mostre a tabela e pergunte a quantidade; 3) cote o valor exato; 4) peça o @ e valide (confirme nome+seguidores); 5) se tem post, peça o link e pergunte se vai dividir em mais posts — colete TODOS os links antes de seguir; 6) colete SÓ nome e e-mail; 7) confirme o resumo (com quantos posts, se dividir) e gere o Pix.',
    'GERAR O PIX — REGRA CRÍTICA: assim que tiver serviço, quantidade, @ (validado), nome e e-mail — e, em curtidas/visualizações, os links do post com a confirmação de que ele terminou de mandar —, chame a ferramenta *gerar_pix* IMEDIATAMENTE, no MESMO turno. NUNCA diga "vou gerar o Pix", "aguarde um momento", "estou processando" e pare sem chamar a ferramenta — isso deixa o cliente esperando pra sempre e o pedido NÃO é criado. Não anuncie a intenção: execute (chame gerar_pix) e só então confirme. Se faltar algum dado, peça só o que falta.',
    'Pix: ao gerar (a ferramenta gerar_pix rodou com ok), NUNCA escreva no texto o código copia-e-cola, o QR Code, nenhum link/URL, nem rótulos tipo "Código Pix:" ou "QR Code:". O sistema envia AUTOMATICAMENTE, em mensagens separadas, a instrução de como copiar e o código copia-e-cola — você NÃO precisa (nem deve) colocar nada disso. Sua mensagem deve ter só: a confirmação do valor/resumo do pedido e o fechamento "Fico no aguardo da confirmação do seu pagamento para liberar seu pedido." NÃO use "se precisar de mais alguma coisa, estou à disposição" nesse momento. Se gerar_pix retornar erro, NÃO diga que deu certo — peça pra tentar de novo ou acione o suporte. Nunca peça CPF (é gerado automaticamente).',
    'TIPO no fechamento (CRÍTICO p/ o preço): ao chamar gerar_pix, use EXATAMENTE o mesmo tipo que você cotou e o cliente confirmou. "brasileiros reais"/"reais"/"de verdade"/"orgânico" = tipo *organicos* — NUNCA mande como "brasileiros" (isso troca o produto e cobra o valor errado). "brasileiros" (sem "reais") = brasileiros simples. Na dúvida do tipo, confirme com o cliente antes de gerar. O valor do Pix TEM que ser o mesmo que você cotou.',
    'E-MAIL: para pedir o e-mail, pergunte de forma simples e direta (ex.: "Pra finalizar, me passa seu melhor e-mail?"). Como normalmente é a PRIMEIRA compra, você NÃO tem o e-mail do cliente — então NUNCA diga "parece que não recebi seu e-mail", "faltou seu e-mail" ou algo que sugira que ele já enviou. Só peça.',
    'ERRO NA REPOSIÇÃO (cliente diz que pediu a reposição e deu erro, erro interno, não consegue, não funciona, manda print de erro): NUNCA mande o link de reposição de novo e NUNCA responda com status do pedido ("está em andamento") — ele JÁ tentou pelo link. Diga que vai acionar o suporte para verificar e resolver, e chame chamar_suporte com o motivo.',
    'PAGAMENTO JÁ CONFIRMADO: se no histórico já existe a mensagem de *Pagamento confirmado* deste pedido, NUNCA peça o pagamento de novo, não reenvie o Pix e não diga que está aguardando a confirmação. Trate o pedido como pago: agradeça e fale da entrega.',
    'TAXA DE QUEDA: "5-6% em mais de um mês" vale SÓ para *brasileiros reais* (orgânicos). NUNCA diga isso de *mistos* ou *brasileiros* — desses diga que pode haver queda com o tempo e que seguidores têm reposição pelo link.',
    'CLIENTE PEDIU "BRASILEIROS" (seguidores ou curtidas): mostre SEMPRE as 2 opções — *Brasileiros* e *Brasileiros reais* (orgânicos, perfis reais, o mais estável) — cada tabela com seu título, e pergunte qual tipo e quantidade. Não escolha por ele nem mostre só uma. Se ele já escolheu um dos dois depois de ver as duas, siga com o escolhido.',
    'CLIENTE PERGUNTA "SÃO REAIS?" / "são de verdade?" / "são reais mesmo?" / "são contas reais?" / "quero reais": NUNCA afirme que *mistos* ou *brasileiros* (simples) são "reais", nem diga que "são contas reais e ativas". Em vez disso, diga que temos a OPÇÃO específica de *Brasileiros reais* (orgânicos) — perfis 100% reais e ativos, o serviço mais estável — e mostre essa opção (a tabela de *organicos*). Explique a diferença em 1 frase: *brasileiros* = seguidores do Brasil; *brasileiros reais* (orgânicos) = perfis reais e ativos, o pacote premium. Deixe o cliente escolher.',
    'VÁRIAS TABELAS:ao mostrar mais de uma tabela de preço, SEMPRE coloque o título do tipo em cima de cada lista (*Mistos*, *Brasileiros*, *Brasileiros reais*). Nunca mande listas seguidas sem dizer de qual tipo é cada uma.',
    'PREÇO SÓ DA FERRAMENTA: qualquer preço ou quantidade que você escrever tem que ter vindo de cotar_preco ou tabela_precos NESTA conversa. Nunca invente quantidade que não está na tabela (ex.: "2.500") nem combine preços.',
    'LINK DE REPOSIÇÃO: mande o link de reposição SÓ quando o cliente falar de queda de seguidores ou pedir reposição. Para quem está esperando a entrega (pedido em andamento), NÃO mande o link.',
    'PEDIDO JÁ PAGO: se gerar_pix responder pedido_ja_pago, ou se o cliente mandou comprovante/disse que pagou, NÃO gere outro Pix e NÃO diga que está aguardando pagamento. Confirme e fale da entrega. Se ele disser que pagou mas o sistema ainda não confirmou, diga que a confirmação pode levar alguns minutos.',
    'RECLAMAÇÃO NÃO VIRA PIX: "não chegou", "não mudou nada", "vai entrar mesmo?", "cadê minhas curtidas" são perguntas sobre um pedido JÁ PAGO — use consultar_pedido e responda. NUNCA chame gerar_pix nessas mensagens. Só use comprar_mais=true em gerar_pix quando o cliente pedir claramente para COMPRAR OUTRO pacote.',
    'PERFIL PRIVADO: se o cliente disser que o perfil está PÚBLICO, não repita que está privado. Agradeça, diga que vai verificar e acione chamar_suporte.',
    'MAIS DE UM SERVIÇO: gere UM Pix por serviço, na ordem, e só depois de ter o link do post daquele serviço. Nunca diga que gerou um pedido que a ferramenta não confirmou. Ao dividir curtidas em vários posts, confirme a quantidade de links que o cliente REALMENTE enviou.',
    'Se o cliente pedir atendente humano ou fizer reclamação séria, use chamar_suporte e pare de vender.',
  ].join('\n');
}

const TOOLS = [
  { type: 'function', function: { name: 'cotar_preco', description: 'Preço oficial de UM pacote de um serviço.', parameters: { type: 'object', properties: { servico: { type: 'string', enum: ['seguidores', 'curtidas', 'visualizacoes'] }, tipo: { type: 'string', description: 'mistos | brasileiros | organicos (só p/ seguidores/curtidas)' }, quantidade: { type: 'integer' } }, required: ['servico', 'quantidade'] } } },
  { type: 'function', function: { name: 'tabela_precos', description: 'Tabela inteira de quantidades e preços de um serviço para o cliente escolher.', parameters: { type: 'object', properties: { servico: { type: 'string', enum: ['seguidores', 'curtidas', 'visualizacoes'] }, tipo: { type: 'string' } }, required: ['servico'] } } },
  { type: 'function', function: { name: 'validar_perfil', description: 'Valida o @ do Instagram e retorna nome + nº de seguidores para confirmar.', parameters: { type: 'object', properties: { usuario: { type: 'string' } }, required: ['usuario'] } } },
  { type: 'function', function: { name: 'consultar_pedido', description: 'Consulta o pedido do cliente que diz "paguei e não recebi". Sem argumentos, busca pelo telefone do contato; passe "usuario" (@) só se pedir depois de não achar pelo telefone. Retorna tipo do serviço, tempo desde o pagamento, se é orgânico (prazo 48h), status no fornecedor e se o perfil está privado.', parameters: { type: 'object', properties: { usuario: { type: 'string', description: '@ ou link do perfil — só quando não achar pelo telefone' } }, required: [] } } },
  { type: 'function', function: { name: 'chamar_suporte', description: 'Aciona o suporte humano e para de vender.', parameters: { type: 'object', properties: { motivo: { type: 'string', description: 'Resumo CURTO (1 frase, poucas palavras) do que o cliente pediu/precisa — vira a notificação do atendente.' } }, required: ['motivo'] } } },
  { type: 'function', function: { name: 'gerar_pix', description: 'Cria o pedido e gera o Pix. Não pede CPF. O preço é definido pelo tipo — passe SEMPRE o tipo exato que o cliente escolheu.', parameters: { type: 'object', properties: { servico: { type: 'string', enum: ['seguidores', 'curtidas', 'visualizacoes'] }, tipo: { type: 'string', enum: ['mistos', 'brasileiros', 'organicos'], description: 'OBRIGATÓRIO para seguidores/curtidas. "brasileiros reais", "reais", "de verdade", "orgânico" = organicos (NÃO brasileiros). "brasileiros" = brasileiros simples. Use EXATAMENTE o tipo cotado com o cliente.' }, quantidade: { type: 'integer' }, usuario: { type: 'string' }, nome: { type: 'string' }, email: { type: 'string' }, post_links: { type: 'array', items: { type: 'string' }, description: 'TODOS os links de post (curtidas/visualizações) que o cliente mandou para ESTE pedido na conversa — a quantidade é dividida igualmente entre eles.' }, comprar_mais: { type: 'boolean', description: 'true SÓ quando o cliente pediu claramente para comprar OUTRO pacote igual a um que ele já pagou. Nunca em reclamação/pergunta sobre entrega.' } }, required: ['servico', 'quantidade', 'usuario', 'nome', 'email'] } } },
];

async function runTool(name, args, ctx) {
  try {
    // Quando o cliente NÃO escolheu o tipo, o catálogo assume *mistos* (internacionais)
    // silenciosamente — e a IA acabava cotando mundial sem avisar. Devolve um aviso
    // explícito para o modelo, que é obrigado a sinalizar o tipo ou perguntar antes.
    const avisaTipo = (r, args2) => {
      try {
        const semTipo = !String((args2 && args2.tipo) || '').trim();
        const aceitaTipo = ['seguidores', 'curtidas'].includes(String((args2 && args2.servico) || '').toLowerCase());
        if (r && r.ok && semTipo && aceitaTipo) {
          r.tipoAssumido = true;
          r.aviso = 'O cliente NÃO escolheu o tipo — assumi *mistos* (internacionais/mundiais). Você DEVE dizer explicitamente que são *mistos* ao mostrar estes preços, ou perguntar antes qual tipo ele quer (*mistos*, *brasileiros* ou *brasileiros reais*). NUNCA mostre estes valores como se fossem os únicos.';
        }
      } catch (_) {}
      return r;
    };
    if (name === 'cotar_preco') return avisaTipo(await sales.quote(args || {}), args);
    if (name === 'tabela_precos') {
      const r = avisaTipo(await sales.priceTable(args || {}), args);
      // "brasileiros" → devolve junto a tabela de brasileiros reais: as 2 opções vão sempre.
      try {
        if (r && r.ok && r.tipo === 'brasileiros' && ['seguidores', 'curtidas'].includes(r.servico)) {
          const reais = await sales.priceTable({ servico: r.servico, tipo: 'organicos' });
          if (reais && reais.ok) {
            r.tabelaBrasileirosReais = reais.itens;
            r.aviso = 'O cliente pediu brasileiros: mostre SEMPRE as 2 opções, cada uma com título — *Brasileiros* (itens) e *Brasileiros reais* (tabelaBrasileirosReais, perfis reais/orgânicos, o mais estável) — e pergunte qual tipo e quantidade.';
          }
        }
      } catch (_) {}
      return r;
    }
    if (name === 'validar_perfil') {
      const pedido = String((args && args.usuario) || '').trim();
      let vr = await sales.validateProfile(pedido);
      // REDE DE SEGURANÇA: o modelo às vezes REDIGITA o @ e come/troca letras
      // (cliente mandou "apluizcarlosmauro", o modelo buscou "apluzcarlosmauro"),
      // fazendo um perfil válido aparecer como inexistente. Se falhou e a mensagem
      // do cliente é um handle diferente do que o modelo passou, tenta o texto CRU.
      if (!vr || !vr.ok) {
        const cru = String(ctx.text || '').trim().replace(/^@+/, '');
        const pareceHandle = /^[a-zA-Z0-9._]{2,30}$/.test(cru) || /instagram\.com\//i.test(String(ctx.text || ''));
        const diferente = cru.toLowerCase() !== pedido.toLowerCase().replace(/^@+/, '');
        if (pareceHandle && diferente && cru) {
          const vr2 = await sales.validateProfile(String(ctx.text || '').trim());
          if (vr2 && vr2.ok) {
            try { console.log('🔁 [agent] @ corrigido pelo texto do cliente:', pedido, '->', cru); } catch (_) {}
            vr = vr2;
          }
        }
      }
      // Cliente mandou LINK do perfil numa mensagem anterior (ex.: instagram.com/jhulieartes_) e o
      // modelo passou o @ sem o "_" final → "não encontrado". Tenta os @ exatos dos links.
      if (!vr || !vr.ok) {
        const norm = (x) => String(x || '').toLowerCase().replace(/^@+/, '').replace(/[._]/g, '');
        const vistos = new Set();
        for (const t of (ctx.recentUserTexts || [])) {
          for (const mm of String(t).matchAll(/instagram\.com\/([A-Za-z0-9_.]+)/gi)) {
            const h = mm[1].toLowerCase();
            if (vistos.has(h) || ['p', 'reel', 'reels', 'stories', 'tv'].includes(h)) continue;
            vistos.add(h);
            if (h === pedido.toLowerCase().replace(/^@+/, '')) continue;
            if (norm(h) !== norm(pedido) && !norm(h).startsWith(norm(pedido))) continue;
            const vr3 = await sales.validateProfile(h);
            if (vr3 && vr3.ok) { try { console.log('🔁 [agent] @ corrigido pelo link do cliente:', pedido, '->', h); } catch (_) {} vr = vr3; break; }
          }
          if (vr && vr.ok) break;
        }
      }
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
      const a2 = Object.assign({}, args || {});
      // O @ do pedido é o que foi VALIDADO na conversa — o modelo chegou a gravar "@rafael.oliveira"
      // (tirado do nome do cliente) em vez de @rafaelrobru, que ele tinha validado.
      let validado = ctx.validatedUsername || ctx.histUsername || '';
      const normU = (x) => String(x || '').toLowerCase().replace(/^@+/, '').trim();
      // Sem @ validado na conversa: só aceita o @ se o CLIENTE escreveu (como @, link ou
      // handle solto) e ele passar na validação. O modelo gravou @marina_kill tirado do
      // e-mail marina_kill@hotmail.com — perfil errado e privado, o despacho travou.
      if (!validado) {
        const h = normU(a2.usuario);
        const escH = h.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        const rxH = new RegExp('(^|[^a-z0-9._@])@?' + escH + '(?![a-z0-9._]|@)', 'i');
        const escreveu = !!h && (ctx.allUserTexts || ctx.recentUserTexts || []).some((t) => rxH.test(String(t || '').replace(/\S+@\S+\.\S+/g, ' ')));
        const vr = escreveu ? await sales.validateProfile(h).catch(() => null) : null;
        if (!vr || !vr.ok) {
          try { console.warn('🛑 [agent] gerar_pix sem @ validado:', a2.usuario, '| cliente escreveu:', escreveu); } catch (_) {}
          return { ok: false, error: 'perfil_nao_validado', message: 'Não gere o Pix ainda: peça o @ do Instagram do cliente e valide com validar_perfil (confirme nome e seguidores). NUNCA use o e-mail ou o nome do cliente como @.' };
        }
        validado = vr.username || h;
        ctx.validatedUsername = validado;
      }
      if (normU(a2.usuario) !== normU(validado)) { try { console.log('🔁 [agent] gerar_pix com @ validado:', a2.usuario, '->', validado); } catch (_) {} a2.usuario = validado; }
      const r = await sales.createPixOrder(Object.assign({ phone: ctx.phone }, a2));
      if (r.ok && r.pixCopiaECola) ctx.pixToSend = r.pixCopiaECola; // enviado em mensagem separada
      if (r.ok && r.reaproveitado) { ctx.pixReused = true; r.resumo = r.resumo + ' — é o MESMO Pix gerado antes para este pedido (não é pedido novo); diga isso ao cliente.'; }
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
  ctx.recentUserTexts = [text].concat((hist || []).filter((h) => h && h.role === 'user').slice(-4).map((h) => String(h.content || '')));
  ctx.allUserTexts = [text].concat((hist || []).filter((h) => h && h.role === 'user').map((h) => String(h.content || '')));
  try {
    const achados = (hist || []).filter((h) => h && h.role === 'assistant').map((h) => String(h.content || '').match(/Achei seu perfil!?\s*@([A-Za-z0-9_.]+)/i)).filter(Boolean);
    if (achados.length) ctx.histUsername = achados[achados.length - 1][1].replace(/[.,]+$/, '');
  } catch (_) {}
  let _dispTipos = null; try { _dispTipos = await sales.tiposDisponiveis(); } catch (_) {}
  const work = [{ role: 'system', content: systemPrompt(_dispTipos) }, ...hist, { role: 'user', content: text }];
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

  // ERRO NA REPOSIÇÃO: o cliente já tentou pelo link e deu erro — mandar o link de novo
  // (ou dizer "está em andamento") não resolve nada e irrita. Aqui a IA para, aciona o
  // suporte humano e avisa no ntfy. Vale quando ele fala em erro/não consegue E o assunto
  // é reposição (pela palavra ou porque já mandamos o link nesta conversa).
  let erroRefilTratado = false;
  try {
    const erroRe = /(erro|falhou|falha|deu ruim|travou|n[ãa]o (consigo|consegui|deu|vai|d[áa]|funciona|funcionou|carrega|abre|abriu|aceita|deixa)|n[ãa]o est[áa] funcionando|indispon[íi]vel|apareceu.*(erro|problema))/i;
    const refilRe = /(repos|refil|refill|reposi[çc][ãa]o|link)/i;
    const ultimasDoCliente = [text].concat((hist || []).filter((h) => h && h.role === 'user').slice(-2).map((h) => String(h.content || '')));
    // Só conta o link se foi a ÚLTIMA coisa que o bot mandou (antes: qualquer link antigo na conversa
    // fazia "o erro é de vocês", dito sobre curtidas, virar "a página de reposição deu erro").
    const ultimaDoBot = (hist || []).filter((h) => h && h.role === 'assistant').slice(-1)[0];
    const jaMandamosLink = !!(ultimaDoBot && /refil\?token=/i.test(String(ultimaDoBot.content || '')));
    // "o erro foi meu", "errei" = o cliente se corrigindo, não é falha do sistema.
    const culpaDoCliente = /(erro\s+(foi|é|e)\s+meu|meu\s+erro|errei|foi\s+mal)/i.test(String(text || ''));
    const falaDeErro = !culpaDoCliente && erroRe.test(String(text || ''));
    const assuntoRefil = refilRe.test(ultimasDoCliente.join(' ')) || jaMandamosLink;
    if (falaDeErro && assuntoRefil && !ctx.pixToSend) {
      finalText = 'Entendi, a página de reposição está dando erro pra você. Nesse caso o link não vai resolver — já estou acionando o *suporte* pra verificar o que aconteceu e resolver a sua reposição. Assim que verificarmos, te retorno por aqui.';
      ctx.support = true;
      ctx.quedaResolved = true;
      erroRefilTratado = true;
      try { await sales.flagSupport(phone, 'Erro ao solicitar reposição pelo link'); } catch (_) {}
      try {
        const c = await getCollection('whatsapp_contacts');
        await c.updateOne({ _id: String(phone) }, { $set: { flag: 'erro_refil', botPaused: true, supportReason: 'Erro na página de reposição', supportAt: new Date().toISOString() } }, { upsert: true });
      } catch (_) {}
      try {
        const base = String(process.env.PUBLIC_BASE_URL || process.env.INTERNAL_BASE || '').replace(/\/+$/, '');
        await sales.sendNtfy({
          title: '+' + String(phone).replace(/\D/g, '') + ' - erro na reposicao',
          message: 'Cliente diz que a página de reposição deu erro. A IA foi pausada — verifique pelo CRM.',
          priority: 'high',
          tags: 'warning',
          click: base ? (base + '/painel/ia-crm') : undefined,
        });
      } catch (_) {}
    }
  } catch (_) {}

  // FORÇA a resposta de QUEDA (não-orgânico): SÓ o link, sem status/entrega/justificativa.
  // O modelo tende a "explicar demais" quando o pedido está concluído — aqui garantimos
  // o tom seco pedido, independente do que ele escreveu. Só quando: intenção de queda
  // (nesta msg ou nas últimas do cliente) + pedido encontrado, NÃO orgânico e com refil.
  try {
    const quedaRe = /(ca[íi]ram|ca[íi]u|sumir|sumiram|sumiu|diminu[íi]|perdi[^.]*seguidor|queda|despenc|baixaram|baixou|reposi[çc][ãa]o|repor\b|refil)/i;
    const userMsgs = [text].concat((hist || []).filter((h) => h && h.role === 'user').slice(-2).map((h) => String(h.content || '')));
    const isQueda = userMsgs.some((t) => quedaRe.test(t));
    const cp = ctx.lastConsulta;
    if (isQueda && cp && cp.encontrado && !ctx.support && !ctx.pixToSend && !erroRefilTratado) {
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
    // Queda/reposição e o pedido NÃO foi achado pelo telefone: pede o @ (não inventa link).
    if (isQueda && cp && cp.ok && !cp.encontrado && cp.buscaPor === 'telefone' && !ctx.support && !ctx.pixToSend && !erroRefilTratado) {
      finalText = 'Pra eu localizar seu pedido, me passa o *nome de usuário* (@) do perfil do Instagram em que você fez a compra?';
      ctx.quedaResolved = true;
    }
  } catch (_) {}

  // Link de reposição INVENTADO: o modelo mandou "refil?token=abc" copiando o formato do
  // prompt. Só passa token que existe num pedido; o resto sai e, se não sobrar nada útil,
  // pede o @ para localizar o pedido.
  try {
    const reLink = /https?:\/\/(?:www\.)?agenciaoppus\.site\/refil\?token=([A-Za-z0-9_-]*)/gi;
    const tokens = [...new Set([...String(finalText || '').matchAll(reLink)].map((mm) => mm[1]))];
    if (tokens.length) {
      const orders = await getCollection('checkout_orders');
      const invalidos = [];
      for (const tk of tokens) {
        const ok = tk.length >= 8 && !!(await orders.findOne({ refilLinkId: tk }, { projection: { _id: 1 } }).catch(() => null));
        if (!ok) invalidos.push(tk);
      }
      if (invalidos.length) {
        try { console.warn('🛑 [agent] link de refil inventado removido:', invalidos.join(',')); } catch (_) {}
        const semLink = String(finalText).replace(reLink, (full, tk) => (invalidos.includes(tk) ? '' : full)).replace(/[ \t]+\n/g, '\n').trim();
        finalText = /agenciaoppus\.site\/refil\?token=/i.test(semLink)
          ? semLink
          : 'Pra eu localizar seu pedido e liberar a reposição, me passa o *nome de usuário* (@) do perfil do Instagram em que você fez a compra?';
      }
    }
  } catch (_) {}

  // Preço inventado → pede ao modelo para reescrever com a tabela oficial; se insistir,
  // responde com a tabela oficial direto.
  try {
    let erradosP = acharPrecosErrados(finalText);
    if (erradosP.length && !erroRefilTratado) {
      try { console.warn('💸 [agent] preço fora da tabela:', JSON.stringify(erradosP)); } catch (_) {}
      const cats = [...new Set(erradosP.map((e) => e.cat))];
      const oficial = cats.map((c) => '### ' + c.toUpperCase() + '\n' + tabelaOficialTexto(c)).join('\n\n');
      work.push({ role: 'system', content: 'CORREÇÃO OBRIGATÓRIA: sua resposta citou preço/quantidade que NÃO existe na tabela oficial (' + erradosP.map((e) => e.trecho).join(' | ') + '). Reescreva a MESMA resposta usando SOMENTE estes preços e quantidades oficiais (se a quantidade pedida não existe, diga quais existem):\n\n' + oficial });
      try { const mC = await openaiChat(work, false); const novo = String((mC && mC.content) || '').trim(); if (novo) finalText = novo; } catch (_) {}
      erradosP = acharPrecosErrados(finalText);
      if (erradosP.length) finalText = 'Deixa eu te passar os valores oficiais certinhos:\n\n' + oficial.replace(/^### .*$/gm, '').trim() + '\n\nQual quantidade você quer?';
    }
  } catch (_) {}
  try {
    const pediuReais = /reais|real\b|org[âa]nic|de verdade/i.test((ctx.recentUserTexts || [text]).slice(0, 2).join(' ').replace(/\d+\s*reais/gi, ''));
    if (!erroRefilTratado && !pediuReais) finalText = garantirBrasileirosReais(finalText);
  } catch (_) {}
  try { finalText = corrigirTaxaQueda(finalText); } catch (_) {}
  try {
    const assuntoReposicao = /(ca[íi]ram|ca[íi]u|sumir|sumiram|sumiu|diminu|perdi|queda|despenc|baixaram|baixou|repos|refil|refill)/i.test((ctx.recentUserTexts || [text]).slice(0, 3).join(' '));
    if (!assuntoReposicao && !ctx.quedaResolved) finalText = tirarLinkReposicao(finalText);
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
  // O modelo às vezes escreve "Mistoss"/"Mistosos" — corrige antes de enviar.
  finalText = finalText.replace(/\b([Mm])istos(?:s+|os)\b/g, '$1istos');
  // Resposta em BALÕES (intro | lista | pergunta), com um respiro entre eles.
  if (finalText) {
    const bolhas = splitIntoBubbles(finalText);
    for (let i = 0; i < bolhas.length; i++) {
      if (i > 0 && BUBBLE_GAP_MS > 0) { try { await new Promise((r) => setTimeout(r, BUBBLE_GAP_MS)); } catch (_) {} }
      try { await sendText(phone, bolhas[i]); } catch (_) {}
    }
  }
  // Entrega do Pix em 3 mensagens SEPARADAS, com 2s entre cada (não despeja tudo de
  // uma vez): 1) instrução de como copiar; 2) o copia-e-cola SOZINHO num balão, pra o
  // cliente copiar tocando/segurando na mensagem (não em link — link não valida).
  if (ctx.pixToSend && ctx.pixReused) {
    const pedeCodigo = /(pix|c[óo]digo|copia|cola|qr|manda de novo|reenvi|mande novamente|n[ãa]o (recebi|chegou|veio)|n[ãa]o (est[áa] )?reconhec)/i.test(text);
    if (!pedeCodigo) {
      try {
        const cm = await getCollection('wa_ia_messages');
        const ja = await cm.findOne({ phone, direction: 'out', text: ctx.pixToSend, createdAt: { $gte: new Date(Date.now() - 2 * 3600e3) } }, { projection: { _id: 1 } });
        if (ja) ctx.pixSkip = true;
      } catch (_) {}
    }
  }
  if (ctx.pixToSend && !ctx.pixSkip) {
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

// ── Quebra a resposta em BALÕES, como uma pessoa escreve no WhatsApp ─────────────
// Tudo num balão só fica pesado (ex.: apresentação dos serviços, tabela de preço).
// Regra: introdução | lista (serviços/tipos/pacotes) | pergunta final, cada um no
// seu balão. Texto curto e sem lista continua num balão só. Máx. BOT_MAX_BUBBLES.
const MAX_BUBBLES = Math.max(1, Number(process.env.BOT_MAX_BUBBLES || 4));
const BUBBLE_GAP_MS = Math.max(0, Number(process.env.BOT_BUBBLE_GAP_MS || 1300));
function splitIntoBubbles(text) {
  const t = String(text || '').replace(/\r/g, '').trim();
  if (!t) return [];
  const isItem = (l) => /^\s*(\d{1,2}\s*[.)\-–]|\d️⃣|[-•▪️✅🔹👉]\s?|\*\d+\*)/.test(l);
  const hasList = t.split('\n').filter(isItem).length >= 2;
  if (t.length <= 280 && !hasList) return [t];
  // 1) blocos por linha em branco; 2) dentro do bloco, separa texto corrido da lista.
  const segs = [];
  for (const block of t.split(/\n\s*\n/)) {
    const lines = block.split('\n').map((l) => l.replace(/\s+$/, '')).filter((l) => l.trim());
    let cur = [], curIsList = null;
    for (const l of lines) {
      // linha de continuação de item (não começa com marcador, mas está dentro da lista)
      const item = isItem(l) || (curIsList === true && /^\s{2,}\S/.test(l));
      if (curIsList !== null && item !== curIsList) { segs.push({ list: curIsList, text: cur.join('\n') }); cur = []; }
      cur.push(l); curIsList = item;
    }
    if (cur.length) segs.push({ list: !!curIsList, text: cur.join('\n') });
  }
  // Junta pedaços muito curtos no vizinho (evita balão de 2 palavras), exceto a pergunta final.
  const out = [];
  for (let i = 0; i < segs.length; i++) {
    const s = segs[i];
    const prev = out[out.length - 1];
    const ehUltimo = i === segs.length - 1;
    if (prev && !s.list && !prev.list && (s.text.length < 60 || prev.text.length < 60) && !(ehUltimo && /\?\s*$/.test(s.text))) { prev.text += '\n\n' + s.text; continue; }
    if (prev && s.list && prev.list) { prev.text += '\n' + s.text; continue; }
    out.push({ list: s.list, text: s.text });
  }
  // Lista muito longa (ex.: 3 itens com explicação grande): divide em partes de até ~550 chars.
  const final = [];
  for (const s of out) {
    if (s.list && s.text.length > 700) {
      let acc = '';
      for (const l of s.text.split('\n')) {
        if (acc && isItem(l) && (acc.length + l.length) > 550) { final.push(acc); acc = l; }
        else acc = acc ? acc + '\n' + l : l;
      }
      if (acc) final.push(acc);
    } else final.push(s.text);
  }
  // Respeita o máximo de balões: junta os menores vizinhos até caber.
  while (final.length > MAX_BUBBLES) {
    let best = 0, bestLen = Infinity;
    for (let i = 0; i < final.length - 1; i++) { const len = final[i].length + final[i + 1].length; if (len < bestLen) { bestLen = len; best = i; } }
    final.splice(best, 2, final[best] + '\n\n' + final[best + 1]);
  }
  return final.map((x) => x.trim()).filter(Boolean);
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
