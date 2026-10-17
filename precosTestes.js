// ══════════════════════════════════════════════════════════════════════════
// PREÇOS DOS TESTES DE SERVIÇOS — atualização diária (padrão 23:30 BRT)
//
// Para cada linha de `service_tests`, busca o preço ATUAL do serviço no fornecedor e grava em
// `preco` como R$ por 1.000 (dólar convertido pela cotação do dia).
//   • serviço que sumiu do catálogo  → ganha "*" no fim do serviceId (preço fica como estava);
//   • serviço com "*" que voltou       → perde o "*" e recebe o preço novo.
//
// Travas de segurança (para nunca marcar asterisco por engano):
//   • catálogo que falhou, veio vazio ou encolheu demais não mexe em nenhuma linha daquele fornecedor;
//   • Nuvra: a API não lista todos os serviços ativos (o 168 não aparece), então lá só se atualiza
//     preço — nunca se põe asterisco;
//   • página pública com moeda diferente de "$" é ignorada (não converte o que não entende);
//   • sem cotação do dólar (nem a do dia, nem a da última execução), fornecedores em dólar ficam
//     de fora naquele dia.
// Cada execução fica registrada em `service_tests_price_runs` (um documento por dia).
// ══════════════════════════════════════════════════════════════════════════
const axios = require('axios');
const { getCollection } = require('./mongodbClient');

const BRT = 3 * 3600e3;
const DIA = 86400e3;
const diaBRT = (ms) => new Date(ms - BRT).toISOString().slice(0, 10);
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140 Safari/537.36';
const limpa = (s) => String(s || '').replace(/<[^>]+>/g, '').replace(/&amp;/g, '&').replace(/&#0?39;|&apos;/g, "'").replace(/&quot;/g, '"').replace(/\s+/g, ' ').trim();
const numero = (s) => { const m = String(s || '').replace(/,/g, '').match(/[\d.]+/); return m ? Number(m[0]) : null; };

async function catalogoApi(url, key) {
  const r = await axios.post(url, new URLSearchParams({ key, action: 'services' }).toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 60000, validateStatus: () => true });
  const lista = Array.isArray(r.data) ? r.data : [];
  const m = {};
  for (const s of lista) if (s && s.service != null && Number.isFinite(Number(s.rate))) m[String(s.service)] = { nome: String(s.name || ''), rate: Number(s.rate) };
  return m;
}
async function paginaPublica(url) {
  const r = await axios.get(url, { headers: { 'User-Agent': UA, Accept: 'text/html' }, timeout: 120000, responseType: 'text', maxContentLength: 80 * 1024 * 1024, maxBodyLength: 80 * 1024 * 1024, validateStatus: () => true });
  if (r.status !== 200) throw new Error('http ' + r.status);
  return String(r.data || '');
}
// Tabela padrão de painel SMM (data-filter-table-service-id) — ex.: smmturk.org/services
function lerTabelaPadrao(html) {
  const m = {};
  const re = /data-filter-table-service-id="(\d+)">\d+<\/td>\s*<td[^>]*data-filter-table-service-name="true">([\s\S]*?)<\/td>\s*<td data-label="Rate per 1000">([\s\S]*?)<\/td>/g;
  let x;
  while ((x = re.exec(html))) { const txt = limpa(x[3]); m[x[1]] = { nome: limpa(x[2]), rate: numero(txt), moedaTxt: txt.replace(/[\d.,\s]/g, '') }; }
  return m;
}
// Layout em blocos (order-history-box) — ex.: worldofsmm.com/services
function lerBlocos(html) {
  const m = {};
  for (const b of html.split('class="order-history-box"').slice(1)) {
    const id = (b.match(/data-text="(\d+)"/) || [])[1];
    if (!id) continue;
    const txt = limpa((b.match(/Rate per 1000:<\/span>([\s\S]*?)<\/h5>/) || [])[1]);
    m[id] = { nome: limpa((b.match(/order-ser-name">[\s\S]*?<b>([\s\S]*?)<\/b>/) || [])[1]), rate: numero(txt), moedaTxt: txt.replace(/[\d.,\s]/g, '') };
  }
  return m;
}

// Fornecedores conhecidos, pela URL gravada em `fornecedor` na linha do teste.
// Catálogo sempre via API (action=services). Campos:
//   moeda: 'BRL' | 'USD' | null  (null = não atualiza preço, só valida existência do ID)
//   marcaAusencia: true  → ID fora do catálogo deixa a linha CINZA (servicoForaDoCatalogo)
//                  false → nunca marca (catálogo da API é incompleto/incerto p/ esse painel)
//   prefixo: prefixo do serviceId que a API pode não usar (ex.: SMMRaja usa 's5573')
const P = (chave, nome, casa, moeda, { url, keyEnv, marca = true, min = 200, prefixo = null } = {}) => ({
  chave, nome, casa, moeda, marcaAusencia: marca, minimo: min, prefixo,
  carregar: () => catalogoApi(url, String(process.env[keyEnv] || '').trim()),
});
const FORNECEDORES = [
  // Painéis grandes com catálogo completo pela API → marcam ausência (fundo cinza).
  P('smmhustle', 'SMMHustle', /smmhustle\.com/i, 'USD', { url: 'https://smmhustle.com/api/v2', keyEnv: 'SMMHUSTLE_API_KEY', min: 300 }),
  P('smmturk', 'SMMTurk', /smmturk\.org/i, 'USD', { url: 'https://smmturk.org/api/v2', keyEnv: 'SMMTURK_API_KEY', min: 1000 }),
  P('worldofsmm', 'WorldOfSMM', /worldofsmm\.com/i, 'USD', { url: 'https://worldofsmm.com/api/v2', keyEnv: 'WORLDOFSMM_API_KEY', min: 300 }),
  P('justanotherpanel', 'JustAnotherPanel', /justanotherpanel\.com/i, 'USD', { url: 'https://justanotherpanel.com/api/v2', keyEnv: 'JUSTANOTHERPANEL_API_KEY', min: 1000 }),
  P('dripfeedpanel', 'DripFeedPanel', /dripfeedpanel\.com/i, 'USD', { url: 'https://dripfeedpanel.com/api/v2', keyEnv: 'DRIPFEEDPANEL_API_KEY', min: 300 }),
  P('followiz', 'Followiz', /followiz\.com/i, 'USD', { url: 'https://followiz.com/api/v2', keyEnv: 'FOLLOWIZ_API_KEY', min: 300 }),
  P('smmcost', 'SMMCost', /smmcost\.com/i, 'USD', { url: 'https://smmcost.com/api/v2', keyEnv: 'SMMCOST_API_KEY', min: 200 }),
  P('bulkfollows', 'BulkFollows', /bulkfollows\.com/i, 'USD', { url: 'https://bulkfollows.com/api/v2', keyEnv: 'BULKFOLLOWS_API_KEY', min: 300 }),
  P('n1panel', 'N1Panel', /n1panel\.com/i, 'USD', { url: 'https://n1panel.com/api/v2', keyEnv: 'N1PANEL_API_KEY', min: 200 }),
  P('peakerr', 'Peakerr', /peakerr\.com/i, 'USD', { url: 'https://peakerr.com/api/v2', keyEnv: 'PEAKERR_API_KEY', min: 300 }),
  P('bulkmedya', 'BulkMedya', /bulkmedya\.(org|com)/i, 'USD', { url: 'https://bulkmedya.org/api/v2', keyEnv: 'BULKMEDYA_API_KEY', min: 200 }),
  P('topfama', 'TopFama', /topfama\.com/i, 'BRL', { url: 'https://topfama.com/api/v2', keyEnv: 'TOPFAMA_API_KEY', min: 50 }),
  P('revendaexclusiva', 'RevendaExclusiva', /revendaexclusiva\.com/i, 'BRL', { url: 'https://revendaexclusiva.com/api/v2', keyEnv: 'REVENDAEXCLUSIVA_API_KEY', min: 30 }),
  P('smmpix', 'SMMPix', /smmpix\.com/i, null, { url: 'https://smmpix.com/api/v2', keyEnv: 'SMMPIX_API_KEY', min: 200 }),
  // Catálogo incompleto/incerto pela API → só atualiza preço quando acha, NUNCA marca ausência.
  P('nuvra', 'Nuvra', /nuvrasmm\.com/i, 'BRL', { url: process.env.NUVRASMM_API_URL || 'https://nuvrasmm.com/api/v2', keyEnv: 'NUVRASMM_API_KEY', marca: false, min: 1 }),
  P('smmraja', 'SMMRaja', /smmraja\.com/i, 'USD', { url: 'https://smmraja.com/api/v2', keyEnv: 'SMMRAJA_API_KEY', marca: false, min: 500, prefixo: 's' }),
  P('hiperseguidores', 'HiperSeguidores', /hiperseguidores\.com/i, 'BRL', { url: 'https://hiperseguidores.com.br/api/v2', keyEnv: 'HIPERSEGUIDORES_API_KEY', marca: false, min: 1 }),
  P('fornecedorsocial', 'FornecedorSocial', /fornecedorsocial\.com/i, 'BRL', { url: 'https://fornecedorsocial.com/api/v2', keyEnv: 'FORNECEDOR_SOCIAL_API_KEY', marca: false, min: 1 }),
  P('worldsmm', 'WorldSMM', /worldsmm\.com\.br/i, 'BRL', { url: 'https://worldsmm.com.br/api/v2', keyEnv: 'WORLDSMM_API_KEY', marca: false, min: 1 }),
];
const fornecedorDaLinha = (url) => FORNECEDORES.find((f) => f.casa.test(String(url || ''))) || null;
// Procura o ID no catálogo tolerando o prefixo do painel (ex.: 's5573' ↔ '5573').
function acharNoCatalogo(cat, base, prefixo) {
  if (cat[base]) return cat[base];
  if (prefixo) {
    const semPref = base.replace(new RegExp('^' + prefixo, 'i'), '');
    if (cat[semPref]) return cat[semPref];
    if (cat[prefixo + base]) return cat[prefixo + base];
  }
  return null;
}

async function cotacaoDolar(runs) {
  try {
    const r = await axios.get('https://economia.awesomeapi.com.br/json/last/USD-BRL', { timeout: 20000, validateStatus: () => true });
    const v = Number(r && r.data && r.data.USDBRL && r.data.USDBRL.bid);
    if (v > 3 && v < 15) return { valor: v, fonte: 'awesomeapi' };
  } catch (_) {}
  try {
    const ultima = await runs.find({ usdBrl: { $gt: 0 } }).sort({ at: -1 }).limit(1).toArray();
    if (ultima[0]) return { valor: Number(ultima[0].usdBrl), fonte: 'última execução (' + ultima[0]._id + ')' };
  } catch (_) {}
  return { valor: null, fonte: 'indisponível' };
}

async function atualizarPrecosTestes({ gravar = true, dia = diaBRT(Date.now()) } = {}) {
  const col = await getCollection('service_tests');
  const runs = await getCollection('service_tests_price_runs');
  const inicio = Date.now();
  const linhas = await col.find({}, { projection: { serviceId: 1, fornecedor: 1, preco: 1, servicoForaDoCatalogo: 1 } }).toArray();

  // tamanho do catálogo na última execução, para detectar catálogo que encolheu demais
  const anterior = (await runs.find({ catalogos: { $exists: true } }).sort({ at: -1 }).limit(1).toArray())[0] || null;
  const usd = await cotacaoDolar(runs);

  const catalogos = {};
  const resumoCat = {};
  const usados = new Set(linhas.map((l) => (fornecedorDaLinha(l.fornecedor) || {}).chave).filter(Boolean));
  for (const f of FORNECEDORES) {
    if (!usados.has(f.chave)) continue;
    if (f.moeda === 'USD' && !usd.valor) { resumoCat[f.chave] = { ok: false, motivo: 'sem cotação do dólar' }; continue; }
    try {
      const cat = await f.carregar();
      const n = Object.keys(cat).length;
      const nAntes = anterior && anterior.catalogos && anterior.catalogos[f.chave] && anterior.catalogos[f.chave].servicos;
      let motivo = '';
      if (n < f.minimo) motivo = `catálogo pequeno demais (${n})`;
      else if (nAntes && n < nAntes * 0.6) motivo = `catálogo encolheu de ${nAntes} para ${n}`;
      resumoCat[f.chave] = { ok: !motivo, servicos: n, motivo: motivo || undefined };
      if (!motivo) catalogos[f.chave] = cat;
    } catch (e) {
      resumoCat[f.chave] = { ok: false, motivo: (e && e.message) || 'erro' };
    }
  }

  const agoraIso = new Date().toISOString();
  const mudancas = { precos: 0, indisponivelNovo: [], voltou: [], semFornecedor: 0, puladas: 0 };
  const ops = [];
  for (const l of linhas) {
    const f = fornecedorDaLinha(l.fornecedor);
    if (!f) { mudancas.semFornecedor++; continue; }
    const cat = catalogos[f.chave];
    if (!cat) { mudancas.puladas++; continue; }
    const idAtual = String(l.serviceId || '').trim();
    const base = idAtual.replace(/\*+$/, '').trim(); // migra IDs legados que ganharam "*"
    if (!base) continue;
    const s = acharNoCatalogo(cat, base, f.prefixo);
    const set = {};
    // Limpa o "*" legado do serviceId em qualquer caso (agora a sinalização é a flag/fundo cinza).
    if (idAtual !== base) set.serviceId = base;
    if (s && Number.isFinite(s.rate)) {
      // Atualiza preço só quando a moeda é conhecida (null = fornecedor só p/ validar existência).
      if (f.moeda === 'BRL' || f.moeda === 'USD') {
        const reais = Math.round((f.moeda === 'USD' ? s.rate * usd.valor : s.rate) * 100) / 100;
        if (Number(l.preco) !== reais) { set.preco = reais; set.precoAnterior = (l.preco != null) ? Number(l.preco) : null; mudancas.precos++; }
        set.precoFonte = f.moeda === 'USD' ? `${f.nome} US$ ${s.rate} x ${usd.valor}` : `${f.nome} R$ ${s.rate}`;
      }
      set.precoAtualizadoEm = agoraIso;
      if (l.servicoForaDoCatalogo) { set.servicoForaDoCatalogo = false; mudancas.voltou.push(`${f.nome} ${base}`); }
    } else if (f.marcaAusencia) {
      // ID não existe mais no catálogo do fornecedor → linha fica CINZA.
      if (!l.servicoForaDoCatalogo) mudancas.indisponivelNovo.push(`${f.nome} ${base}`);
      set.servicoForaDoCatalogo = true;
      set.servicoForaDoCatalogoEm = agoraIso;
      set.precoAtualizadoEm = agoraIso;
    }
    if (Object.keys(set).length) ops.push({ updateOne: { filter: { _id: l._id }, update: { $set: set } } });
  }
  if (gravar && ops.length) await col.bulkWrite(ops, { ordered: false });

  const resumo = {
    _id: dia, at: agoraIso, gravado: !!gravar, linhas: linhas.length, usdBrl: usd.valor, usdFonte: usd.fonte,
    catalogos: resumoCat, precosAlterados: mudancas.precos, indisponivelNovo: mudancas.indisponivelNovo, voltou: mudancas.voltou,
    puladasPorFalhaDeCatalogo: mudancas.puladas, semFornecedorConhecido: mudancas.semFornecedor, ms: Date.now() - inicio,
  };
  if (gravar) { try { await runs.replaceOne({ _id: dia }, resumo, { upsert: true }); } catch (_) {} }
  return resumo;
}

function avisoTexto(r) {
  const p = [];
  if ((r.indisponivelNovo || []).length) p.push('Saíram do catálogo (ficaram cinza): ' + r.indisponivelNovo.join(', '));
  if ((r.voltou || []).length) p.push('Voltaram ao catálogo: ' + r.voltou.join(', '));
  const falhas = Object.entries(r.catalogos || {}).filter(([, v]) => !v.ok).map(([k, v]) => `${k}: ${v.motivo}`);
  if (falhas.length) p.push('Não validei hoje — ' + falhas.join(' | '));
  return p;
}

function startPrecosTestesLoop({ backgroundJobsEnabled, sendNtfy } = {}) {
  if (String(process.env.PRECOS_TESTES_ENABLED || 'true').toLowerCase() === 'false') { try { console.log('⏸️ [precos-testes] desligado (PRECOS_TESTES_ENABLED=false)'); } catch (_) {} return; }
  const [h, m] = String(process.env.PRECOS_TESTES_HORA || '23:30').split(':').map((x) => parseInt(x, 10));
  const agenda = () => {
    const agora = Date.now();
    const brt = new Date(agora - BRT);
    let alvo = Date.UTC(brt.getUTCFullYear(), brt.getUTCMonth(), brt.getUTCDate(), Number.isFinite(h) ? h : 23, Number.isFinite(m) ? m : 30, 0) + BRT;
    if (alvo <= agora) alvo += DIA;
    const t = setTimeout(async () => {
      try {
        if (typeof backgroundJobsEnabled !== 'function' || backgroundJobsEnabled()) {
          const r = await atualizarPrecosTestes({ gravar: true });
          try { console.log('💲 [precos-testes] execução diária:', JSON.stringify(r)); } catch (_) {}
          const aviso = avisoTexto(r);
          if (aviso.length && typeof sendNtfy === 'function') {
            try { await sendNtfy({ title: 'Testes de serviços — preços', message: `${r.precosAlterados} preço(s) atualizado(s).\n` + aviso.join('\n'), priority: 'default', tags: 'moneybag' }); } catch (_) {}
          }
        }
      } catch (e) { try { console.warn('⚠️ [precos-testes] falhou:', e && e.message); } catch (_) {} }
      agenda();
    }, Math.max(1000, alvo - agora));
    try { t.unref && t.unref(); } catch (_) {}
  };
  agenda();
  try { console.log(`💲 [precos-testes] agendado todo dia às ${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')} (BRT)`); } catch (_) {}
}

// Disparo manual pelo painel (admin): POST /api/painel/testes-servicos/atualizar-precos  { previa: true } só simula.
function registerPrecosTestes(app, { requireAdmin } = {}) {
  app.post('/api/painel/testes-servicos/atualizar-precos', requireAdmin, async (req, res) => {
    try {
      const previa = !!(req.body && (req.body.previa === true || req.body.previa === 'true'));
      const r = await atualizarPrecosTestes({ gravar: !previa });
      return res.json({ ok: true, resultado: r });
    } catch (e) { return res.status(500).json({ ok: false, error: (e && e.message) || 'erro' }); }
  });
}

module.exports = { atualizarPrecosTestes, startPrecosTestesLoop, registerPrecosTestes };
