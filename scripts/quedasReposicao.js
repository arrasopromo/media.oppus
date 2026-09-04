// scripts/quedasReposicao.js  (SOMENTE LEITURA — nao envia reposicao, nao grava nada)
// ---------------------------------------------------------------------------
// Perfis que SOLICITARAM reposicao entre 15/08 e 02/09, que NAO foram bloqueados
// por "pagamento no mesmo dia" (paid_today) e cuja queda registrada foi > 100.
// Deduplica por perfil (marca d'agua = maior baseline), consulta o RocketAPI AO
// VIVO para confirmar a queda ATUAL, e calcula por tipo a quantidade a repor + custo.
//
// Saida: tabela no terminal + CSV em quedas_reposicao_<data>.csv (coberto pelo .gitignore).
// ---------------------------------------------------------------------------
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });
const fs = require('fs');
const axios = require('axios');
const { getCollection } = require('../mongodbClient');

// Reusa a MESMA funcao do app (com tratamento de conta business), mas com a
// self-API stubada para nao gastar 5s de timeout por perfil.
const src = fs.readFileSync(path.join(__dirname, '..', 'app.js'), 'utf8');
const L = src.split('\n');
const start = L.findIndex(l => l.startsWith('const fetchInstagramFollowersInfoRocketApi = async'));
let depth = 0, endIdx = -1;
for (let k = start; k < L.length; k++) { for (const ch of L[k]) { if (ch === '{') depth++; else if (ch === '}') depth--; } if (depth === 0 && k > start) { endIdx = k; break; } }
const fnSrc = L.slice(start, endIdx + 1).join('\n');
const stub = 'const fetchProfileSelfApi = async () => ({ success: false });\nvar global = globalThis;\n';
const fetchRocket = new Function('axios', 'process', stub + fnSrc + '\nreturn fetchInstagramFollowersInfoRocketApi;')(axios, process);

// Custo por 1000 para repor. Mistos = custo REAL atual da Nuvra (7,50). Brasileiros =
// cost_settings (9,60) — nao ha custo real medido para brasileiros.
const CUSTO_1K = { mistos: 7.5, brasileiros: 9.6 };
const money = (n) => 'R$ ' + (Number(n) || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

(async () => {
  const col = await getCollection('refil2_requests');
  const ini = new Date('2026-08-15T03:00:00Z'), fim = new Date('2026-09-03T03:00:00Z');
  const docs = await col.find({
    requestedAt: { $gte: ini, $lt: fim },
    decisionReason: { $ne: 'paid_today' },
    'summary.drop': { $gt: 100 }
  }).project({ username: 1, tipo: 1, summary: 1, pedido: 1, requestedAt: 1 }).toArray();

  // dedup por perfil: guarda maior baseline (marca d'agua) + tipo + link
  const M = new Map();
  for (const d of docs) {
    const u = String(d.username || '').toLowerCase().trim(); if (!u) continue;
    const initial = Number(d.summary && d.summary.initial) || 0;
    const cur = M.get(u);
    if (!cur || initial > cur.baseline) {
      M.set(u, { u, baseline: initial, tipo: String(d.tipo || '').toLowerCase().trim() || (cur && cur.tipo) || '', link: (d.pedido && d.pedido.link) || ('https://instagram.com/' + u), pedidoId: (d.pedido && d.pedido.id) || '', dropReg: Number(d.summary && d.summary.drop) || 0 });
    } else if (!cur.tipo && d.tipo) { cur.tipo = String(d.tipo).toLowerCase().trim(); }
  }
  const perfis = [...M.values()];
  console.log('\nperiodo 15/08-02/09 · sem paid_today · queda registrada > 100');
  console.log('registros: ' + docs.length + '  ->  perfis distintos: ' + perfis.length);
  console.log('confirmando queda ATUAL via RocketAPI...\n');

  let i = 0, feitos = 0;
  const CONC = 5;
  const worker = async () => {
    while (i < perfis.length) {
      const p = perfis[i++];
      try {
        const r = await fetchRocket(p.u);
        if (r && r.success && r.profile && Number(r.profile.followersCount) >= 0) {
          p.atual = Math.trunc(Number(r.profile.followersCount));
          p.priv = !!r.profile.isPrivate;
          p.quedaAtual = Math.max(0, p.baseline - p.atual);
        } else { p.erro = (r && r.error) || 'sem_dados'; }
      } catch (e) { p.erro = (e && e.message) ? e.message : 'erro'; }
      feitos++;
      if (feitos % 25 === 0) console.log('  ... ' + feitos + '/' + perfis.length);
      await new Promise(s => setTimeout(s, 120));
    }
  };
  await Promise.all(Array.from({ length: CONC }, worker));

  // ordena por queda atual desc
  perfis.sort((a, b) => (b.quedaAtual || -1) - (a.quedaAtual || -1));

  const linhas = [['instagram', 'tipo', 'pedido_id', 'baseline', 'atual', 'queda_registrada', 'queda_atual', 'qtd_repor', 'custo_1k', 'custo_repor', 'status']];
  const porTipo = {};
  let totalRepor = 0, totalCusto = 0, comQueda = 0, semQueda = 0, falhas = 0;
  const tabela = [];
  for (const p of perfis) {
    let status = '', qtd = 0, custo = 0;
    if (p.erro) { status = 'falha:' + p.erro; falhas++; }
    else if (p.priv) { status = 'privado'; }
    else if (p.quedaAtual > 0) {
      status = 'repor'; qtd = p.quedaAtual;
      const c1k = CUSTO_1K[p.tipo] || 0; custo = (qtd / 1000) * c1k;
      totalRepor += qtd; totalCusto += custo; comQueda++;
      if (!porTipo[p.tipo]) porTipo[p.tipo] = { n: 0, qtd: 0, custo: 0 };
      porTipo[p.tipo].n++; porTipo[p.tipo].qtd += qtd; porTipo[p.tipo].custo += custo;
      tabela.push({ u: p.u, tipo: p.tipo, atual: p.atual, qtd, custo });
    } else { status = 'recuperou'; semQueda++; }
    linhas.push([p.u, p.tipo || '-', p.pedidoId || '', p.baseline || '', (p.atual != null ? p.atual : ''), p.dropReg || '', (p.quedaAtual != null ? p.quedaAtual : ''), qtd || '', (CUSTO_1K[p.tipo] || ''), custo ? custo.toFixed(2) : '', status]);
  }

  const hoje = new Date(Date.now() - 108e5).toISOString().slice(0, 10);
  const arq = path.join(__dirname, '..', 'quedas_reposicao_' + hoje + '.csv');
  const esc = (v) => '"' + String(v == null ? '' : v).replace(/"/g, '""') + '"';
  fs.writeFileSync(arq, linhas.map(r => r.map(esc).join(',')).join('\n'), 'utf8');

  console.log('\n=== PERFIS A REPOR (queda atual > 0) ===');
  console.log('instagram                     tipo         atual     qtd repor   custo');
  for (const t of tabela.slice(0, 60)) console.log('@' + String(t.u).slice(0, 26).padEnd(27) + ' ' + String(t.tipo).padEnd(12) + ' ' + String(t.atual).padEnd(9) + ' ' + String(t.qtd).padStart(8) + '   ' + money(t.custo));
  if (tabela.length > 60) console.log('  ... e mais ' + (tabela.length - 60));

  console.log('\n=== POR TIPO ===');
  for (const [t, v] of Object.entries(porTipo)) console.log('  ' + t.padEnd(12) + ' ' + String(v.n).padStart(3) + ' perfis · repor ' + v.qtd.toLocaleString('pt-BR').padStart(9) + ' seguidores · custo ' + money(v.custo));

  console.log('\n=== RESUMO ===');
  console.log('  perfis distintos analisados : ' + perfis.length);
  console.log('  a repor (queda confirmada)  : ' + comQueda);
  console.log('  ja recuperaram (sem queda)  : ' + semQueda);
  console.log('  falha/privado no RocketAPI  : ' + (falhas + perfis.filter(p => p.priv).length));
  console.log('  TOTAL seguidores a repor    : ' + totalRepor.toLocaleString('pt-BR'));
  console.log('  CUSTO TOTAL estimado        : ' + money(totalCusto));
  console.log('\n  CSV: ' + arq + '\n');
  process.exit(0);
})().catch(e => { console.error('ERRO', e && e.message ? e.message : String(e)); process.exit(1); });
