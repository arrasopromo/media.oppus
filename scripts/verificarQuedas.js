// scripts/verificarQuedas.js  (SOMENTE LEITURA)
// ---------------------------------------------------------------------------
// Verificacao MINUCIOSA dos 152 perfis marcados "repor". Para cada um:
//   1) resolve o pedido no fornecedor (nuvra/fama) pelo pedido_id
//   2) action=status -> start_count (base real), quantidade entregue, status, remains
//   3) baseline REAL = start_count + entregue  (nao o palpite do nosso banco)
//   4) atual de hoje via RocketAPI
//   5) queda confirmada = clamp( baseline_real - atual , 0 , entregue )
//   6) classifica: elegivel a refill so se status Completed e queda > 50
// Saida: tabela + CSV verificacao_quedas_<data>.csv
// ---------------------------------------------------------------------------
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });
const fs = require('fs');
const axios = require('axios');
const { getCollection } = require('../mongodbClient');

// RocketAPI (self-api stubada) reusando a funcao do app.
const src = fs.readFileSync(path.join(__dirname, '..', 'app.js'), 'utf8').split('\n');
const st = src.findIndex(l => l.startsWith('const fetchInstagramFollowersInfoRocketApi = async'));
let dp = 0, en = -1; for (let k = st; k < src.length; k++) { for (const ch of src[k]) { if (ch === '{') dp++; else if (ch === '}') dp--; } if (dp === 0 && k > st) { en = k; break; } }
const fetchRocket = new Function('axios', 'process', 'const fetchProfileSelfApi=async()=>({success:false});var global=globalThis;\n' + src.slice(st, en + 1).join('\n') + '\nreturn fetchInstagramFollowersInfoRocketApi;')(axios, process);

const NUVRA = { url: 'https://nuvrasmm.com/api/v2', key: process.env.NUVRASMM_API_KEY };
const FAMA = { url: 'https://fama24h.net/api/v2', key: process.env.FAMA24H_API_KEY };
const apiFor = (blk, oid) => {
  const p = String((blk && blk.provider) || '').trim().toLowerCase();
  if (p === 'nuvra' || p === 'nuvrasmm') return NUVRA;
  if (p === 'fama24h') return FAMA;
  const n = Number(String(oid || '').trim());
  return (Number.isFinite(n) && n < 1000000) ? NUVRA : FAMA;
};
const num = v => { const s = String(v == null ? '' : v).trim(); if (!s) return null; const n = Number(s.replace(',', '.').replace(/[^\d.-]/g, '')); return Number.isFinite(n) ? n : null; };
const money = n => 'R$ ' + (Number(n) || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const CUSTO_1K = { mistos: 7.5, brasileiros: 9.6 };

async function fetchStatus(api, oid) {
  if (!api.key || !oid) return null;
  try {
    const p = new URLSearchParams({ key: api.key, action: 'status', order: String(oid) });
    const r = await axios.post(api.url, p.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 25000, validateStatus: () => true });
    const d = (r && r.data && typeof r.data === 'object') ? r.data : null;
    if (!d || d.error) return { err: (d && d.error) || 'sem_dados' };
    return d;
  } catch (e) { return { err: (e && e.message) || 'erro' }; }
}

(async () => {
  // le os 152 "repor" do CSV anterior (username, pedido_id, atual de hoje)
  const csv = fs.readFileSync(path.join(__dirname, '..', 'quedas_reposicao_2026-09-03.csv'), 'utf8').trim().split('\n').slice(1)
    .map(l => l.match(/"(?:[^"]|"")*"/g).map(x => x.slice(1, -1).replace(/""/g, '"')));
  const alvo = csv.filter(r => r[10] === 'repor').map(r => ({ u: r[0], tipo: r[1], pedidoId: r[2], baselineNosso: +r[3], atualCsv: +r[4] }));
  console.log('\nverificando ' + alvo.length + ' perfis (status do fornecedor + atual de hoje)...\n');

  const col = await getCollection('checkout_orders');
  let i = 0, done = 0;
  const worker = async () => {
    while (i < alvo.length) {
      const a = alvo[i++];
      try {
        // resolve o pedido pelo orderId do fornecedor
        const oidRe = String(a.pedidoId).trim();
        const o = await col.findOne({ $or: [{ 'fama24h.orderId': oidRe }, { 'fama24h.orderId': Number(oidRe) || -1 }] }, { projection: { fama24h: 1, initialFollowersCount: 1, quantidade: 1, qtd: 1 } });
        const blk = o && o.fama24h ? o.fama24h : null;
        const delivQtd = Number((blk && blk.requestPayload && blk.requestPayload.quantity) || o && (o.quantidade || o.qtd) || 0) || 0;
        const api = apiFor(blk, oidRe);
        const stt = await fetchStatus(api, oidRe);
        // atual de hoje (RocketAPI fresco)
        let atual = a.atualCsv;
        try { const rk = await fetchRocket(a.u); if (rk && rk.success && rk.profile && Number(rk.profile.followersCount) >= 0) atual = Math.trunc(Number(rk.profile.followersCount)); } catch (_) {}
        a.atual = atual;

        if (!stt || stt.err) { a.statusErr = stt ? stt.err : 'nao_resolveu'; }
        else {
          const startCount = num(stt.start_count);
          const remains = num(stt.remains);
          a.provStatus = String(stt.status || '').trim();
          a.startCount = startCount;
          a.remains = remains;
          const entregue = (remains != null && delivQtd) ? Math.max(0, delivQtd - remains) : delivQtd;
          a.entregue = entregue;
          // baseline REAL = seguidores no inicio + o que foi entregue
          const baselineReal = (startCount != null) ? (startCount + entregue) : (a.baselineNosso);
          a.baselineReal = baselineReal;
          const bruta = (atual != null) ? (baselineReal - atual) : null;
          // queda reembolsavel: nao pode passar do que foi entregue
          a.quedaConfirmada = (bruta == null) ? null : Math.max(0, Math.min(bruta, entregue || bruta));
        }
      } catch (e) { a.statusErr = (e && e.message) || 'erro'; }
      done++; if (done % 20 === 0) console.log('  ... ' + done + '/' + alvo.length);
      await new Promise(s => setTimeout(s, 120));
    }
  };
  await Promise.all(Array.from({ length: 5 }, worker));

  // classifica
  const LIM = 50;
  let elegiveis = [], semQueda = [], naoCompleto = [], falha = [];
  for (const a of alvo) {
    if (a.statusErr) { a.classe = 'falha_status:' + a.statusErr; falha.push(a); continue; }
    const completo = /complet|conclu|success|finish|done/i.test(a.provStatus || '');
    if (a.quedaConfirmada == null) { a.classe = 'sem_atual'; falha.push(a); continue; }
    if (a.quedaConfirmada <= LIM) { a.classe = 'sem_queda_real (' + a.quedaConfirmada + ')'; semQueda.push(a); continue; }
    if (!completo) { a.classe = 'queda_mas_nao_completo (' + a.provStatus + ')'; naoCompleto.push(a); continue; }
    a.classe = 'ELEGIVEL'; a.custo = (a.quedaConfirmada / 1000) * (CUSTO_1K[a.tipo] || 0); elegiveis.push(a);
  }
  elegiveis.sort((x, y) => y.quedaConfirmada - x.quedaConfirmada);

  // CSV
  const rows = [['instagram', 'tipo', 'pedido_id', 'status_prov', 'start_count', 'entregue', 'baseline_real', 'baseline_nosso', 'atual_hoje', 'queda_confirmada', 'custo_repor', 'classe']];
  for (const a of alvo) rows.push([a.u, a.tipo, a.pedidoId, a.provStatus || '', a.startCount ?? '', a.entregue ?? '', a.baselineReal ?? '', a.baselineNosso, a.atual ?? '', a.quedaConfirmada ?? '', a.custo ? a.custo.toFixed(2) : '', a.classe]);
  const hoje = new Date(Date.now() - 108e5).toISOString().slice(0, 10);
  const arq = path.join(__dirname, '..', 'verificacao_quedas_' + hoje + '.csv');
  fs.writeFileSync(arq, rows.map(r => r.map(v => '"' + String(v == null ? '' : v).replace(/"/g, '""') + '"').join(',')).join('\n'), 'utf8');

  console.log('\n=== ELEGIVEIS A REFIL (queda real > ' + LIM + ' E pedido completo) ===');
  console.log('instagram                    tipo        base_real  atual    queda   custo');
  for (const a of elegiveis.slice(0, 50)) console.log('@' + String(a.u).slice(0, 26).padEnd(27) + ' ' + String(a.tipo).padEnd(11) + ' ' + String(a.baselineReal).padEnd(9) + ' ' + String(a.atual).padEnd(8) + ' ' + String(a.quedaConfirmada).padStart(6) + '  ' + money(a.custo));
  if (elegiveis.length > 50) console.log('  ... e mais ' + (elegiveis.length - 50));

  const somaQ = elegiveis.reduce((s, a) => s + a.quedaConfirmada, 0);
  const somaC = elegiveis.reduce((s, a) => s + (a.custo || 0), 0);
  const porTipo = {};
  for (const a of elegiveis) { if (!porTipo[a.tipo]) porTipo[a.tipo] = { n: 0, q: 0, c: 0 }; porTipo[a.tipo].n++; porTipo[a.tipo].q += a.quedaConfirmada; porTipo[a.tipo].c += (a.custo || 0); }

  console.log('\n=== POR TIPO (so elegiveis) ===');
  for (const [t, v] of Object.entries(porTipo)) console.log('  ' + t.padEnd(12) + ' ' + String(v.n).padStart(3) + ' perfis · repor ' + v.q.toLocaleString('pt-BR').padStart(8) + ' · custo ' + money(v.c));

  console.log('\n=== RESUMO DA VERIFICACAO (152 analisados) ===');
  console.log('  ELEGIVEIS (queda real + completo) : ' + elegiveis.length + '  ->  repor ' + somaQ.toLocaleString('pt-BR') + ' · custo ' + money(somaC));
  console.log('  sem queda real (<=' + LIM + ')          : ' + semQueda.length + '   (meu palpite estava inflado)');
  console.log('  com queda mas pedido nao-completo : ' + naoCompleto.length);
  console.log('  falha ao verificar status         : ' + falha.length);
  console.log('\n  CSV: ' + arq + '\n');
  process.exit(0);
})().catch(e => { console.error('ERRO', e && e.message ? e.message : String(e)); process.exit(1); });
