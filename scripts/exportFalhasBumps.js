// scripts/exportFalhasBumps.js
// ---------------------------------------------------------------------------
// Exporta (read-only) um CSV das falhas de bump de COMENTÁRIOS (worldsmm) e
// VIEWS (fama24h) — sub-pedidos SEM orderId — com o motivo categorizado e o
// link do post (resolvido mesmo para os travados em "processing", que não têm
// requestPayload). Serve para verificar no painel do fornecedor o que foi/não
// foi entregue e re-despachar.
//
// NÃO grava nada no banco.
//
// USO:  node scripts/exportFalhasBumps.js
//       node scripts/exportFalhasBumps.js --out=falhas.csv
// ---------------------------------------------------------------------------

const path = require('path');
const fs = require('fs');
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });
const { getCollection } = require('../mongodbClient');

function cliArg(name, def = null) {
  const key = `--${name}`;
  const hit = process.argv.find(a => a === key || a.startsWith(`${key}=`));
  if (!hit) return def;
  if (hit === key) return true;
  return hit.split('=').slice(1).join('=');
}

const valid = (v) => { const s = String(v == null ? '' : v).trim().toLowerCase(); return !!s && !['unknown', 'unknow', 'null', 'undefined', '0'].includes(s); };

function extractInfoAny(o, key) {
  if (o.additionalInfoMapPaid && typeof o.additionalInfoMapPaid[key] !== 'undefined') return String(o.additionalInfoMapPaid[key]);
  if (o.additionalInfoMap && typeof o.additionalInfoMap[key] !== 'undefined') return String(o.additionalInfoMap[key]);
  const a = Array.isArray(o.additionalInfoPaid) ? o.additionalInfoPaid : [];
  const ip = a.find(i => i && i.key === key); if (ip) return String(ip.value);
  const b = Array.isArray(o.additionalInfo) ? o.additionalInfo : [];
  const it = b.find(i => i && i.key === key); if (it) return String(it.value);
  return '';
}

function sanitizeInstagramPostLink(u) {
  let v = String(u || '').trim();
  if (!v) return '';
  if (!/^https?:\/\//i.test(v) && /instagram\.com\//i.test(v)) v = 'https://' + v.replace(/^\/+/, '');
  v = v.split('#')[0].split('?')[0];
  const m = v.match(/^https?:\/\/(www\.)?instagram\.com\/(p|reel|tv)\/([A-Za-z0-9_-]+)\/?$/i);
  if (!m) return String(u || '').trim(); // devolve cru se não casar (melhor que vazio p/ conferência)
  return `https://www.instagram.com/${m[2].toLowerCase()}/${m[3]}/`;
}

function postLink(o, tipo) {
  const sub = tipo === 'comentarios' ? o.worldsmm_comments : o.fama24h_views;
  const cands = [
    sub && sub.requestPayload && sub.requestPayload.link,
    extractInfoAny(o, tipo === 'comentarios' ? 'orderbump_post_comments' : 'orderbump_post_views'),
    extractInfoAny(o, 'post_link'),
  ];
  for (const c of cands) { const s = sanitizeInstagramPostLink(c); if (s) return s; }
  return '';
}

function resolveUser(o) {
  return String(extractInfoAny(o, 'instagram_username') || o.instauser || o.instagramUsername || '').replace(/^@+/, '').replace(/\/+$/, '').trim();
}
function resolvePhone(o) {
  const c = o.customer || {};
  return String(c.phone || c.telefone || c.whatsapp || o.telefone || o.phone || extractInfoAny(o, 'telefone') || '').trim();
}
function serviceLabel(o) {
  const cat = extractInfoAny(o, 'categoria_servico');
  const tipo = extractInfoAny(o, 'tipo_servico') || o.tipo || o.tipoServico || '';
  return [cat, tipo].filter(Boolean).join(' / ') || (o.type || '-');
}
function dateSP(o) {
  const c = (o.woovi && o.woovi.paidAt) || o.paidAt || o.createdAt;
  const t = c ? new Date(c).getTime() : NaN;
  if (!Number.isFinite(t)) return '';
  const d = new Date(t - 3 * 3600 * 1000);
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth()+1).padStart(2,'0')}-${String(d.getUTCDate()).padStart(2,'0')} ${String(d.getUTCHours()).padStart(2,'0')}:${String(d.getUTCMinutes()).padStart(2,'0')}`;
}

function motivo(sub) {
  if (!sub) return 'ausente';
  const st = String(sub.status || '').toLowerCase().trim();
  const errRaw = sub.error != null ? (typeof sub.error === 'string' ? sub.error : JSON.stringify(sub.error)) : '';
  const respErr = sub.response && sub.response.error ? (typeof sub.response.error === 'string' ? sub.response.error : JSON.stringify(sub.response.error)) : '';
  const e = (errRaw + ' ' + respErr).toLowerCase();
  if (e.includes('link_duplicate')) return 'link_duplicate (fornecedor recusou: já existia pedido)';
  if (e.includes('invalid_link')) return 'invalid_link (link do post inválido)';
  if (e.includes('not_enough_funds')) return 'not_enough_funds (sem saldo no fornecedor)';
  if (e.includes('502') || e.includes('bad gateway')) return '502 (erro do fornecedor)';
  if (e.includes('timeout')) return 'timeout';
  if (st === 'processing') return 'TRAVADO em processing (disparo não concluído)';
  if (st === 'unknown') return 'unknown (sem confirmação)';
  if (!st) return 'sem status';
  return st;
}

const csvCell = (v) => { const s = String(v == null ? '' : v); return /[",\n;]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s; };

async function main() {
  const outPath = path.resolve(String(cliArg('out', 'falhas_bumps.csv')));
  const col = await getCollection('checkout_orders');
  const paid = { $or: [{ status: 'pago' }, { 'woovi.status': 'pago' }] };

  const rows = [['tipo', 'data_sp', 'usuario', 'telefone', 'servico', 'qtd', 'status', 'motivo', 'orderId', 'link_post', '_id']];
  const resumo = { comentarios: {}, views: {} };

  for (const [tipo, campo] of [['comentarios', 'worldsmm_comments'], ['views', 'fama24h_views']]) {
    const docs = await col.find({ $and: [paid, { [campo]: { $exists: true } }] }).toArray();
    for (const o of docs) {
      const sub = o[campo] || {};
      if (valid(sub.orderId)) continue; // já tem orderid → ok, não é falha
      const mot = motivo(sub);
      const chave = mot.split(' ')[0].replace('(', '');
      resumo[tipo][chave] = (resumo[tipo][chave] || 0) + 1;
      const qtd = (sub.requestPayload && sub.requestPayload.quantity) || (tipo === 'comentarios' ? '' : '');
      rows.push([
        tipo, dateSP(o), '@' + resolveUser(o), resolvePhone(o), serviceLabel(o),
        qtd, String(sub.status || ''), mot, String(sub.orderId || ''), postLink(o, tipo), String(o._id),
      ]);
    }
  }

  const csv = rows.map(r => r.map(csvCell).join(',')).join('\n');
  fs.writeFileSync(outPath, '﻿' + csv, 'utf8'); // BOM p/ Excel abrir acentos certo

  console.log('CSV gerado:', outPath);
  console.log('linhas (fora cabeçalho):', rows.length - 1);
  console.log('resumo COMENTÁRIOS:', JSON.stringify(resumo.comentarios));
  console.log('resumo VIEWS:', JSON.stringify(resumo.views));
  process.exit(0);
}

main().catch(e => { console.error('[exportFalhasBumps] erro:', e && e.message ? e.message : String(e)); process.exit(1); });
