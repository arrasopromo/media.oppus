// scripts/custoDoMes.js
// ---------------------------------------------------------------------------
// Custo operacional REAL do mês (fornecedores) — soma o `charge` (action=status)
// de TODOS os sub-pedidos (principal + bumps) dos pedidos pagos no mês.
// Usa o charge já salvo (statusPayload) quando existe; consulta ao vivo o resto.
//
// USO:
//   node scripts/custoDoMes.js                  # mês atual
//   node scripts/custoDoMes.js --month=2026-07  # mês específico
//   node scripts/custoDoMes.js --persist        # grava o statusPayload consultado (cache p/ próxima vez)
// ---------------------------------------------------------------------------

const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });
const axios = require('axios');
const { getCollection } = require('../mongodbClient');

const PROVIDERS = {
  fama24h:                 { url: 'https://fama24h.net/api/v2',        keyEnv: 'FAMA24H_API_KEY' },
  fama24h_views:           { url: 'https://fama24h.net/api/v2',        keyEnv: 'FAMA24H_API_KEY' },
  fama24h_likes:           { url: 'https://fama24h.net/api/v2',        keyEnv: 'FAMA24H_API_KEY' },
  fornecedor_social:       { url: 'https://fornecedorsocial.com/api/v2', keyEnv: 'FORNECEDOR_SOCIAL_API_KEY' },
  fornecedor_social_likes: { url: 'https://fornecedorsocial.com/api/v2', keyEnv: 'FORNECEDOR_SOCIAL_API_KEY' },
  topfama:                 { url: 'https://topfama.com/api/v2',        keyEnv: 'TOPFAMA_API_KEY' },
  topfama_likes:           { url: 'https://topfama.com/api/v2',        keyEnv: 'TOPFAMA_API_KEY' },
  worldsmm_comments:       { url: 'https://worldsmm.com.br/api/v2',    keyEnv: 'WORLDSMM_API_KEY' },
  ggram:                   { url: 'https://ggram.me/api/v2',           keyEnv: 'GGRAM_API_KEY' },
};
const NAME = { fama24h:'Fama24h', fama24h_views:'Fama24h Views', fama24h_likes:'Fama24h Curtidas', fornecedor_social:'Fornecedor Social', fornecedor_social_likes:'Forn.Social Curtidas', topfama:'TopFama', topfama_likes:'TopFama Curtidas', worldsmm_comments:'WorldSMM Coment.', ggram:'GGRAM' };

const cli = (n, d=null) => { const k=`--${n}`; const h=process.argv.find(a=>a===k||a.startsWith(k+'=')); if(!h)return d; return h===k?true:h.split('=').slice(1).join('='); };
const sleep = (ms) => new Promise(r=>setTimeout(r,ms));
const valid = (v) => { const s=String(v==null?'':v).trim().toLowerCase(); return !!s && !['unknown','unknow','null','undefined','0'].includes(s); };
const parseCharge = (v) => { const s=String(v==null?'':v).trim(); if(!s)return null; const n=Number(s.replace(',','.').replace(/[^\d.-]/g,'')); return Number.isFinite(n)?n:null; };
const chgOf = (p) => { if(!p||typeof p!=='object')return null; for(const k of ['charge','Charge','cost','Cost','price','Price']){const c=parseCharge(p[k]); if(c!=null)return c;} return null; };

function collectSubs(o) {
  const subs = [];
  const push = (slot, d) => { if(!d)return; const oid=d.orderId??d.orderID??d.order_id; if(!valid(oid))return; subs.push({ _id:o._id, slot, provider:slot, orderId:String(oid).trim(), charge:chgOf(d.statusPayload) }); };
  const pushMulti = (slot, d) => { const arr=(d&&Array.isArray(d.orders))?d.orders:[]; for(const it of arr){ const oid=it&&(it.orderId??it.id); if(!valid(oid))continue; subs.push({ _id:o._id, slot, provider:slot, orderId:String(oid).trim(), charge:chgOf(it.statusPayload), multi:true }); } };
  if(o.fama24h) push('fama24h',o.fama24h); else if(o.fama24h_multi) pushMulti('fama24h',o.fama24h_multi);
  if(o.fornecedor_social) push('fornecedor_social',o.fornecedor_social); else if(o.fornecedor_social_multi) pushMulti('fornecedor_social',o.fornecedor_social_multi);
  if(o.topfama) push('topfama',o.topfama);
  if(o.ggram) push('ggram',o.ggram);
  if(o.fama24h_views) push('fama24h_views',o.fama24h_views);
  if(o.fama24h_likes) push('fama24h_likes',o.fama24h_likes);
  if(o.fornecedor_social_likes) push('fornecedor_social_likes',o.fornecedor_social_likes);
  if(o.topfama_likes) push('topfama_likes',o.topfama_likes);
  if(o.worldsmm_comments) push('worldsmm_comments',o.worldsmm_comments);
  return subs;
}

const cache = new Map();
const missingKey = new Set();
async function fetchStatus(provider, orderId) {
  const key = `${provider}:${orderId}`;
  if (cache.has(key)) return cache.get(key);
  const cfg = PROVIDERS[provider];
  const apiKey = cfg ? String(process.env[cfg.keyEnv]||'').trim() : '';
  if (!apiKey) { if(cfg) missingKey.add(cfg.keyEnv); const r={charge:null,err:'no_key'}; cache.set(key,r); return r; }
  try {
    const payload = new URLSearchParams({ key:apiKey, action:'status', order:String(orderId) });
    const resp = await axios.post(cfg.url, payload.toString(), { headers:{'Content-Type':'application/x-www-form-urlencoded'}, timeout:25000 });
    const data = (resp&&resp.data&&typeof resp.data==='object')?resp.data:{};
    const r = { charge: chgOf(data), data };
    cache.set(key, r); return r;
  } catch(e) { const r={charge:null,err:(e&&e.message)?e.message.slice(0,40):'err'}; cache.set(key,r); return r; }
}

async function main() {
  const persist = !!cli('persist', false);
  const monthArg = String(cli('month','') || '');
  let y, m; // m: 0-11
  if (/^\d{4}-\d{2}$/.test(monthArg)) { y=Number(monthArg.slice(0,4)); m=Number(monthArg.slice(5,7))-1; }
  else { const d=new Date(); y=d.getUTCFullYear(); m=d.getUTCMonth(); }
  const startMs = Date.UTC(y, m, 1, 3, 0, 0);        // dia 1 00:00 SP (-03:00) = 03:00 UTC
  const endMs   = Date.UTC(y, m+1, 1, 3, 0, 0);      // 1º do mês seguinte 00:00 SP
  const label = `${String(m+1).padStart(2,'0')}/${y}`;

  const col = await getCollection('checkout_orders');
  const paidOr=[{status:'pago'},{'woovi.status':'pago'},{paidAt:{$exists:true,$nin:[null,'']}},{'woovi.paidAt':{$exists:true,$nin:[null,'']}},{'paghiper.paidAt':{$exists:true,$nin:[null,'']}}];
  const proj={paidAt:1,'woovi.paidAt':1,'paghiper.paidAt':1,createdAt:1,fama24h:1,fama24h_multi:1,fornecedor_social:1,fornecedor_social_multi:1,topfama:1,topfama_likes:1,fama24h_views:1,fama24h_likes:1,fornecedor_social_likes:1,worldsmm_comments:1,ggram:1};
  const all = await col.find({$or:paidOr}).project(proj).limit(300000).toArray();
  const inMonth=(o)=>{ for(const c of [o.paidAt,o.woovi&&o.woovi.paidAt,o.paghiper&&o.paghiper.paidAt,o.createdAt]){const t=c?new Date(c).getTime():NaN; if(Number.isFinite(t))return t>=startMs&&t<endMs;} return false; };
  const orders = all.filter(inMonth);

  // coleta sub-pedidos
  let subs = [];
  for (const o of orders) subs = subs.concat(collectSubs(o));
  const need = subs.filter(s => s.charge == null);
  console.log(`\nMês ${label}: ${orders.length} pedidos pagos · ${subs.length} sub-pedidos c/ orderid · ${subs.length-need.length} charge no banco · ${need.length} p/ consultar ao vivo`);

  // consulta ao vivo (concorrência)
  const CONC = Math.max(1, Math.min(10, parseInt(cli('conc','6'),10)||6));
  let done=0, idx=0;
  const worker = async () => {
    while (idx < need.length) {
      const s = need[idx++];
      const r = await fetchStatus(s.provider, s.orderId);
      s.charge = r.charge;
      if (persist && r.data && r.charge!=null) {
        try {
          if (s.multi) await col.updateOne({_id:s._id}, {$set:{[`${s.slot}_multi.orders.$[o].statusPayload`]:r.data}}, {arrayFilters:[{$or:[{'o.orderId':s.orderId},{'o.orderId':Number(s.orderId)},{'o.id':s.orderId},{'o.id':Number(s.orderId)}]}]});
          else await col.updateOne({_id:s._id}, {$set:{[`${s.slot}.statusPayload`]:r.data}});
        } catch(_){}
      }
      done++; await sleep(120);
      if (done % 200 === 0) console.log(`  … ${done}/${need.length} consultados`);
    }
  };
  await Promise.all(Array.from({length:CONC}, worker));

  // soma
  let total=0, comCharge=0, semCharge=0;
  const byProv = {};
  for (const s of subs) {
    if (s.charge!=null) { total+=s.charge; comCharge++; byProv[s.provider]=(byProv[s.provider]||0)+s.charge; }
    else semCharge++;
  }
  const money=(n)=>'R$ '+(Number(n)||0).toLocaleString('pt-BR',{minimumFractionDigits:2,maximumFractionDigits:2});

  console.log('\n════════════════════════════════════════════════════════════════');
  console.log(`  CUSTO OPERACIONAL (fornecedores) — ${label}`);
  console.log('════════════════════════════════════════════════════════════════');
  console.log('  Por fornecedor:');
  Object.entries(byProv).sort((a,b)=>b[1]-a[1]).forEach(([p,v])=>console.log(`    ${String(NAME[p]||p).padEnd(22)} ${money(v).padStart(14)}`));
  console.log('  ──────────────────────────────────────────────');
  console.log(`  TOTAL DO MÊS: ${money(total)}`);
  console.log(`  (${comCharge} sub-pedidos com charge · ${semCharge} sem charge/erro na consulta)`);
  if (missingKey.size) console.log(`  ⚠️ sem API key: ${[...missingKey].join(', ')} — custo desses não entrou`);
  console.log('');
  process.exit(0);
}
main().catch(e=>{console.error('[custoDoMes] erro:', e&&e.message?e.message:String(e)); process.exit(1);});
