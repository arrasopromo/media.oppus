// scripts/custoDoMesSim.js
// ---------------------------------------------------------------------------
// SIMULAÇÃO do custo do mês: mantém o charge REAL de todos os serviços,
// MAS troca o custo do PRINCIPAL de "seguidores mistos" por uma taxa fixa
// por 1.000 (default R$ 3,50/1.000). Mostra real vs simulado e a diferença.
//
// USO:
//   node scripts/custoDoMesSim.js --month=2026-07 --mistos=3.5
// ---------------------------------------------------------------------------

const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });
const axios = require('axios');
const { getCollection } = require('../mongodbClient');

const PROVIDERS = {
  fama24h:{url:'https://fama24h.net/api/v2',keyEnv:'FAMA24H_API_KEY'},
  fama24h_views:{url:'https://fama24h.net/api/v2',keyEnv:'FAMA24H_API_KEY'},
  fama24h_likes:{url:'https://fama24h.net/api/v2',keyEnv:'FAMA24H_API_KEY'},
  fornecedor_social:{url:'https://fornecedorsocial.com/api/v2',keyEnv:'FORNECEDOR_SOCIAL_API_KEY'},
  fornecedor_social_likes:{url:'https://fornecedorsocial.com/api/v2',keyEnv:'FORNECEDOR_SOCIAL_API_KEY'},
  topfama:{url:'https://topfama.com/api/v2',keyEnv:'TOPFAMA_API_KEY'},
  topfama_likes:{url:'https://topfama.com/api/v2',keyEnv:'TOPFAMA_API_KEY'},
  worldsmm_comments:{url:'https://worldsmm.com.br/api/v2',keyEnv:'WORLDSMM_API_KEY'},
  ggram:{url:'https://ggram.me/api/v2',keyEnv:'GGRAM_API_KEY'},
};
const cli=(n,d=null)=>{const k=`--${n}`;const h=process.argv.find(a=>a===k||a.startsWith(k+'='));if(!h)return d;return h===k?true:h.split('=').slice(1).join('=');};
const sleep=(ms)=>new Promise(r=>setTimeout(r,ms));
const valid=(v)=>{const s=String(v==null?'':v).trim().toLowerCase();return !!s&&!['unknown','unknow','null','undefined','0'].includes(s);};
const pc=(v)=>{const s=String(v==null?'':v).trim();if(!s)return null;const n=Number(s.replace(',','.').replace(/[^\d.-]/g,''));return Number.isFinite(n)?n:null;};
const chgOf=(p)=>{if(!p||typeof p!=='object')return null;for(const k of ['charge','Charge','cost','Cost']){const c=pc(p[k]);if(c!=null)return c;}return null;};

function infoAny(o,key){ if(o.additionalInfoMapPaid&&typeof o.additionalInfoMapPaid[key]!=='undefined')return String(o.additionalInfoMapPaid[key]); if(o.additionalInfoMap&&typeof o.additionalInfoMap[key]!=='undefined')return String(o.additionalInfoMap[key]); const a=Array.isArray(o.additionalInfoPaid)?o.additionalInfoPaid:[];const ip=a.find(i=>i&&i.key===key);if(ip)return String(ip.value); const b=Array.isArray(o.additionalInfo)?o.additionalInfo:[];const it=b.find(i=>i&&i.key===key);if(it)return String(it.value); return ''; }
function isSeguidores(cat,tipo){const c=String(cat||'').toLowerCase();const t=String(tipo||'').toLowerCase();if(/curtida|like|visualiz|view|reel|coment/.test(c))return false;if(c==='seguidores')return true;if(t==='mistos'||t==='brasileiros'||t==='organicos')return true;return false;}
function isMistosSeg(o){const cat=infoAny(o,'categoria_servico');const tipo=(infoAny(o,'tipo_servico')||o.tipo||o.tipoServico||'').toLowerCase();return isSeguidores(cat,tipo)&&tipo==='mistos';}
function qtyMain(subArr,o){ let q=0; for(const s of subArr){ const rq=s.reqQty; if(Number.isFinite(rq)&&rq>0)q+=rq; } if(q>0)return q; const q0=Number(o.quantidade||o.qtd||infoAny(o,'quantidade')||infoAny(o,'qtd'))||0; return q0; }

const cache=new Map();
async function fetchStatus(provider,orderId){ const key=`${provider}:${orderId}`; if(cache.has(key))return cache.get(key); const cfg=PROVIDERS[provider]; const apiKey=cfg?String(process.env[cfg.keyEnv]||'').trim():''; if(!apiKey){const r={charge:null};cache.set(key,r);return r;} try{ const payload=new URLSearchParams({key:apiKey,action:'status',order:String(orderId)}); const resp=await axios.post(cfg.url,payload.toString(),{headers:{'Content-Type':'application/x-www-form-urlencoded'},timeout:25000}); const r={charge:chgOf((resp&&resp.data)||{})}; cache.set(key,r); return r; }catch(e){const r={charge:null};cache.set(key,r);return r;} }

function collectSubs(o){
  const subs=[];
  const reqQ=(d)=>{const q=d&&d.requestPayload&&d.requestPayload.quantity;const n=Number(q);return Number.isFinite(n)?n:0;};
  const push=(slot,d,isMain)=>{if(!d)return;const oid=d.orderId??d.orderID??d.order_id;if(!valid(oid))return;subs.push({_id:o._id,provider:slot,orderId:String(oid).trim(),charge:chgOf(d.statusPayload),isMain:!!isMain,reqQty:reqQ(d)});};
  const pushMulti=(slot,d,isMain)=>{const arr=(d&&Array.isArray(d.orders))?d.orders:[];for(const it of arr){const oid=it&&(it.orderId??it.id);if(!valid(oid))continue;subs.push({_id:o._id,provider:slot,orderId:String(oid).trim(),charge:chgOf(it.statusPayload),isMain:!!isMain,reqQty:reqQ(it)});}};
  // principal
  if(o.fama24h)push('fama24h',o.fama24h,true);else if(o.fama24h_multi)pushMulti('fama24h',o.fama24h_multi,true);
  if(o.fornecedor_social)push('fornecedor_social',o.fornecedor_social,true);else if(o.fornecedor_social_multi)pushMulti('fornecedor_social',o.fornecedor_social_multi,true);
  if(o.topfama)push('topfama',o.topfama,true);
  if(o.ggram)push('ggram',o.ggram,true);
  // bumps (nunca são "principal")
  if(o.fama24h_views)push('fama24h_views',o.fama24h_views,false);
  if(o.fama24h_likes)push('fama24h_likes',o.fama24h_likes,false);
  if(o.fornecedor_social_likes)push('fornecedor_social_likes',o.fornecedor_social_likes,false);
  if(o.topfama_likes)push('topfama_likes',o.topfama_likes,false);
  if(o.worldsmm_comments)push('worldsmm_comments',o.worldsmm_comments,false);
  return subs;
}

async function main(){
  const monthArg=String(cli('month','')||''); let y,m;
  if(/^\d{4}-\d{2}$/.test(monthArg)){y=Number(monthArg.slice(0,4));m=Number(monthArg.slice(5,7))-1;}else{const d=new Date();y=d.getUTCFullYear();m=d.getUTCMonth();}
  const startMs=Date.UTC(y,m,1,3,0,0), endMs=Date.UTC(y,m+1,1,3,0,0); const label=`${String(m+1).padStart(2,'0')}/${y}`;
  const rate=Number(String(cli('mistos','3.5')).replace(',','.'))||3.5;

  const col=await getCollection('checkout_orders');
  const paidOr=[{status:'pago'},{'woovi.status':'pago'},{paidAt:{$exists:true,$nin:[null,'']}},{'woovi.paidAt':{$exists:true,$nin:[null,'']}},{'paghiper.paidAt':{$exists:true,$nin:[null,'']}}];
  const proj={paidAt:1,'woovi.paidAt':1,'paghiper.paidAt':1,createdAt:1,quantidade:1,qtd:1,tipo:1,tipoServico:1,additionalInfoMapPaid:1,additionalInfoPaid:1,additionalInfoMap:1,additionalInfo:1,fama24h:1,fama24h_multi:1,fornecedor_social:1,fornecedor_social_multi:1,topfama:1,topfama_likes:1,fama24h_views:1,fama24h_likes:1,fornecedor_social_likes:1,worldsmm_comments:1,ggram:1};
  const all=await col.find({$or:paidOr}).project(proj).limit(300000).toArray();
  const inMonth=(o)=>{for(const c of [o.paidAt,o.woovi&&o.woovi.paidAt,o.paghiper&&o.paghiper.paidAt,o.createdAt]){const t=c?new Date(c).getTime():NaN;if(Number.isFinite(t))return t>=startMs&&t<endMs;}return false;};
  const orders=all.filter(inMonth);

  // sub-pedidos por pedido, marcando os mistos-seguidores
  const perOrder=[];
  let allSubs=[];
  for(const o of orders){ const subs=collectSubs(o); const mistos=isMistosSeg(o); perOrder.push({o,subs,mistos}); allSubs=allSubs.concat(subs); }
  const need=allSubs.filter(s=>s.charge==null);
  console.log(`\nMês ${label}: ${orders.length} pedidos · ${allSubs.length} sub-pedidos · ${need.length} p/ consultar ao vivo · taxa mistos simulada = R$ ${rate.toFixed(2)}/1.000`);

  let idx=0,done=0; const CONC=6;
  const worker=async()=>{while(idx<need.length){const s=need[idx++];const r=await fetchStatus(s.provider,s.orderId);s.charge=r.charge;done++;await sleep(120);if(done%300===0)console.log(`  … ${done}/${need.length}`);}};
  await Promise.all(Array.from({length:CONC},worker));

  // totais
  let real=0, sim=0;
  let mistosReal=0, mistosSim=0, mistosQty=0, mistosOrders=0, mistosMainSubs=0;
  for(const {o,subs,mistos} of perOrder){
    for(const s of subs){ if(s.charge!=null) real+=s.charge; }
    if(mistos){
      mistosOrders++;
      const mainSubs=subs.filter(s=>s.isMain && s.provider==='fama24h');
      const qty=qtyMain(mainSubs,o);
      const realMain=mainSubs.reduce((a,s)=>a+(s.charge!=null?s.charge:0),0);
      const simMain=(qty/1000)*rate;
      mistosReal+=realMain; mistosSim+=simMain; mistosQty+=qty; mistosMainSubs+=mainSubs.length;
      // simulado = real total - real do principal mistos + simulado do principal mistos
      for(const s of subs){ if(s.charge!=null && !(s.isMain && s.provider==='fama24h')) sim+=s.charge; }
      sim+=simMain;
    } else {
      for(const s of subs){ if(s.charge!=null) sim+=s.charge; }
    }
  }
  const money=(n)=>'R$ '+(Number(n)||0).toLocaleString('pt-BR',{minimumFractionDigits:2,maximumFractionDigits:2});
  console.log('\n════════════════════════════════════════════════════════════════');
  console.log(`  CUSTO DO MÊS ${label} — REAL vs SIMULAÇÃO (mistos @ R$ ${rate.toFixed(2)}/1.000)`);
  console.log('════════════════════════════════════════════════════════════════');
  console.log(`  Seguidores mistos: ${mistosOrders} pedidos · ${mistosQty.toLocaleString('pt-BR')} seguidores despachados (${mistosMainSubs} sub-pedidos fama24h)`);
  console.log(`     custo REAL dos mistos:      ${money(mistosReal)}  (≈ R$ ${(mistosQty>0?mistosReal/mistosQty*1000:0).toFixed(2)}/1.000)`);
  console.log(`     custo SIMULADO dos mistos:  ${money(mistosSim)}  (R$ ${rate.toFixed(2)}/1.000)`);
  console.log('  ──────────────────────────────────────────────');
  console.log(`  TOTAL REAL do mês:      ${money(real)}`);
  console.log(`  TOTAL SIMULADO do mês:  ${money(sim)}   (Δ ${money(sim-real)})`);
  console.log('');
  process.exit(0);
}
main().catch(e=>{console.error('[custoDoMesSim] erro:',e&&e.message?e.message:String(e));process.exit(1);});
