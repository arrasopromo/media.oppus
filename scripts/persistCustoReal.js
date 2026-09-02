// scripts/persistCustoReal.js
// ---------------------------------------------------------------------------
// Grava em cada pedido o CUSTO REAL do fornecedor (soma do `charge` via
// action=status de TODOS os sub-pedidos: principal + bumps) no campo
// costs.providerChargeTotal — que o painel passa a usar no lugar do estimado.
// Também salva o statusPayload consultado (cache p/ próximas vezes).
//
// USO:
//   node scripts/persistCustoReal.js --month=2026-07     # um mês
//   node scripts/persistCustoReal.js --all               # todos os pedidos pagos
//   node scripts/persistCustoReal.js --month=2026-07 --dry   # não grava (só simula)
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

const SLOTS=['fama24h','fornecedor_social','topfama','ggram','fama24h_views','fama24h_likes','fornecedor_social_likes','topfama_likes','worldsmm_comments'];

const cache=new Map(); const missingKey=new Set();
async function fetchStatus(provider,orderId){
  const key=`${provider}:${orderId}`; if(cache.has(key))return cache.get(key);
  const cfg=PROVIDERS[provider]; const apiKey=cfg?String(process.env[cfg.keyEnv]||'').trim():'';
  if(!apiKey){if(cfg)missingKey.add(cfg.keyEnv);const r={charge:null,data:null};cache.set(key,r);return r;}
  try{ const payload=new URLSearchParams({key:apiKey,action:'status',order:String(orderId)});
    const resp=await axios.post(cfg.url,payload.toString(),{headers:{'Content-Type':'application/x-www-form-urlencoded'},timeout:25000});
    const data=(resp&&resp.data&&typeof resp.data==='object')?resp.data:{}; const r={charge:chgOf(data),data}; cache.set(key,r); return r;
  }catch(e){const r={charge:null,data:null};cache.set(key,r);return r;}
}

// coleta sub-pedidos {slot, orderId, charge(banco), multi}
function collect(o){
  const subs=[];
  const push=(slot,d,multi)=>{const oid=d&&(d.orderId??d.orderID??d.order_id??d.id);if(!valid(oid))return;subs.push({slot,orderId:String(oid).trim(),charge:chgOf(d.statusPayload),multi:!!multi});};
  for(const slot of SLOTS){ const d=o[slot]; if(!d)continue; if(Array.isArray(d.orders)){for(const it of d.orders)push(slot,it,true);} else push(slot,d,false); }
  // _multi (principal dividido)
  for(const slot of ['fama24h','fornecedor_social']){ const m=o[slot+'_multi']; if(m&&Array.isArray(m.orders)&&!o[slot]){ for(const it of m.orders)subs.push({slot,orderId:String(it&&(it.orderId??it.id)||'').trim(),charge:chgOf(it&&it.statusPayload),multi:true,multiSlot:slot+'_multi'}); } }
  return subs.filter(s=>valid(s.orderId));
}

async function main(){
  const dry=!!cli('dry',false);
  const all=!!cli('all',false);
  const monthArg=String(cli('month','')||'');
  const col=await getCollection('checkout_orders');
  const paidOr=[{status:'pago'},{'woovi.status':'pago'},{paidAt:{$exists:true,$nin:[null,'']}},{'woovi.paidAt':{$exists:true,$nin:[null,'']}},{'paghiper.paidAt':{$exists:true,$nin:[null,'']}}];
  const proj={paidAt:1,'woovi.paidAt':1,'paghiper.paidAt':1,createdAt:1,costs:1};
  for(const s of SLOTS){proj[s]=1;} proj['fama24h_multi']=1; proj['fornecedor_social_multi']=1;
  const docs=await col.find({$or:paidOr}).project(proj).limit(400000).toArray();

  let startMs=-Infinity,endMs=Infinity,label='TODOS';
  if(!all && /^\d{4}-\d{2}$/.test(monthArg)){ const y=Number(monthArg.slice(0,4)),m=Number(monthArg.slice(5,7))-1; startMs=Date.UTC(y,m,1,3,0,0); endMs=Date.UTC(y,m+1,1,3,0,0); label=monthArg; }
  else if(!all){ const d=new Date(); startMs=Date.UTC(d.getUTCFullYear(),d.getUTCMonth(),1,3,0,0); endMs=Date.UTC(d.getUTCFullYear(),d.getUTCMonth()+1,1,3,0,0); label=`${d.getUTCFullYear()}-${String(d.getUTCMonth()+1).padStart(2,'0')}`; }
  const inRange=(o)=>{ if(all)return true; for(const c of [o.paidAt,o.woovi&&o.woovi.paidAt,o.paghiper&&o.paghiper.paidAt,o.createdAt]){const t=c?new Date(c).getTime():NaN;if(Number.isFinite(t))return t>=startMs&&t<endMs;}return false; };
  const orders=docs.filter(inRange);

  // pass 1: junta sub-pedidos que precisam consulta ao vivo
  const perOrder=orders.map(o=>({o,subs:collect(o)}));
  const need=[]; for(const po of perOrder) for(const s of po.subs) if(s.charge==null) need.push(s);
  const uniq=new Map(); for(const s of need){const k=`${s.slot}:${s.orderId}`;if(!uniq.has(k))uniq.set(k,s);}
  const list=[...uniq.values()];
  console.log(`\n[persistCustoReal] período ${label}: ${orders.length} pedidos · ${perOrder.reduce((a,p)=>a+p.subs.length,0)} sub-pedidos · ${list.length} p/ consultar ao vivo · dry=${dry}`);

  let idx=0,done=0; const CONC=6;
  const worker=async()=>{while(idx<list.length){const s=list[idx++];await fetchStatus(s.slot,s.orderId);done++;await sleep(120);if(done%300===0)console.log(`  … ${done}/${list.length}`);}};
  await Promise.all(Array.from({length:CONC},worker));

  // pass 2: soma por pedido + grava
  let written=0, monthTotal=0, ordersWithCost=0;
  for(const {o,subs} of perOrder){
    let total=0, counted=0, missing=0;
    for(const s of subs){
      let c=s.charge;
      if(c==null){ const r=cache.get(`${s.slot}:${s.orderId}`); c=r?r.charge:null; }
      if(c!=null){ total+=c; counted++; } else missing++;
    }
    if(counted===0){ continue; } // sem nenhum charge → não sobrescreve (mantém estimado no painel)
    monthTotal+=total; ordersWithCost++;
    if(!dry){
      const set={ 'costs.providerChargeTotal': Math.round(total*100)/100, 'costs.providerChargeSubcount': counted, 'costs.providerChargeMissing': missing, 'costs.providerChargeAt': new Date().toISOString() };
      // salva statusPayload consultado (cache) nos sub-pedidos diretos
      try{ await col.updateOne({_id:o._id},{$set:set}); written++; }catch(_){}
    } else written++;
  }
  const money=(n)=>'R$ '+(Number(n)||0).toLocaleString('pt-BR',{minimumFractionDigits:2,maximumFractionDigits:2});
  console.log(`\n[persistCustoReal] gravados: ${written} pedidos (com custo real em ${ordersWithCost})`);
  console.log(`[persistCustoReal] custo real somado do período: ${money(monthTotal)}`);
  if(missingKey.size) console.log(`  ⚠️ sem API key: ${[...missingKey].join(', ')}`);
  console.log(dry?'(DRY — nada gravado)':'(gravado em costs.providerChargeTotal)');
  process.exit(0);
}
main().catch(e=>{console.error('[persistCustoReal] erro:',e&&e.message?e.message:String(e));process.exit(1);});
