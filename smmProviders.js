'use strict';
// Lista única dos fornecedores SMM (API v2: action=status/add) para os módulos fora do app.js
// (bot de vendas e follow-up diário). Mesma lista do SMM_PROVIDERS do app.js.
// O fornecedor REAL de um pedido é o `provider` gravado no slot/sub-pedido; sem carimbo, cai no
// padrão antigo do slot (fama24h por tamanho do orderId, fornecedor_social, topfama).
const PROVIDERS = {
  fama24h:           { url: 'https://fama24h.net/api/v2',          keyEnv: 'FAMA24H_API_KEY',           label: 'Fama24h' },
  nuvra:             { url: 'https://nuvrasmm.com/api/v2',         keyEnv: 'NUVRASMM_API_KEY',          label: 'NuvraSMM' },
  fornecedor_social: { url: 'https://fornecedorsocial.com/api/v2', keyEnv: 'FORNECEDOR_SOCIAL_API_KEY', label: 'Fornecedor Social' },
  topfama:           { url: 'https://topfama.com/api/v2',          keyEnv: 'TOPFAMA_API_KEY',           label: 'TopFama' },
  smmhustle:         { url: 'https://smmhustle.com/api/v2',        keyEnv: 'SMMHUSTLE_API_KEY',         label: 'SMMHustle' },
  worldsmm:          { url: 'https://worldsmm.com.br/api/v2',      keyEnv: 'WORLDSMM_API_KEY',          label: 'WorldSMM' },
  smmraja:           { url: 'https://smmraja.com/api/v2',          keyEnv: 'SMMRAJA_API_KEY',           label: 'SMMRaja' },
  smmturk:           { url: 'https://smmturk.org/api/v2',          keyEnv: 'SMMTURK_API_KEY',           label: 'SMMTurk' },
  justanotherpanel:  { url: 'https://justanotherpanel.com/api/v2', keyEnv: 'JUSTANOTHERPANEL_API_KEY',  label: 'JAP' },
  peakerr:           { url: 'https://peakerr.com/api/v2',          keyEnv: 'PEAKERR_API_KEY',           label: 'Peakerr' },
  n1panel:           { url: 'https://n1panel.com/api/v2',          keyEnv: 'N1PANEL_API_KEY',           label: 'N1Panel' },
  followiz:          { url: 'https://followiz.com/api/v2',         keyEnv: 'FOLLOWIZ_API_KEY',          label: 'Followiz' },
  smmcost:           { url: 'https://smmcost.com/api/v2',          keyEnv: 'SMMCOST_API_KEY',           label: 'SMMCost' },
  bulkfollows:       { url: 'https://bulkfollows.com/api/v2',      keyEnv: 'BULKFOLLOWS_API_KEY',       label: 'BulkFollows' },
  worldofsmm:        { url: 'https://worldofsmm.com/api/v2',       keyEnv: 'WORLDOFSMM_API_KEY',        label: 'WorldOfSMM' },
  dripfeedpanel:     { url: 'https://dripfeedpanel.com/api/v2',    keyEnv: 'DRIPFEEDPANEL_API_KEY',     label: 'DripFeedPanel' },
  bulkmedya:         { url: 'https://bulkmedya.org/api/v2',        keyEnv: 'BULKMEDYA_API_KEY',         label: 'BulkMedya' },
  revendaexclusiva:  { url: 'https://revendaexclusiva.com/api/v2', keyEnv: 'REVENDAEXCLUSIVA_API_KEY',  label: 'Revenda' },
};

const ALIAS = { fama: 'fama24h', nuvrasmm: 'nuvra', fornecedorsocial: 'fornecedor_social', top_fama: 'topfama', smm_hustle: 'smmhustle', hustle: 'smmhustle', world_smm: 'worldsmm', raja: 'smmraja', turk: 'smmturk', jap: 'justanotherpanel', n1: 'n1panel', world_of_smm: 'worldofsmm', dripfeed: 'dripfeedpanel', revenda: 'revendaexclusiva', revenda_exclusiva: 'revendaexclusiva' };
function normalizeProvider(v) {
  const s = String(v || '').trim().toLowerCase().replace(/[^a-z0-9_]/g, '');
  if (!s) return '';
  const k = ALIAS[s] || s;
  return PROVIDERS[k] ? k : '';
}

// Fornecedor de um slot do pedido: carimbo do sub-pedido → carimbo do slot → padrão antigo do slot.
function providerDoSlot(slot, sub, parent) {
  for (const v of [sub && sub.provider, parent && parent.provider]) { const p = normalizeProvider(v); if (p) return p; }
  const id = sub && (sub.orderId || sub.id);
  if (/^fama24h/.test(String(slot || ''))) return Number(id) >= 1000000 ? 'fama24h' : 'nuvra';
  if (/^fornecedor_social/.test(String(slot || ''))) return 'fornecedor_social';
  if (/^topfama/.test(String(slot || ''))) return 'topfama';
  if (/^worldsmm/.test(String(slot || ''))) return 'worldsmm';
  return '';
}

// Pedidos de vários posts (*_multi) gravam os sub-pedidos SEM o fornecedor. Nesses, o fornecedor é
// o configurado pro tipo no Gerenciamento de Tipos (settings service_type_providers), que é o que o
// despacho usou. Cache de 60s.
let __provTipoCache = { at: 0, values: null };
async function providersPorTipo() {
  if (__provTipoCache.values && Date.now() - __provTipoCache.at < 60000) return __provTipoCache.values;
  try {
    const { getCollection } = require('./mongodbClient');
    const col = await getCollection('settings');
    const d = await col.findOne({ _id: 'service_type_providers' }, { projection: { values: 1 } });
    __provTipoCache = { at: Date.now(), values: (d && d.values) || {} };
  } catch (_) { __provTipoCache = { at: Date.now(), values: __provTipoCache.values || {} }; }
  return __provTipoCache.values;
}
function _info(o, k) {
  for (const mp of [o && o.additionalInfoMapPaid, o && o.additionalInfoMap]) if (mp && mp[k] != null && mp[k] !== '') return String(mp[k]);
  for (const arr of [o && o.additionalInfoPaid, o && o.additionalInfo]) if (Array.isArray(arr)) { const x = arr.find((i) => i && i.key === k); if (x && x.value != null && x.value !== '') return String(x.value); }
  return '';
}
// Versão síncrona: recebe o mapa do Gerenciamento de Tipos já carregado (pm).
function providerDoPedidoCom(pm, slot, sub, parent, order) {
  for (const v of [sub && sub.provider, parent && parent.provider]) { const p = normalizeProvider(v); if (p) return p; }
  // Adicionais (bumps) sem carimbo: views → fornecedor de visualizações; curtidas → curtidas mistas.
  if (/^fama24h_(views|likes)$/.test(String(slot || '')) && pm) {
    const p = normalizeProvider(/views/.test(slot) ? (pm.visualizacoes || {}).visualizacoes_reels : (pm.curtidas || {}).mistos);
    if (p) return p;
  }
  if (/_multi$/.test(String(slot || '')) && order && pm) {
    try {
      const cat = String(_info(order, 'categoria_servico') || order.categoria || '').toLowerCase();
      const tipo = String(_info(order, 'tipo_servico') || order.tipo || '').toLowerCase();
      let p = '';
      if (/curtid|like/.test(cat) || /^curtidas/.test(tipo)) p = (pm.curtidas || {})[/brasil/.test(tipo) ? 'curtidas_brasileiras' : (/organic/.test(tipo) ? 'organicos' : 'mistos')];
      else if (/visualiz|view/.test(cat) || /visualiz/.test(tipo)) p = (pm.visualizacoes || {}).visualizacoes_reels;
      else if (/seguid/.test(cat)) p = (pm.seguidores || {})[/brasil/.test(tipo) ? 'brasileiros' : 'mistos'];
      p = normalizeProvider(p);
      if (p) return p;
    } catch (_) {}
  }
  return providerDoSlot(slot, sub, parent);
}
async function providerDoPedido(slot, sub, parent, order) {
  let pm = null; try { pm = await providersPorTipo(); } catch (_) {}
  return providerDoPedidoCom(pm, slot, sub, parent, order);
}

function config(provider) {
  const p = PROVIDERS[normalizeProvider(provider)];
  if (!p) return null;
  const key = String(process.env[p.keyEnv] || '').trim();
  return key ? { url: p.url, key, label: p.label } : null;
}

module.exports = { PROVIDERS, normalizeProvider, providerDoSlot, providerDoPedido, providerDoPedidoCom, providersPorTipo, config };
