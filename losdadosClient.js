// losdadosClient.js
// Cliente HTTP para a API losdados (enriquecimento de dados cadastrais).
// USO EXCLUSIVO: obter CPF/endereço do PRÓPRIO cliente para emissão de nota
// fiscal (NF-e de ebook exige CPF + endereço). Não usar para outra finalidade.
//
// Segredo: a chave vem de LOSDADOS_API_KEY (.env), nunca hardcoded.
// Base: https://app.losdados.com.br/api/v1

const axios = require('axios');

const BASE_URL = 'https://app.losdados.com.br/api/v1';

function envBool(v, def = false) {
  const s = String(v == null ? '' : v).trim().toLowerCase();
  if (s === '') return def;
  return s === 'true' || s === '1' || s === 'sim' || s === 'yes';
}

function getConfig() {
  const apiKey = String(process.env.LOSDADOS_API_KEY || '').trim();
  return {
    apiKey,
    // Habilitado quando há chave e não foi explicitamente desligado.
    enabled: !!apiKey && envBool(process.env.LOSDADOS_ENABLED, true),
    baseUrl: String(process.env.LOSDADOS_BASE_URL || BASE_URL).trim(),
    timeoutMs: Number(process.env.LOSDADOS_TIMEOUT_MS || 20000),
  };
}

function isConfigured() {
  const c = getConfig();
  return !!(c.enabled && c.apiKey);
}

/** Telefone só com dígitos; remove DDI 55 quando presente. */
function normPhone(v) {
  let d = String(v || '').replace(/\D/g, '');
  if (d.length > 11 && d.startsWith('55')) d = d.slice(2);
  return d;
}

function onlyDigits(v) {
  return String(v == null ? '' : v).replace(/\D/g, '');
}

/** Requisição genérica. Nunca lança — retorna { ok, status, data, error }. */
async function request(path, params) {
  const cfg = getConfig();
  if (!cfg.enabled) return { ok: false, status: 0, data: null, error: 'losdados_disabled' };
  if (!cfg.apiKey) return { ok: false, status: 0, data: null, error: 'losdados_missing_key' };

  try {
    const resp = await axios.get(`${cfg.baseUrl}${path}`, {
      params,
      headers: { 'X-API-Key': cfg.apiKey, Accept: 'application/json' },
      timeout: cfg.timeoutMs,
      validateStatus: () => true,
    });
    const ok = resp.status >= 200 && resp.status < 300;
    if (!ok) {
      return { ok: false, status: resp.status, data: resp.data, error: `losdados_http_${resp.status}` };
    }
    return { ok: true, status: resp.status, data: resp.data };
  } catch (e) {
    return { ok: false, status: 0, data: null, error: e && e.message ? e.message : 'losdados_network_error' };
  }
}

/** Consulta por telefone. */
function consultaTelefone(telefone) {
  const t = normPhone(telefone);
  if (!t || t.length < 10) return Promise.resolve({ ok: false, status: 0, data: null, error: 'telefone_invalido' });
  return request('/consulta/telefone', { telefone: t });
}

/** Consulta por CPF. */
function consultaCpf(cpf) {
  const c = onlyDigits(cpf);
  if (c.length !== 11) return Promise.resolve({ ok: false, status: 0, data: null, error: 'cpf_invalido' });
  return request('/consulta/cpf', { cpf: c });
}

module.exports = {
  getConfig,
  isConfigured,
  consultaTelefone,
  consultaCpf,
  request,
  normPhone,
  BASE_URL,
};
