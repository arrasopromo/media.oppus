// spedyClient.js
// Cliente HTTP fino para a API da Spedy (emissão de notas fiscais).
// Documentação: https://api.spedy.com.br/v1
//
// IMPORTANTE: este módulo NÃO faz nada até que SPEEDY_ENABLED=true e uma
// chave de API válida sejam configuradas no .env. Por padrão aponta para o
// AMBIENTE SANDBOX. Veja notaFiscalManager.js para o mapeamento de pedidos.

const axios = require('axios');

const BASE_URLS = {
  sandbox: 'https://sandbox-api.spedy.com.br/v1',
  production: 'https://api.spedy.com.br/v1',
};

function envBool(v) {
  return String(v || '').trim().toLowerCase() === 'true';
}

/**
 * Resolve a configuração da Spedy a partir das variáveis de ambiente.
 * - SPEEDY_ENABLED: liga/desliga toda a integração (default: false)
 * - SPEEDY_ENVIRONMENT: 'sandbox' | 'production' (default: sandbox)
 * - SPEEDY_API_KEY: chave usada como fallback nos dois ambientes
 * - SPEEDY_API_KEY_SANDBOX / SPEEDY_API_KEY_PRODUCTION: chaves específicas
 */
function getConfig() {
  const environment =
    String(process.env.SPEEDY_ENVIRONMENT || 'sandbox').trim().toLowerCase() === 'production'
      ? 'production'
      : 'sandbox';
  const isProd = environment === 'production';

  const apiKey = String(
    (isProd
      ? process.env.SPEEDY_API_KEY_PRODUCTION
      : process.env.SPEEDY_API_KEY_SANDBOX) ||
      process.env.SPEEDY_API_KEY ||
      ''
  ).trim();

  return {
    enabled: envBool(process.env.SPEEDY_ENABLED),
    environment,
    baseUrl: BASE_URLS[environment],
    apiKey,
    companyId: String(process.env.SPEEDY_COMPANY_ID || '').trim(),
    timeoutMs: Number(process.env.SPEEDY_TIMEOUT_MS || 45000),
  };
}

/** true quando a integração está habilitada E há chave configurada. */
function isConfigured() {
  const cfg = getConfig();
  return !!(cfg.enabled && cfg.apiKey);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Executa uma requisição contra a API da Spedy.
 * Retorna sempre { ok, status, data, error, rateLimit }.
 * NUNCA lança — erros são normalizados para facilitar o uso em background.
 */
async function request(method, endpoint, body, opts = {}) {
  const cfg = getConfig();

  if (!cfg.enabled) {
    return { ok: false, status: 0, data: null, error: 'spedy_disabled', message: 'Integração Spedy desligada (SPEEDY_ENABLED != true).' };
  }
  if (!cfg.apiKey) {
    return { ok: false, status: 0, data: null, error: 'spedy_missing_api_key', message: 'SPEEDY_API_KEY não configurada.' };
  }

  const url = `${cfg.baseUrl}${endpoint}`;
  const maxRetries = Number.isFinite(opts.maxRetries) ? opts.maxRetries : 2;

  let attempt = 0;
  // eslint-disable-next-line no-constant-condition
  while (true) {
    attempt += 1;
    try {
      const resp = await axios({
        method,
        url,
        data: body || undefined,
        headers: {
          'X-Api-Key': cfg.apiKey,
          'Content-Type': 'application/json',
          Accept: 'application/json',
          ...(opts.headers || {}),
        },
        timeout: cfg.timeoutMs,
        responseType: opts.responseType || 'json',
        validateStatus: () => true, // tratamos o status manualmente
      });

      const rateLimit = {
        limit: resp.headers?.['x-rate-limit-limit'],
        remaining: resp.headers?.['x-rate-limit-remaining'],
        reset: resp.headers?.['x-rate-limit-reset'],
      };

      // 429 → respeita o rate-limit e tenta novamente (com teto)
      if (resp.status === 429 && attempt <= maxRetries + 1) {
        const waitMs = Math.min(5000, 1000 * attempt);
        console.warn(`[Spedy] 429 rate limit em ${method} ${endpoint} — retry em ${waitMs}ms (tentativa ${attempt}).`);
        await sleep(waitMs);
        continue;
      }

      const ok = resp.status >= 200 && resp.status < 300;
      if (!ok) {
        const errMsg = extractSpedyError(resp.data);
        return {
          ok: false,
          status: resp.status,
          data: resp.data,
          error: `spedy_http_${resp.status}`,
          message: errMsg,
          rateLimit,
        };
      }

      return { ok: true, status: resp.status, data: resp.data, rateLimit };
    } catch (err) {
      // erro de rede/timeout → retry limitado
      if (attempt <= maxRetries) {
        const waitMs = Math.min(4000, 800 * attempt);
        console.warn(`[Spedy] erro de rede em ${method} ${endpoint}: ${err?.message} — retry em ${waitMs}ms.`);
        await sleep(waitMs);
        continue;
      }
      return { ok: false, status: 0, data: null, error: 'spedy_network_error', message: err?.message || 'Falha de rede.' };
    }
  }
}

/** Extrai mensagem legível do formato de erro da Spedy ({ errors: [{message}] }). */
function extractSpedyError(data) {
  try {
    if (!data) return '';
    if (typeof data === 'string') return data.slice(0, 500);
    if (Array.isArray(data.errors) && data.errors.length) {
      return data.errors.map((e) => e && e.message).filter(Boolean).join('; ').slice(0, 500);
    }
    if (data.message) return String(data.message).slice(0, 500);
    return JSON.stringify(data).slice(0, 500);
  } catch (_) {
    return '';
  }
}

// ---- Helpers de endpoints usados pela integração ----

/** POST /v1/orders — cria uma venda (fluxo simplificado). */
function createOrder(payload) {
  return request('post', '/orders', payload);
}

/** GET /v1/service-invoices/{id} — consulta NFS-e. */
function getServiceInvoice(id) {
  return request('get', `/service-invoices/${encodeURIComponent(id)}`);
}

/** GET /v1/product-invoices/{id} — consulta NF-e. */
function getProductInvoice(id) {
  return request('get', `/product-invoices/${encodeURIComponent(id)}`);
}

/** GET genérico de nota por modelo. */
function getInvoice(model, id) {
  if (model === 'productInvoice') return getProductInvoice(id);
  if (model === 'consumerInvoice') return request('get', `/consumer-invoices/${encodeURIComponent(id)}`);
  // default: serviço
  return getServiceInvoice(id);
}

module.exports = {
  getConfig,
  isConfigured,
  request,
  createOrder,
  getServiceInvoice,
  getProductInvoice,
  getInvoice,
  extractSpedyError,
  BASE_URLS,
};
