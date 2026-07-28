// fiscalEnrichmentManager.js
// Enriquecimento de dados cadastrais para EMISSÃO DE NOTA FISCAL.
// Fluxo: telefone do pedido -> losdados -> CPF + nome + endereço -> grava no pedido.
//
// FINALIDADE FISCAL apenas (LGPD): usar só para emitir a nota do próprio cliente.
// Idempotente + cache por telefone (evita consultas repetidas / custo / rate limit).

const losdados = require('./losdadosClient');
const { getCollection } = require('./mongodbClient');

function onlyDigits(v) { return String(v == null ? '' : v).replace(/\D/g, ''); }
function nowIso() { return new Date().toISOString(); }

function isValidCpf(d) {
  const s = onlyDigits(d);
  return s.length === 11 && !/^(\d)\1{10}$/.test(s); // 11 dígitos e não todos iguais
}

// ---- Extração defensiva da resposta (formato da losdados desconhecido) ----

/** Percorre objeto/array e retorna o 1º valor-folha cuja CHAVE casa com algum regex. */
function deepFindByKey(obj, keyRegexes, validate) {
  const seen = new Set();
  const stack = [obj];
  while (stack.length) {
    const cur = stack.shift();
    if (!cur || typeof cur !== 'object' || seen.has(cur)) continue;
    seen.add(cur);
    if (Array.isArray(cur)) { for (const it of cur) if (it && typeof it === 'object') stack.push(it); continue; }
    for (const [k, v] of Object.entries(cur)) {
      const keyMatch = keyRegexes.some((re) => re.test(k));
      if (keyMatch && (typeof v === 'string' || typeof v === 'number')) {
        const val = String(v).trim();
        if (val && (!validate || validate(val))) return val;
      }
      if (v && typeof v === 'object') stack.push(v);
    }
  }
  return '';
}

/** Encontra o 1º objeto que parece um endereço (tem logradouro/rua/cep/bairro). */
function findAddressObject(obj) {
  const seen = new Set();
  const stack = [obj];
  const isAddr = (o) => {
    if (!o || typeof o !== 'object' || Array.isArray(o)) return false;
    const keys = Object.keys(o).map((k) => k.toLowerCase());
    const hasStreet = keys.some((k) => /logradouro|endereco|endereço|^rua$/.test(k));
    const hasCep = keys.some((k) => /cep|postal/.test(k));
    const hasBairro = keys.some((k) => /bairro|distrito|district/.test(k));
    return (hasStreet && (hasCep || hasBairro)) || (hasCep && hasBairro);
  };
  while (stack.length) {
    const cur = stack.shift();
    if (!cur || typeof cur !== 'object' || seen.has(cur)) continue;
    seen.add(cur);
    if (Array.isArray(cur)) { for (const it of cur) stack.push(it); continue; }
    if (isAddr(cur)) return cur;
    for (const v of Object.values(cur)) if (v && typeof v === 'object') stack.push(v);
  }
  return null;
}

/** Normaliza um objeto de endereço para o formato usado na nota (Spedy). */
function extractAddress(raw) {
  const addrObj = findAddressObject(raw) || raw;
  const get = (regexes) => deepFindByKey(addrObj, regexes);
  const street = get([/^logradouro$/i, /^endereco$/i, /^endereço$/i, /^rua$/i, /street/i]);
  const number = get([/^numero$/i, /^número$/i, /^nro$/i, /^num$/i, /number/i]);
  const complement = get([/complemento/i, /complement/i]);
  const district = get([/^bairro$/i, /distrito/i, /district/i]);
  const postalCode = onlyDigits(get([/^cep$/i, /postal/i]));
  const cityName = get([/cidade/i, /municipio/i, /município/i, /city/i]);
  const state = get([/^uf$/i, /estado/i, /^state$/i]);
  const ibge = onlyDigits(get([/ibge/i, /codigo_?municipio/i]));

  const hasAny = street || postalCode || cityName || district;
  if (!hasAny) return null;
  return {
    street: street || '',
    number: number || 'S/N',
    complement: complement || '',
    district: district || '',
    postalCode: postalCode || '',
    cityName: cityName || '',
    state: String(state || '').toUpperCase().slice(0, 2),
    ibge: ibge || '',
  };
}

/** Extrai { cpf, nome, endereco } da resposta bruta, da melhor forma possível. */
function extractFiscalData(raw) {
  const cpf = onlyDigits(deepFindByKey(raw, [/^cpf$/i, /documento/i, /^doc$/i], isValidCpf));
  const nome = deepFindByKey(raw, [/nome_?completo/i, /^nome$/i, /^name$/i, /razao/i]);
  const endereco = extractAddress(raw);
  return {
    cpf: isValidCpf(cpf) ? cpf : '',
    nome: nome || '',
    endereco: endereco || null,
  };
}

// ---- Cache por telefone ----

async function _cacheGet(phone) {
  try {
    const col = await getCollection('fiscal_enrichment_cache');
    return await col.findOne({ phone });
  } catch (_) { return null; }
}
async function _cacheSet(phone, doc) {
  try {
    const col = await getCollection('fiscal_enrichment_cache');
    await col.updateOne({ phone }, { $set: Object.assign({ phone, updatedAt: nowIso() }, doc) }, { upsert: true });
  } catch (_) {}
}

/**
 * Enriquece a partir de um telefone. Usa cache se disponível.
 * @param {string} telefone
 * @param {object} [opts] { force?: boolean, useCache?: boolean }
 * @returns {Promise<{ok, fiscal, raw?, error?, cached?}>}
 */
async function enriquecerPorTelefone(telefone, opts = {}) {
  const phone = losdados.normPhone(telefone);
  if (!phone || phone.length < 10) return { ok: false, error: 'telefone_invalido' };

  if (!opts.force && opts.useCache !== false) {
    const cached = await _cacheGet(phone);
    if (cached && cached.fiscal) {
      return { ok: !!cached.fiscal.cpf || !!cached.fiscal.endereco, fiscal: cached.fiscal, cached: true };
    }
  }

  if (!losdados.isConfigured()) return { ok: false, error: 'losdados_nao_configurado' };

  const resp = await losdados.consultaTelefone(phone);
  if (!resp.ok) {
    await _cacheSet(phone, { lastError: resp.error, lastStatus: resp.status, fetchedAt: nowIso() });
    return { ok: false, error: resp.error, status: resp.status };
  }

  const fiscal = extractFiscalData(resp.data);
  await _cacheSet(phone, { fiscal, raw: resp.data, fetchedAt: nowIso(), lastError: null });
  return { ok: !!(fiscal.cpf || fiscal.endereco), fiscal, raw: resp.data };
}

/**
 * Enriquece um pedido (checkout_orders) e grava sob `fiscalData`.
 * Idempotente: pula se já tem CPF+endereço (a menos que force).
 * @returns {Promise<{ok, skipped?, reason?, fiscal?}>}
 */
async function enriquecerPedido(record, col, opts = {}) {
  try {
    if (!record || !record._id || !col) return { ok: false, skipped: true, reason: 'invalid_input' };

    const jaTem = record.fiscalData && record.fiscalData.cpf && record.fiscalData.endereco;
    if (jaTem && !opts.force) return { ok: true, skipped: true, reason: 'ja_enriquecido', fiscal: record.fiscalData };

    const c = (record.customer && typeof record.customer === 'object') ? record.customer : {};
    const telefone = c.phone_number || c.phone || c.telefone || c.whatsapp || record.telefone || '';
    if (!telefone) return { ok: false, skipped: true, reason: 'sem_telefone' };

    const r = await enriquecerPorTelefone(telefone, { force: opts.force });
    if (!r.ok) {
      await col.updateOne({ _id: record._id }, { $set: { 'fiscalData.lastError': r.error || 'sem_dados', 'fiscalData.updatedAt': nowIso() } });
      return { ok: false, reason: r.error };
    }

    const fiscal = r.fiscal || {};
    const set = {
      'fiscalData.source': 'losdados',
      'fiscalData.viaTelefone': losdados.normPhone(telefone),
      'fiscalData.cpf': fiscal.cpf || (record.fiscalData && record.fiscalData.cpf) || '',
      'fiscalData.nome': fiscal.nome || '',
      'fiscalData.endereco': fiscal.endereco || null,
      'fiscalData.cached': !!r.cached,
      'fiscalData.updatedAt': nowIso(),
      'fiscalData.lastError': null,
    };
    await col.updateOne({ _id: record._id }, { $set: set });
    return { ok: true, fiscal };
  } catch (e) {
    return { ok: false, reason: 'exception', message: e && e.message };
  }
}

/** Converte fiscalData → objeto customer/address no formato da Spedy (para a nota). */
function toSpedyCustomerPatch(fiscalData) {
  if (!fiscalData) return null;
  const patch = {};
  if (fiscalData.cpf) patch.federalTaxNumber = onlyDigits(fiscalData.cpf);
  if (fiscalData.nome) patch.name = fiscalData.nome;
  const e = fiscalData.endereco;
  if (e && (e.street || e.postalCode || e.cityName)) {
    patch.address = {
      street: e.street || '',
      number: e.number || 'S/N',
      district: e.district || '',
      postalCode: onlyDigits(e.postalCode || ''),
      additionalInformation: e.complement || '',
      city: e.ibge
        ? { code: e.ibge }
        : { name: e.cityName || '', state: e.state || '' },
    };
  }
  return patch;
}

module.exports = {
  enriquecerPorTelefone,
  enriquecerPedido,
  extractFiscalData,
  toSpedyCustomerPatch,
  isConfigured: losdados.isConfigured,
  getConfig: losdados.getConfig,
};
