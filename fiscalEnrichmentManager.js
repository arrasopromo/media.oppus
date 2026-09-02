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

// Validação COM dígitos verificadores (evita aceitar um CPF montado errado ao
// completar o zero à esquerda).
function isValidCpfStrict(d) {
  const s = onlyDigits(d);
  if (s.length !== 11 || /^(\d)\1{10}$/.test(s)) return false;
  const calc = (base, factorStart) => {
    let sum = 0;
    for (let i = 0; i < base.length; i++) sum += parseInt(base[i], 10) * (factorStart - i);
    const r = (sum * 10) % 11;
    return r === 10 ? 0 : r;
  };
  const d1 = calc(s.slice(0, 9), 10);
  const d2 = calc(s.slice(0, 10), 11);
  return d1 === parseInt(s[9], 10) && d2 === parseInt(s[10], 10);
}

// A LosDados devolve o documento no campo `cpfCnpj`. Pode vir:
//  - 11 dígitos = CPF; 14 dígitos = CNPJ (número de empresa, não é CPF de pessoa);
//  - 10 dígitos = CPF que perdeu o zero à esquerda (a API guarda como número).
// Retorna { cpf, cnpj } normalizados (só o que for válido).
function normalizeDocumento(v) {
  let d = onlyDigits(v);
  if (d.length === 14) return { cpf: '', cnpj: d };
  if (d.length === 10) d = '0' + d;           // recompõe o zero à esquerda
  if (d.length === 11 && isValidCpfStrict(d)) return { cpf: d, cnpj: '' };
  return { cpf: '', cnpj: '' };
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

/** Extrai { cpf, cnpj, nome, endereco } da resposta bruta, da melhor forma possível. */
function extractFiscalData(raw) {
  // A LosDados usa a chave `cpfCnpj` (não `cpf`). Pega o 1º documento não-vazio.
  const docRaw = deepFindByKey(raw, [/^cpf_?cnpj$/i, /cpfcnpj/i, /^cpf$/i, /documento/i, /^doc$/i], (v) => onlyDigits(v).length >= 10);
  const { cpf, cnpj } = normalizeDocumento(docRaw);
  const nome = deepFindByKey(raw, [/nome_?completo/i, /^nome$/i, /^name$/i, /razao/i]);
  const endereco = extractAddress(raw);
  return {
    cpf: cpf || '',
    cnpj: cnpj || '',
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

// Registra CADA request REAL à losdados (cache-miss) como evento de cobrança.
// Serve para a controladoria contar quantas consultas foram feitas no período e
// multiplicar pelo custo por request (~R$0,05). Append-only; nunca lança.
async function _logRequest(evt) {
  try {
    const col = await getCollection('losdados_requests');
    await col.insertOne({
      at: nowIso(),
      phone: evt.phone || '',
      orderId: evt.orderId || null,
      cpf: evt.cpf || '',
      found: !!evt.found,
      error: evt.error || null,
      status: Number.isFinite(evt.status) ? evt.status : null,
    });
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

  // Tenta o telefone e, se "não encontrado", o formato alternativo do 9 (celular
  // antigo sem o 9 ou vice-versa) — parte dos números só está cadastrada num formato.
  const attempts = [phone];
  const alt = altNovePhone(phone);
  if (alt && alt !== phone) attempts.push(alt);

  let best = null;               // 1ª resposta HTTP-ok (guardada p/ cache mesmo vazia)
  let lastErr = null, lastStatus = null;
  for (const ph of attempts) {
    const resp = await losdados.consultaTelefone(ph); // billable
    if (!resp.ok) {
      lastErr = resp.error; lastStatus = resp.status;
      await _logRequest({ phone: ph, orderId: opts.orderId, found: false, error: resp.error, status: resp.status });
      continue;
    }
    const fiscal = extractFiscalData(resp.data);
    const found = !!(fiscal.cpf || fiscal.endereco);
    await _logRequest({ phone: ph, orderId: opts.orderId, cpf: fiscal.cpf, found, status: resp.status });
    if (!best) best = { fiscal, raw: resp.data };
    if (found) { best = { fiscal, raw: resp.data }; break; }
    lastErr = (resp.data && resp.data.data && resp.data.data.err) || 'nao_encontrado';
    lastStatus = resp.status;
  }

  if (best) {
    const found = !!(best.fiscal.cpf || best.fiscal.endereco);
    await _cacheSet(phone, { fiscal: best.fiscal, raw: best.raw, fetchedAt: nowIso(), lastError: found ? null : (lastErr || 'nao_encontrado') });
    return { ok: found, fiscal: best.fiscal, raw: best.raw };
  }
  await _cacheSet(phone, { lastError: lastErr || 'sem_dados', lastStatus, fetchedAt: nowIso() });
  return { ok: false, error: lastErr || 'sem_dados', status: lastStatus };
}

// Formato alternativo do celular brasileiro: 11 díg (DDD+9+8) <-> 10 díg (DDD+8).
function altNovePhone(phone) {
  const d = onlyDigits(phone);
  if (d.length === 11 && d[2] === '9') return d.slice(0, 2) + d.slice(3); // remove o 9
  if (d.length === 10) return d.slice(0, 2) + '9' + d.slice(2);           // adiciona o 9
  return '';
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

    // CPF que o cliente já informou no checkout (pedido do site = real). Serve de
    // fallback quando a LosDados não localiza o telefone.
    const cpfCheckout = onlyDigits(c.cpf || (record.fiscalData && record.fiscalData.cpf) || '');
    const cpfCheckoutOk = isValidCpfStrict(cpfCheckout);

    if (!telefone) {
      // Sem telefone: só dá pra usar o CPF do checkout, se houver.
      if (cpfCheckoutOk) {
        await col.updateOne({ _id: record._id }, { $set: { 'fiscalData.source': 'checkout', 'fiscalData.cpf': cpfCheckout, 'fiscalData.updatedAt': nowIso(), 'fiscalData.lastError': null } });
        return { ok: true, fiscal: { cpf: cpfCheckout, nome: c.name || '', endereco: null }, source: 'checkout' };
      }
      return { ok: false, skipped: true, reason: 'sem_telefone' };
    }

    const r = await enriquecerPorTelefone(telefone, { force: opts.force, orderId: String(record._id) });
    const fiscal = (r && r.fiscal) ? r.fiscal : {};
    // Fallback do CPF: se a LosDados não trouxe, usa o do checkout.
    let cpfFinal = fiscal.cpf || '';
    let cpfSource = fiscal.cpf ? 'losdados' : '';
    if (!cpfFinal && cpfCheckoutOk) { cpfFinal = cpfCheckout; cpfSource = 'checkout'; }

    // Nada de útil (nem CPF nem endereço) → registra o erro e sai.
    if (!cpfFinal && !(fiscal.endereco)) {
      await col.updateOne({ _id: record._id }, { $set: { 'fiscalData.lastError': (r && r.error) || 'sem_dados', 'fiscalData.updatedAt': nowIso() } });
      return { ok: false, reason: (r && r.error) || 'sem_dados' };
    }

    const set = {
      'fiscalData.source': fiscal.endereco ? 'losdados' : (cpfSource || 'losdados'),
      'fiscalData.cpfSource': cpfSource || 'losdados',
      'fiscalData.viaTelefone': losdados.normPhone(telefone),
      'fiscalData.cpf': cpfFinal,
      'fiscalData.cnpj': fiscal.cnpj || '',
      'fiscalData.nome': fiscal.nome || c.name || '',
      'fiscalData.endereco': fiscal.endereco || null,
      'fiscalData.cached': !!(r && r.cached),
      'fiscalData.updatedAt': nowIso(),
      'fiscalData.lastError': null,
    };
    await col.updateOne({ _id: record._id }, { $set: set });
    return { ok: true, fiscal: Object.assign({}, fiscal, { cpf: cpfFinal }) };
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
