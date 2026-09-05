// notaFiscalManager.js
// Camada de negócio para emissão de nota fiscal (Spedy) a partir dos pedidos
// da coleção `checkout_orders`.
//
// REGRA DE NEGÓCIO (definida com o cliente):
//   - A Spedy emite SOMENTE a nota de EBOOK, e SOMENTE na 1ª compra do cliente.
//   - O valor da nota é o VALOR FRACIONADO do ebook (split % com teto), não o
//     valor cheio do pedido. A parte de serviço é declarada por fora (manual).
//   - "1ª compra" e o split usam a MESMA config da Controladoria (regra OU de
//     e-mail/@/telefone/CPF, data de corte, % e teto do ebook).
//   - Recompra ou pré-corte → NÃO emite (nem marca o pedido).
//
// Emissão via Modo Simplificado (POST /v1/orders): produto "ebook" com o valor
// fracionado; a tributação (imunidade do livro, NCM, etc.) fica no backoffice
// da Spedy. CPF/endereço vêm do checkout ou do enriquecimento (losdados).
//
// SEGURANÇA / ESTADO:
//   - Só emite quando SPEEDY_ENABLED=true e há chave (spedyClient.isConfigured()).
//   - Por padrão aponta para o SANDBOX (nenhuma nota real é emitida).
//   - Idempotente: usa um "claim" atômico no Mongo para nunca emitir 2x.
//   - Sem CPF → represa (held) para correção manual.
//   - Nunca lança: resolve com { ok, ... } e registra estado em `notaFiscal`.

const spedy = require('./spedyClient');
const fiscalEnrich = require('./fiscalEnrichmentManager');
const controladoria = require('./controladoriaManager');
const { getCollection } = require('./mongodbClient');

// Lê a config da Controladoria (settings key 'controladoria') — mesma fonte da tela.
async function getControladoriaConfig() {
  try {
    const s = await getCollection('settings');
    const d = s ? await s.findOne({ key: 'controladoria' }) : null;
    return controladoria.normalizeConfig(d && d.value ? d.value : {});
  } catch (_) {
    return controladoria.normalizeConfig({});
  }
}

/** Data do pedido em ms (paidAt > woovi.paidAt > createdAt > criado). */
function orderDateMs(record) {
  const cands = [record?.paidAt, record?.woovi?.paidAt, record?.createdAt, record?.criado];
  for (const c of cands) { const t = c ? new Date(c).getTime() : NaN; if (Number.isFinite(t)) return t; }
  return NaN;
}

function normPhoneDigits(v) {
  let d = String(v == null ? '' : v).replace(/\D/g, '');
  if (d.length > 11 && d.startsWith('55')) d = d.slice(2);
  return d.length >= 10 ? d : '';
}

/** Tokens de identidade do cliente (e-mail / @ / telefone / CPF), normalizados. */
function customerTokens(record) {
  const c = (record?.customer && typeof record.customer === 'object') ? record.customer : {};
  const m = addInfoMap(record);
  const email = String(c.email || m['email'] || '').trim().toLowerCase();
  const ig = String(m['instagram_username'] || record?.instauser || record?.instagramUsername || '').replace(/^@+/, '').trim().toLowerCase();
  const phone = normPhoneDigits(c.phone_number || c.phone || c.telefone || c.whatsapp || m['telefone'] || m['phone'] || record?.telefone);
  const cpf = onlyDigits(c.cpf || c.federalTaxNumber);
  return { email, ig, phone, cpf: (cpf.length === 11 || cpf.length === 14) ? cpf : '' };
}

/**
 * Verifica se o pedido é RECOMPRA (existe pedido pago anterior do mesmo cliente,
 * casando por e-mail OU @ OU telefone OU CPF). Respeita a data de corte: só
 * considera anteriores a partir do corte (se definido). Datas comparadas em JS.
 * @returns {Promise<boolean>} true = recompra (não deve emitir ebook).
 */
async function isRecompra(record, col, cutoffMs, orderMs) {
  const t = customerTokens(record);
  const or = [];
  if (t.email) or.push({ 'customer.email': { $regex: `^${t.email.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, $options: 'i' } });
  if (t.ig) {
    const rex = { $regex: `^@?${t.ig.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, $options: 'i' };
    or.push({ instauser: rex }, { instagramUsername: rex }, { 'additionalInfoMapPaid.instagram_username': rex }, { 'additionalInfoMap.instagram_username': rex });
  }
  if (t.phone) or.push({ 'customer.phone_number': { $in: [t.phone, `55${t.phone}`] } }, { 'customer.phone': { $in: [t.phone, `55${t.phone}`] } });
  if (t.cpf) or.push({ 'customer.cpf': t.cpf }, { 'customer.federalTaxNumber': t.cpf });
  if (!or.length) return false; // sem identidade → trata como 1ª compra

  const paidOr = [
    { status: { $regex: '^pago$', $options: 'i' } }, { 'woovi.status': { $regex: '^pago$', $options: 'i' } },
    { paidAt: { $exists: true, $nin: [null, ''] } }, { 'woovi.paidAt': { $exists: true, $nin: [null, ''] } },
    { 'paghiper.paidAt': { $exists: true, $nin: [null, ''] } },
  ];
  let candidates = [];
  try {
    candidates = await col.find(
      { $and: [{ $or: paidOr }, { $or: or }] },
      { projection: { _id: 1, paidAt: 1, 'woovi.paidAt': 1, createdAt: 1, criado: 1 }, limit: 300 }
    ).toArray();
  } catch (_) { return false; }

  const thisId = String(record._id || '');
  for (const cand of candidates) {
    if (String(cand._id) === thisId) continue;
    const cms = orderDateMs(cand);
    if (!Number.isFinite(cms)) continue;
    if (Number.isFinite(cutoffMs) && cms < cutoffMs) continue; // anteriores ao corte não contam
    if (Number.isFinite(orderMs) && cms >= orderMs) continue;  // só conta o que é ANTES deste pedido
    return true; // achou uma compra anterior → recompra
  }
  return false;
}

// ---- utilitários ----

function onlyDigits(v) {
  return String(v == null ? '' : v).replace(/\D/g, '');
}

function envBool(v, def = false) {
  const s = String(v == null ? '' : v).trim().toLowerCase();
  if (s === '') return def;
  return s === 'true' || s === '1' || s === 'sim' || s === 'yes';
}

function centsToReais(cents) {
  const n = Number(cents || 0);
  if (!Number.isFinite(n)) return 0;
  return Math.round(n) / 100;
}

function nowIso() {
  return new Date().toISOString();
}

/** Mapa auxiliar a partir do array additionalInfo [{key,value}]. */
function addInfoMap(record) {
  const arr = Array.isArray(record?.additionalInfoPaid) && record.additionalInfoPaid.length
    ? record.additionalInfoPaid
    : (Array.isArray(record?.additionalInfo) ? record.additionalInfo : []);
  const out = {};
  for (const it of arr) {
    const k = String(it?.key || '').trim();
    if (k) out[k] = String(it?.value ?? '').trim();
  }
  return out;
}

/** Traduz o método de pagamento interno para o enum OrderPaymentMethod da Spedy. */
function mapPaymentMethod(record) {
  const raw = String(record?.paymentMethod || '').toLowerCase();
  const hasWoovi = !!(record?.woovi || record?.paghiper || record?.expay);
  if (raw.includes('pix') || hasWoovi) return 'pix';
  if (raw.includes('billet') || raw.includes('boleto')) return 'billetBank';
  if (raw.includes('debit')) return 'debitCard';
  if (raw.includes('credit') || raw.includes('card') || raw.includes('stripe')) return 'creditCard';
  return 'other';
}

/** Nome do cliente com fallbacks. */
function resolveCustomerName(record) {
  const c = record?.customer && typeof record.customer === 'object' ? record.customer : {};
  const m = addInfoMap(record);
  const name = String(
    c.name || c.nome || record.nomeUsuario || m['nome'] || m['name'] || m['customer_name'] || ''
  ).trim();
  return name || 'Consumidor Final';
}

// Descrição fiscal PADRÃO usada em TODAS as notas de serviço. Antes a descrição saía
// com o slug interno + quantidade ("mistos - 500", "visualizacoes_reels - 25000"), o que
// é ruim para um documento fiscal. Agora é uma discriminação única e profissional para
// todas as notas — configurável por env, mas com este texto como padrão.
const DEFAULT_SERVICE_DESCRIPTION = 'Serviços para redes sociais.';
const DEFAULT_SERVICE_CODE = 'servicos-redes-sociais';

/** Descrição/nome do serviço vendido — FIXA para todas as notas (padrão único). */
function resolveProductName(/* record */) {
  const fixed = String(process.env.SPEEDY_SERVICE_DESCRIPTION || DEFAULT_SERVICE_DESCRIPTION).trim();
  return (fixed || DEFAULT_SERVICE_DESCRIPTION).slice(0, 120);
}

/** Código estável do produto (para o cadastro auto-criado na Spedy) — único e fixo. */
function resolveProductCode(/* record */) {
  const code = String(process.env.SPEEDY_SERVICE_CODE || DEFAULT_SERVICE_CODE)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 30);
  return code || 'servico';
}

/** Valor pago do pedido em reais (usa o valor efetivamente pago). */
function resolveAmountReais(record) {
  const cents = Number(record?.valueCents ?? record?.expectedValueCents ?? record?.stripe?.amount ?? 0);
  return centsToReais(cents);
}

/**
 * Endereço-padrão (fallback) usado quando o cliente não tem endereço e não foi
 * possível enriquecer. Configurável por env (SPEEDY_FALLBACK_*). Ver .env.example.
 */
function fallbackAddress() {
  const cep = onlyDigits(process.env.SPEEDY_FALLBACK_CEP || '01001000');
  const ibge = onlyDigits(process.env.SPEEDY_FALLBACK_IBGE || '');
  const addr = {
    street: String(process.env.SPEEDY_FALLBACK_STREET || 'Praça da Sé').trim(),
    number: String(process.env.SPEEDY_FALLBACK_NUMBER || 'S/N').trim(),
    district: String(process.env.SPEEDY_FALLBACK_DISTRICT || 'Sé').trim(),
    postalCode: cep,
    additionalInformation: String(process.env.SPEEDY_FALLBACK_INFO || 'Endereço não informado pelo cliente').trim(),
  };
  if (ibge) addr.city = { code: ibge };
  else addr.city = { name: String(process.env.SPEEDY_FALLBACK_CITY || 'São Paulo').trim(), state: String(process.env.SPEEDY_FALLBACK_STATE || 'SP').trim().toUpperCase().slice(0, 2) };
  return addr;
}

/** CPF final do pedido (checkout tem prioridade; senão o enriquecido). */
function resolveCpf(record) {
  const c = (record?.customer && typeof record.customer === 'object') ? record.customer : {};
  const fromCheckout = onlyDigits(c.cpf || c.federalTaxNumber || c.documento);
  if (fromCheckout.length === 11 || fromCheckout.length === 14) return fromCheckout;
  const fromEnrich = onlyDigits(record?.fiscalData?.cpf);
  if (fromEnrich.length === 11 || fromEnrich.length === 14) return fromEnrich;
  return '';
}

/**
 * Monta o payload de POST /v1/orders a partir de um pedido interno.
 * @param {object} record
 * @param {object} [opts] { amount, productName, productCode } — para a nota de EBOOK,
 *        passe o valor fracionado e o produto ebook. Sem opts, usa o valor cheio.
 * Retorna null quando não há dados mínimos para emitir.
 */
function mapOrderToSpedyPayload(record, opts = {}) {
  const amount = (Number.isFinite(opts.amount) && opts.amount > 0) ? Number(opts.amount) : resolveAmountReais(record);
  if (!(amount > 0)) return null;

  const c = record?.customer && typeof record.customer === 'object' ? record.customer : {};
  const cpf = onlyDigits(c.cpf || c.federalTaxNumber || c.documento || c.cnpj);
  const email = String(c.email || '').trim();
  const phone = onlyDigits(c.phone_number || c.phone || c.telefone || c.whatsapp);

  const transactionId = String(record?._id || record?.identifier || record?.correlationID || '').trim();

  const customer = { name: resolveCustomerName(record) };
  if (cpf) customer.federalTaxNumber = cpf; // CPF (11) ou CNPJ (14), só dígitos
  if (email) customer.email = email;
  if (phone) customer.phone = phone;

  // Mescla dados enriquecidos (CPF/nome/endereço via losdados), quando houver.
  // O que já veio do checkout tem prioridade; o enriquecimento só COMPLETA o que falta.
  try {
    const patch = fiscalEnrich.toSpedyCustomerPatch(record?.fiscalData);
    if (patch) {
      if (!customer.federalTaxNumber && patch.federalTaxNumber) customer.federalTaxNumber = patch.federalTaxNumber;
      if (patch.name && (customer.name === 'Consumidor Final' || !customer.name)) customer.name = patch.name;
      if (!customer.address && patch.address) customer.address = patch.address;
    }
  } catch (_) {}

  // Endereço do checkout, se houver (só quando ainda não temos endereço).
  if (!customer.address && c.address && (c.address.street || c.address.postalCode)) {
    customer.address = c.address;
  }
  // Sem endereço nenhum → usa o endereço-padrão (fallback configurável).
  if (!customer.address || !(customer.address.street || customer.address.postalCode)) {
    customer.address = fallbackAddress();
  }

  const productName = opts.productName || resolveProductName(record);
  const productCode = opts.productCode || resolveProductCode(record);

  const payload = {
    transactionId,
    date: record?.paidAt || record?.createdAt || record?.criado || nowIso(),
    amount,
    // Já chamamos isto DEPOIS do pagamento confirmado, então a venda entra
    // como aprovada e a nota é emitida imediatamente.
    autoIssueMode: 'immediately',
    status: 'approved',
    paymentMethod: mapPaymentMethod(record),
    sendEmailToCustomer: envBool(process.env.SPEEDY_SEND_EMAIL, false),
    customer,
    items: [
      {
        // `description` (item) + `product.name` = discriminação do serviço na NFS-e.
        // O override invoices[] (PostInvoiceDto) não tem campo de descrição, então a
        // discriminação vem daqui. Mantemos o padrão único em ambos.
        description: productName,
        quantity: 1,
        price: amount,
        amount,
        discountAmount: 0,
        product: {
          name: productName,
          code: productCode,
          price: amount,
        },
      },
    ],
  };

  return payload;
}

// ---- estados persistidos no pedido (campo notaFiscal.emissionState) ----
// claimed  → reservado para emissão (lock)
// enqueued → enviado à Spedy, aguardando processamento
// authorized / rejected / canceled / denied → estados finais reportados
// error    → falha ao chamar a Spedy (pode reprocessar)

const CLAIMABLE_BLOCK = ['claimed', 'enqueued', 'authorized'];

/**
 * Tenta "reservar" o pedido para emissão de forma atômica.
 * Retorna true se conseguiu o lock; false se já estava reservado/emitido.
 */
async function claimOrder(col, record, environment) {
  try {
    const res = await col.updateOne(
      {
        _id: record._id,
        $or: [
          { notaFiscal: { $exists: false } },
          { 'notaFiscal.emissionState': { $nin: CLAIMABLE_BLOCK } },
        ],
      },
      {
        $set: {
          'notaFiscal.provider': 'spedy',
          'notaFiscal.environment': environment,
          'notaFiscal.emissionState': 'claimed',
          'notaFiscal.transactionId': String(record._id || ''),
          'notaFiscal.requestedAt': nowIso(),
          'notaFiscal.updatedAt': nowIso(),
        },
      }
    );
    return (res && (res.modifiedCount > 0 || res.upsertedCount > 0)) || false;
  } catch (e) {
    console.warn('[Spedy] claimOrder falhou:', e?.message);
    return false;
  }
}

async function persistNota(col, id, patch) {
  const $set = {};
  for (const [k, v] of Object.entries(patch)) $set[`notaFiscal.${k}`] = v;
  $set['notaFiscal.updatedAt'] = nowIso();
  try {
    await col.updateOne({ _id: id }, { $set });
  } catch (e) {
    console.warn('[Spedy] persistNota falhou:', e?.message);
  }
}

/** Extrai a 1ª nota da resposta de POST /v1/orders. */
function pickInvoiceFromOrderResponse(data) {
  try {
    const inv = Array.isArray(data?.invoices) && data.invoices.length ? data.invoices[0] : null;
    return inv || null;
  } catch (_) {
    return null;
  }
}

// Liga o modo EBOOK (split na 1ª compra). Enquanto false (padrão), o sistema emite
// SÓ nota de SERVIÇO do valor cheio para todo pedido pago. Ligar só após a troca de CNAE.
function ebookEnabled() {
  return String(process.env.SPEEDY_EBOOK_ENABLED || '').trim().toLowerCase() === 'true';
}
// Tomador (receiver) da NFS-e. A NFS-e IDENTIFICA o tomador por nome + e-mail + telefone
// mesmo SEM CPF (confirmado na Spedy/Vespasiano). Então montamos o tomador com o que
// houver (+ CPF quando existir). Só fica "sem tomador" quando não há NENHUM dado real.
function buildServiceReceiver(record) {
  const c = (record.customer && typeof record.customer === 'object') ? record.customer : {};
  const cpf = resolveCpf(record);
  const name = resolveCustomerName(record); // nome real, ou 'Consumidor Final' se não houver
  const email = String(c.email || '').trim();
  const phone = onlyDigits(c.phone_number || c.phone || c.telefone || c.whatsapp);
  if (!cpf && !email && !phone && name === 'Consumidor Final') return null; // anônimo → sem tomador
  const rec = { name };
  if (cpf) rec.federalTaxNumber = cpf;
  if (email) rec.email = email;
  if (phone) rec.phoneNumber = phone;
  return rec;
}
// Emite uma NOTA DE SERVIÇO (NFS-e) do valor `amount` via POST /service-invoices.
// Sem tomador quando não há CPF (permitido em NFS-e). Grava o resultado em notaFiscal.
async function emitirNotaServico(record, col, amount, opts = {}) {
  if (!(amount > 0)) return { ok: false, skipped: true, reason: 'zero_amount' };
  const cfg = spedy.getConfig();
  if (!opts.force && !opts._alreadyClaimed) {
    const claimed = await claimOrder(col, record, cfg.environment);
    if (!claimed) return { ok: false, skipped: true, reason: 'already_claimed_or_issued' };
  } else if (opts.force) {
    await persistNota(col, record._id, { emissionState: 'claimed', environment: cfg.environment, requestedAt: nowIso() });
  }
  await persistNota(col, record._id, { kind: 'servico', orderAmount: resolveAmountReais(record), serviceAmount: amount });
  // Nota de SERVIÇO é emitida SEM TOMADOR (sem receiver) — decisão do negócio.
  const desc = String(process.env.SPEEDY_SERVICE_DESCRIPTION || DEFAULT_SERVICE_DESCRIPTION).trim() || DEFAULT_SERVICE_DESCRIPTION;
  const payload = {
    description: desc,
    issue: true,
    sendEmailToCustomer: envBool(process.env.SPEEDY_SEND_EMAIL, false),
    effectiveDate: new Date(orderDateMs(record) || Date.now()).toISOString(),
    total: { invoiceAmount: amount, netAmount: amount },
  };
  const resp = await spedy.createServiceInvoice(payload);
  if (!resp.ok) {
    await persistNota(col, record._id, { emissionState: 'error', error: resp.message || resp.error || 'erro', httpStatus: resp.status });
    return { ok: false, reason: resp.error, message: resp.message, status: resp.status };
  }
  const d = resp.data || {};
  const st = String(d.status || '').toLowerCase();
  await persistNota(col, record._id, {
    model: 'serviceInvoice',
    kind: 'servico',
    emissionState: st === 'authorized' ? 'authorized' : ((st === 'canceled' || st === 'cancelled') ? 'canceled' : 'enqueued'),
    invoiceId: String(d.id || ''),
    number: (d.number != null ? d.number : null),
    status: String(d.status || ''),
    semTomador: true,
    error: null,
  });
  return { ok: true, invoiceId: d.id, number: d.number, status: d.status, amount, semTomador: true };
}

/**
 * Emite a nota fiscal de um pedido pago.
 * @param {object} record  documento de checkout_orders (deve ter _id)
 * @param {object} col     coleção Mongo `checkout_orders`
 * @param {object} [opts]  { force?: boolean }
 * @returns {Promise<{ok:boolean, skipped?:boolean, reason?:string, ...}>}
 */
async function emitirNotaParaPedido(record, col, opts = {}) {
  try {
    if (!spedy.isConfigured()) {
      return { ok: false, skipped: true, reason: 'not_configured' };
    }
    if (!record || !record._id || !col) {
      return { ok: false, skipped: true, reason: 'invalid_input' };
    }

    // Não emite para pedidos gratuitos / de refil / sem valor.
    const amount = resolveAmountReais(record);
    if (!(amount > 0)) {
      return { ok: false, skipped: true, reason: 'zero_amount' };
    }

    // ── MODO SERVIÇO (padrão, enquanto SPEEDY_EBOOK_ENABLED != true) ──
    // Emite nota de SERVIÇO do valor CHEIO para TODO pedido pago (recompra inclusa;
    // sem tomador quando não há CPF). Aguardando a troca de CNAE para ligar o ebook.
    // O bloco de EBOOK abaixo só roda com a flag ligada.
    if (!ebookEnabled()) {
      return await emitirNotaServico(record, col, amount, opts);
    }

    // ── REGRA (modo EBOOK): Spedy emite SÓ a nota de EBOOK, SÓ na 1ª compra ──
    const ctrlCfg = await getControladoriaConfig();
    const orderMs = orderDateMs(record);

    // Pré-corte: pedido antes da data de corte não gera nota de ebook.
    if (Number.isFinite(ctrlCfg.cutoffMs) && Number.isFinite(orderMs) && orderMs < ctrlCfg.cutoffMs) {
      return { ok: false, skipped: true, reason: 'pre_corte' };
    }
    // Recompra: cliente já comprou antes (e-mail/@/telefone/CPF) → não gera ebook.
    if (!opts.force) {
      const recompra = await isRecompra(record, col, ctrlCfg.cutoffMs, orderMs);
      if (recompra) return { ok: false, skipped: true, reason: 'recompra' };
    }
    // Valor fracionado do ebook = min(valor × %, teto).
    const split = controladoria.splitOrder(amount, true, ctrlCfg);
    const ebookAmount = Number(split && split.ebook) || 0;
    if (!(ebookAmount > 0)) return { ok: false, skipped: true, reason: 'ebook_zero' };

    const cfg = spedy.getConfig();

    // Idempotência: só segue quem conseguir o lock (a menos que force=true).
    if (!opts.force) {
      const claimed = await claimOrder(col, record, cfg.environment);
      if (!claimed) {
        return { ok: false, skipped: true, reason: 'already_claimed_or_issued' };
      }
    } else {
      await persistNota(col, record._id, { emissionState: 'claimed', environment: cfg.environment, requestedAt: nowIso() });
    }
    // Marca que é nota de ebook e o valor fracionado (para auditoria/painel).
    await persistNota(col, record._id, { kind: 'ebook', ebookAmount, orderAmount: amount });

    // Enriquecimento fiscal: se faltar CPF ou endereço, busca pelo telefone (losdados).
    // Não bloqueia a emissão se falhar — apenas emite com o que tiver.
    try {
      const c = (record.customer && typeof record.customer === 'object') ? record.customer : {};
      const temCpf = !!resolveCpf(record);
      const temEnd = !!(record.fiscalData && record.fiscalData.endereco) || !!(c.address && (c.address.street || c.address.postalCode));
      if ((!temCpf || !temEnd) && fiscalEnrich.isConfigured()) {
        const enr = await fiscalEnrich.enriquecerPedido(record, col, {});
        if (enr && enr.ok && enr.fiscal) {
          record.fiscalData = Object.assign({}, record.fiscalData, enr.fiscal);
        }
      }
    } catch (_) {}

    // REPRESA: sem CPF não dá pra emitir a nota de ebook (NF-e exige CPF).
    // Endereço faltando NÃO represa — usa o endereço-padrão.
    if (!resolveCpf(record)) {
      await persistNota(col, record._id, {
        emissionState: 'held',
        heldReason: 'sem_cpf',
        heldAt: nowIso(),
        viaTelefone: onlyDigits(record?.customer?.phone_number || record?.customer?.phone || record?.customer?.telefone || ''),
        error: null,
      });
      console.warn(`⏸️ [Spedy] Pedido ${record._id} REPRESADO: sem CPF (nem no checkout nem via losdados).`);
      return { ok: false, held: true, reason: 'sem_cpf' };
    }

    // Payload da nota de EBOOK: valor fracionado + produto "ebook".
    const payload = mapOrderToSpedyPayload(record, {
      amount: ebookAmount,
      productName: String(process.env.SPEEDY_EBOOK_PRODUCT_NAME || 'Ebook').trim(),
      productCode: String(process.env.SPEEDY_EBOOK_PRODUCT_CODE || 'ebook').trim(),
    });
    if (!payload) {
      await persistNota(col, record._id, { emissionState: 'error', error: 'payload_invalido' });
      return { ok: false, reason: 'payload_invalido' };
    }

    const resp = await spedy.createOrder(payload);

    if (!resp.ok) {
      await persistNota(col, record._id, {
        emissionState: 'error',
        error: resp.message || resp.error || 'erro_desconhecido',
        httpStatus: resp.status,
      });
      console.warn(`[Spedy] Falha ao emitir nota do pedido ${record._id}: ${resp.error} ${resp.message || ''}`);
      return { ok: false, reason: resp.error, message: resp.message, status: resp.status };
    }

    const data = resp.data || {};
    const invoice = pickInvoiceFromOrderResponse(data);

    await persistNota(col, record._id, {
      emissionState: invoice?.status === 'authorized' ? 'authorized' : 'enqueued',
      spedyOrderId: String(data.id || ''),
      invoiceId: invoice ? String(invoice.id || '') : '',
      model: invoice ? String(invoice.model || '') : '',
      status: invoice ? String(invoice.status || '') : String(data.status || ''),
      error: null,
    });

    console.log(`✅ [Spedy] Venda criada p/ pedido ${record._id} → spedyOrderId=${data.id} invoice=${invoice?.id || 'n/a'} status=${invoice?.status || data.status}`);
    return { ok: true, spedyOrderId: data.id, invoice };
  } catch (e) {
    console.error('[Spedy] emitirNotaParaPedido erro inesperado:', e?.message);
    try { await persistNota(col, record._id, { emissionState: 'error', error: e?.message || 'exceção' }); } catch (_) {}
    return { ok: false, reason: 'exception', message: e?.message };
  }
}

/**
 * Dispara a emissão em background (fire-and-forget), sem bloquear o fluxo
 * de fulfillment. Seguro para chamar em qualquer lugar: se a integração
 * estiver desligada, é um no-op silencioso.
 */
function emitirEmBackground(record, col) {
  try {
    if (!spedy.isConfigured()) return; // no-op quando desligado
    Promise.resolve()
      .then(() => emitirNotaParaPedido(record, col))
      .catch((e) => console.warn('[Spedy] emitirEmBackground:', e?.message));
  } catch (_) {}
}

/**
 * Processa um evento de webhook da Spedy e atualiza o pedido correspondente.
 * @param {object} body  payload do webhook { event, data:{...} }
 * @param {object} col   coleção `checkout_orders`
 */
async function handleWebhookEvent(body, col) {
  try {
    const event = String(body?.event || '');
    const data = body?.data || {};
    const invoiceId = String(data.id || '');
    const spedyOrderId = String(data.order?.id || '');
    const transactionId = String(data.order?.transactionId || '');
    const status = String(data.status || '');

    // Localiza o pedido por invoiceId, spedyOrderId ou transactionId (=_id interno).
    const or = [];
    if (invoiceId) or.push({ 'notaFiscal.invoiceId': invoiceId });
    if (spedyOrderId) or.push({ 'notaFiscal.spedyOrderId': spedyOrderId });
    if (transactionId) or.push({ 'notaFiscal.transactionId': transactionId });

    if (!or.length) return { ok: false, reason: 'sem_identificadores' };

    const order = await col.findOne({ $or: or });
    if (!order) return { ok: false, reason: 'pedido_nao_encontrado' };

    const emissionState =
      status === 'authorized' ? 'authorized'
      : status === 'rejected' ? 'rejected'
      : status === 'canceled' ? 'canceled'
      : status === 'denied' ? 'denied'
      : (order.notaFiscal?.emissionState || 'enqueued');

    await persistNota(col, order._id, {
      emissionState,
      status,
      invoiceId: invoiceId || order.notaFiscal?.invoiceId || '',
      spedyOrderId: spedyOrderId || order.notaFiscal?.spedyOrderId || '',
      model: String(data.model || order.notaFiscal?.model || ''),
      number: data.number ?? order.notaFiscal?.number ?? null,
      accessKey: data.accessKey || order.notaFiscal?.accessKey || null,
      authorization: data.authorization || order.notaFiscal?.authorization || null,
      processingDetail: data.processingDetail || null,
      lastEvent: event,
    });

    console.log(`📩 [Spedy][webhook] ${event} → pedido ${order._id} status=${status}`);
    return { ok: true, orderId: String(order._id), status };
  } catch (e) {
    console.error('[Spedy] handleWebhookEvent erro:', e?.message);
    return { ok: false, reason: 'exception', message: e?.message };
  }
}

/** Consulta a nota na Spedy e sincroniza o estado no pedido (polling manual). */
async function sincronizarStatus(record, col) {
  try {
    if (!spedy.isConfigured()) return { ok: false, reason: 'not_configured' };
    const nf = record?.notaFiscal || {};
    const invoiceId = nf.invoiceId;
    if (!invoiceId) return { ok: false, reason: 'sem_invoiceId' };

    const resp = await spedy.getInvoice(nf.model, invoiceId);
    if (!resp.ok) return { ok: false, reason: resp.error, message: resp.message };

    const data = resp.data || {};
    const status = String(data.status || '');
    const emissionState =
      status === 'authorized' ? 'authorized'
      : status === 'rejected' ? 'rejected'
      : status === 'canceled' ? 'canceled'
      : status === 'denied' ? 'denied'
      : (nf.emissionState || 'enqueued');

    await persistNota(col, record._id, {
      emissionState,
      status,
      number: data.number ?? nf.number ?? null,
      accessKey: data.accessKey || nf.accessKey || null,
      authorization: data.authorization || nf.authorization || null,
      processingDetail: data.processingDetail || null,
    });

    return { ok: true, status, data };
  } catch (e) {
    return { ok: false, reason: 'exception', message: e?.message };
  }
}

module.exports = {
  emitirNotaParaPedido,
  emitirEmBackground,
  handleWebhookEvent,
  sincronizarStatus,
  mapOrderToSpedyPayload,
  // reexports úteis
  isConfigured: spedy.isConfigured,
  getConfig: spedy.getConfig,
};
