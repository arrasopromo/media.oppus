// controladoriaManager.js
// Camada INTERNA de controladoria (reclassificação fiscal ebook/serviço).
//
// O QUE ESTE MÓDULO FAZ (e o que NÃO faz):
//   - Reclassifica a RECEITA de cada pedido pago entre "ebook" e "serviço",
//     apenas para fins de controladoria/planejamento fiscal.
//   - NÃO emite nota, NÃO altera checkout, NÃO altera painel de vendas/dash,
//     NÃO altera LTV, NÃO altera custo. É 100% leitura/relatório.
//
// REGRAS (definidas pelo cliente):
//   - Split só na PRIMEIRA compra do cliente (a partir da data de corte).
//   - Compras seguintes do mesmo cliente = 100% serviço.
//   - Cliente é o MESMO se bater e-mail OU instagram OU CPF (regra OU) com
//     alguma compra anterior (a partir da data de corte).
//   - Contagem começa na data de corte (default 2026-07-13). Compras antes
//     da data de corte são ignoradas (não contam como "primeira compra"),
//     então a 1ª compra a partir do corte recebe o ebook.
//   - Valor do ebook = min(valor × ebookPct, ebookCapReais). O teto evita
//     que pedidos grandes gerem um "ebook" de valor absurdo.
//   - Custo permanece integral (não é rateado): controladoria só divide receita.

const DEFAULTS = {
  ebookPct: 0.70,        // 70% da venda vira ebook na 1ª compra
  ebookCapReais: 140,    // teto do valor do ebook (R$)
  ebookTaxPct: 0.05,     // imposto sobre o ebook (5%)
  servicoTaxPct: 0.13,   // imposto sobre o serviço (13%)
  adsTaxPct: 0,          // imposto sobre o gasto de ads (definido pelo usuário)
  // Sem data de corte padrão: quem define é o usuário no painel.
};

/** Normaliza uma alíquota: aceita 5 ou 0.05, faz clamp 0..1. */
function normRate(v, def) {
  let r = Number(v);
  if (!Number.isFinite(r)) r = def;
  if (r > 1) r = r / 100;
  return Math.max(0, Math.min(1, r));
}

function round2(n) {
  const v = Number(n);
  if (!Number.isFinite(v)) return 0;
  return Math.round(v * 100) / 100;
}

/** Interpreta uma data YYYY-MM-DD como início do dia no fuso de Brasília (-03:00). Retorna null se inválida. */
function parseCutoffMs(cutoffDate) {
  const ms = new Date(`${String(cutoffDate).slice(0, 10)}T00:00:00-03:00`).getTime();
  return Number.isFinite(ms) ? ms : null;
}

/** Normaliza a config vinda do banco/painel, com defaults e clamps. */
function normalizeConfig(raw) {
  const v = raw && typeof raw === 'object' ? raw : {};

  let pct = Number(v.ebookPct);
  if (!Number.isFinite(pct)) pct = DEFAULTS.ebookPct;
  if (pct > 1) pct = pct / 100; // aceita 70 ou 0.70
  pct = Math.max(0, Math.min(1, pct));

  let cap = Number(v.ebookCapReais);
  if (!Number.isFinite(cap) || cap < 0) cap = DEFAULTS.ebookCapReais;

  // Data de corte é OPCIONAL. Vazia = split de ebook ainda não vale (tudo serviço).
  const cutoffRaw = String(v.cutoffDate || '').slice(0, 10);
  const hasCutoff = /^\d{4}-\d{2}-\d{2}$/.test(cutoffRaw);

  return {
    ebookPct: pct,
    ebookCapReais: round2(cap),
    ebookTaxPct: normRate(v.ebookTaxPct, DEFAULTS.ebookTaxPct),
    servicoTaxPct: normRate(v.servicoTaxPct, DEFAULTS.servicoTaxPct),
    adsTaxPct: normRate(v.adsTaxPct, DEFAULTS.adsTaxPct),
    cutoffDate: hasCutoff ? cutoffRaw : '',
    cutoffMs: hasCutoff ? parseCutoffMs(cutoffRaw) : null,
  };
}

/** Normaliza tokens de identidade do cliente. */
function normEmail(v) { return String(v || '').trim().toLowerCase() || null; }
function normIg(v) { return String(v || '').replace(/^@+/, '').trim().toLowerCase() || null; }
function normCpf(v) { const d = String(v || '').replace(/\D/g, ''); return d.length ? d : null; }
// Telefone: só dígitos; remove DDI 55 quando presente, para casar 55DDXXXX com DDXXXX.
function normPhone(v) { let d = String(v || '').replace(/\D/g, ''); if (d.length > 11 && d.startsWith('55')) d = d.slice(2); return d.length >= 10 ? d : null; }

/** Calcula o split de UMA venda. */
function splitOrder(valueReais, isFirst, cfg) {
  const value = round2(valueReais);
  if (!(value > 0)) return { ebook: 0, servico: 0 };
  if (!isFirst) return { ebook: 0, servico: value };
  const rawEbook = value * cfg.ebookPct;
  const ebook = round2(Math.min(rawEbook, cfg.ebookCapReais));
  const servico = round2(value - ebook);
  return { ebook, servico };
}

/**
 * Monta o relatório de controladoria.
 * @param {Array} entries  [{ id, dateMs, valueReais, email, instagram, cpf, categoria, tipo, costReais }]
 * @param {object} cfg     config normalizada (normalizeConfig)
 * @param {object} [range] { startMs, endMs } janela de EXIBIÇÃO (a detecção de
 *                         1ª compra sempre roda desde o corte).
 * @returns {{ cfg, totals, rows }}
 */
function buildReport(entries, cfg, range = {}) {
  // A janela de EXIBIÇÃO pode ir ANTES do corte (para ver o histórico).
  // A REGRA de ebook e a detecção de 1ª compra continuam valendo só do corte
  // em diante — pedidos antes do corte aparecem como 100% serviço.
  const startMs = Number.isFinite(range.startMs) ? range.startMs : Number.NEGATIVE_INFINITY;
  const endMs = Number.isFinite(range.endMs) ? range.endMs : Number.POSITIVE_INFINITY;

  // Processa TODOS os pedidos pagos com valor > 0, em ordem cronológica
  // (necessário para detectar a 1ª compra corretamente).
  const pool = (Array.isArray(entries) ? entries : [])
    .filter((e) => e && Number.isFinite(e.dateMs) && Number(e.valueReais) > 0)
    .sort((a, b) => a.dateMs - b.dateMs);

  const seenEmail = new Set();
  const seenIg = new Set();
  const seenCpf = new Set();
  const seenPhone = new Set();

  const rows = [];
  const totals = {
    orders: 0, revenue: 0, ebook: 0, servico: 0, cost: 0,
    firstCount: 0, repeatCount: 0, preCutoffCount: 0,
    firstRevenue: 0, repeatRevenue: 0, preCutoffRevenue: 0,
    // Contadores de nota fiscal (geral e só dos pedidos com ebook = 1ª compra)
    nf: { total: 0, authorized: 0, queued: 0, held: 0, error: 0, rejected: 0, none: 0 },
    nfEbook: { total: 0, authorized: 0, queued: 0, held: 0, error: 0, rejected: 0, none: 0 },
  };

  // Classifica o estado da nota em um "balde" simples.
  const nfBucket = (st) => {
    const s = String(st || '').toLowerCase();
    if (s === 'authorized') return 'authorized';
    if (s === 'enqueued' || s === 'claimed' || s === 'received') return 'queued';
    if (s === 'held') return 'held';
    if (s === 'rejected' || s === 'denied' || s === 'canceled') return 'rejected';
    if (s === 'error') return 'error';
    return 'none';
  };

  for (const e of pool) {
    const email = normEmail(e.email);
    const ig = normIg(e.instagram);
    const cpf = normCpf(e.cpf);
    const phone = normPhone(e.phone);

    // Sem data de corte definida = SIMULAÇÃO: o split de ebook vale para todo o
    // histórico (detecção de 1ª compra desde o início). Com corte definido, só
    // vale a partir dele (antes vira pré-corte / 100% serviço).
    const hasCutoff = Number.isFinite(cfg.cutoffMs);
    const afterCutoff = !hasCutoff || e.dateMs >= cfg.cutoffMs;

    let isFirst = false;
    let split;

    if (afterCutoff) {
      // Regra OU: recorrente se qualquer token já foi visto antes (só conta do corte em diante).
      const isReturning =
        (!!email && seenEmail.has(email)) ||
        (!!ig && seenIg.has(ig)) ||
        (!!phone && seenPhone.has(phone)) ||
        (!!cpf && seenCpf.has(cpf));
      isFirst = !isReturning;

      // Registra tokens para as próximas compras.
      if (email) seenEmail.add(email);
      if (ig) seenIg.add(ig);
      if (phone) seenPhone.add(phone);
      if (cpf) seenCpf.add(cpf);

      split = splitOrder(e.valueReais, isFirst, cfg);
    } else {
      // Antes do corte: 100% serviço e NÃO conta para a detecção de 1ª compra.
      split = { ebook: 0, servico: round2(e.valueReais) };
    }

    // Só entra nos totais/tabela se estiver na janela de exibição.
    if (e.dateMs >= startMs && e.dateMs <= endMs) {
      totals.orders += 1;
      totals.revenue = round2(totals.revenue + Number(e.valueReais));
      totals.ebook = round2(totals.ebook + split.ebook);
      totals.servico = round2(totals.servico + split.servico);
      totals.cost = round2(totals.cost + Number(e.costReais || 0));
      const val = Number(e.valueReais);
      if (afterCutoff) {
        if (isFirst) { totals.firstCount += 1; totals.firstRevenue = round2(totals.firstRevenue + val); }
        else { totals.repeatCount += 1; totals.repeatRevenue = round2(totals.repeatRevenue + val); }
      } else if (hasCutoff) { // só conta pré-corte se há corte definido
        totals.preCutoffCount += 1; totals.preCutoffRevenue = round2(totals.preCutoffRevenue + val);
      }

      // Contadores de nota fiscal
      const bucket = nfBucket(e.nfState);
      totals.nf.total += 1;
      totals.nf[bucket] += 1;
      if (afterCutoff && isFirst && split.ebook > 0) { // pedidos que geram nota de EBOOK
        totals.nfEbook.total += 1;
        totals.nfEbook[bucket] += 1;
      }

      rows.push({
        id: e.id,
        dateMs: e.dateMs,
        valueReais: round2(e.valueReais),
        categoria: e.categoria || '',
        tipo: e.tipo || '',
        email: email || '',
        instagram: ig || '',
        phone: phone || '',
        cpf: cpf || '',
        preCutoff: hasCutoff && !afterCutoff,
        isFirst,
        ebook: split.ebook,
        servico: split.servico,
        costReais: round2(e.costReais || 0),
        nfState: String(e.nfState || ''),
        nfBucket: nfBucket(e.nfState),
      });
    }
  }

  // Impostos: 5% ebook + 13% serviço (alíquotas configuráveis)
  totals.ebookTax = round2(totals.ebook * cfg.ebookTaxPct);
  totals.servicoTax = round2(totals.servico * cfg.servicoTaxPct);
  totals.taxTotal = round2(totals.ebookTax + totals.servicoTax);
  // Referência: imposto se tudo fosse serviço (para ver a economia do split)
  totals.taxIfAllService = round2(totals.revenue * cfg.servicoTaxPct);
  totals.taxSaving = round2(totals.taxIfAllService - totals.taxTotal);

  totals.lucro = round2(totals.revenue - totals.cost);
  totals.lucroLiquido = round2(totals.lucro - totals.taxTotal);
  totals.ebookPctReal = totals.revenue > 0 ? round2((totals.ebook / totals.revenue) * 100) : 0;
  totals.servicoPctReal = round2(100 - totals.ebookPctReal);
  // % do faturamento que veio de 1ª compra, e quanto dela virou ebook
  totals.firstPctOfTotal = totals.revenue > 0 ? round2((totals.firstRevenue / totals.revenue) * 100) : 0;
  totals.ebookPctOfFirst = totals.firstRevenue > 0 ? round2((totals.ebook / totals.firstRevenue) * 100) : 0;
  rows.sort((a, b) => b.dateMs - a.dateMs); // mais recentes primeiro na tabela

  return { cfg, totals, rows };
}

module.exports = {
  DEFAULTS,
  normalizeConfig,
  parseCutoffMs,
  splitOrder,
  buildReport,
  round2,
  _norm: { normEmail, normIg, normCpf },
};
