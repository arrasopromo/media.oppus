#!/usr/bin/env node
// =============================================================================
// Exportador DataCrazy via terminal — sem dependências (Node 18+).
//
// Fluxo: pede o token -> lista instâncias -> você escolhe -> escolhe a pasta
// -> baixa TODAS as conversas das instâncias em arquivos .txt
// (uma conversa por arquivo), no formato:  [data] nome: mensagem
//
// Uso:
//   node datacrazy-export.mjs
//   node datacrazy-export.mjs --token dc_xxx --instances id1,id2 --out ./pasta
//   node datacrazy-export.mjs --all --out ./pasta   (todas as instâncias)
//
// O token NÃO é salvo em lugar nenhum; fica só na memória do processo.
// =============================================================================

import readline from "node:readline";
import fs from "node:fs";
import path from "node:path";
import process from "node:process";

const BASE = "https://api.g1.datacrazy.io/api/v1";
const API_PAGE_SIZE = 1000;
const TIMEOUT_MS = 30000;
const MAX_RETRIES = 6; // tentativas em caso de 429/erro de rede antes de desistir

// ---------- args ----------
function getArg(name) {
  const i = process.argv.indexOf(`--${name}`);
  return i !== -1 && process.argv[i + 1] ? process.argv[i + 1] : undefined;
}
const flagAll = process.argv.includes("--all");
const flagYes = process.argv.includes("--yes") || process.argv.includes("-y");
// Quantas conversas baixar ao mesmo tempo. Como a API limita 60 req/min POR
// ROTA e o script já se auto-regula pelos headers de rate limit, 1 trilha bem
// ritmada já usa a cota cheia. Aumentar só ajuda se a sua cota for maior.
const CONCURRENCY = Math.max(1, Math.min(20, Number(getArg("concurrency") || 1)));
// Pausa extra (ms) entre requisições, opcional. Padrão 0 (deixa o auto-throttle decidir).
const RATE_DELAY_MS = Math.max(0, Number(getArg("delay") || 0));

// ---------- prompts ----------
function ask(query) {
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  return new Promise((resolve) => rl.question(query, (a) => { rl.close(); resolve(a.trim()); }));
}
function askHidden(query) {
  return new Promise((resolve) => {
    const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
    process.stdout.write(query);
    rl.question("", (val) => { rl.close(); process.stdout.write("\n"); resolve(val.trim()); });
    rl._writeToOutput = () => {}; // não ecoa o token
  });
}

// ---------- fetch util com auto-throttle ----------
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// A API DataCrazy permite 60 req/min POR ROTA e devolve os headers
// X-RateLimit-Remaining / X-RateLimit-Reset. Em vez de chutar um delay, lemos
// esses valores e espalhamos as requisições uniformemente até o próximo reset.
// O estado é mantido por rota (o path sem o ID).
const rateState = new Map(); // routeKey -> { remaining, resetAt }

function routeKey(pathUrl) {
  // normaliza: remove querystring e troca o {id} por :id para agrupar a rota
  const p = pathUrl.split("?")[0];
  return p.replace(/\/conversations\/[^/]+\//, "/conversations/:id/");
}

async function rateGate(key) {
  const st = rateState.get(key);
  if (!st) return; // primeira chamada da rota: manda direto
  const msLeft = Math.max(0, st.resetAt - Date.now());
  if (st.remaining <= 0) {
    // cota zerada: espera o reset
    if (msLeft > 0) await sleep(msLeft + 300);
  } else {
    // espalha as requisições restantes igualmente até o reset (ritmo suave)
    const spacing = msLeft / (st.remaining + 1);
    if (spacing > 0) await sleep(Math.min(spacing, 1500));
  }
}

function updateRate(key, res) {
  const remaining = Number(res.headers.get("x-ratelimit-remaining"));
  const reset = Number(res.headers.get("x-ratelimit-reset"));
  const limit = Number(res.headers.get("x-ratelimit-limit"));
  if (Number.isFinite(remaining)) {
    rateState.set(key, {
      remaining,
      limit: Number.isFinite(limit) ? limit : undefined,
      resetAt: Date.now() + (Number.isFinite(reset) ? reset * 1000 : 60000),
    });
  }
}

async function dcFetch(pathUrl, token, attempt = 1) {
  const key = routeKey(pathUrl);
  await rateGate(key); // espera proativamente para não estourar o limite

  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), TIMEOUT_MS);
  try {
    const res = await fetch(`${BASE}${pathUrl}`, {
      headers: { Authorization: `Bearer ${token}`, Accept: "application/json" },
      signal: ctrl.signal,
    });

    updateRate(key, res); // aprende a cota a partir dos headers

    // 429 = passou do limite. Respeita Retry-After (ou o reset) e tenta de novo.
    if (res.status === 429) {
      if (attempt > MAX_RETRIES) {
        throw new Error("HTTP 429 (limite de requisições) após várias tentativas");
      }
      const retryAfter = Number(res.headers.get("retry-after"));
      const reset = Number(res.headers.get("x-ratelimit-reset"));
      const secs = Number.isFinite(retryAfter) && retryAfter > 0
        ? retryAfter
        : Number.isFinite(reset) && reset > 0
          ? reset
          : Math.min(2 * attempt, 15);
      // marca a cota como zerada até o reset
      rateState.set(key, { remaining: 0, resetAt: Date.now() + secs * 1000 });
      await sleep(secs * 1000 + 300);
      return dcFetch(pathUrl, token, attempt + 1);
    }

    if (!res.ok) {
      const body = await res.text().catch(() => "");
      throw new Error(`HTTP ${res.status} ${res.statusText} ${body.slice(0, 300)}`);
    }
    return res.json();
  } catch (err) {
    // erro de rede/timeout: tenta de novo algumas vezes
    const isAbort = err?.name === "AbortError";
    const isNetwork = isAbort || err?.code === "ENOTFOUND" || err?.code === "ECONNRESET" ||
      /fetch failed|network/i.test(err?.message || "");
    if (isNetwork && attempt <= MAX_RETRIES) {
      await sleep(Math.min(2000 * attempt, 15000));
      return dcFetch(pathUrl, token, attempt + 1);
    }
    throw err;
  } finally {
    clearTimeout(t);
  }
}

// ---------- normalização ----------
function asArray(payload) {
  if (Array.isArray(payload)) return payload;
  if (payload && typeof payload === "object") {
    if (Array.isArray(payload.data)) return payload.data;
    if (Array.isArray(payload.messages)) return payload.messages;
    if (payload.id) return [payload];
  }
  return [];
}

function fmtDate(s) {
  if (!s) return "sem data";
  const d = new Date(s);
  if (isNaN(d.getTime())) return "sem data";
  return d.toLocaleString("pt-BR", {
    day: "2-digit", month: "2-digit", year: "numeric",
    hour: "2-digit", minute: "2-digit",
  });
}

function safeName(s) {
  return (s || "conversa")
    .normalize("NFD").replace(/[̀-ͯ]/g, "")
    .replace(/[^a-zA-Z0-9-_]+/g, "_").slice(0, 60);
}

// Formata uma duração em minutos para algo legível: "5h 33min", "45min", "<1min".
function formatDuration(minutes) {
  if (minutes < 1) return "menos de 1 min";
  const h = Math.floor(minutes / 60);
  const m = Math.round(minutes % 60);
  if (h === 0) return `${m} min`;
  return `${h}h ${String(m).padStart(2, "0")}min`;
}

function buildTxt(conv, messages) {
  const header = conv.name || conv.id;
  const lines = [header, "=".repeat(header.length), ""];
  // ordena por data
  const sorted = [...messages].sort((a, b) =>
    new Date(a.createdAt || 0) - new Date(b.createdAt || 0));
  for (const m of sorted) {
    const name = m.contact?.name || (m.received ? (conv.contact?.name || "Contato") : "Atendente");
    let text = (m.body || "").trim();
    if (!text) {
      const att = Array.isArray(m.attachments) ? m.attachments : [];
      text = att.length > 0 ? `[${att.length} anexo(s)]` : "[sem texto]";
    }
    lines.push(`[${fmtDate(m.createdAt)}] ${name}: ${text}`);
  }
  return lines.join("\n");
}

// ---------- passos ----------
async function main() {
  console.log("\n=== Exportador DataCrazy (terminal) ===\n");

  // 1) token
  let token = getArg("token") || process.env.DC_TOKEN;
  if (!token) token = await askHidden("Cole o token DataCrazy (não aparece na tela): ");
  if (!token) { console.error("Token obrigatório."); process.exit(1); }

  // 2) instâncias
  let selectedIds = [];
  let instances = [];
  try {
    process.stdout.write("Buscando instâncias... ");
    instances = asArray(await dcFetch("/instances", token))
      .map((i) => ({ id: String(i.id ?? i._id ?? ""), name: i.name ?? "(sem nome)", status: i.status }))
      .filter((i) => i.id);
    console.log(`${instances.length} encontrada(s).\n`);
  } catch (e) {
    console.error("\nFalha ao listar instâncias:", e.message);
    process.exit(1);
  }

  const argInstances = getArg("instances");
  if (flagAll) {
    selectedIds = []; // vazio = todas
    console.log("Selecionado: TODAS as instâncias.\n");
  } else if (argInstances) {
    selectedIds = argInstances.split(",").map((s) => s.trim()).filter(Boolean);
  } else {
    instances.forEach((i, idx) =>
      console.log(`  [${idx + 1}] ${i.name}  (${i.id})${i.status ? " — " + i.status : ""}`));
    console.log("  [0] TODAS\n");
    const pick = await ask("Escolha a(s) instância(s) (ex: 1,3 ou 0 para todas): ");
    if (pick === "0" || pick.toLowerCase() === "todas") {
      selectedIds = [];
    } else {
      selectedIds = pick.split(",").map((s) => instances[Number(s.trim()) - 1]?.id).filter(Boolean);
      if (selectedIds.length === 0) { console.error("Nenhuma instância válida."); process.exit(1); }
    }
  }

  // 3) pasta de saída
  let outDir = getArg("out");
  if (!outDir) outDir = await ask("Pasta de destino (Enter = ./export-datacrazy): ");
  if (!outDir) outDir = "./export-datacrazy";
  outDir = path.resolve(outDir);
  fs.mkdirSync(outDir, { recursive: true });
  console.log(`\nSalvando em: ${outDir}\n`);

  // 4) baixa conversas (paginando) e depois mensagens uma a uma
  const filterQs = selectedIds.length > 0
    ? `filter[instances]=${encodeURIComponent(selectedIds.join(","))}&`
    : "";

  console.log("Carregando lista de conversas...");
  const conversations = [];
  const seen = new Set();
  for (let page = 0; page < 100; page++) {
    const skip = page * API_PAGE_SIZE;
    const payload = await dcFetch(`/conversations?${filterQs}skip=${skip}&take=${API_PAGE_SIZE}`, token);
    const batch = asArray(payload);
    for (const c of batch) if (c?.id && !seen.has(c.id)) { seen.add(c.id); conversations.push(c); }
    process.stdout.write(`\r  ${conversations.length} conversas...`);
    if (batch.length < API_PAGE_SIZE) break;
  }
  const total = conversations.length;
  console.log(`\n\nTotal: ${total} conversas encontradas.`);

  // Caminho do arquivo de uma conversa (mesma regra usada na gravação).
  function fileFor(c) {
    const instName = c.instance?.name ? safeName(c.instance.name) : "sem_instancia";
    const dir = path.join(outDir, instName);
    return { dir, file: path.join(dir, `${safeName(c.name || c.id)}_${String(c.id).slice(0, 8)}.txt`) };
  }
  const exists = (f) => fs.existsSync(f) && fs.statSync(f).size > 0;

  // Quantas já foram baixadas (não gastam tempo) e quantas faltam.
  const jaBaixadas = conversations.filter((c) => exists(fileFor(c).file)).length;
  const faltam = total - jaBaixadas;

  // Limite real da API (lido dos headers durante a listagem; padrão 30/min).
  const limitState = rateState.get("/conversations") || rateState.get("/instances");
  const reqsPorMin = (limitState && Number.isFinite(limitState.limit)) ? limitState.limit : 30;
  // +8% de folga por overhead (latência + janela de reset).
  const minutosEstimados = (faltam / reqsPorMin) * 1.08;

  console.log(`Já baixadas (serão puladas): ${jaBaixadas}`);
  console.log(`Faltam baixar: ${faltam}`);
  // Cada conversa = 1 requisição (traz todas as mensagens dela de uma vez),
  // então o limite de N req/min equivale a N conversas processadas por minuto.
  console.log(`Limite da sua conta: ${reqsPorMin} conversas por minuto (${reqsPorMin} req/min)`);
  console.log(`\n⏱  Tempo estimado: ~${formatDuration(minutosEstimados)}  (pode parar e continuar depois)\n`);

  if (faltam === 0) {
    console.log("Tudo já foi baixado. Nada a fazer. ✅");
    return;
  }

  // Pergunta se quer continuar (a menos que --yes / -y).
  if (!flagYes) {
    const resp = (await ask("Deseja continuar? (s/n): ")).toLowerCase();
    if (resp !== "s" && resp !== "sim" && resp !== "y" && resp !== "yes") {
      console.log("Cancelado. Nada foi baixado.");
      return;
    }
  }
  console.log(`\nBaixando mensagens (${CONCURRENCY} em paralelo, ritmo automático)...\n`);

  let ok = 0, fail = 0, skipped = 0, done = 0;
  const errors = [];

  // Processa uma conversa: baixa mensagens e grava o .txt.
  async function processConversation(c) {
    const { dir, file } = fileFor(c);
    fs.mkdirSync(dir, { recursive: true });

    // pula se já existe e não está vazio (permite retomar sem gastar a cota)
    if (exists(file)) {
      skipped++;
    } else {
      try {
        const msgs = asArray(await dcFetch(`/conversations/${encodeURIComponent(c.id)}/messages`, token));
        // gravação atômica: escreve num .tmp e renomeia, para nunca deixar
        // um arquivo pela metade se o processo for interrompido no meio.
        const tmp = file + ".tmp";
        fs.writeFileSync(tmp, buildTxt(c, msgs), "utf8");
        fs.renameSync(tmp, file);
        ok++;
      } catch (e) {
        fail++;
        errors.push({ id: c.id, name: c.name, error: e.message });
      }
      if (RATE_DELAY_MS > 0) await sleep(RATE_DELAY_MS);
    }
    done++;
    process.stdout.write(`\r[${done}/${total}] ok=${ok} erros=${fail} pulados=${skipped}   `);
  }

  // Pool de trabalhadores: cada um pega a próxima conversa da fila.
  let cursor = 0;
  async function worker() {
    while (cursor < conversations.length) {
      const c = conversations[cursor++];
      await processConversation(c);
    }
  }
  await Promise.all(Array.from({ length: CONCURRENCY }, () => worker()));

  console.log(`\n\n=== Concluído ===`);
  console.log(`Sucessos: ${ok} | Erros: ${fail} | Pulados (já existiam): ${skipped}`);
  console.log(`Arquivos em: ${outDir}`);
  if (errors.length > 0) {
    const errFile = path.join(outDir, "_erros.json");
    fs.writeFileSync(errFile, JSON.stringify(errors, null, 2), "utf8");
    console.log(`Lista de erros salva em: ${errFile}`);
  }
}

main().catch((e) => { console.error("\nErro fatal:", e.message); process.exit(1); });
