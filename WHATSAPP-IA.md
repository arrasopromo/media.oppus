# IA de Vendas do WhatsApp — FamaApp

Atendente de vendas automático no WhatsApp que **fecha a venda dos 6 serviços** (seguidores, curtidas, visualizações, comentários, compartilhamentos e stories), cota preços oficiais, valida o perfil do Instagram e **gera o Pix (PagHiper)** — tudo pela conversa, sem mandar o cliente pro site.

> Documento gerado em 2026-08 refletindo o estado atual do código. Fonte da verdade: `whatsappCloud.js`, `whatsappAgent.js`, `whatsappSales.js`.

---

## 1. Arquitetura (3 arquivos)

| Arquivo | Responsabilidade |
|---|---|
| **`whatsappCloud.js`** | Infra do webhook Meta Cloud API: recebe/envia mensagem, **debounce**, mídia, checa **pausa**, encaminha cópia pro DataCrazy, harness de teste local. |
| **`whatsappAgent.js`** | O "cérebro": agente OpenAI (function-calling), system prompt, ferramentas, histórico por telefone, detecção de pedido múltiplo, rede de segurança. |
| **`whatsappSales.js`** | Catálogo/preços, validação de perfil, criação de pedido + Pix (PagHiper), geração de CPF, flag de suporte. Reaproveita os endpoints internos do site. |

### Fluxo de uma mensagem

```
Meta → POST /api/whatsapp/webhook (whatsappCloud.js)
   → handleInboundMessage(msg)
        1. checa botPaused (whatsapp_contacts) → se pausado, IGNORA (atendente humano assume)
        2. DEBOUNCE ~8s: junta mensagens picadas do mesmo telefone
        3. processInbound → agent.handleAgentMessage(msg, sendWhatsAppText)
   → whatsappAgent: loop OpenAI (tool_choice: auto)
        cotar_preco / tabela_precos / validar_perfil / chamar_suporte / gerar_pix
   → resposta enviada via sendWhatsAppText
```

- **Debounce** (`DEBOUNCE_MS`, padrão **8000ms**): agrupa mensagens curtas seguidas pra ter contexto mais assertivo antes de responder (evita responder a "oi" / "quero" / "seguidores" separados). Mídia/não-texto processa na hora.
- **Histórico** por telefone na coleção `whatsapp_agent_chats` (fallback em memória), guardando as últimas **14** mensagens (`MAX_TURNS`).

---

## 2. Serviços e preços

O `CATALOG` (whatsappSales.js) tem os 6 serviços, todos com `checkout: true`:

| Serviço | Tipos | Precisa de post? | Fonte do preço |
|---|---|---|---|
| **seguidores** | mistos / brasileiros / organicos | não (mira o @) | tabela local (espelha o site) |
| **curtidas** | mistos / brasileiros / organicos | sim | `pricing.js` |
| **visualizações** | — | sim | `pricing.js` |
| **comentários** | — | sim | **R$ 1,50/unidade** (`perUnitCents: 150`) |
| **compartilhamentos** | — | sim | `pricing.js` |
| **stories** | mundial / brasileiro | sim | `pricing.js` |

- **Preço é sempre server-side** (`sales.quote` / `sales.priceTable`). O modelo **nunca inventa nem calcula** preço.
- **`normalizeTipo`**: `"brasileiros reais"`, `"reais"`, `"de verdade"`, `"orgânico"` → **organicos** (rede de segurança no backend, aplicada em `quote` e `priceTable`). Só `"brasileiros"` sozinho oferece as 2 opções.

---

## 3. Ferramentas (function calling)

| Ferramenta | O que faz |
|---|---|
| **`cotar_preco`** (servico, tipo?, quantidade) | Preço oficial de UM pacote (`sales.quote`). |
| **`tabela_precos`** (servico, tipo?) | Tabela inteira de quantidades+preços pra o cliente escolher (`sales.priceTable`). |
| **`validar_perfil`** (usuario) | Valida o @ (aceita @, link ou texto cru) e retorna **nome do perfil + nº de seguidores** pra confirmar. |
| **`chamar_suporte`** (motivo) | Aciona o suporte humano (`sales.flagSupport` → `flag:'suporte'`), notifica no /inbox. |
| **`gerar_pix`** (servico, quantidade, usuario, nome, email, post_links?) | Cria pedido + Pix (PagHiper). **NÃO** pede CPF (gerado automático). |

O loop chama a ferramenta, injeta o resultado e re-chama o modelo (até 5 rodadas). **Rede de segurança:** se o loop esgotar só chamando ferramentas (ex.: pedido múltiplo) sem gerar texto, força **uma resposta final** com `tool_choice: 'none'` — o cliente nunca fica sem retorno.

---

## 4. Regras de conversa (comportamento)

Tudo no `systemPrompt()` (whatsappAgent.js). Resumo do que o bot faz:

- **Fecha tudo pelo WhatsApp** — não manda pro site.
- **Sem emojis.** Negrito só com **um** asterisco (`*assim*`, estilo WhatsApp) — nunca `**`.
- **Preços exatos:** copia o que a ferramenta retorna, na mesma mensagem — nunca diz só "vou cotar" e para.
- **"mais barato" / menor pacote** → sempre a menor quantidade da tabela (ex.: 150).
- **Serviços com post:** a pessoa **informa o link** (o bot não lista posts) e o bot **pergunta se quer dividir em mais posts** (split — a quantidade divide igualmente).
- **Confirma o perfil** mostrando nome + seguidores: *"Achei seu perfil! @x, o nome é NOME e você tem N seguidores. Confere?"*
- **@ / link / espaços:** `parseIgUsername` aceita `@user`, link `instagram.com/user`, e junta espaços (`"cris tiano"` → `cristiano`).
- **Pedido múltiplo** (vários serviços numa msg): avisa que faz **um de cada vez** (mais assertivo/organizado) e começa pelo primeiro.
- **Não volunteia ressalvas** ("não garante interação", "mistos não são reais") — só se o cliente perguntar.
- **Reais x brasileiros:** só os brasileiros orgânicos são 100% reais/ativos; mistos e brasileiros simples **não** são reais.

### Fluxo de venda
1. descobre serviço + tipo → 2. mostra a **tabela** e pergunta a quantidade → 3. cota o valor exato → 4. pede o @ e **valida** (confirma nome+seguidores) → 5. se tem post, pede o link (+ split) → 6. coleta **só nome e e-mail** → 7. confirma o resumo e **gera o Pix**.

---

## 5. Pagamento (PagHiper) e CPF

- `createPixOrder` monta o payload e chama **o MESMO endpoint do site**: `POST /api/paghiper/charge` (`INTERNAL_BASE`). A `notification_url` é setada no próprio endpoint (server-side) → mesma cobrança, webhook e fulfillment do site. `source: wpp_agent` e `correlationID: WppAgent_*` rastreiam.
- **CPF:** o cliente **não** informa CPF. `generateValidCPF()` gera um CPF válido (mesma lógica de `public/js/checkout.js`) — a PagHiper exige um CPF no pagador. `gerar_pix` não tem CPF no schema.
- Pra serviços com post, o payload manda `post_link` (1º) + `post_links` (lista) → a fulfillment divide a quantidade entre os posts (split).
- Depois do Pix: o bot avisa que o **copia-e-cola vem em mensagem separada** e não repete o código.

---

## 6. Validação de perfil

- `validateProfile(@)` chama `POST /api/check-instagram-profile` (usa **RocketAPI**), retornando `username`, `fullName`, `followers`, `isPrivate`, `postsCount`.
- O endpoint, por padrão, **já busca e salva os últimos 12 posts** em `validated_insta_users` (igual o site). O bot usa só o perfil básico (nome/seguidores) — os posts de fulfillment vêm dos **links que o cliente envia**.

---

## 7. Suporte humano + /inbox (pausar/iniciar)

- **`chamar_suporte`**: quando o cliente pede atendente OU faz reclamação séria, o bot aciona (`flagSupport` → `flag:'suporte'` em `whatsapp_contacts`), avisa que o suporte vai assumir e **para de vender**. No `/inbox` aparece banner + bubble vermelho pulsante por chat.
- **Comandos no /inbox** (digitados pelo responsável, NÃO vão pro cliente):
  - `/pause` ou `/pausar` → **pausa a IA** nesse chat (`botPaused: true`); `handleInboundMessage` ignora chats pausados (o humano assume; a msg do cliente ainda é salva).
  - `/start` (ou `/iniciar`, `/começar`, `/resume`, `/retomar`) → **retoma a IA** e **zera o contexto** (`whatsappAgent.clearHistory` apaga `whatsapp_agent_chats` + memória) → responde do zero só na próxima mensagem do cliente.
  - Botão verde **"Iniciar IA"** / **"Pausar IA"** no header do chat faz o mesmo.

---

## 8. Mídia

`sendWhatsAppMedia(to, kind, source, caption, via)` em whatsappCloud.js envia **imagem / vídeo / documento** por URL pública ou `media_id`. Exportado, pronto pra uso.

---

## 9. Configuração (variáveis de ambiente)

| Var | Uso |
|---|---|
| `OPENAI_API_KEY` | liga o agente (sem ela, `isEnabled()` = false → fallback por regras). |
| `OPENAI_MODEL` | modelo (padrão **`gpt-4o-mini`**). |
| `WHATSAPP_DEBOUNCE_MS` | janela do debounce (padrão 8000). |
| `WHATSAPP_TOKEN` / `WHATSAPP_PHONE_NUMBER_ID` / `WHATSAPP_VERIFY_TOKEN` / `WHATSAPP_APP_SECRET` / `WHATSAPP_WEBHOOK_PATH` | credenciais/infra da Meta Cloud API. |
| `WHATSAPP_AUTO_REPLY` | `false` desliga a resposta automática. |
| `DATACRAZY_WEBHOOK_URL` | espelha as conversas no DataCrazy (opcional). |
| `ROCKETAPI_TOKEN` | validação de perfil / posts. |
| `PAGHIPER_*` | cobrança Pix (compartilhado com o site). |

> Segredos ficam **só no `.env`** (nunca commitados).

---

## 10. Teste local (sem WhatsApp / sem deploy)

Endpoint de harness: **`POST /api/whatsapp/agent-test`** — roda o agente e devolve as respostas, reusando o histórico por telefone (dá pra testar multi-turno).

```bash
curl -X POST localhost:3200/api/whatsapp/agent-test \
  -H 'Content-Type: application/json' \
  -d '{"text":"quero 1000 seguidores brasileiros reais","phone":"5511999"}'
```

- **WhatsApp real** precisa de URL pública (túnel/deploy) — 1 webhook por número; cuidado pra não desviar produção.
- **Depois de editar `whatsappCloud.js`/`whatsappAgent.js`/`whatsappSales.js`/`pricing.js`, reinicie o servidor** (são `require`'d — cache de módulo).

---

## 11. Coleções MongoDB

| Coleção | Conteúdo |
|---|---|
| `whatsapp_agent_chats` | histórico da conversa por telefone (`_id` = telefone). |
| `whatsapp_contacts` | contato/nome/label/`flag`/`botPaused`. |
| `whatsapp_messages` | mensagens (inbox). |
| `checkout_orders` | pedidos gerados (com `source: wpp_agent`). |
| `validated_insta_users` | perfis validados (+ 12 posts). |

---

## 12. Notas rápidas

- O agente **fecha os 6 serviços** — antes só fazia seguidores.
- Preço, tipo (organicos vs brasileiros), CPF e confirmação de perfil são **blindados no servidor**; o modelo não decide valor.
- Fallback por regras (`handleSalesMessage`, determinístico) existe pra quando não há `OPENAI_API_KEY`.
