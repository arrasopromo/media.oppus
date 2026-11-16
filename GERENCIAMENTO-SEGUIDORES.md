# Gerenciamento de Seguidores — como funciona e erros encontrados

> Página do admin: **`/painel/gerenciamento-seguidores`** (Oppus).
> Código: `app.js` linhas **32122–38427** (rota da página + APIs `/api/painel/gerenciamento-seguidores/*`), estimativa de custo em **47653**, reposição do cliente em **`/api/refil/simple` (27813)**. Tela: bloco `isFollowersMgmtView` em `views/painel.ejs` (~539–1100 e scripts ~2400+).
> Levantamento feito em 15/11/2026, só leitura (nada foi alterado).

---

## 1. Para que serve

Acompanhar se os clientes de **seguidores** mantiveram o que compraram e agir quando houve **queda**:

1. lista o último pedido pago de seguidores de cada perfil;
2. compara **inicial + contratado** com o **número atual** de seguidores;
3. mostra a **queda** (diferença), a **garantia** de reposição e o fornecedor;
4. permite verificar perfis, avisar clientes (e-mail + SMS), pedir reposição em massa (Refil2), criar pedidos novos para cobrir a queda e estender garantias vencidas.

---

## 2. Quais pedidos aparecem

**Query base** (`app.js:32140–32194`):

- **Pago:** `status`/`woovi.status` = "pago" **ou** existe `paidAt`, `woovi.paidAt`, `payment.paidAt`, `paghiper.paidAt` ou `efi.paidAt`.
- **Seguidores:** `categoria_servico = seguidores` (em `additionalInfoPaid`, `additionalInfoMapPaid` ou `additionalInfoMap`), **ou** `pacote` contendo "seguidores", **ou** `tipo`/`tipoServico` contendo "seguidores". Conferido no banco: pega os 2.638 pedidos de seguidores pagos desde setembro.
- Sem filtro de data na query: varre a coleção inteira, mais novo primeiro.

**Um pedido por perfil** (`32565–32575`): o @ sai de `instagram_username` → `instagramUsername` → `instauser`, em minúsculas e sem `@`. Fica só o pedido **mais recente** do perfil.

**Linha descartada quando** (`33012–33019`):

- não tem @;
- o perfil já teve **qualquer** pedido pago marcado como privado (exceto no filtro "notificados");
- o monitor diz que o perfil está privado;
- a quantidade contratada é 0;
- é curtida ou visualização;
- não há orderId de fornecedor (olha `fornecedor_social` e depois `fama24h`).

---

## 3. Como cada coluna é calculada

| Coluna | De onde vem |
|---|---|
| **OrderID** | ID do pedido no fornecedor (`fornecedor_social` ou `fama24h`). |
| **Fornecedor** | `slot.provider` → último `reorders[].provider` → nome do slot (`32337–32401`). |
| **Contratado** | Quantidade do pedido + upgrade do bump `upgrade:N` (`32320`). |
| **Inicial** | `initialFollowersCount` do pedido. Se faltar ou tiver sido medido mais de 30 min depois da compra, usa o último check de `validated_insta_users` até a hora da compra. **A tela grava esse valor de volta no pedido** (`32961`, `bulkWrite` dentro do GET). |
| **Resultado** | Inicial + Contratado. |
| **Atual** | `followers_monitor.followersCount` → último check > 0 em `validated_insta_users`. Fica vazio se a checagem for de antes da compra (`32818–32902`). |
| **Diferença / %** | Resultado − Atual; % = diferença ÷ contratado. Diferença > 0 = queda. |
| **Reposição (garantia)** | Só mistos e brasileiros. Prazo pelo bump: vitalícia, 12m, 6m, 4m, 3m ou 30 dias (padrão), contado do pagamento até o fim do dia (horário de Brasília). Depois é cruzado com `temporary_links` (`purpose: 'refil'`), ficando o maior prazo (`32659–32734`, `33033–33215`). |
| **VIP** | A última compra orgânica do perfil é mais nova que a última mista/brasileira. |
| **Refil2 auto** | Existe `refil2_requests` com `automated: true` para o @. |
| **Em andamento** | Só para a **página que está na tela**: consulta o status no fornecedor (lotes de 100, cache de 10 min, corte em 8 s). Se não estiver concluído nem cancelado, esconde a diferença (`33535–33599`). |

**Filtros da tela** (`33232–33336`): fornecedor, só erros, e-mail falhou, notificados, reposição expirada, só preenchidos, diferença mínima, garantia, datas De/Até (dia de Brasília, por compra ou por "checado"), tipo, busca por @ ou orderId, refil2 auto, ocultos, só OK e faixa de %.
**Ordenação/páginas:** qualquer coluna; páginas de 10 a 200 linhas.

---

## 4. Ações da tela

### Por linha
- **Atualizar** → `GET /current?force=1` → `followersMgmtGetCurrent` (`34163`). Busca os seguidores agora: RocketAPI → instagram_proxy → RocketAPI → Apify; se faltar saber se é privado, consulta a API de perfil. Grava em `followers_monitor`. Fila de 4,5 s entre consultas; com menos de 2 dias usa o cache.
- **Enviar Email+SMS** → `POST /notify` → `followersMgmtNotifyRecoveryForOrder` (`35873`). Só aparece com queda ≥ 100. Envia o e-mail "Recupere os seguidores perdidos" (no máximo 2 por pedido, 1 por dia, respeitando o teto diário) e 1 SMS Mex10 por pedido/telefone. Grava em `recoveryNotify`.

### Em massa (usam **o filtro inteiro**, até 5.000 linhas, não só a página)

| Botão | Endpoint | O que faz |
|---|---|---|
| Verificar / Forçar | `/verify-filtered/*` | Atualiza os seguidores de cada @ filtrado (mesma função do "Atualizar"). Tem pausar/retomar. |
| Validação geral | `/validate-general/*` | Mesma coisa para **todos** os pedidos de seguidores (pula privados, sem orderId, pagos há menos de 1 dia — 2 para orgânico — e ocultos). |
| Pausar tudo | `/pause-all` | Pausa só as duas validações acima. |
| Auto e-mail+SMS | `/auto-notify/*` | Escolhe candidatos (`34448`: último pedido do @, queda ≥ 100, sem orgânicos, 1º e-mail ou 2º após nova checagem) dentro do orçamento diário de e-mail e chama o notify a cada 900 ms. **Não usado desde 27/05/2026.** |
| Solicitar Refil2 | `/refil2/bulk-start` | Para cada @ misto/brasileiro com queda ≥ 101, chama `/api/refil/simple` (com `x-internal-secret`). Esse endpoint escolhe o pedido mais recente, aplica trava de 24 h (`refil_throttle`) e manda `action=refill` ao fornecedor. |
| Calcular gasto → Enviar pedidos | `/refil-cost-estimate` → `/refil-new-orders/start` | Cria um **pedido novo (pago)** no fornecedor principal com quantidade = queda. **Nunca foi usado** (nenhum registro em `refil2_requests`). |
| Usar inicial do Mongo | `/refil2/use-mongo-initial/mark` | Grava `refil2_initial_overrides` (`manualInitial = inicial + qtd`) para a âncora do refil2. |
| Corrigir inicial | `/fix-initial/*` | Busca o `start_count` do pedido no `refilfama24h.com/api_proxy.php` e grava como `initialFollowersCount`. |
| Corrigir checado | `/fix-checked`, `/fix-checked-global` | Apaga a checagem do monitor quando ela é do mesmo dia da compra ou anterior (dado de antes da entrega). |
| Estender expirada +30d | `/extend-refil-expired` | Para pedidos com garantia vencida: estende o link de refil ou cria um novo de 30 dias. Tem `dryRun`. |
| Ocultar / Desocultar | `/hide`, `/unhide` | `followers_monitor.hidden` por @. |

**Rotinas em segundo plano:** ficam **só em memória** (`global.followersMgmt*Job`). Se o servidor reiniciar, o job some e não retoma sozinho. O Refil2 continua protegido pela trava de 24 h e o aviso por `recoveryNotify` (ambos no banco). O "Enviar pedidos" não tem proteção nenhuma.

### Coleções usadas
`checkout_orders` (pedidos), `followers_monitor` (seguidores atuais, oculto, privado), `validated_insta_users` (checks do checkout), `temporary_links` (links de refil e garantia), `refil2_requests`, `refil_throttle`, `refil2_initial_overrides`, `settings` (`followers_mgmt_settings`).

---

## 5. Erros encontrados

Todos conferidos no código. Classificação:
- **🔴 grave:** gasta dinheiro, nega reposição devida ou manda mensagem errada ao cliente;
- **🟠 médio:** mostra informação errada ou esconde clientes;
- **🟡 baixo:** desempenho ou relatório.

### 🔴 Graves

**1. Reposição negada depois de um reenvio de parcial** — `app.js:27964` + `4425`
- O reenvio de parcial sobrescreve `fama24h.requestPayload.quantity` com **só o que faltava**. O `/api/refil/simple` usa esse campo como "quantidade contratada".
- Exemplo: comprou 1000 com inicial 500, entregou 600 e foi reenviado 400. O sistema passa a achar que o contratado era 500 + 400 = 900. Com o perfil em 1200 (já perdeu 300), responde *"quantidade atual é maior que a contratada"* e **nega a reposição**.
- Além disso, o `refill` vai para o pedido do reenvio (400), e a parte de 600 fica sem reposição.
- **Afeta pedidos reais**, inclusive os 4 parciais reenviados em 14/11 (@051_luiz_fernando, @toninhoalves103, @safelytransporteexecutivo, @hgnaildesigner).
- Correção: guardar a quantidade original (ex.: `fama24h.quantidadeOriginal`) ou somar a do `parcialHistorico`.

**2. Refil em massa / "Enviar pedidos" usam queda de pedido ainda não entregue** — `app.js:33601`
- O "Em andamento" só é calculado para a **página visível**. Os alvos do Refil2 e do "Enviar pedidos" são montados do **filtro inteiro**, com a diferença crua.
- Um pedido da página 3 ainda entregando (inicial 500, contratado 1000, atual 600) entra como "queda de 900".
- No "Enviar pedidos" isso vira um **pedido novo pago de 900**.

**3. "Enviar pedidos" repete o `add` sozinho** — `app.js:37119`
- Usa `postFormWithRetry(..., 3 tentativas)`. Se o fornecedor demorar mais de 60 s para responder, envia o `add` de novo: até **3 pedidos cobrados** para o mesmo perfil.
- Fere a regra "nunca repetir add".

**4. "Enviar pedidos" sem trava nem validação** — `app.js:37105–37119`
- Não confere se o perfil está **público**, nem a garantia.
- A quantidade vem do navegador, sem limite pela contratada.
- Não marca o pedido: clicar de novo cria pedidos de novo.
- Não tem botão de pausar.
- Exige a chave da Nuvra mesmo quando o fornecedor principal é outro (`37100`).

**5. `/api/refil/simple` não confere garantia nem perfil público** — `app.js:27813–28166`
- O Refil2 em massa (e o cliente pelo link) dispara reposição fora da garantia ou com perfil privado. Já tinha sido visto em outubro: 23 de 325.
- O bulk também manda o `fornecedorOrderId`, mas o endpoint ignora e escolhe o pedido sozinho.

**6. Aviso de queda para quem acabou de recomprar** — `app.js:34605–34781`
- Os candidatos do auto-aviso usam `validated_insta_users`, gravado no checkout **antes** da entrega, e não olham se a entrega terminou.
- Quem recompra 1000 aparece com "queda de 1000" e recebe **"você perdeu seguidores"** enquanto ainda está recebendo.
- *(Auto-aviso parado desde 27/05/2026 — só acontece se for religado.)*

**7. Aviso de queda para perfil privado** — `app.js:34709`
- `if (isPrivateOrder(o)) stats.ordersSkippedPrivate += 1;` não tem `continue`: conta como "pulado", mas **segue**.
- O cliente privado recebe o convite para pedir reposição, que depois falha.

**8. SMS diz que mandou e-mail quando não mandou** — `app.js:36189` / `36214`
- O SMS ("Enviamos no seu e-mail cadastrado as orientações...") só é bloqueado quando o motivo é teto diário.
- Também sai com e-mail ausente, falha de SMTP, limite por pedido, "já enviado hoje" e orgânicos.

**9. "Corrigir inicial" pula um pedido a cada item ignorado** — `app.js:37543–37547`, `37577–37581` + `37626`
- O `continue` dentro do `try` soma `job.done += 1` e o `finally` soma de novo.
- Cada item sem @, sem orderId ou de tipo não elegível faz o pedido seguinte ser pulado sem correção.

### 🟠 Médios

**10. ✅ CORRIGIDO em 15/11 (sem commit) — "Partial" e erro do fornecedor ficavam "Em andamento" para sempre** — `app.js:33592–33597`
- Só "complet/conclu" conta como entregue e só "cancel/refund/estorn" como encerrado.
- Ficam como "em andamento", com a queda escondida da tela:
  - pedido **Partial** (status final);
  - resposta `Incorrect order ID` (fornecedor errado).
- Já o **Canceled** (nada entregue) aparece como queda cheia e entra no refil.

**11. Fornecedor errado na checagem de entrega** *(parte corrigida em 15/11: a tela agora usa o slot com o pedido mais recente; o mapeamento "Fama24h"→Nuvra continua)* — `app.js:33540`, `32389–32397`
- O rótulo "Fama24h" é sempre consultado na **Nuvra**.
- Quando o provider vem de `reorders[-1]`, o orderId usado continua sendo o original. O status sai de outro pedido ou vira erro, e aí cai no item 10.

**12. Perfil que já foi privado some da tela para sempre** — `app.js:32232–32253`, `33014`
- Um único pedido antigo com `isPrivate: true` tira o @ da lista, mesmo que hoje esteja público e tenha comprado de novo.

**13. Só o último pedido do perfil conta** — `app.js:32565–32575`, `33016–33018`
- Se o último pedido ainda não tem orderId (upsell travado, pedido novo não despachado), o perfil **some**, escondendo o pedido anterior já entregue.
- Principal + upsell no mesmo dia: só o upsell entra no cálculo. A queda real fica escondida (ex.: mostra "OK" com 1800 quando o esperado era 2500).

**14. ✅ CORRIGIDO em 15/11 (sem commit) — Garantia de 3 meses aparecia como "30 dias" (só no painel; o /refil do cliente sempre mostrou ~90 dias)** — `views/painel.ejs:898–904`
- O rótulo não trata o modo `3m`, que é a garantia vendida hoje no checkout.
- O filtro "Reposição" e o relatório também não têm 3m.

**15. Ocultar vale para o perfil, não para o pedido** — `app.js:32904`, `33324`
- Uma compra nova de um @ ocultado nunca aparece.

**16. "Estender expirada" em modo marcado mexe em pedidos não marcados** — `app.js:35466–35471`, `35450`
- Marcar um @ estende ou cria link de 30 dias para **todos** os pedidos de seguidores desse @, inclusive **orgânicos** (que não têm garantia).

**17. "Estender expirada" junta clientes pelo telefone** — `app.js:35763–35769`
- O fallback por telefone reaproveita links sem @. Com o telefone genérico do refil_extensao (47997086876), pedidos de clientes diferentes caem no link de outra pessoa.
- Se o update dos pedidos falhar (`35846`, erro engolido), os links ficam órfãos e a resposta diz que deu certo.

**18. Inicial errado gravado de forma permanente** — `app.js:32952–32961`
- Se `validated_insta_users.checkedAt` estiver vazio, o código aceita a contagem (que pode ser de depois da entrega) como inicial e **grava no pedido**.
- Infla o resultado e cria queda falsa.

**19. Envio duplicado de e-mail/SMS** — `app.js:36060–36072`, `38124`
- O notify lê o contador, envia e só depois grava, sem trava atômica.
- O "já está rodando" do auto-aviso é checado antes de um cálculo demorado: dois cliques = dois jobs, e o "pausar" só para um.

**20. Checagem nova com número antigo** — `app.js:34402–34407`
- Quando a fonte só devolve se o perfil é privado, o código mantém o número antigo mas atualiza a data.
- Isso libera o 2º e-mail de aviso e conta como "verificado OK" na validação.

### 🟡 Baixos

**21. Configurações sem efeito** — `dailyLimit` e `order` de `followers_mgmt_settings` são salvos, mas nada os usa.

**22. Relatório conta como "Não OK" perfis checados antes da compra** — `app.js:33412–33421`.

**23. Estimativa de custo maior que o real** — `app.js:47713`
- Cobra no mínimo 100 por queda de 1 a 100, mas o job ignora quedas abaixo de 101.

**24. Filtro "Checado em"** — `app.js:33282–33291`
- Usa `new Date()` em datas que podem estar como dd/mm/aaaa, e essas linhas somem.
- De/Até com 1 dia de diferença invertido volta vazio.

**25. Gráfico por hora da auditoria de e-mail em UTC** — `app.js:35174`
- O pico das 10h aparece às 13h. Falta `timezone: 'America/Sao_Paulo'`.

**26. Desempenho**
- O GET da página varre a coleção inteira com regex em 9 campos e mais dois scans de milhares de pedidos.
- Consulta fornecedores por até 8 s e grava no banco.
- Vários `catch (_) {}` escondem falhas: garantia some, "entregue" falso etc.
- O notify roda contagens na coleção inteira a cada envio.
- Os limites de 4.000 em `ordersMeta` e `tlByUser` cortam dados em silêncio.

---

## 6. Correções já aplicadas (15/11/2026, sem commit)

- **Último pedido:** a tela usa o slot com o pedido mais recente (`fornecedor_social` x `fama24h`, por `reorderAt`/`requestedAt`), não mais sempre o `fornecedor_social` primeiro.
- **Parcial/Cancelado:** se o último pedido está Partial, Canceled ou Refunded no fornecedor, a linha mostra **"Cancelado"** (vermelho) em Atual e %, sem queda — o que falta vai pela Gestão Parcial, não pelo refil. Concluído mostra o número normal; o resto continua "Em andamento".
- **3 meses:** rótulo, selo, filtro "Reposição" e relatório por garantia agora reconhecem 3 meses.

## 7. Ordem sugerida de correção

1. **Item 1** (reposição negada após reenvio): afeta clientes hoje. Gravar a quantidade original no pedido e usá-la no `/api/refil/simple`. Corrigir também os pedidos já reenviados.
2. **Itens 2, 3, 4** antes de qualquer uso do "Enviar pedidos":
   - um `add` só, sem retry;
   - checar perfil público e entrega concluída no servidor;
   - travar o pedido;
   - limitar a quantidade;
   - adicionar pausa.
3. **Item 5**: conferir garantia e perfil público no `/api/refil/simple`.
4. **Itens 10 e 11**: tratar Partial como encerrado e usar o provider real do slot na checagem de entrega.
5. **Item 14**: rótulo e filtro de 3 meses.
6. **Itens 6, 7, 8, 19**: obrigatórios antes de religar o auto-aviso de queda.
7. **Itens 12, 13, 15**: perfis escondidos da lista.
8. Demais (9, 16–18, 20–26).
