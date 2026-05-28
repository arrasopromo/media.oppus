# Documentação do Projeto (Agência OPPUS)

## Visão geral
- Stack: Node.js (Express), EJS (views), MongoDB.
- Objetivo: checkout de serviços (seguidores/curtidas/visualizações), gestão de refil/reposição, refil2, painel admin e automações (verificação, e-mails de recuperação, auditorias).

## Estrutura principal
- Backend: [app.js](file:///c:/Users/Raynan-user/Documents/meuapp/app.js)
- Pricing/tabela de preços: [pricing.js](file:///c:/Users/Raynan-user/Documents/meuapp/pricing.js)
- Views (EJS): pasta [views](file:///c:/Users/Raynan-user/Documents/meuapp/views)
- Assets (JS/CSS): pasta [public](file:///c:/Users/Raynan-user/Documents/meuapp/public)

## Checkout (Front)
### Páginas
- Checkout principal: [checkout.ejs](file:///c:/Users/Raynan-user/Documents/meuapp/views/checkout.ejs)
- Serviços:
  - Seguidores: [servicos-instagram.ejs](file:///c:/Users/Raynan-user/Documents/meuapp/views/servicos-instagram.ejs)
  - Curtidas: [servicos-curtidas.ejs](file:///c:/Users/Raynan-user/Documents/meuapp/views/servicos-curtidas.ejs)
  - Visualizações: [servicos-visualizacoes.ejs](file:///c:/Users/Raynan-user/Documents/meuapp/views/servicos-visualizacoes.ejs)

### Fluxo (alto nível)
- Etapa 1: seleção do tipo/pacote.
- Etapa 2: validação do perfil/link e dados do cliente (telefone/e-mail).
- Etapa 3: pagamento (Pix e Cartão).

### Pagamento
- Pix:
  - Gateway principal no projeto: PagHiper via endpoint `/api/paghiper/charge`.
  - Status Pix: `/api/paghiper/charge-status`.
- Cartão:
  - Provedor atual: Pagar.me (tokenização no front e criação de cobrança no backend).
  - O front força o provider do cartão para Pagar.me nas páginas de checkout/serviços.

### Validação de telefone
- Regra: aceitar 10 ou 11 dígitos (BR), rejeitar sequências repetidas e normalizar para dígitos.
- Front: [servicos-instagram.js](file:///c:/Users/Raynan-user/Documents/meuapp/public/js/servicos-instagram.js)

## Pedido (pós-compra)
- Página: [pedido.ejs](file:///c:/Users/Raynan-user/Documents/meuapp/views/pedido.ejs)
- Exibe dados do pedido, status e informações de reposição/refil quando aplicável.

## Refil (Reposição tradicional)
- Página do refil: [refil.ejs](file:///c:/Users/Raynan-user/Documents/meuapp/views/refil.ejs)
- Links temporários:
  - Sistema de links temporários gerados e validados via Mongo.
  - Uso para restringir acesso ao refil e a rotas sensíveis.

## Refil2
### O que é
- Fluxo de reposição/automação mais robusto, com auditoria, execução, decisão e histórico.

### Pontos do painel
- Auditoria e verificação de “current” (seguidores atuais) no painel:
  - Endpoint: `/api/painel/refil2/audit-verify-current`
  - Quando o `instagram_proxy` retorna 0, há fallback para Rocket API.

## Painel Admin
### Página base
- View principal do painel e blocos do gerenciamento de seguidores/refil2: [painel.ejs](file:///c:/Users/Raynan-user/Documents/meuapp/views/painel.ejs)

### Funcionalidades relevantes (resumo)
- Dashboard: métricas gerais e cards de acompanhamento.
- Vendas: listagem e filtros.
- Recuperação de vendas (Pix pendente/abandono): rotinas que disparam e-mails por estágios e aplicam regras anti-spam.
- Gerenciamento de Seguidores:
  - Verificações em massa:
    - Verificar marcados (loop no front).
    - Verificar filtro (job backend).
    - Forçar verificação do filtro (ignora regra de não re-checar no mesmo dia).
  - Pausar (tudo): interrompe jobs em andamento.
  - Relatórios: gráficos de distribuição e saúde (queda, OK/NOK, etc).
  - Corrigir inicial (marcados/filtro): ajusta `initialFollowersCount` usando dados do fornecedor.
  - Regras: orgânicos são tratados de forma diferenciada e podem ser excluídos de rotinas específicas.

## Pedidos WhatsApp (Admin)
- Página: [painel_whatsapp.ejs](file:///c:/Users/Raynan-user/Documents/meuapp/views/painel_whatsapp.ejs)
- Objetivo: criar venda manual já como “pago” para contabilizar no painel.
- Fluxo:
  - Sugestão de preço via tabela: `/api/painel/whatsapp-orders/price`
  - Criação do pedido: `/api/painel/whatsapp-orders/create`
  - Link de reposição: retornado quando aplicável.
- Flag “Não enviar para fornecedor”:
  - Registra venda e cria link de reposição, mas bloqueia despacho do pedido no fulfillment.

## Integrações com fornecedor
### Fama24h
- Uso típico:
  - Envio de pedidos via API (ex.: `/api/v2` com `action=add`, `service`, `link`, `quantity`).
  - Proxy para contagens/consulta: endpoints externos como `instagram_proxy.php` e `api_proxy.php`.

### Fornecedor Social
- Usado em cenários específicos (ex.: curtidas “reais”/orgânicas), com `serviceId` dedicado.

### Rocket API
- Fallback para buscar contagens/privacidade quando o proxy falha ou retorna dados inválidos (ex.: `followersCount=0`).

## Banco de dados (MongoDB)
Coleções principais (alto nível):
- `checkout_orders`: pedidos do checkout (Pix/Cartão), pedidos manuais (WhatsApp), status, payloads e campos de auditoria.
- `temporary_links`: links temporários (refil, restrito, etc).
- `refil2_requests`: solicitações do refil2, auditoria e execução.
- `followers_monitor` e `validated_insta_users`: cache e histórico de verificações de perfil/contagens.
- `checkout_leads`: leads abandonados (recuperação).
- `page_views`: tracking (ex.: acessos a `/pix`).

## Variáveis de ambiente (sem valores)
Exemplos de variáveis usadas no projeto:
- `PAGHIPER_API_KEY`, `PAGHIPER_TOKEN`
- `PAGARME_PUBLIC_KEY` (front) e chaves privadas no backend (se configuradas no projeto)
- `FAMA24H_API_KEY`
- `FORNECEDOR_SOCIAL_API_KEY`
- `ROCKETAPI_TOKEN`
- `SITE_URL` / `PUBLIC_URL` / `APP_URL` / `BASE_URL`

## Observações de segurança/boas práticas
- Nunca versionar chaves/segredos.
- Logs devem evitar dados sensíveis (CPF, tokens, brCode Pix).

