# RocketAPI — como a Oppus consulta o Instagram

Documento de referência das consultas de perfil, posts e reels feitas pela Oppus
(`app.js`), e da estratégia usada quando o perfil é **comercial (business)**.

> Linhas citadas são do `app.js` em 18/11/2026 (commit `117327c`). O arquivo muda
> muito; procure pelo nome da função se a linha não bater.

---

## 1. Básico da API

- Base: `https://v1.rocketapi.io/instagram/...`
- Toda chamada é **POST** com corpo JSON e cabeçalho `Authorization: Token <ROCKETAPI_TOKEN>`
  (token no `.env`).
- A resposta vem embrulhada: `{ status: 'ok' | 'done', response: { status_code, body } }`.
  - `body` às vezes vem como **string JSON** → fazer `JSON.parse` antes de ler
    (ver `rkBody` / `_body` no código).
  - O dado útil fica em `body.data.user` (get_info, get_web_profile_info) ou em
    `body.user` (get_info_by_id) ou em `body.items` (get_media, get_clips).
- Cada chamada conta no plano. Por isso existe cache, ID salvo e uma API própria
  gratuita antes/depois do RocketAPI (seção 5).

### Endpoints usados

| Endpoint | Entrada | Para quê | Onde |
|---|---|---|---|
| `user/get_info` | `{ username }` | Perfil completo (mobile): seguidores, privado, avatar, às vezes os 12 posts em `edge_owner_to_timeline_media` | `verifyInstagramProfile`, `fetchFollowerCount`, `_postsViaRocketApi`, `fetchInstagramFollowersInfoRocketApi` |
| `user/get_web_profile_info` | `{ username }` | Mesmo dado pela rota **web**; segunda tentativa quando o get_info falha | `fetchFollowerCountRocket`, `segParcialPerfil`, `fetchInstagramFollowersInfoRocketApi` |
| `user/search` | `{ query }` | Achar o **ID (pk)** de um @ — base da estratégia business | `verifyInstagramProfile`, `fetchInstagramFollowersInfoRocketApi` |
| `instagram/search` | `{ query }` | Busca geral; usada na listagem de posts para achar o ID | `_postsViaRocketApi` |
| `user/get_info_by_id` | `{ id }` | Perfil pelo ID — **rota que funciona para conta business** | `verifyInstagramProfile`, `fetchInstagramFollowersInfoRocketApi` |
| `user/get_media` | `{ id, count: 12 }` | Últimos posts do feed pelo ID | `verifyInstagramProfile`, `_postsViaRocketApi` |
| `user/get_clips` | `{ id, count: 12 }` | Últimos **reels** (itens vêm como `{ media: {...} }`) — tela de visualizações | `_postsViaRocketApi` (modo reels) |
| `media/get_info_by_shortcode` | `{ shortcode }` | Curtidas de um post, se é vídeo, curtidas ocultas | `fetchPostLikeCount`, `isInstagramVideoPost` |

### Campos que importam

- Seguidores: `edge_followed_by.count` → senão `follower_count` (get_info_by_id usa este).
- Seguindo: `edge_follow.count` → `following_count`.
- Posts: `edge_owner_to_timeline_media.count` → `media_count`.
- Privado: `is_private`. Verificado: `is_verified`. ID: `id` ou `pk`.
- Avatar: `profile_pic_url_hd` → `profile_pic_url` → `hd_profile_pic_url_info.url` (by_id).
- Post (get_media/get_clips): `code` = shortcode, `taken_at`, `media_type === 2` = vídeo.
- Curtidas de post: `like_count` → `edge_media_preview_like.count` → `edge_liked_by.count`.
  `like_and_view_counts_disabled: true` (ou `like_count: -1`) = **curtidas ocultas**:
  o número que vier não serve para medir entrega.

---

## 2. Consulta de perfil (checkout, refil, painel)

Função principal: `verifyInstagramProfile(username, ..., bypassCache, { purpose, skipPosts })`
(app.js ~6624). Ordem:

1. **Cache em memória** (5 min; "não encontrado" fica 2 min). Pedidos iguais em paralelo
   esperam a mesma consulta (`PROFILE_INFLIGHT`). `bypassCache = true` pula o cache.
2. **RocketAPI `get_info`** (timeout 8 s).
   - Se não vierem posts nas edges e o perfil for público → `get_media` pelo ID.
   - Resposta com "expired / renew / plan is" → RocketAPI **desligado por 10 min**
     (`global.rocketApiDisabledUntil`) e todo mundo cai nos fallbacks.
3. **Estratégia business** (seção 3): `user/search` → `get_info_by_id` → `get_media`.
4. **Self-API** (API própria na VPS, grátis, 5 s) — `fetchProfileSelfApi`.
5. **Apify** (pago, mais lento). Se a cota mensal estourou, é pulado e o perfil volta
   como "não encontrado".

Toda consulta bem-sucedida grava em `validated_insta_users` (perfil, seguidores, privado,
posts, `source`) e acrescenta um snapshot em `checks` (últimos 60). O `source` diz por
onde veio: `verifyInstagramProfile_ROCKETAPI`, `..._ROCKETAPI_BYID`, `..._SELFAPI`.
Checagens de fundo de refil usam `refilcheck_*` para **não** contarem como validação do cliente.

`purpose: 'refil' | 'refil2'` ou `skipPosts: true` → não busca posts (economiza chamada).

### Contagem só de seguidores

- `fetchInstagramFollowersInfoRocketApi` (~34245) — usada no gerenciamento de seguidores,
  reposição, conferência de queda. Ordem: **self-API → get_info → get_web_profile_info →
  estratégia business**. Retorna `rocket_business_schema` quando nem a rota by_id resolve.
- `fetchFollowerCount` (~3346): self-API → get_info → Apify.
- `fetchFollowerCountRocket` (~3321): só RocketAPI (get_info → get_web_profile_info), sem
  self-API, porque a self-API chegou a devolver número furado. Usada na auditoria de entrega.

> Atenção: a ordem não é a mesma em todo lugar. `verifyInstagramProfile` usa RocketAPI
> primeiro e a self-API como fallback; as funções de contagem usam a self-API primeiro.

---

## 3. Estratégia para perfil comercial (business)

### O problema

Para **contas comerciais**, `user/get_info` e `user/get_web_profile_info` falham com
HTTP 400 dentro da resposta:

```
Asset ig_business_category_subvertical has been deleted
```

O Instagram removeu esse campo, mas a RocketAPI ainda pede ele nessas duas rotas.
Contas **pessoais** e de **criador/profissional** não são afetadas.

Sinais de que é esse caso (o código checa todos):
- `response.status_code === 400`
- texto contém `has been deleted`, `cannot use this schema` ou `ig_business_category_subvertical`

Quando aparece, **não adianta tentar a outra rota** (get_web_profile_info tem o mesmo
problema): o código sai do laço e vai direto para o caminho por ID.

### O caminho que funciona

```
user/search { query: "<@>" }
   └─ procura na lista o usuário com username EXATAMENTE igual (minúsculas, sem @)
      └─ pega pk (ou pk_id / id)
         └─ user/get_info_by_id { id: pk }      → seguidores, privado, avatar
            └─ user/get_media { id: pk, count: 12 }   → posts (se público)
               (ou user/get_clips para reels)
```

Regras:
- **Match exato do @.** A busca devolve vários perfis parecidos; usar o primeiro resultado
  pega o perfil errado. Só aceita `username.toLowerCase() === @ pedido`.
- `get_info_by_id` devolve o usuário em `body.user` (não em `body.data.user`) e os
  seguidores em `follower_count` (não em `edge_followed_by`).
- Se o perfil for privado, não chama `get_media` (não traz nada e gasta chamada).
- Custo: 2 chamadas para o perfil (search + by_id) e mais 1 para os posts, contra 1 do
  get_info normal. Por isso o by_id é **fallback**, não o caminho padrão.

### Guardar o ID

Depois que o ID é descoberto, a listagem de posts grava `igUserId` em
`validated_insta_users`. Na próxima vez, `_postsViaRocketApi` vai **direto no
`get_media`/`get_clips` com o ID salvo**, sem get_info nem search. Para perfil business
isso corta de 3 chamadas para 1. O ID de uma conta não muda quando ela troca o @.

---

## 4. Posts e reels para curtidas/visualizações

Rota do site: `GET /api/instagram/posts?username=...` (~26650), protegida:
- Exige o token de sessão `x-oppus-api-tk` (gerado na sessão do cliente) — terceiros não
  conseguem usar a rota para consultar perfis com o nosso plano.
- Limite de **60 consultas por IP a cada 10 min**.
- `refresh=1` ignora o cache (cliente acabou de postar).
- `reels=1` busca reels (`get_clips`) em vez do feed.

`_postsViaRocketApi(username, { reels })` (~26527):
1. `igUserId` salvo → `get_media`/`get_clips` direto.
2. `get_info` (timeout curto, 5 s) — pode já trazer as 12 edges do feed.
3. Sem ID → `instagram/search` com match exato do @.
4. `get_media`/`get_clips` pelo ID (se público).
5. Salva o ID.

Erros devolvidos: `rocket_user_not_found` (não achou o ID), `rocket_no_posts` (achou, mas
sem posts ou privado).

---

## 5. Self-API (API própria)

- `fetchProfileSelfApi` / `fetchPostSelfApi`, URL em `SELF_PROFILE_API_URL` /
  `SELF_POST_API_URL` (VPS própria). Grátis, timeout de 5 s.
- Desliga com `SELF_PROFILE_API_ENABLED=0`.
- Só é aceita com **seguidores > 0** (perfil) ou **curtidas > 0** (post). Zero ou oculto
  cai na RocketAPI, que sabe diferenciar curtidas ocultas.
- Não serve para auditoria de entrega (já devolveu contagem errada); lá usar
  `fetchFollowerCountRocket`.

---

## 6. Boas práticas

- **Antes de despachar qualquer pedido**, conferir o perfil **fresco** (`bypassCache`) e
  público. Perfil privado não envia.
- Curtidas ocultas (`like_and_view_counts_disabled`) não provam entrega nem falta dela.
- Não chamar a RocketAPI direto do navegador; sempre pelas rotas do servidor (token fica
  só no `.env`).
- Em scripts avulsos, reaproveitar a mesma ordem: `get_info` → se erro de schema business,
  `user/search` + `get_info_by_id`.
- Exemplo mínimo (Node):

```js
const H = { headers: { Authorization: 'Token ' + process.env.ROCKETAPI_TOKEN }, validateStatus: () => true };
const body = (d) => { let b = d && d.response ? d.response.body : null; return typeof b === 'string' ? JSON.parse(b) : b; };

async function perfil(username) {
  const u = username.replace(/^@/, '').toLowerCase();
  const r = await axios.post('https://v1.rocketapi.io/instagram/user/get_info', { username: u }, H);
  const user = body(r.data)?.data?.user;
  if (user) return user;                                   // pessoal / criador
  // business: search → get_info_by_id
  const s = await axios.post('https://v1.rocketapi.io/instagram/user/search', { query: u }, H);
  const lista = body(s.data)?.users || [];
  const alvo = lista.map((w) => w.user || w).find((x) => String(x.username).toLowerCase() === u);
  if (!alvo) return null;
  const i = await axios.post('https://v1.rocketapi.io/instagram/user/get_info_by_id', { id: String(alvo.pk || alvo.id) }, H);
  return body(i.data)?.user || null;                      // seguidores em follower_count
}
```
