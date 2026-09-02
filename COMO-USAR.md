# Como exportar suas conversas da DataCrazy

Este guia te ensina, **passo a passo**, a baixar suas conversas da DataCrazy
para o seu computador. Não precisa saber nada de programação. É só seguir na
ordem. 🙂

---

## O que este programa faz?

Ele acessa a DataCrazy, pega as conversas das instâncias que você escolher e
salva **cada conversa em um arquivo de texto separado** (`.txt`), assim:

```
[22/05/2026 14:30] Maria Silva: Bom dia!
[22/05/2026 14:31] Atendente: Olá, como posso ajudar?
```

Os arquivos ficam organizados em pastas no seu computador, prontos para abrir,
ler ou guardar.

> 🔒 **Seu token é seguro.** O token da DataCrazy **não é salvo em lugar
> nenhum**. Ele é usado só na hora de baixar e some quando o programa termina.

---

## Antes de começar: instalar o Node.js

O programa precisa de um "motor" chamado **Node.js** (versão 18 ou mais nova)
para funcionar. É gratuito.

### Como saber se você já tem

1. Abra o terminal:
   - **Mac:** aperte `Cmd + Espaço`, digite **Terminal** e dê Enter.
   - **Windows:** aperte a tecla Windows, digite **cmd** e dê Enter.
2. Digite o comando abaixo e aperte Enter:

   ```
   node --version
   ```

3. Se aparecer algo como `v18.17.0`, `v20.x` ou `v24.x`, está tudo certo! ✅
   Se aparecer um erro como `command not found`, você ainda **não tem** o
   Node.js — veja abaixo.

### Como instalar (se precisar)

1. Acesse **https://nodejs.org**
2. Baixe a versão que aparece como **LTS** (a recomendada).
3. Instale normalmente (avançar, avançar, concluir).
4. Feche e abra o terminal de novo e repita o `node --version` para conferir.

---

## Passo a passo (Mac)

### 1. Crie a pasta do exportador

Para evitar problemas de permissão do macOS, **não use a Mesa (Desktop)**.
Use a pasta **Documentos**.

Crie uma pasta chamada `exportador` dentro de **Documentos** (pode criar pelo
Finder mesmo, do jeito normal). O caminho final fica assim:

```
Documentos/exportador
```

### 2. Coloque o arquivo do programa dentro dela

Copie o arquivo **`datacrazy-export.mjs`** para dentro de
`Documentos/exportador`.

### 3. Entre na pasta pelo terminal

No Terminal, digite o comando abaixo e aperte Enter:

```
cd ~/Documents/exportador
```

> 💡 `cd` significa "entrar na pasta". O `~` é um atalho para a sua pasta de
> usuário.

### 4. Confira se o arquivo está lá

Digite e aperte Enter:

```
ls
```

Deve aparecer `datacrazy-export.mjs` na lista. Se aparecer, ótimo — você está no
lugar certo. ✅

### 5. Rode o programa

Digite e aperte Enter:

```
node datacrazy-export.mjs
```

> ⚠️ **Importante ao copiar comandos:** copie e digite **apenas o comando**.
> Nunca inclua símbolos que aparecem antes dele no terminal, como `%`, `$` ou
> `bash-3.2$`. Esses símbolos são do próprio terminal — eles **não fazem parte
> do comando**. Por exemplo, se você vir:
>
> ```
> bash-3.2$ node datacrazy-export.mjs
> ```
>
> você digita **só** `node datacrazy-export.mjs`.

---

## O que o programa vai te perguntar

Depois de rodar, ele faz 3 perguntas simples:

1. **"Cole o token DataCrazy"**
   Cole o seu token e aperte Enter.
   👉 É normal o token **não aparecer na tela** enquanto você cola — isso é
   proposital, por segurança. Pode colar e apertar Enter com confiança.

2. **"Escolha a(s) instância(s)"**
   Ele mostra uma lista numerada das suas instâncias, por exemplo:

   ```
   [1] API Oficial Coexistência  (69bae51e...)
   [2] Maxwell  (697779cb...)
   [3] Melissa  (69b2bfc0...)
   [0] TODAS
   ```

   - Digite **um número** (ex: `2`) para baixar de uma instância.
   - Digite **vários separados por vírgula** (ex: `1,3`) para mais de uma.
   - Digite **`0`** para baixar de **todas** as instâncias.

3. **"Pasta de destino"**
   Só **aperte Enter** para usar a pasta padrão `export-datacrazy` (ela é criada
   automaticamente ali mesmo). Se preferir outro lugar, digite o caminho.

4. **Resumo e confirmação**
   Antes de começar, o programa descobre quantas conversas existem e mostra um
   resumo com o **tempo estimado**, por exemplo:

   ```
   Total: 10000 conversas encontradas.
   Já baixadas (serão puladas): 2000
   Faltam baixar: 8000
   Limite da sua conta: 30 conversas por minuto (30 req/min)

   ⏱  Tempo estimado: ~4h 48min  (pode parar e continuar depois)

   Deseja continuar? (s/n):
   ```

   Digite **`s`** e Enter para começar, ou **`n`** para cancelar (nada é
   baixado se você cancelar).

Pronto! Agora é só esperar. Ele mostra o progresso na tela.

---

## Como saber que deu certo

Enquanto roda, aparece uma linha de progresso que vai atualizando, tipo:

```
[120/438] ok=119 erros=1 pulados=0
```

Quando termina, aparece um resumo assim:

```
=== Concluído ===
Sucessos: 437 | Erros: 1 | Pulados (já existiam): 0
Arquivos em: /Users/voce/Documents/exportador/export-datacrazy
```

Se você viu **`=== Concluído ===`**, deu tudo certo! 🎉

---

## Onde ficam os arquivos

Tudo fica dentro da pasta **`export-datacrazy`**, que o programa cria dentro de
`Documentos/exportador`. Lá dentro, as conversas ficam separadas por instância:

```
export-datacrazy/
├── API_Oficial_Coexistencia/
│   ├── Maria_Silva_6a102776.txt
│   ├── Joao_Souza_6a106487.txt
│   └── ...
├── Maxwell/
│   └── ...
└── _erros.json        (só aparece se alguma conversa der erro)
```

Cada `.txt` é uma conversa, com as mensagens em ordem.

### Abrir a pasta no Mac

Para abrir a pasta direto no Finder, digite no terminal:

```
open ~/Documents/exportador/export-datacrazy
```

---

## Erros comuns e como resolver

### `command not found: node`
O Node.js não está instalado (ou o terminal não o encontrou). Volte na seção
**"Instalar o Node.js"**, instale e abra o terminal de novo.

### `Cannot find module ...`
Você está rodando o comando **fora da pasta** onde está o arquivo, ou o nome do
arquivo está errado. Confira:
1. Entre na pasta certa: `cd ~/Documents/exportador`
2. Veja se o arquivo está lá: `ls` (precisa aparecer `datacrazy-export.mjs`)
3. Rode de novo: `node datacrazy-export.mjs`

### `No such file or directory`
A pasta ou o arquivo não existe nesse caminho. Geralmente é erro de digitação no
`cd`. Confirme o nome da pasta (maiúsculas e acentos importam) e tente de novo.
No Mac, a pasta "Documentos" no terminal se chama `Documents`.

### `EPERM: operation not permitted`
O macOS bloqueou o acesso à pasta (acontece muito na **Mesa/Desktop**). Solução:
use a pasta **Documentos**, como recomendado neste guia:
```
cd ~/Documents/exportador
```

### `HTTP 401`
Seu token expirou ou está incorreto. Gere um **token novo** na DataCrazy e rode
o programa de novo, colando o token atualizado.

### O token não aparece quando eu digito/colo
Isso é **normal e proposital** — o token fica escondido por segurança. Pode
colar e apertar Enter normalmente, mesmo sem ver nada.

### Copiei `bash-3.2$` (ou `%`/`$`) junto com o comando
Esses símbolos são do terminal, **não** do comando. Apague-os e deixe só o
comando. Exemplo certo:
```
node datacrazy-export.mjs
```

---

## Parou no meio? É só continuar

Se a internet caiu ou você fechou o terminal no meio da exportação, **não se
preocupe**. Rode o programa de novo com os mesmos passos:

```
node datacrazy-export.mjs
```

Ele **pula automaticamente** as conversas que já foram baixadas e continua de
onde parou. Você vê isso no resumo, no campo `Pulados`.

---

## Quer baixar tudo de novo, do zero?

Basta apagar a pasta `export-datacrazy` antes de rodar.

> ⚠️ **Cuidado:** isso apaga **todos os arquivos `.txt` já exportados**. Só faça
> se tiver certeza de que não precisa mais deles (ou se já tiver uma cópia).

No Mac, você pode apagar pelo Finder (arrastando para a Lixeira) ou pelo
terminal:

```
rm -rf ~/Documents/exportador/export-datacrazy
```

Depois é só rodar `node datacrazy-export.mjs` de novo.

---

## Modo avançado (opcional)

> Esta parte é só para quem já se sentir à vontade. **Você não precisa dela**
> para usar o programa — o passo a passo acima já faz tudo.

Dá para passar as respostas direto no comando, sem o programa perguntar nada:

```
# Uma ou mais instâncias específicas (use os IDs das instâncias)
node datacrazy-export.mjs --token dc_xxxxx --instances ID1,ID2 --out ./minha-pasta

# Todas as instâncias
node datacrazy-export.mjs --token dc_xxxxx --all --out ./minha-pasta
```

O que cada opção faz:
- `--token` — informa o token direto no comando.
- `--instances` — lista de IDs de instâncias separados por vírgula.
- `--all` — baixa de todas as instâncias.
- `--out` — escolhe a pasta de destino.

Se preferir não deixar o token escrito no comando, dá para guardá-lo em uma
"variável de ambiente" (assim ele não fica no histórico do terminal):

```
# Mac/Linux
export DC_TOKEN=dc_xxxxx
node datacrazy-export.mjs --all --out ./minha-pasta
```
