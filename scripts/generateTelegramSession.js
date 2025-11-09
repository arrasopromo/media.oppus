require('dotenv').config();
const readline = require('readline');
const { TelegramClient } = require('telegram');
const { StringSession } = require('telegram/sessions');

const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
const ask = (q) => new Promise((resolve) => rl.question(q, (ans) => resolve(ans.trim())));

async function main() {
  const apiId = Number(process.env.TELEGRAM_API_ID || await ask('TELEGRAM_API_ID: '));
  const apiHash = process.env.TELEGRAM_API_HASH || await ask('TELEGRAM_API_HASH: ');
  const stringSession = new StringSession(process.env.TELEGRAM_STRING_SESSION || '');

  const client = new TelegramClient(stringSession, apiId, apiHash, { connectionRetries: 5 });
  console.log('Iniciando login no Telegram...');
  await client.start({
    phoneNumber: async () => await ask('Seu telefone (com código do país, ex: +5511999999999): '),
    password: async () => await ask('Senha 2FA (se houver): '),
    phoneCode: async () => await ask('Código enviado pelo Telegram: '),
    onError: (err) => console.error(err),
  });
  console.log('Login concluído. Salve a sessão abaixo em TELEGRAM_STRING_SESSION:');
  console.log('--- STRING SESSION INÍCIO ---');
  console.log(client.session.save());
  console.log('--- STRING SESSION FIM ---');
  rl.close();
  process.exit(0);
}

main().catch((err) => {
  console.error('Falha ao gerar sessão:', err);
  rl.close();
  process.exit(1);
});