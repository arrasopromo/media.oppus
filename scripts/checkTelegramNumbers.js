require('dotenv').config();
const { TelegramClient } = require('telegram');
const { StringSession } = require('telegram/sessions');
const { Api } = require('telegram');

// Lista fornecida pelo usuário
const rawNumbers = [
  '5573998618134', '5511967081409', '5541998607040', '5519982073747', '5551984841007',
  '5548991150895', '5541999525338', '5521980777103', '5531989867664', '5538998451575',
  '5594984190429', '5542998320861', '5582982316964', '5587988347404', '5588997643201',
  '5541999675483', '5511982863693', '5581999506563', '5562981481941', '5541988475268',
  '5569992119941', '5585998669886', '5549999022749', '5522998276777', '5561991095739'
];

const numbers = rawNumbers.map(n => n.startsWith('+') ? n : `+${n}`);

async function check() {
  const apiId = Number(process.env.TELEGRAM_API_ID);
  const apiHash = process.env.TELEGRAM_API_HASH;
  const string = process.env.TELEGRAM_STRING_SESSION || '';
  if (!apiId || !apiHash || !string) {
    console.error('Faltam variáveis de ambiente: TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_STRING_SESSION');
    console.error('Use "npm run tg:session" para gerar TELEGRAM_STRING_SESSION e configure .env');
    process.exit(1);
  }

  const client = new TelegramClient(new StringSession(string), apiId, apiHash, { connectionRetries: 5 });
  await client.connect();

  // Montar contatos para importação
  const contacts = numbers.map((phone, idx) => new Api.InputPhoneContact({
    clientId: BigInt(idx + 1),
    phone,
    firstName: 'Check',
    lastName: ''
  }));

  console.log('Importando contatos no Telegram para verificação...');
  const result = await client.invoke(new Api.contacts.ImportContacts({ contacts }));

  const importedByClientId = new Map();
  for (const imp of result.imported) {
    // imp.clientId: bigint; imp.userId: bigint
    importedByClientId.set(String(imp.clientId), imp.userId);
  }

  const clientIdToPhone = new Map();
  contacts.forEach((c, idx) => clientIdToPhone.set(String(BigInt(idx + 1)), c.phone));

  const foundUsers = new Map();
  for (const user of result.users) {
    foundUsers.set(String(user.id), user);
  }

  const summary = numbers.map((phone, idx) => {
    const cid = String(BigInt(idx + 1));
    const uid = importedByClientId.get(cid);
    const hasTelegram = !!uid && foundUsers.has(String(uid));
    return { phone, hasTelegram };
  });

  console.log('Resultado:');
  for (const item of summary) {
    console.log(`${item.phone} => ${item.hasTelegram ? 'Possui Telegram' : 'Não encontrado no Telegram'}`);
  }

  // Limpar contatos adicionados (opcional)
  if (result.users && result.users.length > 0) {
    try {
      const ids = result.users.map(u => new Api.InputUser({ userId: u.id, accessHash: u.accessHash }));
      await client.invoke(new Api.contacts.DeleteContacts({ id: ids }));
      console.log('Contatos temporários removidos.');
    } catch (err) {
      console.warn('Falha ao remover contatos temporários:', err.message);
    }
  }

  await client.disconnect();
}

check().catch(err => {
  console.error('Erro ao verificar números no Telegram:', err);
  process.exit(1);
});