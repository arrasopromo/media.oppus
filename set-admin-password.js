// Utilitário one-off: define a senha do admin (hash scrypt) a partir da variável
// de ambiente NEW_ADMIN_PASSWORD. A senha NUNCA fica neste arquivo — vem do ambiente.
//
// Uso no Windows (cmd.exe), dentro da pasta do projeto:
//   set NEW_ADMIN_PASSWORD=SuaNovaSenhaAqui
//   node set-admin-password.js
//   set NEW_ADMIN_PASSWORD=
//
// (PowerShell:  $env:NEW_ADMIN_PASSWORD='SuaNovaSenha'; node set-admin-password.js; Remove-Item Env:\NEW_ADMIN_PASSWORD )
require('dotenv').config();
const crypto = require('crypto');
const { getCollection } = require('./mongodbClient');

(async () => {
  const p = String(process.env.NEW_ADMIN_PASSWORD || '');
  if (p.length < 10) {
    console.error('❌ Defina NEW_ADMIN_PASSWORD com no mínimo 10 caracteres.');
    process.exit(1);
  }
  const salt = crypto.randomBytes(16);
  const key = crypto.scryptSync(p, salt, 64, { N: 16384, r: 8, p: 1 });
  const hash = 'scrypt$' + salt.toString('base64') + '$' + key.toString('base64');
  const admins = await getCollection('admins');
  const r = await admins.updateOne({ username: 'admin' }, { $set: { password: hash } });
  console.log(r.matchedCount ? '✅ Senha do admin atualizada (scrypt).' : '⚠️ Admin "admin" não encontrado no banco.');
  process.exit(0);
})().catch((e) => { console.error('❌', e.message); process.exit(1); });
