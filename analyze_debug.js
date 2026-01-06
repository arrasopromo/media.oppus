const fs = require('fs');
const path = require('path');

const filePath = path.join(__dirname, 'debug_embed_fail.html');
const content = fs.readFileSync(filePath, 'utf8');

console.log('Tamanho do arquivo:', content.length);

const keywords = ['username', 'profile_pic', 'followers', 'following', 'user', 'graphql'];

keywords.forEach(kw => {
    const index = content.indexOf(kw);
    if (index !== -1) {
        console.log(`\nEncontrado "${kw}" na posição ${index}`);
        console.log('Contexto:', content.substring(index - 50, index + 100));
    } else {
        console.log(`\n"${kw}" não encontrado.`);
    }
});

// Verificar se tem algum JSON grande que pareça dados
const jsonRegex = /<script type="application\/json"[^>]*>([\s\S]*?)<\/script>/g;
let match;
let count = 0;
while ((match = jsonRegex.exec(content)) !== null) {
    count++;
    if (match[1].length > 1000) {
        console.log(`\nJSON grande encontrado (${match[1].length} chars):`);
        console.log(match[1].substring(0, 200) + '...');
    }
}
console.log(`\nTotal de blocos JSON encontrados: ${count}`);
