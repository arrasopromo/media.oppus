const axios = require('axios');

const usernames = [
    "Dosarabe.imports",
    "leandrinhomc_ofc",
    "Professorahellenchristine",
    "pedro_alexandre_sc",
    "elizangelacabralpsicologa",
    "camiih1213",
    "Juliorodri7",
    "drrogerioferreira",
    "Aartedecolecionar",
    "amoraa_secret",
    "pinholeandro89",
    "josegibin_ofc",
    "Dr_Herickson_Malini",
    "dmac.camaraarbitral",
    "r.e.z.o.s",
    "JOKASLANCHES_ITAPEVI",
    "Quielflores",
    "edumek",
    "Belonigomes",
    "_ferdog"
];

const headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Cache-Control': 'no-cache',
    'Pragma': 'no-cache',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Upgrade-Insecure-Requests': '1'
};

async function checkUser(username) {
    const start = Date.now();
    // Usando a estratégia do /embed que parece ser a padrão no app.js
    const url = `https://www.instagram.com/${username}/embed`;
    
    try {
        const response = await axios.get(url, {
            headers: headers,
            timeout: 15000,
            validateStatus: () => true // Resolve promise for all status codes
        });
        
        const duration = Date.now() - start;
        return {
            username: username,
            status: response.status,
            duration: `${(duration / 1000).toFixed(3)}s`,
            // Opcional: verificar se o conteúdo parece válido
            valid: response.status === 200
        };
    } catch (error) {
        const duration = Date.now() - start;
        return {
            username: username,
            status: error.code || 'ERROR',
            duration: `${(duration / 1000).toFixed(3)}s`,
            valid: false
        };
    }
}

async function runBenchmark() {
    console.log(`🚀 Iniciando benchmark para ${usernames.length} usuários simultaneamente...`);
    console.log('--------------------------------------------------');

    const promises = usernames.map(user => checkUser(user));
    
    const startTotal = Date.now();
    const results = await Promise.all(promises);
    const totalTime = Date.now() - startTotal;

    console.table(results);
    
    const fs = require('fs');
    let output = "Username | Status | Duration | Valid\n";
    output += "---|---|---|---\n";
    results.forEach(r => {
        output += `${r.username} | ${r.status} | ${r.duration} | ${r.valid}\n`;
    });
    output += `\nTotal Time: ${(totalTime / 1000).toFixed(3)}s`;
    fs.writeFileSync('benchmark_result.txt', output);
    console.log('Results saved to benchmark_result.txt');
    
    console.log('--------------------------------------------------');
    console.log(`⏱️ Tempo total de execução: ${(totalTime / 1000).toFixed(3)}s`);
}

runBenchmark();
