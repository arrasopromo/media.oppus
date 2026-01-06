const axios = require('axios');

const usernames = [
    'instagram', 'cristiano', 'leomessi', 'neymarjr', 'anitta',
    'beyonce', 'taylorswift', 'justinbieber', 'kimkardashian', 'kyliejenner'
];

const API_URL = 'http://localhost:3000/api/check-instagram-profile';

async function runBenchmark() {
    console.log(`🚀 Iniciando benchmark com ${usernames.length} requisições simultâneas...`);
    const startTotal = Date.now();

    const promises = usernames.map(async (username) => {
        const start = Date.now();
        try {
            const response = await axios.post(API_URL, { username }, {
                timeout: 60000 // 60s timeout for client
            });
            const duration = (Date.now() - start) / 1000;
            console.log(`✅ [${username}] Sucesso em ${duration.toFixed(2)}s - Status: ${response.status}`);
            return { username, success: true, duration };
        } catch (error) {
            const duration = (Date.now() - start) / 1000;
            console.log(`❌ [${username}] Falha em ${duration.toFixed(2)}s - Erro: ${error.message}`);
            return { username, success: false, duration, error: error.message };
        }
    });

    const results = await Promise.all(promises);
    const endTotal = Date.now();
    const totalDuration = (endTotal - startTotal) / 1000;

    const successCount = results.filter(r => r.success).length;
    const avgTime = results.reduce((acc, r) => acc + r.duration, 0) / results.length;

    console.log('\n📊 Resultados do Benchmark:');
    console.log(`Total de Requisições: ${results.length}`);
    console.log(`Sucessos: ${successCount}`);
    console.log(`Falhas: ${results.length - successCount}`);
    console.log(`Tempo Total (parede): ${totalDuration.toFixed(2)}s`);
    console.log(`Tempo Médio por Requisição: ${avgTime.toFixed(2)}s`);
}

runBenchmark();
