const axios = require('axios');

const profiles = [
    'marcos', 
    'caio', 
    'junior', 
    'preto', 
    'azul', 
    'Dsrssilv', 
    'Romildo_rs7', 
    'mariaantonia_advogada', 
    'Everaldo.rosa.12', 
    'Josuedls7', 
    'ide_africa_mocambique', 
    'apnhoficial', 
    'donizetimoreira.88', 
    'Christianterapeutasistemica', 
    'zeladestrador', 
    'jorois', 
    'Ta.logisticaoficial', 
    'elietecontadora',
    'marcos', // Duplicate 1
    'caio'    // Duplicate 2
];

const URL = 'http://localhost:3000/api/check-instagram-profile';

async function checkProfile(username) {
    const start = Date.now();
    try {
        const response = await axios.post(URL, { username }, { timeout: 60000 }); // 60s timeout just in case
        const duration = Date.now() - start;
        return {
            username,
            status: response.status,
            success: response.data.success,
            duration,
            error: null
        };
    } catch (error) {
        const duration = Date.now() - start;
        return {
            username,
            status: error.response ? error.response.status : 'ERR',
            success: false,
            duration,
            error: error.message
        };
    }
}

async function runBenchmark() {
    console.log(`🚀 Iniciando benchmark com ${profiles.length} requisições simultâneas...`);
    console.log(`🎯 Endpoint: ${URL}`);
    
    const startTime = Date.now();
    const promises = profiles.map(p => checkProfile(p));
    const results = await Promise.all(promises);
    const totalBenchmarkTime = Date.now() - startTime;

    console.log('\n📊 Resultados do Benchmark:');
    console.table(results.map(r => ({
        Username: r.username,
        Status: r.status,
        Success: r.success ? '✅ OK' : '❌ FAIL',
        'Time (ms)': r.duration,
        Error: r.error ? r.error.substring(0, 30) : ''
    })));

    const successCount = results.filter(r => r.success).length;
    const avgTime = results.reduce((acc, r) => acc + r.duration, 0) / results.length;

    console.log('\n📈 Resumo:');
    console.log(`Total de Requisições: ${results.length}`);
    console.log(`Sucessos: ${successCount}`);
    console.log(`Falhas: ${results.length - successCount}`);
    console.log(`Tempo Médio de Resposta: ${avgTime.toFixed(2)} ms`);
    console.log(`Tempo Máximo: ${Math.max(...results.map(r => r.duration))} ms`);
    console.log(`Tempo Mínimo: ${Math.min(...results.map(r => r.duration))} ms`);
    console.log(`Tempo Total do Benchmark: ${totalBenchmarkTime} ms`);
}

runBenchmark();
