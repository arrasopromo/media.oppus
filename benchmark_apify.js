const axios = require('axios');

const usernames = [
    'patricia.silva0110', 'aylag2014_', 'collection_3d_', 'fernandobatistasanto', 
    'portalmulheramazonica', 'Dahoramix_store', 'JosueAntonio_pantaneiro', 'adv_patriciaparma', 
    'Ogaladoforrooficial', 'Kauavictor_9', 'Glayc_', 'jooannakrum', 
    'patricia.silva0110', 'jrexpertisecontabil', 'psicologo.valdir', 'aline_lelles', 
    'draanapaulamilano', 'ForestInktattoo', 'inconsciente_organizacional', 'emerson.ooliveira', 
    'Vinnysartori', 'Jevanesportes', 'achadosdatatai', 'cosmaalvesofc'
];

const API_URL = 'http://localhost:3000/api/check-instagram-profile';

async function runBenchmark() {
    const BATCH_SIZE = 5; // Limite de concorrência para evitar erro 402 do Apify
    console.log(`🚀 Iniciando benchmark com ${usernames.length} requisições (lotes de ${BATCH_SIZE})...`);
    const startTotal = Date.now();

    const results = [];
    
    // Processar em lotes
    for (let i = 0; i < usernames.length; i += BATCH_SIZE) {
        const batch = usernames.slice(i, i + BATCH_SIZE);
        console.log(`\n🔄 Processando lote ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(usernames.length / BATCH_SIZE)} (${batch.length} perfis)...`);
        
        const batchPromises = batch.map(async (username) => {
            const start = Date.now();
            try {
                const response = await axios.post(API_URL, { username, bypassCache: true }, {
                    timeout: 60000 // 60s timeout for client
                });
                const duration = (Date.now() - start) / 1000;
                return { username, status: response.status, duration: duration.toFixed(2), success: true };
            } catch (error) {
                const duration = (Date.now() - start) / 1000;
                const errorDetails = error.response && error.response.data ? (error.response.data.details || error.response.data.error) : error.message;
                return { username, status: error.response ? error.response.status : 'ERR', duration: duration.toFixed(2), success: false, error: errorDetails };
            }
        });

        const batchResults = await Promise.all(batchPromises);
        results.push(...batchResults);
    }

    const endTotal = Date.now();
    const totalDuration = (endTotal - startTotal) / 1000;

    console.log('\n📊 Tabela de Resultados:');
    console.table(results.map(r => ({
        Username: r.username,
        'Tempo (s)': r.duration,
        Status: r.status,
        Resultado: r.success ? '✅ Sucesso' : '❌ Falha',
        Erro: r.error || '-'
    })));

    console.log('\n📈 Resumo:');
    console.log(`Tempo Total (parede): ${totalDuration.toFixed(2)}s`);
    const avgTime = results.reduce((acc, r) => acc + parseFloat(r.duration), 0) / results.length;
    console.log(`Tempo Médio por Requisição: ${avgTime.toFixed(2)}s`);
}

runBenchmark();
