const axios = require('axios');

async function testMainProfile(username) {
    // Tentar com parametro json
    const url = `https://www.instagram.com/${username}/?__a=1&__d=dis`;
    const headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json',
    };

    console.log(`Testando ${url}...`);
    try {
        const response = await axios.get(url, { headers, timeout: 10000 });
        const html = response.data;
        console.log('Status:', response.status);
        console.log('HTML length:', html.length);

        const keywords = ['username', 'profile_pic_url', 'edge_followed_by', 'edge_follow'];
        keywords.forEach(kw => {
            if (html.includes(kw)) {
                console.log(`[OK] Encontrado "${kw}"`);
            } else {
                console.log(`[FAIL] "${kw}" não encontrado`);
            }
        });

        // Tentar encontrar JSON
        const match = html.match(/<script type="application\/json"[^>]*>([\s\S]*?)<\/script>/g);
        if (match) {
            console.log(`Encontrados ${match.length} blocos de script JSON`);
        }

    } catch (error) {
        console.error('Erro:', error.message);
        if (error.response) {
            console.log('Status erro:', error.response.status);
        }
    }
}

testMainProfile('neymarjr');
