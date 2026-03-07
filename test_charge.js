
async function testCharge() {
    const payload = {
        correlationID: 'test-' + Date.now(),
        value: 10, // 10 cents
        comment: 'Test Charge',
        customer: {
            name: 'Test User',
            phone: '11999999999'
        },
        additionalInfo: [
            { key: 'tipo_servico', value: 'mistos' },
            { key: 'categoria_servico', value: 'seguidores' },
            { key: 'quantidade', value: '50' },
            { key: 'pacote', value: '50 Seguidores - R$ 0,10' }
        ]
    };

    try {
        console.log('Sending payload:', JSON.stringify(payload, null, 2));
        const response = await fetch('http://localhost:3000/api/woovi/charge', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        
        const data = await response.json();
        console.log('Status:', response.status);
        console.log('Response:', JSON.stringify(data, null, 2));
    } catch (error) {
        console.error('Error:', error.message);
    }
}

testCharge();
