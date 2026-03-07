const axios = require('axios');

async function testWooviCharge() {
    const payload = {
        correlationID: "test_" + Date.now(),
        value: 10, // 10 cents
        comment: "Checkout Engajamento Novo",
        customer: {
            name: "Cliente Teste",
            phone: "11999999999",
            email: "teste@example.com"
        },
        additionalInfo: [
            { key: "tipo_servico", value: "mistos" },
            { key: "categoria_servico", value: "seguidores" },
            { key: "quantidade", value: "50" },
            { key: "pacote", value: "50 Seguidores - R$ 0,10" },
            { key: "phone", value: "11999999999" },
            { key: "instagram_username", value: "teste_user" },
            { key: "order_bumps_total", value: "R$ 0,00" },
            { key: "order_bumps", value: "" },
            { key: "cupom", value: "" }
        ],
        profile_is_private: false
    };

    console.log('Sending payload:', JSON.stringify(payload, null, 2));

    try {
        const response = await axios.post('http://localhost:3000/api/woovi/charge', payload);
        
        console.log('Response Status:', response.status);
        console.log('Response Data:', JSON.stringify(response.data, null, 2));
    } catch (error) {
        if (error.response) {
            console.error('Response Status:', error.response.status);
            console.error('Response Data:', JSON.stringify(error.response.data, null, 2));
        } else {
            console.error('Error:', error.message);
        }
    }
}

testWooviCharge();
