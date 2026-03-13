const { calculatePrice, verifyPrice } = require('./pricing');

(async () => {
    const additionalInfo = [
        { key: 'categoria_servico', value: 'seguidores' }
    ];

    console.log('--- Test: Calculate Price (mistos, 500) ---');
    const price = await calculatePrice('mistos', 500, additionalInfo);
    console.log(`price_cents=${price}`);

    console.log('--- Test: Verify Price (mistos, 500, 1990) ---');
    const verification = await verifyPrice('mistos', 500, additionalInfo, 1990);
    console.log(JSON.stringify(verification, null, 2));
    process.exit(0);
})().catch((e) => {
    console.error(e);
    process.exit(1);
});
