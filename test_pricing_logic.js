const { calculatePrice, verifyPrice } = require('./pricing');

console.log('--- Test 1: Direct Calculation ---');
const additionalInfo = [
    { key: 'tipo_servico', value: 'mistos' },
    { key: 'categoria_servico', value: 'seguidores' },
    { key: 'quantidade', value: '50' },
    { key: 'pacote', value: '50 Seguidores - R$ 0,10' }
];
const price = calculatePrice('mistos', 50, additionalInfo);
console.log(`Price for 50 followers mistos: ${price} cents`);

console.log('\n--- Test 2: Verify Price ---');
const verification = verifyPrice('mistos', 50, additionalInfo, 10);
console.log('Verification result:', verification);

console.log('\n--- Test 3: Debug Table Access ---');
// Try to inspect what table it picks (we can't modify pricing.js easily but we can infer)
const priceWithoutCategory = calculatePrice('mistos', 50, [{ key: 'tipo_servico', value: 'mistos' }]);
console.log(`Price without category: ${priceWithoutCategory} cents`);
