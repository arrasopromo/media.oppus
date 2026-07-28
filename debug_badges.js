
const quantityBadges = {
    50: 'PACOTE TESTE',
    150: 'PACOTE INICIAL',
    500: 'PACOTE BÁSICO',
    1000: 'MAIS PEDIDO',
    3000: 'EXCLUSIVO',
    5000: 'VIP',
    10000: 'ELITE'
};

const tabelaCurtidas = {
    mistos: [
      { q: 150, p: 'R$ 5,90' },
      { q: 500, p: 'R$ 9,90' },
      { q: 1000, p: 'R$ 19,90' },
      { q: 3000, p: 'R$ 29,90' },
      { q: 5000, p: 'R$ 39,90' },
      { q: 10000, p: 'R$ 69,90' },
    ]
};

const servicesConfig = [
      { 
        id: 'section-likes',
        service: 'likes', 
        type: 'mistos', 
        title: 'Curtidas Mundiais', 
        tabela: tabelaCurtidas.mistos,
        badgeType: 'mistos'
      }
];

const availableUpgrades = {};

servicesConfig.forEach(config => {
    console.log(`Processing ${config.service} ${config.type}`);
    
    if (!availableUpgrades[config.service]) availableUpgrades[config.service] = {};
    if (!availableUpgrades[config.service][config.type]) availableUpgrades[config.service][config.type] = [];

    config.tabela.forEach(plano => {
        const qNum = Number(plano.q);
        let badgeText = '';
        let isGold = false;

        // --- Badge Logic (Igual servicos-instagram) ---
        if (config.badgeType === 'mistos') {
            if (config.service === 'likes') {
                if (qNum === 150) badgeText = 'PACOTE INICIAL';
                if (qNum === 500) badgeText = 'PACOTE BÁSICO';
                if (qNum === 1000) badgeText = 'MELHOR PREÇO';
                if (qNum === 3000) { badgeText = 'MAIS PEDIDO'; isGold = true; }
                if (qNum === 5000) badgeText = 'VIP';
                if (qNum === 10000) badgeText = 'ELITE';
            } else {
                // Followers mistos
                if (qNum === 1000) badgeText = 'MELHOR PREÇO';
                if (qNum === 3000) { badgeText = 'MAIS PEDIDO'; isGold = true; }
            }
        }

        // Only apply generic badges for followers
        if (!badgeText && config.service === 'followers' && quantityBadges[qNum]) {
            badgeText = quantityBadges[qNum];
        }

        console.log(`Qty: ${qNum}, Badge: "${badgeText}"`);

        if (!badgeText) {
             availableUpgrades[config.service][config.type].push({ ...plano });
             return;
        }
        
        console.log(`-> DISPLAY CARD: ${qNum}`);
    });
});

console.log('Upgrades:', JSON.stringify(availableUpgrades, null, 2));
