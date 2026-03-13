
const { getCollection } = require('./mongodbClient');

const parsePrecoToCents = (precoStr) => {
    if (!precoStr) return 0;
    const cleaned = precoStr.replace(/[^\d,]/g, '').replace(',', '.');
    const value = Math.round(parseFloat(cleaned) * 100);
    return isNaN(value) ? 0 : value;
};

// Merged tables from servicos-instagram.js, engajamento-novo.js and checkout.js
const tabelaSeguidores = {
    mistos: [
        { q: 150, p: 'R$ 7,90' },
        { q: 300, p: 'R$ 12,90' },
        { q: 500, p: 'R$ 19,90' },
        { q: 700, p: 'R$ 24,90' },
        { q: 1000, p: 'R$ 29,90' },
        { q: 2000, p: 'R$ 54,90' },
        { q: 3000, p: 'R$ 89,90' },
        { q: 4000, p: 'R$ 109,90' },
        { q: 5000, p: 'R$ 129,90' },
        { q: 7500, p: 'R$ 169,90' },
        { q: 10000, p: 'R$ 199,90' },
        { q: 15000, p: 'R$ 329,90' },
    ],
    brasileiros: [
        { q: 150, p: 'R$ 12,90' },
        { q: 300, p: 'R$ 24,90' },
        { q: 500, p: 'R$ 39,90' },
        { q: 700, p: 'R$ 49,90' },
        { q: 1000, p: 'R$ 79,90' },
        { q: 2000, p: 'R$ 129,90' },
        { q: 3000, p: 'R$ 179,90' },
        { q: 4000, p: 'R$ 249,90' },
        { q: 5000, p: 'R$ 279,90' },
        { q: 7500, p: 'R$ 399,90' },
        { q: 10000, p: 'R$ 499,90' },
        { q: 15000, p: 'R$ 799,90' },
    ],
    organicos: [
        { q: 150, p: 'R$ 39,90' },
        { q: 300, p: 'R$ 49,90' },
        { q: 500, p: 'R$ 69,90' },
        { q: 700, p: 'R$ 89,90' },
        { q: 1000, p: 'R$ 129,90' },
        { q: 2000, p: 'R$ 199,90' },
        { q: 3000, p: 'R$ 249,90' },
        { q: 4000, p: 'R$ 329,90' },
        { q: 5000, p: 'R$ 499,90' },
        { q: 7500, p: 'R$ 599,90' },
        { q: 10000, p: 'R$ 899,90' },
        { q: 15000, p: 'R$ 1.299,90' },
    ],
};

const tabelaCurtidas = {
    mistos: [
        { q: 150, p: 'R$ 5,90' },
        { q: 300, p: 'R$ 7,90' },
        { q: 500, p: 'R$ 9,90' },
        { q: 700, p: 'R$ 14,90' },
        { q: 1000, p: 'R$ 19,90' },
        { q: 2000, p: 'R$ 24,90' },
        { q: 3000, p: 'R$ 29,90' },
        { q: 4000, p: 'R$ 34,90' },
        { q: 5000, p: 'R$ 39,90' },
        { q: 7500, p: 'R$ 49,90' },
        { q: 10000, p: 'R$ 69,90' },
        { q: 15000, p: 'R$ 89,90' },
    ],
    brasileiros: tabelaSeguidores.brasileiros, // Reused in frontend
    organicos: [
        { q: 150, p: 'R$ 4,90' },
        { q: 300, p: 'R$ 9,90' },
        { q: 500, p: 'R$ 14,90' },
        { q: 700, p: 'R$ 29,90' },
        { q: 1000, p: 'R$ 39,90' },
        { q: 2000, p: 'R$ 49,90' },
        { q: 3000, p: 'R$ 59,90' },
        { q: 4000, p: 'R$ 69,90' },
        { q: 5000, p: 'R$ 79,90' },
        { q: 7500, p: 'R$ 109,90' },
        { q: 10000, p: 'R$ 139,90' },
        { q: 15000, p: 'R$ 199,90' },
    ],
    curtidas_reais: [
        { q: 150, p: 'R$ 4,90' },
        { q: 300, p: 'R$ 9,90' },
        { q: 500, p: 'R$ 14,90' },
        { q: 700, p: 'R$ 19,90' },
        { q: 1000, p: 'R$ 24,90' },
        { q: 2000, p: 'R$ 34,90' },
        { q: 3000, p: 'R$ 49,90' },
        { q: 4000, p: 'R$ 59,90' },
        { q: 5000, p: 'R$ 69,90' },
        { q: 7500, p: 'R$ 89,90' },
        { q: 10000, p: 'R$ 109,90' },
        { q: 15000, p: 'R$ 159,90' },
    ],
};

const tabelaVisualizacoes = {
    visualizacoes_reels: [
        { q: 1000, p: 'R$ 4,90' },
        { q: 2500, p: 'R$ 9,90' },
        { q: 5000, p: 'R$ 14,90' },
        { q: 10000, p: 'R$ 19,90' },
        { q: 25000, p: 'R$ 24,90' },
        { q: 50000, p: 'R$ 34,90' },
        { q: 100000, p: 'R$ 49,90' },
        { q: 150000, p: 'R$ 59,90' },
        { q: 200000, p: 'R$ 69,90' },
        { q: 250000, p: 'R$ 89,90' },
        { q: 500000, p: 'R$ 109,90' },
        { q: 1000000, p: 'R$ 159,90' }
    ]
};

// Order Bumps / Upsells logic
const calculateOrderBumps = (bumpsStr) => {
    if (!bumpsStr) return 0;
    let total = 0;
    const bumps = bumpsStr.split(';'); // "likes:150;views:1000"
    
    bumps.forEach(bump => {
        const [key, qtyStr] = bump.split(':');
        const q = parseInt(qtyStr, 10) || 1;
        
        if (key === 'likes') {
            // Reutiliza tabela de curtidas_reais (preço promocional usado no upsell do frontend)
            const table = tabelaCurtidas.curtidas_reais;
            const item = table.find(x => x.q === q);
            if (item) total += parsePrecoToCents(item.p);
            
        } else if (key === 'views') {
            const item = tabelaVisualizacoes.visualizacoes_reels.find(x => x.q === q);
            if (item) total += parsePrecoToCents(item.p);
            
        } else if (key === 'comments') {
            // 1 comment = R$ 1,50 (150 cents)
            total += q * 150;
            
        } else if (key === 'warranty' || key === 'warranty30') {
             // 30 days -> R$ 9,90
             total += 990; 
        } else if (key === 'warranty_life' || key === 'warranty_lifetime') {
             // Lifetime -> R$ 19,90
             total += 1990;
        } else if (key === 'warranty60') {
             // Lifetime Promo -> R$ 9,90
             total += 990;
        }
    });
    
    return total;
};

const calculatePrice = async (type, quantity, additionalInfo = []) => {
    let totalCents = 0;
    const q = parseInt(quantity, 10);
    
    // Extract category from additionalInfo if available
    let category = '';
    let isUpsell = false;
    let coupon = null;

    if (Array.isArray(additionalInfo)) {
        const catItem = additionalInfo.find(x => x.key === 'categoria_servico');
        if (catItem) category = String(catItem.value).toLowerCase();
        
        // Check for upsell flags
        const upsellItem = additionalInfo.find(x => 
            x.key === 'is_upsell' || 
            x.key === 'upsell_followers' ||
            x.key === 'upsell'
        );
        if (upsellItem && (upsellItem.value === true || upsellItem.value === 'true' || upsellItem.value === '1')) {
            isUpsell = true;
        }

        // Coupon Logic Placeholder
        const couponItem = additionalInfo.find(x => x.key === 'coupon' || x.key === 'cupom');
        if (couponItem && couponItem.value) {
            coupon = String(couponItem.value).trim().toUpperCase();
        }
    }

    // 1. Base Price
    let table = [];
    
    // Explicit category check to resolve ambiguity
    if (category === 'curtidas' && tabelaCurtidas[type]) {
        table = tabelaCurtidas[type];
    } else if (category === 'seguidores' && tabelaSeguidores[type]) {
        table = tabelaSeguidores[type];
    } else if (category === 'visualizacoes' && (type === 'visualizacoes_reels' || type === 'views' || type === 'reels')) {
        table = tabelaVisualizacoes.visualizacoes_reels;
    }
    // Fallback logic (if no category provided)
    else {
        if (type === 'visualizacoes_reels' || type === 'views' || type === 'reels') {
            table = tabelaVisualizacoes.visualizacoes_reels;
        } else if (tabelaSeguidores[type]) {
            // Se existir em seguidores E em curtidas (ambiguidade 'mistos'), prefere seguidores se não especificado
            // Mas idealmente o frontend DEVE mandar categoria.
            table = tabelaSeguidores[type];
        } else if (tabelaCurtidas[type]) {
            table = tabelaCurtidas[type];
        }
    }
    
    // Try to find exact quantity match
    const item = table.find(x => x.q === q);
    if (item) {
        let priceCents = parsePrecoToCents(item.p);
        
        // Apply Upsell Discount (25% OFF) if eligible
        // Conditions: Followers service, Quantity >= 500, isUpsell flag present
        // Verifica se a tabela usada é realmente de seguidores para evitar aplicar desconto em curtidas
        const isFollowersTable = Object.values(tabelaSeguidores).includes(table);
        
        if (isUpsell && q >= 500 && isFollowersTable) {
            priceCents = Math.round(priceCents * 0.75);
        }

        // --- COUPON LOGIC START ---
        if (coupon) {
            try {
                const col = await getCollection('coupons');
                const couponDoc = await col.findOne({ code: coupon, isActive: true });
                
                if (couponDoc) {
                    const discount = couponDoc.discountPercentage / 100;
                    priceCents = Math.round(priceCents * (1 - discount));
                }
            } catch (error) {
                console.error('Error fetching coupon:', error);
            }
        }
        // --- COUPON LOGIC END ---

        totalCents = priceCents;
    } else {
        // Price not found for quantity
    }
    
    // 2. Order Bumps
    // Handle both string format "likes:150;views:1000" and potential array format if changed later
    let bumpsStr = '';
    
    // Check if additionalInfo has 'order_bumps'
    const orderBumpsItem = additionalInfo.find(x => x.key === 'order_bumps');
    if (orderBumpsItem) {
        bumpsStr = orderBumpsItem.value;
    }
    
    if (bumpsStr) {
        totalCents += calculateOrderBumps(bumpsStr);

        // --- UPGRADE LOGIC START ---
        // Mirroring logic from servicos-instagram.js updateOrderBump
        if (/(^|;)upgrade:\d+/i.test(bumpsStr)) {
            const q = parseInt(quantity, 10);
            let targetQ = 0;
            
            if (type === 'visualizacoes_reels' || type === 'views' || type === 'reels') {
                 const map = {
                    1000: 2500,
                    5000: 10000,
                    25000: 50000,
                    100000: 150000,
                    200000: 250000,
                    500000: 1000000
                 };
                 targetQ = map[q];
            } else if ((type === 'brasileiros' || type === 'organicos') && q === 1000) {
                targetQ = 2000;
            } else {
                // Generic (works for followers and likes)
                const map = { 
                    150: 300, 300: 500, 500: 700, 700: 1000, 
                    1000: 2000, 1200: 2000, 2000: 3000, 3000: 4000, 4000: 5000, 
                    5000: 7500, 7500: 10000, 10000: 15000 
                };
                targetQ = map[q];
            }
            
            if (targetQ && table && Array.isArray(table)) {
                const baseItem = table.find(x => x.q === q);
                const targetItem = table.find(x => x.q === targetQ);
                
                if (baseItem && targetItem) {
                    const basePrice = parsePrecoToCents(baseItem.p);
                    const targetPrice = parsePrecoToCents(targetItem.p);
                    const upgradePrice = Math.max(0, targetPrice - basePrice);
                    totalCents += upgradePrice;
                }
            }
        }
        // --- UPGRADE LOGIC END ---
    }

    return totalCents;
};

const validatePrice = async (type, quantity, additionalInfo, valuePaid) => {
    const verification = await verifyPrice(type, quantity, additionalInfo, valuePaid);
    return verification.isValid;
};

const verifyPrice = async (type, quantity, additionalInfo, valuePaid) => {
    // Calculate standard price (with upsell logic applied if flag present)
    const calculatedPrice = await calculatePrice(type, quantity, additionalInfo);
    
    // Also try to calculate WITHOUT upsell flag, in case the user didn't send the flag but sent the discounted price (legacy/fallback)
    // Or vice versa
    
    // But for strict validation, we should trust the calculatedPrice based on input.
    // However, to be robust against frontend glitches, we might want to check both scenarios?
    // No, strict validation means: if you want discount, you MUST tell me it's an upsell.
    
    let isValid = false;
    let mismatchDetails = null;

    if (valuePaid === calculatedPrice) {
        isValid = true;
    } else {
        mismatchDetails = {
            expected: calculatedPrice,
            paid: valuePaid,
            diff: valuePaid - calculatedPrice
        };
    }

    return { isValid, matchedPrice: isValid ? calculatedPrice : null, expectedPrice: calculatedPrice, mismatchDetails };
};

module.exports = { calculatePrice, validatePrice, verifyPrice, parsePrecoToCents };
