(() => {
  /* try { if (typeof fbq === 'function' && window._oppusPixelReady) fbq('track', 'PageView'); } catch(e) {} */
  // Helper to get persistent browser/session ID
  function getBrowserSessionId() {
      let bid = '';
      try { bid = localStorage.getItem('oppus_browser_id'); } catch(_) {}
      if (!bid) {
          bid = 'bid_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
          try { localStorage.setItem('oppus_browser_id', bid); } catch(_) {}
      }
      return bid;
  }
  // Initialize browser ID immediately
  try { getBrowserSessionId(); } catch(_) {}

  // Funções para gerar CPF válido e cache
  function generateValidCPF() {
    // Gera os 9 primeiros dígitos aleatórios
    const cpf = Array.from({length: 9}, () => Math.floor(Math.random() * 10));
    
    // Calcula o primeiro dígito verificador
    let sum = 0;
    for (let i = 0; i < 9; i++) {
      sum += cpf[i] * (10 - i);
    }
    let firstDigit = 11 - (sum % 11);
    if (firstDigit >= 10) firstDigit = 0;
    cpf.push(firstDigit);
    
    // Calcula o segundo dígito verificador
    sum = 0;
    for (let i = 0; i < 10; i++) {
      sum += cpf[i] * (11 - i);
    }
    let secondDigit = 11 - (sum % 11);
    if (secondDigit >= 10) secondDigit = 0;
    cpf.push(secondDigit);
    
    // Formata o CPF (000.000.000-00)
    return cpf.join('').replace(/(\d{3})(\d{3})(\d{3})(\d{2})/, '$1.$2.$3-$4');
  }

  function getUserFingerprint() {
    try {
      // Cria fingerprint baseado em dados do navegador e sessão
      const browserId = getBrowserSessionId();
      const userAgent = navigator.userAgent || '';
      const screen = `${window.screen.width}x${window.screen.height}`;
      const timezone = Intl.DateTimeFormat().resolvedOptions().timeZone || '';
      
      // Pega dados do formulário se disponíveis
      const nome = document.getElementById('contactNameInput')?.value || '';
      const telefone = document.getElementById('contactPhoneInput')?.value || '';
      const email = document.getElementById('contactEmailInput')?.value || '';
      
      // Cria hash único baseado nos dados
      const fingerprintData = `${browserId}|${nome}|${telefone}|${email}|${userAgent}|${screen}|${timezone}`;
      return btoa(fingerprintData).slice(0, 32); // Limita o tamanho
    } catch (e) {
      return getBrowserSessionId(); // Fallback para browser ID
    }
  }

  function getCachedCPF() {
    try {
      const fingerprint = getUserFingerprint();
      const cached = localStorage.getItem(`cpf_cache_${fingerprint}`);
      
      if (cached) {
        const data = JSON.parse(cached);
        // Verifica se o cache é válido por 24 horas
        if (Date.now() - data.timestamp < 24 * 60 * 60 * 1000) {
          return data.cpf;
        }
      }
      return null;
    } catch (e) {
      return null;
    }
  }

  function cacheCPF(cpf) {
    try {
      const fingerprint = getUserFingerprint();
      const data = {
        cpf: cpf,
        timestamp: Date.now()
      };
      localStorage.setItem(`cpf_cache_${fingerprint}`, JSON.stringify(data));
    } catch (e) {
      // Ignora erro de cache
    }
  }

  function getOrGenerateCPF() {
    // Tenta pegar do cache primeiro
    const cachedCPF = getCachedCPF();
    if (cachedCPF) {
      return cachedCPF;
    }
    
    // Gera novo CPF
    const newCPF = generateValidCPF();
    
    // Cacheia o novo CPF
    cacheCPF(newCPF);
    
    return newCPF;
  }

  const tipoSelect = document.getElementById('tipoSelect');
  const qtdSelect = document.getElementById('quantidadeSelect');
  const tipoCards = document.getElementById('tipoCards');
  const planCards = document.getElementById('planCards');
  const resumo = document.getElementById('resumo');
  const resTipo = document.getElementById('resTipo');
  const resQtd = document.getElementById('resQtd');
  const resPreco = document.getElementById('resPreco');
  const btnPedido = document.getElementById('realizarPedidoBtn');
  const pixResultado = document.getElementById('pixResultado');
  const pixGatewaySelect = document.getElementById('pixGatewaySelect');
  const btnInstagram = document.querySelector('.platform-btn.instagram');
  const btnTikTok = document.querySelector('.platform-btn.tiktok');
  let selectedPlatform = (btnInstagram && btnInstagram.getAttribute('aria-pressed') === 'true') ? 'instagram' : 'tiktok';
  // Use window.basePriceCents if defined (integration with engajamento-novo.js), otherwise local
  let localBasePriceCents = 0;
  
  // Expose functions for external control
  window.updatePaymentMethodVisibility = updatePaymentMethodVisibility;
  window.populateInstallments = populateInstallments;
  window.selectPaymentMethod = selectPaymentMethod;
  window.calculateTotalCents = calculateTotalCents;
  window.calculateSubtotalCents = calculateSubtotalCents;
  window.showTutorialStep = showTutorialStep;

  let paymentPollInterval = null;
  let paymentEventSource = null;

  // Lógica de Pagamento (Cartão vs Pix)
  let currentPaymentMethod = 'pix'; // 'pix' or 'credit_card'
  let currentPixGateway = 'woovi';
  const updatePixCpfVisibility = () => {
    try {
      const wrap = document.getElementById('pixCpfWrap');
      if (!wrap) return;
      const labelEl = wrap.querySelector('label[for="pixCpfInput"]');
      const hintEl = wrap.querySelector('.phone-hint');
      const cpfInput = document.getElementById('pixCpfInput');
      const isPix = String(currentPaymentMethod || 'pix') === 'pix';
      const selectedPixGateway = String((pixGatewaySelect && pixGatewaySelect.value) ? pixGatewaySelect.value : 'woovi').trim().toLowerCase() || 'woovi';
      const gw = (selectedPixGateway === 'expay' || selectedPixGateway === 'paghiper') ? selectedPixGateway : 'woovi';
      const shouldShow = isPix && (gw === 'expay' || gw === 'paghiper');
      const shouldHideByDefaultCpf = isPix && gw === 'expay' && !!(window && window.__EXPAY_DEFAULT_CPF_ENABLED);
      
      // Para PagHiPer, sempre oculta o campo mas mantém preenchido
      if (gw === 'paghiper') {
        wrap.style.display = 'none'; // Sempre oculto para PagHiPer
        if (cpfInput) {
          const autoCPF = getOrGenerateCPF();
          cpfInput.value = autoCPF;
          cpfInput.readOnly = true;
        }
      } else {
        // Para outros gateways, mantém a lógica original
        wrap.style.display = (!shouldShow || shouldHideByDefaultCpf) ? 'none' : '';
        if (cpfInput && gw !== 'paghiper') {
          cpfInput.readOnly = false; // Remove readonly se não for PagHiPer
        }
      }
      
      if (labelEl) labelEl.textContent = (gw === 'paghiper') ? 'CPF (para Pix PagHiper)' : (gw === 'expay') ? 'CPF (para Pix ExPay)' : 'CPF (para Pix)';
      if (hintEl) hintEl.textContent = (gw === 'paghiper') ? 'A PagHiper exige CPF do pagador.' : (gw === 'expay') ? 'A ExPay exige CPF do pagador.' : 'Alguns gateways de Pix exigem CPF do pagador.';
    } catch (_) {}
  };
  try {
    if (pixGatewaySelect) {
      pixGatewaySelect.addEventListener('change', () => {
        try {
          const selectedPixGateway = String((pixGatewaySelect && pixGatewaySelect.value) ? pixGatewaySelect.value : 'woovi').trim().toLowerCase() || 'woovi';
          currentPixGateway = (selectedPixGateway === 'expay' || selectedPixGateway === 'paghiper') ? selectedPixGateway : 'woovi';
          window.currentPixGateway = currentPixGateway;
        } catch (_) {}
        try { updatePixCpfVisibility(); } catch (_) {}
        
        // Se mudou para PagHiPer, gera/atualiza CPF automaticamente
        if (currentPixGateway === 'paghiper' && String(currentPaymentMethod || 'pix') === 'pix') {
          const cpfInput = document.getElementById('pixCpfInput');
          if (cpfInput && !cpfInput.value) {
            const autoCPF = getOrGenerateCPF();
            cpfInput.value = autoCPF;
            cpfInput.readOnly = true;
          }
        }
      });
    }
  } catch (_) {}
  try { updatePixCpfVisibility(); } catch (_) {}
  
  // Upsell Logic State
  let upsellShown = false;
  let upsellAccepted = false;
  let pendingPaymentFn = null;
  window.upsellPriceCents = 2990; // Default fallback
  window.upsellQty = 1000;

  function getServiceCategory() {
    if (window.__ENG_MODE__) return 'seguidores';
    try {
      const path = window.location && window.location.pathname ? window.location.pathname : '';
      if (path.indexOf('/servicos-curtidas') === 0) return 'curtidas';
      if (path.indexOf('/servicos-visualizacoes') === 0) return 'visualizacoes';
    } catch(_) {}
    return 'seguidores';
  }

  function updateUpsellModalContent() {
      const tipo = tipoSelect ? tipoSelect.value : 'mistos';
      const elQty = document.getElementById('upsellQty');
      const elPrice = document.getElementById('upsellPrice');
      
      let offerQty = 1000;
      let offerPriceCents = 2990;
      
      // Tenta calcular 25% OFF baseado no preço de 1000 da categoria atual
      try {
          // Se for TikTok, usa tabela mistos (mapeado na linha 414: tabela.seguidores_tiktok = tabela.mistos)
          // Mas findPrice usa tabela[tipo]
          const price1kStr = findPrice(tipo, 1000);
          if (price1kStr) {
              const baseCents = parsePrecoToCents(price1kStr);
              offerPriceCents = Math.round(baseCents * 0.75);
          }
      } catch(e) { console.error('Upsell price calc error:', e); }

      window.upsellPriceCents = offerPriceCents;
      window.upsellQty = offerQty;
      
      const offerPriceStr = formatCentsToBRL(offerPriceCents);
      
      if (elQty) elQty.textContent = offerQty;
      if (elPrice) elPrice.textContent = offerPriceStr;
  }

  function showUpsellModal(callback) {
      const modal = document.getElementById('upsellModal');
      if (!modal) {
          if (callback) callback();
          return;
      }
      updateUpsellModalContent();
      pendingPaymentFn = callback;
      modal.style.display = 'flex';
  }

  const handleUpsellDecision = async (accepted) => {
      upsellAccepted = accepted;
      upsellShown = true;
      const modal = document.getElementById('upsellModal');
      if (modal) modal.style.display = 'none';
      
      // Update total immediately
      try { updatePromosSummary(); } catch(e) { console.error(e); }
      
      // Proceed with payment
      if (pendingPaymentFn) {
          const fn = pendingPaymentFn;
          pendingPaymentFn = null; // Prevent double call
          await fn();
      }
  };

  // Setup upsell buttons safely
  function setupUpsellListeners() {
    const btnUpsellYes = document.getElementById('btnUpsellYes');
    const btnUpsellNo = document.getElementById('btnUpsellNo');
    const upsellClose = document.getElementById('upsellClose');
    
    if (btnUpsellYes) {
        btnUpsellYes.onclick = () => handleUpsellDecision(true);
    }
    if (btnUpsellNo) {
        btnUpsellNo.onclick = () => handleUpsellDecision(false);
    }
    if (upsellClose) {
        upsellClose.onclick = () => handleUpsellDecision(false);
    }
  }

  // Configura listeners para campos de contato para atualizar CPF quando dados mudarem
  function setupContactFieldListeners() {
    const nameInput = document.getElementById('contactNameInput');
    const phoneInput = document.getElementById('contactPhoneInput');
    const emailInput = document.getElementById('contactEmailInput');
    
    const updateCPFFromContactFields = () => {
      // Apenas atualiza se for PagHiPer
      const selectedPixGateway = String((pixGatewaySelect && pixGatewaySelect.value) ? pixGatewaySelect.value : 'woovi').trim().toLowerCase() || 'woovi';
      const isPagHiPer = selectedPixGateway === 'paghiper';
      const isPix = String(currentPaymentMethod || 'pix') === 'pix';
      
      if (isPix && isPagHiPer) {
        // Limpa o cache antigo para gerar novo CPF com base nos novos dados
        try {
          const fingerprint = getUserFingerprint();
          localStorage.removeItem(`cpf_cache_${fingerprint}`);
        } catch (e) {
          // Ignora erro
        }
        
        // Atualiza o campo CPF
        const cpfInput = document.getElementById('pixCpfInput');
        if (cpfInput) {
          const newCPF = getOrGenerateCPF();
          cpfInput.value = newCPF;
        }
      }
    };
    
    if (nameInput) nameInput.addEventListener('blur', updateCPFFromContactFields);
    if (phoneInput) phoneInput.addEventListener('blur', updateCPFFromContactFields);
    if (emailInput) emailInput.addEventListener('blur', updateCPFFromContactFields);
  }

  // Try immediately in case script is at bottom
  setupUpsellListeners();
  setupContactFieldListeners();
  // And also on DOMContentLoaded just in case
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
      setupUpsellListeners();
      setupContactFieldListeners();
    });
  }


  function getPagarmeInstallmentConfig() {
    const max = Math.max(1, Math.min(18, Number(window.PAGARME_INSTALLMENTS_MAX || 12) || 12));
    // Política: limitar parcelas por ticket (subtotal, sem acréscimo)
    function capBySubtotal(subtotal) {
      const n = Number(subtotal) || 0;
      if (n < 1500) return 1;        // < R$15
      if (n < 3000) return 2;        // R$15–29,99
      if (n < 6000) return 6;        // R$30–59,99
      if (n < 10000) return 8;       // R$60–99,99
      if (n < 15000) return 10;      // R$100–149,99
      return 12;                     // >= R$150
    }
    const subtotal = calculateSubtotalCents();
    const policyCap = capBySubtotal(subtotal);
    return { max: Math.min(max, policyCap) };
  }

  function getSelectedInstallments() {
    try {
      const el = document.getElementById('cardInstallments');
      const v = String(el && el.value ? el.value : '').trim();
      const n = parseInt(v || '1', 10);
      return Number.isFinite(n) && n > 0 ? n : 1;
    } catch (_) {
      return 1;
    }
  }

  function calculateSubtotalCents() {
    let promosTotal = 0;
    try { promosTotal = calcPromosTotalCents(getSelectedPromos()); } catch(_) {}
    
    let price = window.basePriceCents !== undefined ? window.basePriceCents : localBasePriceCents;

    if (!price) {
        try {
            const qs = document.getElementById('quantidadeSelect');
            if (qs) {
                const opt = qs.options[qs.selectedIndex];
                const p = opt ? opt.dataset.preco : '';
                if (p) {
                    price = parsePrecoToCents(p);
                    if (window.basePriceCents !== undefined) window.basePriceCents = price;
                    else localBasePriceCents = price;
                }
            }
        } catch(_) {}
    }

    const discount = (function () {
      try {
        const code = String(window.couponCode || '').trim();
        const d = Number(window.couponDiscount || 0);
        if (!code) return 0;
        if (!Number.isFinite(d) || d <= 0 || d >= 1) return 0;
        return d;
      } catch (_) {
        return 0;
      }
    })();

    let base = Number(price || 0);
    if (discount > 0) {
      base = Math.max(0, Math.round(base * (1 - discount)));
    }

    return Math.max(0, Number(base) + Number(promosTotal));
  }

  // Tabela de acréscimo por parcelas (cartão)
  function cardSurchargeRate(inst) {
    const table = {
      1: 4.97, 2: 6.33, 3: 7.24, 4: 8.14, 5: 9.05, 6: 9.95,
      7: 11.10, 8: 12.00, 9: 12.91, 10: 13.81, 11: 14.71, 12: 15.62
    };
    const keys = Object.keys(table).map(k => parseInt(k,10)).sort((a,b)=>a-b);
    const maxKey = keys[keys.length-1] || 12;
    const k = Math.max(1, Math.min(maxKey, Number(inst) || 1));
    return Number(table[k] || 0);
  }

  function calculateTotalCents() {
    const subtotal = calculateSubtotalCents();
    try {
      const method = String(window.currentPaymentMethod || '').trim();
      if (method !== 'credit_card') return subtotal;
      const cfg = getPagarmeInstallmentConfig();
      const inst = Math.max(1, Math.min(cfg.max, getSelectedInstallments()));
      const rate = cardSurchargeRate(inst);
      return Math.round(Number(subtotal) * (1 + Math.max(0, rate)/100));
    } catch(_) {
      return subtotal;
    }
  }

  function populateInstallments(subtotalCents) {
    const select = document.getElementById('cardInstallments');
    if (!select) return;
    
    const prevValue = String(select.value || '').trim();
    select.innerHTML = '';
    
    const minInstallment = 500;
    const maxInstallments = getPagarmeInstallmentConfig().max;

    // Adiciona opção padrão placeholder
    const defaultOption = document.createElement('option');
    defaultOption.value = "";
    defaultOption.disabled = true;
    defaultOption.selected = true;
    defaultOption.textContent = "Selecione as parcelas...";
    select.appendChild(defaultOption);

    if (!subtotalCents || subtotalCents <= 0) return;
    const base = Math.round(Number(subtotalCents) || 0);

    for (let i = 1; i <= maxInstallments; i++) {
      const rate = cardSurchargeRate(i);
      const totalForI = Math.round(base * (1 + Math.max(0, rate)/100));
      const installmentValue = Math.floor(totalForI / i);
      
      // Regra: Parcela mínima de R$ 5,00
      if (installmentValue < minInstallment && i > 1) break;
      
      const option = document.createElement('option');
      option.value = i;
      const labelExtra = i === 1 ? 'à vista no crédito' : 'com juros';
      option.textContent = `${i}x de ${formatCentsToBRL(installmentValue)} (${labelExtra})`;
      select.appendChild(option);
    }
    
    try {
      if (prevValue) {
        const exists = Array.from(select.options).some(o => String(o.value) === prevValue);
        if (exists) select.value = prevValue;
      }
    } catch (_) {}

    try {
      select.onchange = () => {
        try { updatePromosSummary(); } catch(_) {}
      };
    } catch(_) {}
  }

  function updatePaymentMethodVisibility() {
    const total = calculateTotalCents();
    const selector = document.getElementById('paymentMethodSelector');
    
    const publicKey = String(window.PAGARME_PUBLIC_KEY || '').trim();
    const isPublicKeyValid = publicKey && publicKey !== 'pk_change_me' && publicKey.length > 8 && /^pk_/i.test(publicKey);

    if (selector) {
      // Exibir para pedidos acima de R$ 1,00 (100 centavos)
      if (total >= 100) {
        if (selector.style.display !== 'flex') {
            selector.style.display = 'flex';
        }
        
        let warningEl = document.getElementById('pagarme-config-warning');
        if (!isPublicKeyValid) {
            if (!warningEl) {
                warningEl = document.createElement('div');
                warningEl.id = 'pagarme-config-warning';
                warningEl.style.color = 'red';
                warningEl.style.padding = '10px';
                warningEl.style.background = '#fee2e2';
                warningEl.style.borderRadius = '8px';
                warningEl.style.marginTop = '10px';
                warningEl.textContent = 'Configuração Pagar.me incompleta: PAGARME_PUBLIC_KEY inválida no .env. O pagamento com cartão pode falhar.';
                selector.parentNode.insertBefore(warningEl, selector);
            }
        } else if (warningEl) {
            warningEl.remove();
        }

        populateInstallments(calculateSubtotalCents());
        
        // Se nenhum método selecionado, define Pix como padrão
        if (typeof currentPaymentMethod === 'undefined' || !currentPaymentMethod) selectPaymentMethod('pix');
      } else {
        selector.style.display = 'none';
        // Revert to PIX if hidden
        if (currentPaymentMethod !== 'pix') {
          selectPaymentMethod('pix');
        }
      }
      try { updatePixCpfVisibility(); } catch (_) {}
    }
  }

  function selectPaymentMethod(method) {
    currentPaymentMethod = method;
    window.currentPaymentMethod = method;
    
    // 1. Update Radio Inputs
    const radioPix = document.querySelector('input[name="paymentMethod"][value="pix"]');
    const radioCard = document.querySelector('input[name="paymentMethod"][value="credit_card"]');
    if (radioPix) radioPix.checked = (method === 'pix');
    if (radioCard) radioCard.checked = (method === 'credit_card');

    // 2. Update Visual Styles (Option Cards)
    const optionPix = document.getElementById('optionPix');
    const optionCard = document.getElementById('optionCard');

    // Helper to reset style
    const resetStyle = (el) => {
        if (!el) return;
        el.style.borderColor = '#e5e7eb';
        el.style.backgroundColor = '#fff';
        const title = el.querySelector('.pm-title');
        if (title) title.style.color = '#111827';
        const subtitle = el.querySelector('.pm-subtitle');
        if (subtitle) subtitle.style.color = '#6b7280';
    };

    resetStyle(optionPix);
    resetStyle(optionCard);

    // Apply Active Style
    if (method === 'pix' && optionPix) {
        optionPix.style.borderColor = '#10b981';
        optionPix.style.backgroundColor = '#ecfdf5';
        const title = optionPix.querySelector('.pm-title');
        if (title) title.style.color = '#065f46';
        const subtitle = optionPix.querySelector('.pm-subtitle');
        if (subtitle) subtitle.style.color = '#065f46';
    } else if (method === 'credit_card' && optionCard) {
        optionCard.style.borderColor = '#3b82f6';
        optionCard.style.backgroundColor = '#eff6ff';
        const title = optionCard.querySelector('.pm-title');
        if (title) title.style.color = '#1e40af';
        const subtitle = optionCard.querySelector('.pm-subtitle');
        if (subtitle) subtitle.style.color = '#1d4ed8';
        
        // Populate Installments when Card is selected
        if (typeof populateInstallments === 'function') {
            const subtotal = (typeof window.calculateSubtotalCents === 'function') ? window.calculateSubtotalCents() : (window.basePriceCents || 0);
            populateInstallments(subtotal);
        }
    }

    // 3. Toggle Sections
    const cardForm = document.getElementById('cardPaymentContent');
    const pixBtnContainer = document.getElementById('pixPaymentBtnContainer');
    const contentPix = document.getElementById('pixPaymentContent'); // Success Screen QR
    const pagarmeBadgeCard = document.getElementById('pagarmeBadgeCard');
    const stripeBadgeCard = document.getElementById('stripeBadgeCard');
    const pagarmeCardFields = document.getElementById('pagarmeCardFields');
    const stripeCardFields = document.getElementById('stripeCardFields');
    const localCardMetaFields = document.getElementById('localCardMetaFields');
    const stripeEmbeddedWrapper = document.getElementById('stripeEmbeddedWrapper');
    const stripeEmbeddedMount = document.getElementById('stripeEmbeddedCheckout');

    if (method === 'credit_card') {
        if (cardForm) cardForm.style.display = 'block';
        if (pixBtnContainer) pixBtnContainer.style.display = 'none';
        if (contentPix) contentPix.style.display = 'none';
        const provider = String(window.CARD_PROVIDER || 'pagarme').trim().toLowerCase();
        const isStripe = provider === 'stripe';
        const useCheckout = window.STRIPE_USE_CHECKOUT === true || String(window.STRIPE_USE_CHECKOUT || '').toLowerCase() === 'true';
        if (pagarmeBadgeCard) pagarmeBadgeCard.style.display = isStripe ? 'none' : 'flex';
        if (stripeBadgeCard) stripeBadgeCard.style.display = isStripe ? 'flex' : 'none';
        if (pagarmeCardFields) pagarmeCardFields.style.display = isStripe ? 'none' : 'block';
        if (stripeCardFields) stripeCardFields.style.display = (isStripe && !useCheckout) ? 'block' : 'none';
        if (localCardMetaFields) localCardMetaFields.style.display = (isStripe && useCheckout) ? 'none' : 'block';
        if (stripeEmbeddedWrapper && stripeEmbeddedMount) {
          if (isStripe && useCheckout) {
            stripeEmbeddedWrapper.style.display = 'block';
            if (!(stripeEmbeddedMounted && stripeEmbeddedCheckout)) {
              stripeEmbeddedMount.innerHTML = '<div style=\"padding:14px; text-align:center; color:#6b7280; font-size:0.95rem;\">Digite seu telefone e clique em <b>Realizar pedido</b> para abrir o checkout da Stripe.</div>';
            }
            try {
              const run = () => { prefetchStripeEmbeddedCheckoutSession().catch(() => {}); };
              if (typeof window.requestIdleCallback === 'function') window.requestIdleCallback(run, { timeout: 1800 });
              else setTimeout(run, 250);
            } catch (_) {}
            try {
              setTimeout(() => { try { maybeAutoMountStripeEmbeddedCheckout(); } catch(_) {} }, 650);
            } catch (_) {}
          } else {
            stripeEmbeddedWrapper.style.display = 'none';
            stripeEmbeddedMount.innerHTML = '';
          }
        }
        try {
          const ids = ['cardNumber', 'cardExpiry', 'cardCvv'];
          ids.forEach((id) => {
            const el = document.getElementById(id);
            if (!el) return;
            el.required = !isStripe;
          });
        } catch (_) {}
        try {
          const elName = document.getElementById('cardHolderName');
          const elCpf = document.getElementById('cardHolderCpf');
          if (elName) elName.required = !(isStripe && useCheckout);
          if (elCpf) elCpf.required = !(isStripe && useCheckout);
        } catch (_) {}
        if (isStripe && !useCheckout) {
          try { ensureStripeMounted(); } catch (_) {}
        }
        
        // Auto-focus first field if needed
        // setTimeout(() => document.getElementById('cardNumber')?.focus(), 100);
    } else {
        if (cardForm) cardForm.style.display = 'none';
        if (pixBtnContainer) pixBtnContainer.style.display = 'block';
        if (contentPix) contentPix.style.display = 'block';
        if (pagarmeBadgeCard) pagarmeBadgeCard.style.display = 'none';
        if (stripeBadgeCard) stripeBadgeCard.style.display = 'none';
    }
    
    // Update Step 5 Tutorial Highlight
    if (typeof window.showTutorialStep === 'function') {
         if (window.currentStep === 3) window.showTutorialStep(5);
    }
    try { updatePixCpfVisibility(); } catch (_) {}
  }



  // Event Listeners for Payment Methods
  // Note: DOMContentLoaded might have already fired if script is deferred/async
  const radioInputs = document.querySelectorAll('input[name="paymentMethod"]');
  radioInputs.forEach(radio => {
    radio.addEventListener('change', (e) => {
      selectPaymentMethod(e.target.value);
    });
  });
  const checkoutPhoneInput = document.getElementById('checkoutPhoneInput');
  function onlyDigits(v){ return String(v||'').replace(/\D+/g,''); }
  function maskBrPhone(v){
    const s = onlyDigits(v).slice(0,11);
    if (!s) return '';
    const ddd = s.slice(0,2);
    const first = s.slice(2,3);
    const mid = s.slice(3,7);
    const end = s.slice(7,11);
    let out = '';
    if (ddd.length < 2) {
      out = `(${ddd}`; // mostra parcialmente enquanto digita o DDD
    } else {
      out = `(${ddd})`;
    }
    if (first) out += ` ${first}`;
    if (mid) out += mid;
    if (end) out += `-${end}`;
    return out;
  }
  function attachPhoneMask(input){
    if (!input) return;
    input.addEventListener('input', ()=>{ input.value = maskBrPhone(input.value); });
    input.addEventListener('keydown', (e)=>{
      if (e.key === 'Backspace') {
        const selStart = input.selectionStart, selEnd = input.selectionEnd;
        const hasSelection = selStart !== selEnd;
        if (!hasSelection) {
          const digits = onlyDigits(input.value);
          if (digits.length > 0) {
            e.preventDefault();
            input.value = maskBrPhone(digits.slice(0, -1));
          }
        }
      }
    });
    input.addEventListener('paste', (e)=>{
      const txt = (e.clipboardData || window.clipboardData)?.getData('text');
      if (txt) { e.preventDefault(); input.value = maskBrPhone(txt); }
    });
  }
  // Perfil Instagram (checkout)
  const perfilCard = document.getElementById('perfilCard');
  const usernameCheckoutInput = document.getElementById('usernameCheckoutInput');
  const checkCheckoutButton = document.getElementById('checkCheckoutButton');
  const statusCheckoutMessage = document.getElementById('statusCheckoutMessage');
  const loadingCheckoutSpinner = document.getElementById('loadingCheckoutSpinner');
  const profilePreview = document.getElementById('profilePreview');
  const checkoutProfileImage = document.getElementById('checkoutProfileImage');
  const checkoutProfileUsername = document.getElementById('checkoutProfileUsername');
  const checkoutFollowersCount = document.getElementById('checkoutFollowersCount');
  const checkoutFollowingCount = document.getElementById('checkoutFollowingCount');
  const checkoutPostsCount = document.getElementById('checkoutPostsCount');

  // Âncora suave para botão "Comprar Seguidores Agora"
  const buyFollowersBtn = document.getElementById('buyFollowersBtn');
  if (buyFollowersBtn) {
    buyFollowersBtn.addEventListener('click', function(e) {
      e.preventDefault();
      const target = document.getElementById('plataformaCard');
      if (target) {
         const rect = target.getBoundingClientRect();
         const isMobile = window.matchMedia('(max-width: 767px)').matches;
         const offset = isMobile ? 340 : 220;
         const top = (window.scrollY || window.pageYOffset || 0) + rect.top - offset;
         window.scrollTo({ top, behavior: 'smooth' });
      }
    });
  }
  // Tutoriais sequenciais
  const tutorial1Tipo = document.getElementById('tutorial1Tipo');
  const tutorial2Pacote = document.getElementById('tutorial2Pacote');
  const tutorial3Usuario = document.getElementById('tutorial3Usuario');
  const tutorial4Validar = document.getElementById('tutorial4Validar');
  const tutorial5Pedido = document.getElementById('tutorial5Pedido');
  const grupoTipo = document.getElementById('grupoTipo');
  const grupoQuantidade = document.getElementById('grupoQuantidade');
  const grupoUsername = document.getElementById('grupoUsername');
  let tutorial3Suppressed = false;
  const grupoPedido = document.getElementById('grupoPedido');
  // carrossel removido
  let isInstagramVerified = false;
  let isInstagramPrivate = false;
  // Captura phone da URL: /checkout?phone=... (default 11111111)
  let phoneFromUrl = new URLSearchParams(window.location.search).get('phone') || '';

  const tabela = {
    mistos: [
      { q: 50, p: 'R$ 0,01' },
      { q: 150, p: 'R$ 7,90' },
      { q: 300, p: 'R$ 12,90' },
      { q: 500, p: 'R$ 16,90' },
      { q: 700, p: 'R$ 22,90' },
      { q: 1000, p: 'R$ 29,90' },
      { q: 2000, p: 'R$ 49,90' },
      { q: 3000, p: 'R$ 79,90' },
      { q: 4000, p: 'R$ 99,90' },
      { q: 5000, p: 'R$ 129,90' },
      { q: 7500, p: 'R$ 169,90' },
      { q: 10000, p: 'R$ 229,90' },
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
    curtidas_brasileiras: [
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
    curtidas_mistos: [
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
    curtidas_organicos: [
      { q: 150, p: 'R$ 16,90' },
      { q: 300, p: 'R$ 28,90' },
      { q: 500, p: 'R$ 49,90' },
      { q: 1000, p: 'R$ 69,90' },
      { q: 2000, p: 'R$ 104,90' },
      { q: 3000, p: 'R$ 139,90' },
      { q: 4000, p: 'R$ 174,90' },
      { q: 5000, p: 'R$ 224,90' },
      { q: 7500, p: 'R$ 279,90' },
      { q: 10000, p: 'R$ 349,90' },
      { q: 15000, p: 'R$ 449,90' },
    ],
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
      { q: 1000000, p: 'R$ 159,90' },
    ],
  };

  const promoPricing = {
    likes: { old: 'R$ 49,90', price: 'R$ 9,90', discount: 80 },
    views: { old: 'R$ 89,90', price: 'R$ 19,90', discount: 78 },
    comments: { old: 'R$ 29,90', price: 'R$ 9,90', discount: 67 },
    warranty: { old: 'R$ 39,90', price: 'R$ 14,90', discount: 63 },
    warranty60: { old: 'R$ 39,90', price: 'R$ 9,90', discount: 75 },
  };
  try { window.promoPricing = promoPricing; } catch(_) {}

  function renderPromoPrices() {
    const blocks = document.querySelectorAll('.promo-prices');
    blocks.forEach(b => {
      const key = b.getAttribute('data-promo');
      // Não sobrescrever preços dinâmicos de likes, views e comments
      if (key === 'likes' || key === 'views' || key === 'comments' || key === 'warranty60') return;
      const conf = promoPricing[key];
      if (!conf) return;
      const oldEl = b.querySelector('.old-price');
      const newEl = b.querySelector('.new-price');
      const discEl = b.querySelector('.discount-badge');
      if (oldEl) oldEl.textContent = conf.old;
      if (newEl) newEl.textContent = conf.price;
      if (discEl) discEl.textContent = `${conf.discount}% OFF`;
    });
  }

  let warrantyMode = 'life';
  try { window.warrantyMode = warrantyMode; } catch(_) {}
  const wLabel = document.getElementById('warrantyModeLabel');
  const wHighlight = document.getElementById('warrantyHighlight');
  const wOld = document.getElementById('warrantyOldPrice');
  const wNew = document.getElementById('warrantyNewPrice');
  const wDisc = document.getElementById('warrantyDiscount');
  function applyWarrantyMode(){
    const isLife = true;
    if (wLabel) wLabel.textContent = '6 meses';
    if (wHighlight) wHighlight.textContent = 'REPOSIÇÃO POR 6 MESES';
    if (wOld) wOld.textContent = 'R$ 39,90';
    if (wNew) wNew.textContent = 'R$ 9,90';
    if (wDisc) wDisc.textContent = '75% OFF';
    try { updatePromosSummary(); } catch(_) {}
  }
  applyWarrantyMode();

  tabela.seguidores_tiktok = tabela.mistos;
  function getAllowedQuantities(tipo) {
    const without50 = [150, 500, 1000, 3000, 5000, 10000];
    const base = [50, 150, 500, 1000, 3000, 5000, 10000];
    if (tipo === 'mistos') {
      return without50;
    }
    if (tipo === 'seguidores_tiktok') {
      return without50;
    }
    if (tipo === 'brasileiros' || tipo === 'organicos') {
      return without50;
    }
    return base;
  }

  function isFollowersTipo(tipo) {
    return ['mistos', 'brasileiros', 'organicos', 'seguidores_tiktok'].includes(tipo);
  }

  function getPricingTipoKey(tipo) {
    const t = String(tipo || '');
    const cat = getServiceCategory();
    if (cat === 'curtidas' && t === 'organicos') return 'curtidas_organicos';
    if (cat === 'curtidas' && t === 'mistos') return 'curtidas_mistos';
    return t;
  }

  function findPrice(tipo, qtd) {
    const key = getPricingTipoKey(tipo);
    const arr = tabela[key] || [];
    const it = arr.find(x => Number(x.q) === Number(qtd));
    return it ? it.p : '';
  }

  function formatCentsToBRL(cents) {
    const valor = Math.max(0, Number(cents) || 0);
    const reais = Math.floor(valor / 100);
    const centavos = valor % 100;
    return `R$ ${reais.toLocaleString('pt-BR')},${String(centavos).padStart(2, '0')}`;
  }

  function updateOrderBump(tipo, baseQtd) {
    const orderInline = document.getElementById('orderBumpInline');
    if (!orderInline) return;
    const labelSpan = document.getElementById('orderBumpText');
    const checkbox = document.getElementById('orderBumpCheckboxInline');
    const upgradePrices = document.querySelector('.promo-prices[data-promo="upgrade"]');
    const upOld = upgradePrices ? upgradePrices.querySelector('.old-price') : null;
    const upNew = upgradePrices ? upgradePrices.querySelector('.new-price') : null;
    const upDisc = upgradePrices ? upgradePrices.querySelector('.discount-badge') : null;
    const upHighlight = document.getElementById('orderBumpHighlight');
    if (!isFollowersTipo(tipo) || !baseQtd) {
      if (checkbox) {
        checkbox.checked = false;
        checkbox.disabled = true;
      }
      orderInline.style.display = 'none';
      return;
    }
    // Sempre mostrar o card de Promoções para serviços de seguidores
    orderInline.style.display = 'block';
    if (checkbox) {
      checkbox.checked = false;
      checkbox.disabled = false;
    }

    // Promos específicas: 1000 -> 2000 com extras para brasileiros/organicos
    if ((tipo === 'brasileiros' || tipo === 'organicos') && Number(baseQtd) === 1000) {
      const targetQtd = 2000;
      const basePrice = findPrice(tipo, 1000);
      const targetPrice = findPrice(tipo, 2000);
      const diffCents = parsePrecoToCents(targetPrice) - parsePrecoToCents(basePrice);
      const diffStr = formatCentsToBRL(diffCents);
      const extras = '(+400 Curtidas e 15.000 visualizações)';
      if (labelSpan) labelSpan.textContent = `Por mais ${diffStr}, atualize para ${targetQtd} ${getUnitForTipo(tipo)} ${extras}.`;
      if (upHighlight) upHighlight.textContent = `+ ${targetQtd - 1000} seguidores`;
      if (upOld) upOld.textContent = targetPrice || '—';
      if (upNew) upNew.textContent = diffStr;
      if (upDisc) {
        const targetCents = parsePrecoToCents(targetPrice);
        const pct = targetCents ? Math.round(((targetCents - diffCents) / targetCents) * 100) : 0;
        upDisc.textContent = `${pct}% OFF`;
      }
      return;
    }

    // Upgrade genérico para demais pacotes
    const upsellTargets = { 150: 300, 500: 700, 1000: 2000, 3000: 4000, 5000: 7500, 10000: 15000 };
    const targetQtd = upsellTargets[Number(baseQtd)];
    if (!targetQtd) {
      if (checkbox) {
        checkbox.checked = false;
        checkbox.disabled = true;
      }
      if (labelSpan) labelSpan.textContent = 'Nenhum upgrade disponível para este pacote.';
      if (upOld) upOld.textContent = '—';
      if (upNew) upNew.textContent = '—';
      if (upDisc) upDisc.textContent = 'OFERTA';
      return;
    }
    if (checkbox) checkbox.disabled = false;
    const basePrice = findPrice(tipo, baseQtd);
    const targetPrice = findPrice(tipo, targetQtd);
    const diffCents = parsePrecoToCents(targetPrice) - parsePrecoToCents(basePrice);
    const addQtd = targetQtd - baseQtd;
    const diffStr = formatCentsToBRL(diffCents);
    if (labelSpan) labelSpan.textContent = `Por mais ${diffStr}, adicione ${addQtd} seguidores e atualize para ${targetQtd}.`;
    if (upHighlight) upHighlight.textContent = `+ ${addQtd} seguidores`;
    if (upOld) upOld.textContent = targetPrice || '—';
    if (upNew) upNew.textContent = diffStr;
    if (upDisc) {
      const targetCents = parsePrecoToCents(targetPrice);
      const pct = targetCents ? Math.round(((targetCents - diffCents) / targetCents) * 100) : 0;
      upDisc.textContent = `${pct}% OFF`;
    }
  }

  // UI por cards (tipo e planos) em todas as visões
  function isDesktop() { return window.innerWidth >= 1024; }

  function selectTipo(tipo) {
    if (!tipoSelect) return;
    tipoSelect.value = tipo;
    popularQuantidades(tipo);
    clearResumo();
    updatePerfilVisibility();
    updateWarrantyVisibility();
    showTutorialStep(2);
    renderPlanCards(tipo);
    renderTipoDescription(tipo);
    try { applyCheckoutFlow(); } catch(_) {}
    // Marcar card ativo
    const cards = tipoCards?.querySelectorAll('.service-card[data-role="tipo"]') || [];
    cards.forEach(c => {
      c.classList.toggle('active', c.dataset.tipo === tipo);
    });
  }

  function renderTipoCards() {
    if (!tipoCards) return;
    tipoCards.innerHTML = '';
    tipoCards.style.display = 'grid';
    const category = getServiceCategory();
    const tipos = selectedPlatform === 'tiktok'
      ? [
          { key: 'seguidores_tiktok', label: 'Seguidores' }
        ]
      : (category === 'curtidas'
          ? [
              { key: 'mistos', label: 'Curtidas Mistas' },
              { key: 'curtidas_brasileiras', label: 'Curtidas Brasileiras' },
              { key: 'organicos', label: 'Curtidas Brasileiras Reais' }
            ]
          : [
              { key: 'mistos', label: 'Seguidores Mistos' },
              { key: 'brasileiros', label: 'Seguidores Brasileiros' },
              { key: 'organicos', label: 'Seguidores brasileiros e reais' }
            ]);
    if (selectedPlatform === 'tiktok') {
      try {
        tipoCards.style.placeContent = 'center';
        tipoCards.style.placeItems = 'center';
        tipoCards.style.justifyContent = 'center';
        tipoCards.style.justifyItems = 'center';
        tipoCards.style.alignContent = 'start';
        tipoCards.style.alignItems = 'start';
        tipoCards.style.gridTemplateColumns = '1fr minmax(260px, 380px) 1fr';
        tipoCards.style.minHeight = '';
        tipoCards.style.margin = '0 auto';
      } catch(_) {}
    } else {
      try {
        tipoCards.style.placeContent = '';
        tipoCards.style.placeItems = '';
        tipoCards.style.gridTemplateColumns = '';
        tipoCards.style.minHeight = '';
        tipoCards.style.margin = '';
  } catch(_) {}
  (function initWhyOppusSlider(){
    try {
      const cont = document.querySelector('.why-oppus-slider .slider-container');
      if (!cont) return;
      const slides = Array.from(cont.querySelectorAll('.slider-slide'));
      if (!slides.length) return;
      let activeIdx = 0;
      function setActive(i){
        slides.forEach((s,idx) => { if (idx === i) s.classList.add('active'); else s.classList.remove('active'); });
        activeIdx = i;
        const el = slides[i];
        if (el) {
          const left = el.offsetLeft - (cont.clientWidth - el.clientWidth) / 2;
          cont.scrollTo({ left, behavior: 'smooth' });
        }
      }
      slides.forEach((s,idx) => s.addEventListener('click', () => setActive(idx)));
      let t;
      function snapToCenter(){
        const cRect = cont.getBoundingClientRect();
        const cCenter = cRect.left + cRect.width / 2;
        let best = 0; let bestDist = Infinity;
        slides.forEach((s,idx) => {
          const r = s.getBoundingClientRect();
          const sCenter = r.left + r.width / 2;
          const d = Math.abs(sCenter - cCenter);
          if (d < bestDist) { bestDist = d; best = idx; }
        });
        setActive(best);
      }
      cont.addEventListener('scroll', () => { if (t) clearTimeout(t); t = setTimeout(snapToCenter, 120); });
      setActive(0);
    } catch(_) {}
  })();
}
    if (tipoCards) tipoCards.style.display = 'grid';
    for (const t of tipos) {
      if (selectedPlatform !== 'tiktok' && category === 'curtidas' && t.key === 'curtidas_brasileiras') continue;
      const el = document.createElement('div');
      el.className = 'service-card option-card' + (selectedPlatform === 'tiktok' ? ' disabled' : '');
      el.dataset.role = 'tipo';
      el.dataset.tipo = t.key;
      
      const content = selectedPlatform === 'tiktok' ? 
          `<div class="card-title" style="text-align:center;">${t.label}</div><div class="card-desc" style="text-align:center;"><span class="status-warning"><svg class="status-maint-icon" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><path fill="currentColor" d="M3 12a9 9 0 1118 0 9 9 0 01-18 0zm9-4a1 1 0 011 1v3.586l2.293 2.293a1 1 0 11-1.414 1.414l-2.586-2.586A2 2 0 0110 12V9a1 1 0 011-1z"/></svg> Serviço em manutenção</span></div>` :
          `<div class="card-title" style="text-align:center;">${t.label}</div>`;

      el.innerHTML = `<div class="card-content">${content}</div>`;
      if (selectedPlatform === 'tiktok') {
        try { el.style.gridColumn = '2'; } catch(_) {}
      }
      if (selectedPlatform !== 'tiktok') {
        el.addEventListener('click', () => selectTipo(t.key));
      }
      tipoCards.appendChild(el);
    }
  }

  function renderPlanCards(tipo) {
    if (!planCards) return;
    planCards.innerHTML = '';
    if (tipo === 'seguidores_tiktok') { planCards.style.display = 'none'; return; }
    const pricingKey = getPricingTipoKey(tipo);
    let plans = tabela[pricingKey] || [];
    if (isFollowersTipo(tipo) && getServiceCategory() === 'seguidores') {
      const allowed = getAllowedQuantities(tipo);
      plans = plans.filter(x => allowed.includes(Number(x.q)));
    }
    if (!plans.length) { planCards.style.display = 'none'; return; }
    planCards.style.display = 'grid';
    for (const item of plans) {
      const card = document.createElement('div');
      card.className = 'service-card';
      card.dataset.role = 'plano';
      const unit = getUnitForTipo(tipo);
      const baseText = String(item.p);
      const baseStr = baseText.replace(/[^0-9,\.]/g, '');
      let base = 0;
      try { base = parseFloat(baseStr.replace('.', '').replace(',', '.')); } catch(_) {}
      const inc = base * 1.15;
      const ceilInt = Math.ceil(inc);
      const increasedRounded = (ceilInt - 0.10);
      const increasedText = `R$ ${increasedRounded.toFixed(2).replace('.', ',')}`;

      // Badge logic e Destaque (Gold Card)
      const qNum = Number(item.q);
      let badgeHtml = '';
      let badgeText = '';

      // Mapa base de badges (copiado de servicos-instagram.js para consistência, se não existir globalmente)
      const quantityBadges = {
        20: 'PACOTE TESTE',
        50: 'PACOTE TESTE',
        150: 'PACOTE INICIAL',
        500: 'PACOTE BÁSICO',
        1000: 'MAIS PEDIDO',
        3000: 'EXCLUSIVO',
        5000: 'VIP',
        10000: 'ELITE'
      };

      if (tipo === 'mistos') {
        if (qNum === 1000) badgeText = 'MELHOR PREÇO';
        if (qNum === 3000) { badgeText = 'MAIS PEDIDO'; card.classList.add('gold-card'); }
      } else if (tipo === 'brasileiros') {
        if (qNum === 1000) { badgeText = 'MAIS PEDIDO'; card.classList.add('gold-card'); }
      } else if (tipo === 'organicos') {
        if (qNum === 1000) { badgeText = 'MAIS PEDIDO'; card.classList.add('gold-card'); }
      } else if (tipo === 'visualizacoes_reels') {
        if (qNum === 1000) badgeText = 'PACOTE INICIAL';
        if (qNum === 5000) badgeText = 'PACOTE BÁSICO';
        if (qNum === 25000) badgeText = 'MELHOR PREÇO';
        if (qNum === 100000) { badgeText = 'MAIS PEDIDO'; card.classList.add('gold-card'); }
        if (qNum === 200000) badgeText = 'VIP';
        if (qNum === 500000) badgeText = 'ELITE';
      }

      // Fallback
      if (!badgeText && isFollowersTipo(tipo) && quantityBadges[qNum]) {
          badgeText = quantityBadges[qNum];
      }

      if (badgeText) {
        badgeHtml = `<div class="plan-badge">${badgeText}</div>`;
      }

      const qtyFormatted = qNum.toLocaleString('pt-BR');
      card.innerHTML = `${badgeHtml}<div class="card-content"><div class="card-title">${qtyFormatted} ${unit}</div><div class="card-desc"><span class="price-old">${increasedText}</span> <span class="price-new">${baseText}</span></div></div>`;
      card.dataset.qtd = String(item.q);
      card.dataset.preco = baseText;
      card.addEventListener('click', () => {
        // Sincroniza selects e resumo
        qtdSelect.value = String(item.q);
        const opt = Array.from(qtdSelect.options).find(o => o.value === String(item.q));
        if (!opt) { popularQuantidades(tipo); }
        const selectedOpt = Array.from(qtdSelect.options).find(o => o.value === String(item.q));
        if (selectedOpt) { selectedOpt.selected = true; }
        resTipo.textContent = getLabelForTipo(tipo);
        resQtd.textContent = `${item.q} ${unit}`;
        resPreco.textContent = baseText;
        try { basePriceCents = parsePrecoToCents(baseText); } catch(_) { basePriceCents = 0; }
        showResumoIfAllowed();
        updateOrderBump(tipo, Number(item.q));
        try { updatePromosSummary(); } catch(_) {}
        try {
          const paymentCardEl = document.getElementById('paymentCard');
          if (paymentCardEl) paymentCardEl.style.display = 'block';
        } catch (e) {}
        try { sessionStorage.setItem('oppus_qtd', String(item.q)); } catch(_) {}
        try { sessionStorage.setItem('oppus_servico', tipo); } catch(_) {}
        showTutorialStep(4);
        renderTipoDescription(tipo);
        // Marcar ativo
        const cards = planCards.querySelectorAll('.service-card[data-role="plano"]');
        cards.forEach(c => c.classList.toggle('active', c === card));
        updatePerfilVisibility();
        updateWarrantyVisibility();
        try { applyCheckoutFlow(); } catch(_) {}
        // Âncora: ao selecionar pacote, focar no perfil no mobile
        try {
          const isMobile = window.innerWidth <= 640;
          if (isMobile) {
            if (perfilCard) perfilCard.style.display = 'block';
            const target = document.getElementById('grupoUsername') || perfilCard;
        if (target) {
           const rect = target.getBoundingClientRect();
            const top = (window.scrollY || window.pageYOffset || 0) + rect.top - 280;
            window.scrollTo({ top, behavior: 'smooth' });
        }
          }
        } catch(_) {}
      });
      planCards.appendChild(card);
    }
  }

  function getLabelForTipo(t) {
    const category = getServiceCategory();
    if (category === 'curtidas') {
      if (t === 'mistos') return 'Curtidas Mistas';
      if (t === 'organicos') return 'Curtidas Brasileiras Reais';
    }
    switch (t) {
      case 'mistos': return 'Seguidores Mistos';
      case 'brasileiros': return 'Seguidores Brasileiros';
      case 'organicos': return 'Seguidores brasileiros e reais';
      case 'seguidores_tiktok': return 'Seguidores';
      case 'curtidas_brasileiras': return 'Curtidas Brasileiras';
      case 'curtidas_organicos': return 'Curtidas Brasileiras Reais';
      case 'visualizacoes_reels': return 'Visualizações Reels';
      default: return String(t).replace(/_/g, ' ');
    }
  }

  function getTipoDescription(tipo) {
    const t = String(tipo || '');
    const category = (t.indexOf('curtidas_') === 0 || t === 'curtidas_brasileiras') ? 'curtidas' : getServiceCategory();
    if (category === 'curtidas') {
      const curtTipo = (t === 'curtidas_brasileiras') ? 'brasileiros' : ((t === 'curtidas_organicos') ? 'organicos' : t);
      switch (curtTipo) {
        case 'mistos':
          return `
            <p>Curtidas com entrega rápida e estável para impulsionar suas publicações.</p>
            <ul>
              <li>✅ 100% seguro e confidencial, sem precisar da sua senha.</li>
              <li>🌍 Curtidas de perfis internacionais para melhorar a prova social do post.</li>
              <li>📈 Ideal para dar força inicial em conteúdos estratégicos.</li>
            </ul>
          `;
        case 'brasileiros':
          return `
            <p>Curtidas de perfis brasileiros para impulsionar suas publicações.</p>
            <ul>
              <li>✅ 100% seguro e confidencial, sem precisar da sua senha.</li>
              <li>🇧🇷 Perfis brasileiros para reforçar prova social.</li>
              <li>📈 Ideal para posts que você quer destacar.</li>
            </ul>
          `;
        case 'organicos':
          return `
            <p>Curtidas de perfis brasileiros e reais para máxima qualidade e credibilidade nas suas publicações.</p>
            <ul>
              <li>✅ 100% seguro e confidencial, sem precisar da sua senha.</li>
              <li>🇧🇷 Perfis brasileiros e reais para reforçar autoridade.</li>
              <li>📈 Ideal para posts que você quer destacar com mais autoridade.</li>
            </ul>
          `;
        default:
          return '';
      }
    }
    switch (tipo) {
      case 'mistos':
        return `
          <p>Perfis variados com entrega rápida e estável, com alguns seguidores reais de vários países.</p>
          <ul>
            <li>✅ 100% seguro e confidencial, sem precisar da sua senha.</li>
            <li>🌍 Alguns seguidores reais de vários países para crescer sua base.</li>
            <li>🛠 Ferramenta de reposição de seguidores: não perca nenhum seguidor.</li>
          </ul>
        `;
      case 'brasileiros':
        return `
          <p>Base nacional com nomes locais e seguidores brasileiros.</p>
          <ul>
            <li>✅ 100% seguro e confidencial, sem precisar da sua senha.</li>
            <li>🇧🇷 Foco total no público brasileiro e mais credibilidade.</li>
            <li>🛠 Ferramenta de reposição de seguidores: não perca nenhum seguidor.</li>
          </ul>
        `;
      case 'organicos':
        return `
          <p>Brasileiros e reais: perfis ativos e selecionados, com maior credibilidade.</p>
          <ul>
            <li>✅ 100% seguro e confidencial, sem precisar da sua senha.</li>
            <li>✨ Perfis mais qualificados para reforçar autoridade do perfil.</li>
            <li>📉 Serviço com baixa queda de seguidores.</li>
          </ul>
        `;
      default:
        return '';
    }
  }

  function renderTipoDescription(tipo) {
    const card = document.getElementById('tipoDescCard');
    const title = document.getElementById('tipoDescTitle');
    const content = document.getElementById('tipoDescContent');
    if (!card || !title || !content) return;
    title.textContent = 'Descrição do serviço';
    content.innerHTML = getTipoDescription(tipo);
    card.style.display = 'block';
  }

  function getUnitForTipo(tipo) {
    const category = getServiceCategory();
    if (category === 'curtidas' && (tipo === 'mistos' || tipo === 'organicos')) return 'curtidas';
    switch (tipo) {
      case 'mistos':
      case 'brasileiros':
      case 'organicos':
      case 'seguidores_tiktok':
        return 'seguidores';
      case 'curtidas_brasileiras':
      case 'curtidas_organicos':
        return 'curtidas';
      case 'visualizacoes_reels':
        return 'visualizações';
      default:
        return 'itens';
    }
  }

  // Helpers Instagram
  function normalizeInstagramUsername(input) {
    if (!input) return '';
    const cleaned = input.replace(/\s/g, '').replace('@', '');
    const match = cleaned.match(/^https?:\/\/(www\.)?instagram\.com\/([^\/\?\s]+)/i);
    return match ? match[2] : cleaned;
  }

  function isValidInstagramUsername(username) {
    const regex = /^[a-zA-Z0-9_]([a-zA-Z0-9._]{0,28}[a-zA-Z0-9_])?$/;
    return regex.test(username);
  }

  // Helpers para links de post (p/reel/tv)
  function extractShortcodeFromInput(input) {
    if (!input) return '';
    const trimmed = input.trim();
    const decoded = decodeURIComponent(trimmed);
    const match = decoded.match(/^(?:https?:\/\/)?(?:www\.)?instagram\.com\/(?:p|reel|tv)\/([A-Za-z0-9_-]+)/i);
    if (match) return match[1];
    const shortcodeMatch = trimmed.match(/^([A-Za-z0-9_-]{5,})$/);
    return shortcodeMatch ? shortcodeMatch[1] : '';
  }

  function normalizePostLink(input) {
    const shortcode = extractShortcodeFromInput(input);
    if (!shortcode) return '';
    // Normaliza reels para /p/ e garante https + barra final
    return `https://www.instagram.com/p/${shortcode}/`;
  }

  function isValidPostLink(input) {
    const link = normalizePostLink(input);
    return /^https?:\/\/(www\.)?instagram\.com\/p\/[A-Za-z0-9_-]+\/$/i.test(link);
  }

  function showStatusMessageCheckout(msg, type = 'info') {
    if (!statusCheckoutMessage) return;
    statusCheckoutMessage.textContent = msg;
    statusCheckoutMessage.style.display = 'block';
    statusCheckoutMessage.style.textAlign = 'center';
    const isLight = document.body.classList.contains('theme-light');
    if (type === 'success') {
      statusCheckoutMessage.style.color = '#22C55E';
    } else if (type === 'error') {
      statusCheckoutMessage.style.color = isLight ? '#7f1d1d' : '#ffb4b4';
    } else {
      statusCheckoutMessage.style.color = isLight ? '#000000' : '#ffffff';
    }
  }

  function applyCheckoutFlow() {
    const followers = isFollowersSelected();
    const verified = !!isInstagramVerified;
    const orderInline = document.getElementById('orderBumpInline');
    const resumoCard = document.getElementById('resumo');
    const paymentCardEl = document.getElementById('paymentCard');
    const grupoPedidoEl = document.getElementById('grupoPedido');
    const hasPlanSelected = Boolean(
      (qtdSelect && qtdSelect.value) ||
      (planCards && planCards.querySelector('.service-card[data-role="plano"].active'))
    );
    if (followers) {
      if (!verified) {
        if (orderInline) orderInline.style.display = 'none';
        if (resumoCard) { resumoCard.hidden = true; resumoCard.style.display = 'none'; }
        if (paymentCardEl) paymentCardEl.style.display = 'none';
        if (grupoPedidoEl) grupoPedidoEl.style.display = 'none';
        if (perfilCard) perfilCard.style.display = hasPlanSelected ? 'block' : 'none';
      } else {
        if (orderInline) orderInline.style.display = 'block';
        if (resumoCard) { resumoCard.hidden = false; resumoCard.style.display = 'block'; }
        if (paymentCardEl) paymentCardEl.style.display = 'block';
        if (grupoPedidoEl) grupoPedidoEl.style.display = 'block';
      }
    } else {
      if (orderInline) orderInline.style.display = 'block';
      if (resumoCard) { resumoCard.hidden = false; resumoCard.style.display = 'block'; }
      if (paymentCardEl) paymentCardEl.style.display = 'block';
      if (grupoPedidoEl) grupoPedidoEl.style.display = 'block';
    }
  }

  function hideStatusMessageCheckout() {
    if (!statusCheckoutMessage) return;
    statusCheckoutMessage.style.display = 'none';
    statusCheckoutMessage.textContent = '';
  }

  function showLoadingCheckout() {
    if (loadingCheckoutSpinner) loadingCheckoutSpinner.style.display = 'block';
  }

  function hideLoadingCheckout() {
    if (loadingCheckoutSpinner) loadingCheckoutSpinner.style.display = 'none';
  }

  function clearProfilePreview() {
    if (profilePreview) profilePreview.style.display = 'none';
    if (checkoutProfileImage) checkoutProfileImage.src = '';
    if (checkoutProfileUsername) checkoutProfileUsername.textContent = '';
    if (checkoutFollowersCount) checkoutFollowersCount.textContent = '-';
    if (checkoutFollowingCount) checkoutFollowingCount.textContent = '-';
    if (checkoutPostsCount) checkoutPostsCount.textContent = '-';
  }

  function updatePerfilVisibility() {
    if (!perfilCard || !tipoSelect) return;
    const tipo = tipoSelect.value;
    const label = getLabelForTipo(tipo).toLowerCase();
    const isFollowersService = /seguidores/i.test(label);
    const hasPlanSelected = Boolean(
      (qtdSelect && qtdSelect.value) ||
      (planCards && planCards.querySelector('.service-card[data-role="plano"].active'))
    );
    const show = selectedPlatform === 'instagram' && isFollowersService && tipo && hasPlanSelected;
    perfilCard.style.display = show ? 'block' : 'none';
    if (!isFollowersService) {
      clearProfilePreview();
      hideStatusMessageCheckout();
      isInstagramVerified = false; // não exige verificação para outros serviços
    }
    updatePedidoButtonState();
  }

  function isFollowersSelected() {
    const selectedOption = tipoSelect.options[tipoSelect.selectedIndex];
    return !!(selectedOption && /seguidores/i.test(selectedOption.textContent));
  }

  function updatePedidoButtonState() {
    if (!btnPedido) return;
    // Mantém o botão sempre clicável para exibir alertas de pendência
    btnPedido.disabled = false;
  }

  function setPlatform(p) {
    selectedPlatform = p;
    if (btnInstagram) btnInstagram.setAttribute('aria-pressed', String(p === 'instagram'));
    if (btnTikTok) btnTikTok.setAttribute('aria-pressed', String(p === 'tiktok'));
    tipoSelect.value = '';
    qtdSelect.innerHTML = '';
    clearResumo();
    if (planCards) { planCards.innerHTML = ''; planCards.style.display = 'none'; }
    const descCard = document.getElementById('tipoDescCard');
    if (descCard) descCard.style.display = 'none';
    updatePerfilVisibility();
    renderTipoCards();
    // Auto-selecionar Seguidores Mistos ao clicar em Instagram
    if (p === 'instagram' && tipoSelect) {
      selectTipo('mistos');
      tipoSelect.classList.add('selected');
      showTutorialStep(3);
    } else {
      tipoSelect.classList.remove('selected');
      showTutorialStep(2);
    }
  }
  window.setPlatform = setPlatform;

  (function initBuyFollowersBtn(){
    const btn = document.getElementById('buyFollowersBtn');
    if (!btn) return;
    btn.addEventListener('click', function(e){
      try { e.preventDefault(); } catch(_) {}
      try {
        const targetPlat = document.getElementById('plataformaCard');
        if (targetPlat) {
          const isMobile = window.matchMedia('(max-width: 767px)').matches;
          const isEngPage = !!(window.__ENG_MODE__);

          // Em /engajamento no mobile, ancora mais embaixo, na altura dos cards
          let targetEl = targetPlat;
          let offset = isMobile ? 340 : 220;

          if (isMobile && isEngPage) {
            const cardsDiv = document.getElementById('engajamentoServiceCards');
            if (cardsDiv) {
              targetEl = cardsDiv;
              offset = 160;
            }
          }

          const rect = targetEl.getBoundingClientRect();
          const top = (window.scrollY || window.pageYOffset || 0) + rect.top - offset;
          window.scrollTo({ top, behavior: 'smooth' });
        }
        showTutorialStep(2);
        const tutorialPlatform = document.getElementById('tutorialPlatform');
        if (tutorialPlatform) tutorialPlatform.style.display = 'block';
      } catch(_) {}
    });
  })();

  function hideAllTutorials() {
    if (tutorial1Tipo) tutorial1Tipo.style.display = 'none';
    if (tutorial2Pacote) tutorial2Pacote.style.display = 'none';
    if (tutorial3Usuario) tutorial3Usuario.style.display = 'none';
    if (tutorial4Validar) tutorial4Validar.style.display = 'none';
    if (tutorial5Pedido) tutorial5Pedido.style.display = 'none';
    if (grupoTipo) grupoTipo.classList.remove('tutorial-highlight');
    if (grupoQuantidade) grupoQuantidade.classList.remove('tutorial-highlight');
    if (grupoUsername) grupoUsername.classList.remove('tutorial-highlight');
    if (grupoPedido) grupoPedido.classList.remove('tutorial-highlight');
    if (checkoutPhoneInput) checkoutPhoneInput.classList.remove('tutorial-highlight');
    if (btnPedido) btnPedido.classList.remove('tutorial-highlight');
  }

  function showTutorialStep(step) {
    hideAllTutorials();
    
    // Detect New Page (engajamento-novo.ejs)
    const isNewPage = !!document.getElementById('step1Container');
    
    if (isNewPage) {
        // Step 1: Serviço (Optional, no tutorial needed or handled by UI)
        // Step 2: Pacote (Optional, handled by UI)
        // Step 3: Perfil
        if (step === 3) {
            if (tutorial3Usuario) {
                tutorial3Usuario.style.display = 'block';
                // Try to find the username input
                const userInput = document.getElementById('usernameCheckoutInput');
                if (userInput) {
                    userInput.classList.add('tutorial-highlight');
                }
            }
        }
        // Step 4: Validação/Telefone
        if (step === 4) {
            if (tutorial4Validar) {
                tutorial4Validar.style.display = 'block';
                if (checkoutPhoneInput) {
                    checkoutPhoneInput.classList.add('tutorial-highlight');
                    try { checkoutPhoneInput.focus(); } catch(_) {}
                }
                const emailInput = document.getElementById('contactEmailInput');
                if (emailInput) {
                     emailInput.classList.add('tutorial-highlight');
                }
                
                // Highlight Card Fields if present in Step 4 context (requested by user)
                const cardFieldsStep4 = ['cardHolderName', 'cardHolderCpf', 'cardHolderBirth'];
                cardFieldsStep4.forEach(fid => {
                    const el = document.getElementById(fid);
                    if (el && !el.value) {
                        el.classList.add('tutorial-highlight');
                    }
                });
            }
        }
        // Step 5: Pagamento
        if (step === 5) {
            if (tutorial5Pedido) {
                tutorial5Pedido.style.display = 'block';
                
                // Highlight based on payment method
                if (typeof currentPaymentMethod !== 'undefined' && currentPaymentMethod === 'credit_card') {
                    const cardFields = [
                        'cardNumber', 'cardHolderName', 'cardHolderCpf', 'cardHolderBirth', 'cardExpiry', 'cardCvv',
                        'billingCep', 'billingStreet', 'billingNumber', 'billingNeighborhood', 'billingCity', 'billingState'
                    ];
                    let focused = false;
                    let hasEmpty = false;
                    for (const fid of cardFields) {
                        const el = document.getElementById(fid);
                        if (el) {
                            if (!el.value) {
                                el.classList.add('tutorial-highlight');
                                hasEmpty = true;
                                if (!focused) {
                                    try { el.focus(); focused = true; } catch(_) {}
                                }
                            } else {
                                el.classList.remove('tutorial-highlight');
                            }
                        }
                    }
                    
                    // Se todos preenchidos ou não focado, highlight no botão
                    if (!hasEmpty && btnPedido) {
                         btnPedido.classList.add('tutorial-highlight');
                    } else if (!focused && document.getElementById('cardNumber')) {
                         // Fallback focus
                         try { document.getElementById('cardNumber').focus(); } catch(_) {}
                    }
                } else {
                    if (btnPedido) {
                        btnPedido.classList.add('tutorial-highlight');
                    }
                }
            }
        }
        
        try { positionTutorials(); } catch(_) {}
        setTimeout(()=>{ try { positionTutorials(); } catch(_) {} }, 120);
        return;
    }

    switch (step) {
      case 1:
        const tutorialAudio = document.getElementById('tutorialAudio');
        if (tutorialAudio) tutorialAudio.style.display = 'block';
        try { positionTutorials(); } catch(_) {}
        setTimeout(()=>{ try { positionTutorials(); } catch(_) {} }, 120);
        break;
      case 2:
        const tutorialPlatform = document.getElementById('tutorialPlatform');
        if (tutorialPlatform) tutorialPlatform.style.display = 'block';
        try { positionTutorials(); } catch(_) {}
        setTimeout(()=>{ try { positionTutorials(); } catch(_) {} }, 120);
        break;
      case 3:
        if (tutorial2Pacote) tutorial2Pacote.style.display = 'block';
        if (grupoQuantidade) grupoQuantidade.classList.add('tutorial-highlight');
        break;
      case 4:
        if (isFollowersSelected()) {
          if (!tutorial3Suppressed) {
            if (tutorial3Usuario) tutorial3Usuario.style.display = 'block';
            if (grupoUsername) grupoUsername.classList.add('tutorial-highlight');
            try { positionTutorials(); } catch(_) {}
            setTimeout(() => { try { positionTutorials(); } catch(_) {} }, 120);
          }
        }
        break;
      case 5:
        const t5 = document.getElementById('tutorial5Pedido');
        if (t5) t5.style.display = 'block';
        if (btnPedido) btnPedido.classList.add('tutorial-highlight');
        
        // Highlight Card Fields if Credit Card is selected
        if (typeof currentPaymentMethod !== 'undefined' && currentPaymentMethod === 'credit_card') {
             const cardFields = [
                'cardNumber', 'cardExpiry', 'cardCvv', 
                'cardHolderName', 'cardHolderEmail', 'cardHolderCpf', 'cardHolderBirth',
                'billingCep', 'billingStreet', 'billingNumber', 'billingNeighborhood', 'billingCity', 'billingState'
             ];
             let focused = false;
             cardFields.forEach(id => {
                 const el = document.getElementById(id);
                 if (el && !el.value) {
                     el.classList.add('tutorial-highlight');
                     if (!focused) {
                         try { el.focus(); focused = true; } catch(_) {}
                     }
                 }
             });
        }

        try { positionTutorials(); } catch(_) {}
        setTimeout(() => { try { positionTutorials(); } catch(_) {} }, 120);

        // Add URL hash for GTM tracking (Initiate Checkout) - ONLY for /servicos-instagram
        if (window.location.pathname.includes('/servicos-instagram') && window.location.hash !== '#checkout') {
          history.pushState(null, null, '#checkout');
        }
        break;
      default:
        break;
    }
  }

  function positionTutorials() {
    try {
      const anchorEl = document.getElementById('audioSpeed15x');
      const audioTip = document.getElementById('tutorialAudio');
      const audioParent = anchorEl ? anchorEl.closest('.audio-controls') : null;
      if (audioTip && audioParent) {
        const manualLeft = audioTip.getAttribute('data-tip-left');
        const manualTop = audioTip.getAttribute('data-tip-top');
        const manualArrow = audioTip.getAttribute('data-tip-arrow-left');
        const mode = (audioTip.getAttribute('data-tip-mode') || 'auto').toLowerCase();
      const dx = parseFloat(audioTip.getAttribute('data-tip-dx') || '0') || 0;
      const dy = parseFloat(audioTip.getAttribute('data-tip-dy') || '0') || 0;
      if (manualLeft || manualTop) {
        if (manualLeft) audioTip.style.left = `${parseFloat(manualLeft) || 0}px`;
        if (manualTop) audioTip.style.top = `${parseFloat(manualTop) || 0}px`;
        if (manualArrow) audioTip.style.setProperty('--tip-arrow-left', `${parseFloat(manualArrow) || 12}px`);
        return;
      }
      if (mode === 'manual') {
        const parentRect = audioParent.getBoundingClientRect();
        const bubbleWidth = Math.max(180, audioTip.offsetWidth || 0);
        const parentWidth = audioParent.clientWidth || parentRect.width;
        let bubbleLeft = Math.max(0, Math.min(parentWidth - bubbleWidth, (parentWidth - bubbleWidth) / 2 + dx));
        const compTop = parseFloat((window.getComputedStyle(audioTip).top || '0').toString()) || 0;
        let bubbleTop = Math.max(0, compTop + dy);
        audioTip.style.left = `${bubbleLeft}px`;
        audioTip.style.top = `${bubbleTop}px`;
        const arrowLeft = Math.max(12, Math.min(bubbleWidth - 12, (bubbleWidth / 2)));
        audioTip.style.setProperty('--tip-arrow-left', `${arrowLeft}px`);
        return;
      }
      if (!anchorEl) return;
        const btnRect = anchorEl.getBoundingClientRect();
        const parentRect = audioParent.getBoundingClientRect();
        const leftRel = btnRect.left - parentRect.left;
        const topRel = btnRect.top - parentRect.top;
        const btnCenter = leftRel + (btnRect.width / 2);
        const bubbleWidth = Math.max(180, audioTip.offsetWidth || 0);
        const bubbleHeight = Math.max(48, audioTip.offsetHeight || 0);
        const parentWidth = audioParent.clientWidth || parentRect.width;
        let bubbleLeft = btnCenter - (bubbleWidth / 2);
        bubbleLeft = Math.max(0, Math.min(parentWidth - bubbleWidth, bubbleLeft));
        let bubbleTop = Math.max(0, topRel + btnRect.height + 8); // posiciona abaixo do botão 1.5x
        if (mode === 'auto') {
          bubbleLeft = bubbleLeft + dx;
          bubbleTop = bubbleTop + dy + 10;
        } else {
          bubbleTop = bubbleTop + 10;
        }
        audioTip.style.left = `${bubbleLeft}px`;
        audioTip.style.top = `${bubbleTop}px`;
        const arrowLeft = Math.max(12, Math.min(bubbleWidth - 12, btnCenter - bubbleLeft));
        audioTip.style.setProperty('--tip-arrow-left', `${arrowLeft}px`);
      }
    } catch(_) {}
    try {
      const platformTip = document.getElementById('tutorialPlatform');
      const instaBtn = document.querySelector('.platform-btn.instagram');
      const platformParent = document.querySelector('.platform-toggle');
      if (platformTip && instaBtn && platformParent) {
        const btnRect = instaBtn.getBoundingClientRect();
        const parentRect = platformParent.getBoundingClientRect();
        const leftRel = btnRect.left - parentRect.left + 4;
        const topRel = btnRect.top - parentRect.top;
        const btnCenter = leftRel + (btnRect.width / 2);
        const bubbleWidth = platformTip.offsetWidth || 220;
        const parentWidth = platformParent.clientWidth || parentRect.width;
        let bubbleLeft = btnCenter - (bubbleWidth / 2);
        bubbleLeft = Math.max(0, Math.min(parentWidth - bubbleWidth, bubbleLeft));
        const bubbleTop = Math.max(0, topRel + btnRect.height + 120);
        platformTip.style.left = `${bubbleLeft}px`;
        platformTip.style.top = `${bubbleTop}px`;
        const arrowLeft = Math.max(12, Math.min(bubbleWidth - 12, btnCenter - bubbleLeft));
      platformTip.style.setProperty('--tip-arrow-left', `${arrowLeft}px`);
      }
    } catch(_) {}
    try {
      const userTip = document.getElementById('tutorial3Usuario');
      const inputEl = document.getElementById('usernameCheckoutInput');
      const groupEl = document.getElementById('grupoUsername');
      if (userTip && inputEl && groupEl && userTip.style.display !== 'none') {
        const inputRect = inputEl.getBoundingClientRect();
        const groupRect = groupEl.getBoundingClientRect();
        const leftRel = inputRect.left - groupRect.left;
        const topRel = inputRect.top - groupRect.top;
        const center = leftRel + (inputRect.width / 2);
        const bubbleWidth = userTip.offsetWidth || 220;
        let bubbleLeft = center - (bubbleWidth / 2);
        bubbleLeft = Math.max(8, Math.min(groupEl.clientWidth - bubbleWidth - 8, bubbleLeft));
        const bubbleTop = Math.max(0, topRel + inputRect.height + 28);
        userTip.style.left = `${bubbleLeft}px`;
        userTip.style.top = `${bubbleTop}px`;
        const arrowLeft = Math.max(12, Math.min(bubbleWidth - 12, center - bubbleLeft));
        userTip.style.setProperty('--tip-arrow-left', `${arrowLeft}px`);
      }
    } catch(_) {}
    try {
      const phoneTip = document.getElementById('tutorial4Validar');
      const phoneInput = document.getElementById('checkoutPhoneInput');
      const phoneField = phoneInput ? phoneInput.closest('.phone-field') : null;
      if (phoneTip && phoneInput && phoneField && phoneTip.style.display !== 'none') {
        const inputRect = phoneInput.getBoundingClientRect();
        const fieldRect = phoneField.getBoundingClientRect();
        const leftRel = inputRect.left - fieldRect.left;
        const topRel = inputRect.top - fieldRect.top;
        const bubbleWidth = phoneTip.offsetWidth || 220;
        const fieldWidth = phoneField.clientWidth || fieldRect.width;
        const bubbleLeft = Math.max(8, Math.min(fieldWidth - bubbleWidth - 8, leftRel));
        const isMobile = (window.innerWidth || 0) <= 640;
        const extraOffset = isMobile ? 130 : 100;
        const bubbleTop = Math.max(0, topRel + inputRect.height + extraOffset);
        phoneTip.style.left = `${bubbleLeft}px`;
        phoneTip.style.top = `${bubbleTop}px`;
        const arrowLeft = Math.max(12, Math.min(bubbleWidth - 12, 14));
        phoneTip.style.setProperty('--tip-arrow-left', `${arrowLeft}px`);
      }
    } catch(_) {}
    try {
      const confirmTip = document.getElementById('tutorial5Pedido');
      const confirmBtn = document.getElementById('realizarPedidoBtn');
      const btnContainer = confirmBtn ? confirmBtn.closest('.button-container') : null;
      if (confirmTip && confirmBtn && btnContainer && confirmTip.style.display !== 'none') {
        const btnRect = confirmBtn.getBoundingClientRect();
        const contRect = btnContainer.getBoundingClientRect();
        const leftRel = btnRect.left - contRect.left;
        const topRel = btnRect.top - contRect.top;
        const bubbleWidth = confirmTip.offsetWidth || 220;
        const contWidth = btnContainer.clientWidth || contRect.width;
        const bubbleLeft = Math.max(8, Math.min(contWidth - bubbleWidth - 8, leftRel));
        const bubbleTop = Math.max(0, topRel + btnRect.height + 52);
        confirmTip.style.left = `${bubbleLeft}px`;
        confirmTip.style.top = `${bubbleTop}px`;
        const arrowLeft = Math.max(12, Math.min(bubbleWidth - 12, 14));
        confirmTip.style.setProperty('--tip-arrow-left', `${arrowLeft}px`);
      }
    } catch(_) {}
  }

  window.addEventListener('resize', () => { try { positionTutorials(); } catch(_) {} });
  window.addEventListener('load', () => {
    try {
      positionTutorials();
      setTimeout(positionTutorials, 100);
      setTimeout(positionTutorials, 300);
      enableTipDrag();
      updateWarrantyVisibility();
      try {
        const isMobile = window.innerWidth <= 640;
        if (isMobile) {
          const phoneTipInit = document.getElementById('tutorial4Validar');
          const phoneInputInit = document.getElementById('checkoutPhoneInput');
          const phoneFieldInit = phoneInputInit ? phoneInputInit.closest('.phone-field') : null;
          if (phoneTipInit && phoneFieldInit) {
            phoneTipInit.classList.remove('hide');
            phoneTipInit.style.display = 'block';
          }
        }
      } catch(_) {}
    } catch(_) {}
  });
  window.addEventListener('resize', () => { try { positionTutorials(); } catch(_) {} });
  window.addEventListener('orientationchange', () => { try { positionTutorials(); } catch(_) {} });

  function clearResumo() {
    if (resumo) resumo.hidden = true;
    if (resTipo) resTipo.textContent = '';
    if (resQtd) resQtd.textContent = '';
    if (resPreco) resPreco.textContent = '';
  }

  function popularQuantidades(tipo) {
    qtdSelect.innerHTML = '';
    const pricingKey = getPricingTipoKey(tipo);
    if (!tipo || !tabela[pricingKey]) {
      qtdSelect.disabled = true;
      const opt = document.createElement('option');
      opt.value = '';
      opt.textContent = 'Selecione o tipo primeiro...';
      qtdSelect.appendChild(opt);
      clearResumo();
      return;
    }
    qtdSelect.disabled = false;
    let opts = tabela[pricingKey];
    if (isFollowersTipo(tipo) && getServiceCategory() === 'seguidores') {
      const allowed = getAllowedQuantities(tipo);
      opts = opts.filter(x => allowed.includes(Number(x.q)));
    }
    const placeholder = document.createElement('option');
    placeholder.value = '';
    placeholder.textContent = 'Selecione a quantidade...';
    qtdSelect.appendChild(placeholder);
    for (const item of opts) {
      const o = document.createElement('option');
      o.value = String(item.q);
      // Evita caracteres Unicode não permitidos pela API (substitui travessão por hífen)
      o.textContent = `${item.q} ${getUnitForTipo(tipo)} - ${item.p}`;
      o.dataset.preco = item.p;
      qtdSelect.appendChild(o);
    }
  }

  function isPostSelected() {
    const selectedOption = tipoSelect.options[tipoSelect.selectedIndex];
    return !!(selectedOption && /(curtidas|visualiza\u00e7oes|visualizações)/i.test(selectedOption.textContent));
  }

  function updateWarrantyVisibility() {
    const tipo = (tipoSelect && tipoSelect.value) || '';
    const inp = document.getElementById('promoWarranty60');
    const item = inp ? inp.closest('.promo-item') : null;
    if (!item) return;
    const show = (tipo === 'mistos' || tipo === 'brasileiros');
    item.style.display = show ? '' : 'none';
    if (!show && inp) { inp.checked = false; }
    try { updatePromosSummary(); } catch(_) {}
  }

  function computePostFieldsCount(tipo, qtd) {
    if (tipo === 'curtidas_brasileiras') {
      if (qtd >= 1000) return 3;
      if (qtd >= 500) return 2;
      return 1;
    }
    if (tipo === 'visualizacoes_reels') {
      if (qtd >= 100000) return 3;
      if (qtd >= 25000) return 2;
      return 1;
    }
    return 0;
  }

  function renderPostLinksCarousel(count) {
    if (!postCarouselContainer || !carouselTrack || !carouselViewport || !carouselIndicators) return;
    // Reset estrutura
    carouselTrack.innerHTML = '';
    carouselIndicators.innerHTML = '';
    carouselIndex = 0;
    slideCount = count;
    if (count <= 0) { postCarouselContainer.style.display = 'none'; return; }
    postCarouselContainer.style.display = 'block';
    // Dimensões: gap 12px; viewport = largura do card; exibe ~1.5 slides
    const gap = 12;
    const viewportWidth = carouselViewport.clientWidth || (postLinksCard ? postLinksCard.clientWidth : 420);
    const slideWidth = Math.max(240, Math.round((viewportWidth - gap) / 1.5));

    for (let i = 0; i < count; i++) {
      const slide = document.createElement('div');
      slide.className = 'carouselSlide';
      slide.style.width = `${slideWidth}px`;
      slide.style.flex = `0 0 ${slideWidth}px`;
      slide.style.background = 'transparent';
      slide.style.borderRadius = '8px';
      slide.style.padding = '0';

      const input = document.createElement('input');
      input.type = 'text';
      input.placeholder = `Link do post ${i+1}`;
      input.className = 'select-input';
      input.id = `postLinkInput_${i+1}`;

      const actions = document.createElement('div');
      actions.style.display = 'flex';
      actions.style.alignItems = 'center';
      actions.style.justifyContent = 'space-between';
      actions.style.marginTop = '0.5rem';

      const validateBtn = document.createElement('button');
      validateBtn.type = 'button';
      validateBtn.className = 'continue-button';
      validateBtn.style.padding = '0.4rem 0.75rem';
      validateBtn.innerHTML = '<span class="button-text">Validar</span>';

      const msg = document.createElement('div');
      msg.style.fontSize = '0.85rem';
      msg.style.marginLeft = '0.5rem';
      msg.style.color = '#fff';
      msg.textContent = '';

      actions.appendChild(validateBtn);
      actions.appendChild(msg);

      slide.appendChild(input);
      slide.appendChild(actions);
      carouselTrack.appendChild(slide);

      // Indicadores
      const dot = document.createElement('span');
      dot.style.width = '8px';
      dot.style.height = '8px';
      dot.style.borderRadius = '50%';
      dot.style.display = 'inline-block';
      dot.style.background = i === 0 ? '#fff' : '#777';
      dot.style.cursor = 'pointer';
      dot.addEventListener('click', () => {
        carouselIndex = i;
        updateCarousel(slideWidth, gap);
      });
      carouselIndicators.appendChild(dot);

      // Validação individual do slide
      validateBtn.addEventListener('click', () => {
        const normalized = normalizePostLink(input.value);
        if (normalized && isValidPostLink(normalized)) {
          slide.dataset.valid = 'true';
          slide.dataset.link = normalized;
          msg.textContent = 'Link válido';
          msg.style.color = '#b8ffb8';
        } else {
          slide.dataset.valid = 'false';
          slide.dataset.link = '';
          msg.textContent = 'Link inválido';
          msg.style.color = '#ffb4b4';
        }
        currentPostLinks = collectValidPostLinks();
        isPostsValidated = currentPostLinks.length > 0;
        updatePedidoButtonState();
      });
    }

    updateCarousel(slideWidth, gap);
  }

  function updateCarousel(slideWidth = Math.max(240, Math.round((carouselViewport.clientWidth - 12) / 1.5)), gap = 12) {
    const offset = carouselIndex * (slideWidth + gap);
    carouselTrack.style.transform = `translateX(-${offset}px)`;
    // Atualiza indicadores
    const dots = carouselIndicators.children;
    for (let i = 0; i < dots.length; i++) {
      const el = dots[i];
      el.style.background = i === carouselIndex ? '#fff' : '#777';
    }
    // Habilitar/desabilitar setas
    if (carouselPrev) carouselPrev.disabled = carouselIndex === 0;
    if (carouselNext) carouselNext.disabled = carouselIndex >= (slideCount - 1);
  }

  function collectValidPostLinks() {
    const links = [];
    if (!carouselTrack) return links;
    const slides = carouselTrack.querySelectorAll('.carouselSlide');
    slides.forEach(slide => {
      if (slide.dataset.valid === 'true' && slide.dataset.link) {
        links.push(slide.dataset.link);
      }
    });
    return links;
  }

  function showStatusPostsMessage(msg, type = 'info') {
    if (!statusPostsMessage) return;
    statusPostsMessage.textContent = msg;
    statusPostsMessage.style.display = 'block';
    statusPostsMessage.style.color = type === 'error' ? '#ffb4b4' : (type === 'success' ? '#b8ffb8' : '#ffffff');
  }

  function hideStatusPostsMessage() { /* removido: mensagens por slide */ }

  function clearPostsInputs() {
    if (postCarouselContainer) {
      postCarouselContainer.style.display = 'none';
    }
    if (carouselTrack) {
      carouselTrack.innerHTML = '';
    }
    if (carouselIndicators) {
      carouselIndicators.innerHTML = '';
    }
    carouselIndex = 0;
    slideCount = 0;
  }

  if (tipoSelect) tipoSelect.addEventListener('change', () => {
    const tipo = tipoSelect.value;
    popularQuantidades(tipo);
    clearResumo();
    updatePerfilVisibility();
    if (tipo) {
      showTutorialStep(3);
    } else {
      showTutorialStep(1);
    }
    renderPlanCards(tipo);
    updateWarrantyVisibility();
    try { if (typeof updateLikesPrice === 'function') updateLikesPrice(Number(document.getElementById('likesQty')?.textContent || 150)); } catch(_) {}
    try { if (typeof updateViewsPrice === 'function') updateViewsPrice(Number(document.getElementById('viewsQty')?.textContent || 1000)); } catch(_) {}
  });

  if (qtdSelect) qtdSelect.addEventListener('change', () => {
    const tipo = tipoSelect.value;
    const qtd = qtdSelect.value;
    const opt = qtdSelect.options[qtdSelect.selectedIndex];
    const preco = opt ? (opt.dataset.preco || '') : '';
    if (!tipo || !qtd) {
      clearResumo();
      return;
    }
    resTipo.textContent = String(tipo).replace(/_/g, ' ');
    resQtd.textContent = `${qtd} ${getUnitForTipo(tipo)}`;
    const baseStr = String(preco).replace(/[^0-9,\.]/g, '');
    let base = 0; try { base = parseFloat(baseStr.replace('.', '').replace(',', '.')); } catch(_) {}
    const inc = base * 1.15;
    const ceilInt = Math.ceil(inc);
    const increasedRounded = (ceilInt - 0.10);
    const increasedText = `R$ ${increasedRounded.toFixed(2).replace('.', ',')}`;
    resPreco.textContent = preco;
    try { basePriceCents = parsePrecoToCents(preco); } catch(_) { basePriceCents = 0; }
    showResumoIfAllowed();
    try { sessionStorage.setItem('oppus_qtd', String(qtd || '')); } catch(_) {}
    updatePedidoButtonState();
    updatePerfilVisibility();
    updatePromosSummary();
    updateWarrantyVisibility();
    showTutorialStep(4);
    try {
      const isMobile = window.innerWidth <= 640;
      if (isMobile) {
        if (perfilCard) perfilCard.style.display = 'block';
        const target = document.getElementById('grupoUsername') || perfilCard;
        if (target) {
          try {
             const rect = target.getBoundingClientRect();
             const offset = 340;
             const top = (window.scrollY || window.pageYOffset || 0) + rect.top - offset;
             window.scrollTo({ top, behavior: 'smooth' });
          } catch(_) {
             if (typeof target.scrollIntoView === 'function') target.scrollIntoView({ behavior: 'smooth', block: 'center' });
          }
        }
      }
    } catch(_) {}
  });

  const likesPromoTables = {
    mistos: [
      { q: 150, price: 'R$ 4,90' },
      { q: 300, price: 'R$ 9,90' },
      { q: 500, price: 'R$ 14,90' },
      { q: 700, price: 'R$ 19,90' },
      { q: 1000, price: 'R$ 24,90' },
      { q: 2000, price: 'R$ 34,90' },
      { q: 3000, price: 'R$ 49,90' },
      { q: 4000, price: 'R$ 59,90' },
      { q: 5000, price: 'R$ 69,90' },
      { q: 7500, price: 'R$ 89,90' },
      { q: 10000, price: 'R$ 109,90' },
      { q: 15000, price: 'R$ 159,90' }
    ],
    brasileiros: [
      { q: 150, price: 'R$ 5,90' },
      { q: 300, price: 'R$ 9,90' },
      { q: 500, price: 'R$ 14,90' },
      { q: 700, price: 'R$ 29,90' },
      { q: 1000, price: 'R$ 39,90' },
      { q: 2000, price: 'R$ 49,90' },
      { q: 3000, price: 'R$ 59,90' },
      { q: 4000, price: 'R$ 69,90' },
      { q: 5000, price: 'R$ 79,90' },
      { q: 7500, price: 'R$ 109,90' },
      { q: 10000, price: 'R$ 139,90' },
      { q: 15000, price: 'R$ 199,90' }
    ],
    organicos: [
      { q: 150, price: 'R$ 16,90' },
      { q: 300, price: 'R$ 28,90' },
      { q: 500, price: 'R$ 49,90' },
      { q: 1000, price: 'R$ 69,90' },
      { q: 2000, price: 'R$ 104,90' },
      { q: 3000, price: 'R$ 139,90' },
      { q: 4000, price: 'R$ 174,90' },
      { q: 5000, price: 'R$ 224,90' },
      { q: 7500, price: 'R$ 279,90' },
      { q: 10000, price: 'R$ 349,90' },
      { q: 15000, price: 'R$ 449,90' }
    ]
  };
  const likesQtyEl = document.getElementById('likesQty');
  const likesDec = document.getElementById('likesDec');
  const likesInc = document.getElementById('likesInc');
  const likesPrices = document.querySelector('.promo-prices[data-promo="likes"]');
  function formatCurrencyBR(n) { return `R$ ${n.toFixed(2).replace('.', ',')}`; }
  function parseCurrencyBR(s) { const cleaned = String(s).replace(/[R$\s]/g, '').replace('.', '').replace(',', '.'); const val = parseFloat(cleaned); return isNaN(val) ? 0 : val; }
  function getLikesPromoVariant() {
    const cat = getServiceCategory();
    if (cat === 'visualizacoes') return 'organicos';
    const base = String((tipoSelect && tipoSelect.value) || '').toLowerCase();
    if (base.includes('organicos')) return 'organicos';
    if (base.includes('brasileiros') || base.includes('curtidas_brasileiras')) return 'brasileiros';
    return 'mistos';
  }
  function getLikesPromoTable() {
    const v = getLikesPromoVariant();
    return likesPromoTables[v] || likesPromoTables.mistos;
  }
  function findNearestQtyEntry(table, qty) {
    try {
      const arr = Array.isArray(table) ? table : [];
      const q = Number(qty) || 0;
      if (!arr.length) return null;
      let best = arr[0];
      let bestDist = Math.abs(Number(best.q) - q);
      for (let i = 1; i < arr.length; i++) {
        const it = arr[i];
        const d = Math.abs(Number(it.q) - q);
        if (d < bestDist) { best = it; bestDist = d; }
      }
      return best;
    } catch (_) { return null; }
  }
  function updateLikesPrice(q) {
    const table = getLikesPromoTable();
    let entry = table.find(e => e.q === q);
    if (!entry) entry = findNearestQtyEntry(table, q);
    if (!entry) return;
    if (likesQtyEl) likesQtyEl.textContent = String(entry.q);
    const newEl = likesPrices ? likesPrices.querySelector('.new-price') : null;
    const oldEl = likesPrices ? likesPrices.querySelector('.old-price') : null;
    if (newEl) newEl.textContent = entry.price;
    if (oldEl) { const newVal = parseCurrencyBR(entry.price); const oldVal = newVal * 1.70; oldEl.textContent = formatCurrencyBR(oldVal); }
    const hl = document.querySelector('.promo-item.likes .promo-highlight');
    if (hl) hl.textContent = `+ ${entry.q} CURTIDAS`;
  }
  function stepLikes(dir) {
    const current = Number(likesQtyEl?.textContent || 150);
    const table = getLikesPromoTable();
    let idx = table.findIndex(e => e.q === current);
    if (idx < 0) {
      const nearest = findNearestQtyEntry(table, current);
      idx = nearest ? table.findIndex(e => e.q === nearest.q) : 0;
    }
    let nextIdx = idx >= 0 ? idx + dir : 0;
    if (nextIdx < 0) nextIdx = 0;
    if (nextIdx >= table.length) nextIdx = table.length - 1;
    const next = table[nextIdx].q;
    if (likesQtyEl) likesQtyEl.textContent = String(next);
    updateLikesPrice(next);
    try { updatePromosSummary(); } catch(_) {}
  }
  if (likesDec) likesDec.addEventListener('click', (e) => { if (e && typeof e.stopPropagation==='function') e.stopPropagation(); stepLikes(-1); });
  if (likesInc) likesInc.addEventListener('click', (e) => { if (e && typeof e.stopPropagation==='function') e.stopPropagation(); stepLikes(1); });
  if (likesQtyEl) updateLikesPrice(Number(likesQtyEl.textContent || 150));

  // Balão indicativo nas curtidas (mesma estética dos outros balões)
  (function setupLikesHint(){
    const likesControl = document.getElementById('likesControl');
    const likesTip = document.getElementById('likesPromoTip');
    if (!likesControl || !likesTip) return;
    let shown = false;
    function showTip(){
      if (shown) return;
      likesTip.style.display = 'flex';
      shown = true;
    }
    const io = new IntersectionObserver((entries)=>{
      entries.forEach(e=>{ if (e.isIntersecting) showTip(); });
    }, { threshold: 0.2 });
    io.observe(likesControl);
    // Fallback: mostrar após curto atraso ao carregar a página
    setTimeout(()=>{ if (!shown) showTip(); }, 1500);
  })();

  // Slider de Visualizações (Order Bump)
  const viewsTable = [
    { q: 1000, price: 'R$ 4,90' },
    { q: 2500, price: 'R$ 9,90' },
    { q: 5000, price: 'R$ 14,90' },
    { q: 10000, price: 'R$ 19,90' },
    { q: 25000, price: 'R$ 24,90' },
    { q: 50000, price: 'R$ 34,90' },
    { q: 100000, price: 'R$ 49,90' },
    { q: 150000, price: 'R$ 59,90' },
    { q: 200000, price: 'R$ 69,90' },
    { q: 250000, price: 'R$ 89,90' },
    { q: 500000, price: 'R$ 109,90' },
    { q: 1000000, price: 'R$ 159,90' }
  ];
  const viewsQtyEl = document.getElementById('viewsQty');
  const viewsDec = document.getElementById('viewsDec');
  const viewsInc = document.getElementById('viewsInc');
  const viewsPrices = document.querySelector('.promo-prices[data-promo="views"]');
  function formatCurrencyBR(n) {
    return `R$ ${n.toFixed(2).replace('.', ',')}`;
  }
  function parseCurrencyBR(s) {
    const cleaned = String(s).replace(/[R$\s]/g, '').replace('.', '').replace(',', '.');
    const val = parseFloat(cleaned);
    return isNaN(val) ? 0 : val;
  }
  function updateViewsPrice(q) {
    const entry = viewsTable.find(e => e.q === q);
    const newEl = viewsPrices ? viewsPrices.querySelector('.new-price') : null;
    const oldEl = viewsPrices ? viewsPrices.querySelector('.old-price') : null;
    if (newEl && entry) newEl.textContent = entry.price;
    // Calcular preço cortado (antes) assumindo 30% off
    if (oldEl && entry) {
      const newVal = parseCurrencyBR(entry.price);
      const oldVal = newVal / 0.7;
      oldEl.textContent = formatCurrencyBR(oldVal);
    }
    const hl = document.querySelector('.promo-item.views .promo-highlight');
    if (hl) hl.textContent = `+ ${q} VISUALIZAÇÕES`;
  }
  function stepViews(dir) {
    const current = Number(viewsQtyEl?.textContent || 1000);
    const idx = viewsTable.findIndex(e => e.q === current);
    let nextIdx = idx >= 0 ? idx + dir : 0;
    if (nextIdx < 0) nextIdx = 0;
    if (nextIdx >= viewsTable.length) nextIdx = viewsTable.length - 1;
    const next = viewsTable[nextIdx].q;
    if (viewsQtyEl) viewsQtyEl.textContent = String(next);
    updateViewsPrice(next);
    try { updatePromosSummary(); } catch(_) {}
  }
  if (viewsDec) viewsDec.addEventListener('click', (e) => { if (e && typeof e.stopPropagation==='function') e.stopPropagation(); stepViews(-1); });
  if (viewsInc) viewsInc.addEventListener('click', (e) => { if (e && typeof e.stopPropagation==='function') e.stopPropagation(); stepViews(1); });
  if (viewsQtyEl) updateViewsPrice(Number(viewsQtyEl.textContent || 1000));

  function getPostModalRefs(){
    return {
      postModal: document.getElementById('postSelectModal'),
      postModalGrid: document.getElementById('postModalGrid'),
      postModalTitle: document.getElementById('postModalTitle'),
      postModalClose: document.getElementById('postModalClose'),
    };
  }
  let postModalOpenLock = false;
  let suppressOpenPostModalOnce = false;
  let cachedPosts = null;
  let cachedPostsUser = '';
  function ensureSpinnerCSS(){
    if (document.getElementById('oppusSpinnerStyles')) return;
    const style = document.createElement('style');
    style.id = 'oppusSpinnerStyles';
    style.textContent = "@keyframes oppusSpin{from{transform:rotate(0deg)}to{transform:rotate(360deg)}} .oppus-spinner{width:32px;height:32px;border:4px solid rgba(255,255,255,0.25);border-top-color:#7c3aed;border-radius:50%;animation:oppusSpin 1s linear infinite} .oppus-spinner-wrap{grid-column:1/-1;display:flex;justify-content:center;align-items:center;gap:8px;padding:24px;color:var(--text-secondary)}";
    document.head.appendChild(style);
  }
  function spinnerHTML(){ ensureSpinnerCSS(); return '<div class="oppus-spinner-wrap"><div class="oppus-spinner"></div><span>Carregando...</span></div>'; }
  const openPostBtns = Array.from(document.querySelectorAll('.open-post-modal-btn'));
  function openPostModal(kind){
    if (postModalOpenLock) return;
    postModalOpenLock = true;
    setTimeout(function(){ postModalOpenLock = false; }, 600);
    const refs = getPostModalRefs();
    if (!refs.postModal || !refs.postModalGrid) return;
    const user = (checkoutProfileUsername && checkoutProfileUsername.textContent && checkoutProfileUsername.textContent.trim()) || '';
    if (refs.postModalTitle) refs.postModalTitle.textContent = kind === 'views' ? 'Selecionar reels' : 'Selecionar post';
    try {
      if (refs.postModal.parentNode !== document.body) {
        document.body.appendChild(refs.postModal);
      }
    } catch(_) {}
    try { document.body.style.overflow = 'hidden'; } catch(_) {}
    refs.postModal.style.display = 'flex';
    try {
      const dlg = refs.postModal.querySelector('.modal-dialog');
      if (dlg && typeof dlg.scrollIntoView === 'function') { dlg.scrollIntoView({ block: 'center', inline: 'center' }); }
    } catch(_) {}
    if (isInstagramPrivate) {
      refs.postModalGrid.innerHTML = '<div class="inline-msg" style="grid-column:1/-1;color:#ef4444;">Deixe o perfil no modo público para selecionar o post</div>' + spinnerHTML();
    } else {
      refs.postModalGrid.innerHTML = spinnerHTML();
    }
    const renderFrom = function(arr){
      const items = (Array.isArray(arr) ? arr : []).filter(p => {
        if (kind === 'views') return !!p.isVideo || (String(p.typename||'').toLowerCase().includes('video') || String(p.typename||'').toLowerCase().includes('clip'));
        return true;
      }).slice(0, 8);
      const html = items.map(function(p){
        const dsrc = p.displayUrl ? ('/image-proxy?url=' + encodeURIComponent(p.displayUrl)) : null;
        const vsrc = p.videoUrl ? ('/image-proxy?url=' + encodeURIComponent(p.videoUrl)) : null;
        const media = (dsrc)
          ? ('<div class="media-frame"><img src="'+dsrc+'" loading="lazy" decoding="async"/></div>')
          : (p.isVideo && vsrc
            ? ('<div class="media-frame"><video data-src="'+vsrc+'" muted playsinline preload="none"></video></div>')
            : ('<div class="media-frame"><iframe src="https://www.instagram.com/p/'+p.shortcode+'/embed" loading="lazy" allowtransparency="true" allow="encrypted-media; picture-in-picture" scrolling="no"></iframe></div>'));
        return '<div class="service-card"><div class="card-content pick-post-card" data-kind="'+kind+'" data-shortcode="'+p.shortcode+'">'+media+'<div class="inline-msg" style="margin-top:6px">'+(p.takenAt? new Date(Number(p.takenAt)*1000).toLocaleString('pt-BR') : '-')+'</div><div style="margin-top:8px;display:flex;justify-content:center;align-items:center;"><button type="button" class="continue-button select-post-btn" style="width:100%; text-align:center;" data-shortcode="'+p.shortcode+'" data-kind="'+kind+'">Selecionar</button></div></div></div>';
      }).join('');
      if (!html) {
          const manualHtml = `
            <div style="grid-column:1/-1; text-align:center; padding: 1rem;">
                <p style="margin-bottom:0.5rem; color:var(--text-secondary);">Não conseguimos carregar os posts automaticamente.</p>
                <div style="display:flex; gap:0.5rem; max-width:400px; margin:0 auto;">
                    <input type="text" id="manualPostLinkInput" placeholder="Cole o link do post aqui..." style="flex:1; padding:0.6rem; border-radius:6px; border:1px solid var(--border-color); background:var(--bg-primary); color:var(--text-primary);" />
                    <button type="button" id="manualPostLinkBtn" class="continue-button" style="padding:0.6rem 1rem;">Usar Link</button>
                </div>
                <div id="manualLinkMsg" style="margin-top:0.5rem; font-size:0.9rem;"></div>
            </div>
          `;
          refs.postModalGrid.innerHTML = manualHtml;
          setTimeout(() => {
              const btn = document.getElementById('manualPostLinkBtn');
              const inp = document.getElementById('manualPostLinkInput');
              const msg = document.getElementById('manualLinkMsg');
              if(btn && inp) {
                  btn.addEventListener('click', () => {
                      const val = inp.value.trim();
                      if(!val || !val.includes('instagram.com/')) {
                          if(msg) { msg.textContent = 'Link inválido'; msg.style.color = '#ff4444'; }
                          return;
                      }
                      let sc = '';
                      const m = val.match(/\/(?:p|reel)\/([A-Za-z0-9_-]+)/);
                      if(m) sc = m[1];
                      if(!sc) {
                           if(msg) { msg.textContent = 'Link inválido (não foi possível extrair ID)'; msg.style.color = '#ff4444'; }
                           return;
                      }
                      const user2 = (checkoutProfileUsername && checkoutProfileUsername.textContent && checkoutProfileUsername.textContent.trim()) || '';
                      fetch('/api/instagram/select-post-for', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ username: user2, shortcode: sc, kind: kind }) })
                        .then(r=>r.json())
                        .then(function(){ 
                            if(msg) { msg.textContent = 'Link selecionado!'; msg.style.color = '#44ff44'; }
                            setTimeout(() => {
                                const refs = getPostModalRefs(); 
                                if(refs.postModal) refs.postModal.style.display='none';
                                try { document.body.style.overflow=''; } catch(_) {}
                            }, 500);
                        });
                  });
              }
          }, 100);
      } else {
          refs.postModalGrid.innerHTML = html;
      }
      const highlightSelected = function(kind, sc){ try{ const cards = Array.from(refs.postModalGrid.querySelectorAll('.card-content')); cards.forEach(function(c){ c.classList.remove('selected-mark'); }); const target = refs.postModalGrid.querySelector('.card-content[data-shortcode="'+sc+'"]'); if (target) target.classList.add('selected-mark'); }catch(_){} };
      Array.from(refs.postModalGrid.querySelectorAll('.select-post-btn')).forEach(function(btn){
        btn.addEventListener('click', function(){
          const sc = this.getAttribute('data-shortcode');
          const k = this.getAttribute('data-kind');
          const user2 = (checkoutProfileUsername && checkoutProfileUsername.textContent && checkoutProfileUsername.textContent.trim()) || '';
          fetch('/api/instagram/select-post-for', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ username: user2, shortcode: sc, kind: k }) })
            .then(r=>r.json())
            .then(function(){ highlightSelected(k, sc); });
        });
      });
      Array.from(refs.postModalGrid.querySelectorAll('.pick-post-card')).forEach(function(card){
        card.addEventListener('click', function(){
          const sc = this.getAttribute('data-shortcode');
          const k = this.getAttribute('data-kind');
          const user2 = (checkoutProfileUsername && checkoutProfileUsername.textContent && checkoutProfileUsername.textContent.trim()) || '';
          fetch('/api/instagram/select-post-for', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ username: user2, shortcode: sc, kind: k }) })
            .then(r=>r.json())
            .then(function(){ highlightSelected(k, sc); });
        });
      });
      try { fetch('/api/instagram/selected-for').then(r=>r.json()).then(function(d){ const obj = d && d.selectedFor ? d.selectedFor : {}; const cur = obj[kind]; if (cur && cur.shortcode) { highlightSelected(kind, cur.shortcode); } }); } catch(_) {}
    };
    const useCache = !!cachedPosts && cachedPostsUser === user;
    if (useCache) {
      renderFrom(cachedPosts);
    } else {
      const url = '/api/instagram/posts?username=' + encodeURIComponent(user);
      refs.postModalGrid.innerHTML = isInstagramPrivate ? ('<div class="inline-msg" style="grid-column:1/-1;color:#ef4444;">Deixe o perfil no modo público para selecionar o post</div>' + spinnerHTML()) : spinnerHTML();
      fetch(url).then(r=>r.json()).then(d=>{
        const arr = Array.isArray(d.posts) ? d.posts : [];
        cachedPosts = arr; cachedPostsUser = user;
        renderFrom(arr);
      }).catch(function(){ const refs3 = getPostModalRefs(); if(refs3.postModalGrid) refs3.postModalGrid.innerHTML = '<div style="grid-column:1/-1;color:#ef4444;">'+(isInstagramPrivate?'Deixe o perfil no modo público para selecionar o post':'Erro ao carregar posts.')+'</div>'; });
    }
  }
  (function(){ const { postModalClose } = getPostModalRefs(); if (postModalClose) postModalClose.addEventListener('click', function(){ const refs = getPostModalRefs(); if(refs.postModal) { refs.postModal.style.display='none'; try { document.body.style.overflow=''; } catch(_) {} } }); })();
  (function(){ const refs = getPostModalRefs(); const btn = document.getElementById('postModalClose2'); if (btn) btn.addEventListener('click', function(){ const r = getPostModalRefs(); if(r.postModal) { r.postModal.style.display='none'; try { document.body.style.overflow=''; } catch(_) {} } }); })();
  (function(){ const refs = getPostModalRefs(); if (refs.postModal) refs.postModal.addEventListener('click', function(e){ if (e.target === refs.postModal) { refs.postModal.style.display = 'none'; try { document.body.style.overflow=''; } catch(_) {} } }); })();
  (function(){ document.addEventListener('keydown', function(e){ if (e.key === 'Escape') { const refs = getPostModalRefs(); if (refs.postModal && refs.postModal.style.display !== 'none') { refs.postModal.style.display = 'none'; try { document.body.style.overflow=''; } catch(_) {} } } }); })();
  openPostBtns.forEach(function(btn){ btn.addEventListener('click', function(){ const k = this.getAttribute('data-kind'); openPostModal(k); }); });
  const promoLikes = document.getElementById('promoLikes');
  const promoViews = document.getElementById('promoViews');
  const promoComments = document.getElementById('promoComments');
  if (promoLikes) promoLikes.addEventListener('change', function(){ if (suppressOpenPostModalOnce) { suppressOpenPostModalOnce=false; return; } if (this.checked) openPostModal('likes'); });
  if (promoViews) promoViews.addEventListener('change', function(){ if (suppressOpenPostModalOnce) { suppressOpenPostModalOnce=false; return; } if (this.checked) openPostModal('views'); });
  if (promoComments) promoComments.addEventListener('change', function(){ if (suppressOpenPostModalOnce) { suppressOpenPostModalOnce=false; return; } if (this.checked) openPostModal('comments'); });
  // Também abrir ao clicar na área do card após marcar
  ['likes','views','comments'].forEach(function(kind){
    const label = document.querySelector('label.promo-item.'+kind);
    if (label) label.addEventListener('click', function(e){
      if (e && e.target && e.target.closest('.likes-control')) return;
      const input = document.getElementById(kind==='likes'?'promoLikes':(kind==='views'?'promoViews':'promoComments'));
      setTimeout(function(){ if (suppressOpenPostModalOnce) { suppressOpenPostModalOnce=false; return; } if (input && input.checked) openPostModal(kind); }, 0);
    });
    const priceBlock = document.querySelector('.promo-prices[data-promo="'+(kind==='likes'?'likes':(kind==='views'?'views':'comments'))+'"]');
    if (priceBlock) priceBlock.addEventListener('click', function(e){ e.stopPropagation(); const input = document.getElementById(kind==='likes'?'promoLikes':(kind==='views'?'promoViews':'promoComments')); if (input && input.checked) openPostModal(kind); });
  });
  try {
    const likesLabel = document.querySelector('label.promo-item.likes');
    const viewsLabel = document.querySelector('label.promo-item.views');
    const commentsLabel = document.querySelector('label.promo-item.comments');
    if (likesLabel) likesLabel.addEventListener('click', function(e){ if (e && e.target && e.target.closest('.likes-control')) return; const input = document.getElementById('promoLikes'); setTimeout(function(){ if (suppressOpenPostModalOnce) { suppressOpenPostModalOnce=false; return; } if (input && input.checked) openPostModal('likes'); }, 0); });
    if (viewsLabel) viewsLabel.addEventListener('click', function(e){ if (e && e.target && e.target.closest('.likes-control')) return; const input = document.getElementById('promoViews'); setTimeout(function(){ if (suppressOpenPostModalOnce) { suppressOpenPostModalOnce=false; return; } if (input && input.checked) openPostModal('views'); }, 0); });
    if (commentsLabel) commentsLabel.addEventListener('click', function(e){ if (e && e.target && e.target.closest('.likes-control')) return; const input = document.getElementById('promoComments'); setTimeout(function(){ if (suppressOpenPostModalOnce) { suppressOpenPostModalOnce=false; return; } if (input && input.checked) openPostModal('comments'); }, 0); });
  } catch(_) {}
  try { updatePromosSummary(); } catch(_) {}

  // Quantidade de Comentários (R$ 1,00 cada)
  const commentsQtyEl = document.getElementById('commentsQty');
  const commentsDec = document.getElementById('commentsDec');
  const commentsInc = document.getElementById('commentsInc');
  const commentsPrices = document.querySelector('.promo-prices[data-promo="comments"]');
  function updateCommentsPrice(q) {
    const newEl = commentsPrices ? commentsPrices.querySelector('.new-price') : null;
    const oldEl = commentsPrices ? commentsPrices.querySelector('.old-price') : null;
    if (newEl) newEl.textContent = formatCurrencyBR(q * 1.5);
    if (oldEl) { const oldVal = (q * 1.5) * 1.7; oldEl.textContent = formatCurrencyBR(oldVal); }
    const hl = document.querySelector('.promo-item.comments .promo-highlight');
    if (hl) hl.textContent = `+ ${q} COMENTÁRIOS`;
  }
  function stepComments(dir) {
    const current = Number(commentsQtyEl?.textContent || 1);
    let next = current + dir;
    if (next < 1) next = 1;
    if (next > 100) next = 100;
    if (commentsQtyEl) commentsQtyEl.textContent = String(next);
    updateCommentsPrice(next);
    try { updatePromosSummary(); } catch(_) {}
  }
  if (commentsDec) commentsDec.addEventListener('click', (e) => { if (e && typeof e.stopPropagation==='function') e.stopPropagation(); stepComments(-1); });
  if (commentsInc) commentsInc.addEventListener('click', (e) => { if (e && typeof e.stopPropagation==='function') e.stopPropagation(); stepComments(1); });
  if (commentsQtyEl) updateCommentsPrice(Number(commentsQtyEl.textContent || 1));

  function getSelectedPromos() {
    const promos = [];
    try {
      const likesChecked = !!document.getElementById('promoLikes')?.checked;
      const viewsChecked = !!document.getElementById('promoViews')?.checked;
      const commentsChecked = !!document.getElementById('promoComments')?.checked;
      const warrantyChecked = !!document.getElementById('promoWarranty60')?.checked;
      const upgradeChecked = !!document.getElementById('orderBumpCheckboxInline')?.checked;
      if (likesChecked) {
        const qty = Number(document.getElementById('likesQty')?.textContent || 150);
        let priceStr = document.querySelector('.promo-prices[data-promo="likes"] .new-price')?.textContent || '';
        if (!priceStr) priceStr = promoPricing.likes?.price || '';
        const tipo = String((tipoSelect && tipoSelect.value) || '').toLowerCase();
        const cat = getServiceCategory();
        const label = (function(t){
          if (cat === 'visualizacoes') return `Curtidas brasileiras reais (${qty})`;
          if (t === 'organicos') return `Curtidas orgânicas (${qty})`;
          if (t === 'brasileiros' || t === 'curtidas_brasileiras') return `Curtidas brasileiras (${qty})`;
          if (t === 'mistos') return `Curtidas mistas (${qty})`;
          return `Curtidas (${qty})`;
        })(tipo);
        promos.push({ key: 'likes', qty, label, priceCents: parsePrecoToCents(priceStr) });
      }
      if (viewsChecked) {
        const qty = Number(document.getElementById('viewsQty')?.textContent || 1000);
        let priceStr = document.querySelector('.promo-prices[data-promo="views"] .new-price')?.textContent || '';
        if (!priceStr) priceStr = promoPricing.views?.price || '';
        promos.push({ key: 'views', qty, label: `Visualizações Reels (${qty})`, priceCents: parsePrecoToCents(priceStr) });
      }
      if (commentsChecked) {
        const qty = Number(document.getElementById('commentsQty')?.textContent || 1);
        const priceCents = qty * 150;
        promos.push({ key: 'comments', qty, label: `Comentários (${qty})`, priceCents });
      }
    if (warrantyChecked) {
        let priceStr = document.querySelector('.promo-prices[data-promo="warranty60"] .new-price')?.textContent || '';
        if (!priceStr) priceStr = (window.promoPricing && window.promoPricing.warranty60 ? window.promoPricing.warranty60.price : '') || 'R$ 9,90';
        const label = 'Reposição por 6 meses';
        promos.push({ key: 'warranty_6m', qty: 1, label, priceCents: parsePrecoToCents(priceStr) });
      }
      if (upgradeChecked) {
        let priceStr = document.querySelector('.promo-prices[data-promo="upgrade"] .new-price')?.textContent || '';
        const highlight = document.getElementById('orderBumpHighlight')?.textContent || '';
        promos.push({ key: 'upgrade', qty: 1, label: `Upgrade de pacote ${highlight ? `(${highlight})` : ''}`.trim(), priceCents: parsePrecoToCents(priceStr) });
      }
    } catch (_) {}
    return promos;
  }

  function calcPromosTotalCents(promos) {
    try { return (Array.isArray(promos) ? promos : []).reduce((acc, p) => acc + (Number(p.priceCents) || 0), 0); } catch (_) { return 0; }
  }

  function parsePrecoToCents(precoStr) {
    // Converte 'R$ 7,90' -> 790
    if (!precoStr) return 0;
    const cleaned = precoStr.replace(/[^\d,]/g, '').replace(',', '.');
    const value = Math.round(parseFloat(cleaned) * 100);
    return isNaN(value) ? 0 : value;
  }

  async function checkInstagramProfileCheckout() {
    if (!usernameCheckoutInput) return;
    const rawInput = usernameCheckoutInput.value.trim();
    if (!rawInput) {
      showStatusMessageCheckout('Digite o usuário ou URL do Instagram.', 'error');
      return;
    }
    const selectedOption = tipoSelect.options[tipoSelect.selectedIndex];
    const isFollowersService = selectedOption && /seguidores/i.test(selectedOption.textContent);
    if (!isFollowersService) {
      showStatusMessageCheckout('Selecione um tipo de serviço de seguidores primeiro.', 'error');
      return;
    }
    const username = normalizeInstagramUsername(rawInput);
    if (!isValidInstagramUsername(username)) {
      showStatusMessageCheckout('Nome de usuário inválido. Use letras, números, pontos e underscores.', 'error');
      return;
    }
    if (username !== rawInput) {
      usernameCheckoutInput.value = username;
    }
    hideStatusMessageCheckout();
    clearProfilePreview();
    showLoadingCheckout();
    try {
      const params = new URLSearchParams(window.location.search);
      const utms = {
          source: params.get('utm_source') || '',
          medium: params.get('utm_medium') || '',
          campaign: params.get('utm_campaign') || '',
          term: params.get('utm_term') || '',
          content: params.get('utm_content') || ''
      };

      // Merge with sessionStorage if empty (Persistence fix)
      try {
        const stored = sessionStorage.getItem('oppus_utms');
        if (stored) {
            const parsed = JSON.parse(stored);
            if (!utms.source && parsed.utm_source) utms.source = parsed.utm_source;
            if (!utms.medium && parsed.utm_medium) utms.medium = parsed.utm_medium;
            if (!utms.campaign && parsed.utm_campaign) utms.campaign = parsed.utm_campaign;
            if (!utms.term && parsed.utm_term) utms.term = parsed.utm_term;
            if (!utms.content && parsed.utm_content) utms.content = parsed.utm_content;
        }
      } catch(_) {}
      const resp = await fetch('/api/check-instagram-profile', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username, utms })
      });
      const data = await resp.json();
      hideLoadingCheckout();
      if (data.success) {
        const profile = data.profile || {};
        if (checkoutProfileImage && profile.profilePicUrl) {
          checkoutProfileImage.src = profile.profilePicUrl;
        }
        if (checkoutProfileUsername) {
          checkoutProfileUsername.textContent = profile.username || username;
        }
        if (checkoutFollowersCount && typeof profile.followersCount === 'number') {
          checkoutFollowersCount.textContent = String(profile.followersCount);
        }
        if (checkoutFollowingCount && typeof profile.followingCount === 'number') {
          checkoutFollowingCount.textContent = String(profile.followingCount);
        }
        if (checkoutPostsCount && typeof profile.postsCount === 'number') {
          checkoutPostsCount.textContent = String(profile.postsCount);
        }
        if (profilePreview) profilePreview.style.display = 'block';
        
        // Atualiza perfil na Etapa 3 (Final)
        const step3Img = document.getElementById('checkoutProfileImageFinal');
        const step3User = document.getElementById('checkoutProfileUsernameFinal');
        const step3Posts = document.getElementById('checkoutPostsCountFinal');
        const step3Followers = document.getElementById('checkoutFollowersCountFinal');
        const step3Following = document.getElementById('checkoutFollowingCountFinal');
        
        if (step3Img) {
             const mainUrl = profile.driveImageUrl || profile.profilePicUrl;
             if (mainUrl) {
                 step3Img.src = mainUrl;
                 step3Img.onerror = function() {
                     const fallback = profile.profilePicUrl || profile.originalProfilePicUrl;
                     if (this.src !== fallback && fallback) this.src = fallback;
                 };
             }
        }
        
        if (step3User) step3User.textContent = profile.username || username;
        if (step3Posts && typeof profile.postsCount === 'number') step3Posts.textContent = String(profile.postsCount);
        if (step3Followers && typeof profile.followersCount === 'number') step3Followers.textContent = String(profile.followersCount);
        if (step3Following && typeof profile.followingCount === 'number') step3Following.textContent = String(profile.followingCount);
        try { sessionStorage.setItem('oppus_instagram_username', profile.username || username); } catch(e) {}
        isInstagramVerified = true;
        try { isInstagramPrivate = !!(profile.isPrivate || profile.is_private); } catch(_) { isInstagramPrivate = false; }
        updatePedidoButtonState();
        showResumoIfAllowed();
        try { updatePromosSummary(); } catch(_) {}
        try { applyCheckoutFlow(); } catch(_) {}
        showStatusMessageCheckout('Perfil verificado com sucesso.', 'success');
        try {
          const trackUrl = '/api/instagram/validet-track';
          let bid = '';
          try { bid = getBrowserSessionId(); } catch(_) {}
          await fetch(trackUrl, { 
              method: 'POST', 
              headers: { 'Content-Type': 'application/json' }, 
              body: JSON.stringify({ username: profile.username || username, browserId: bid }) 
          });
        } catch (_) {}
        // Avança para o passo final
        showTutorialStep(5);

        try {
          const url = '/api/instagram/posts?username=' + encodeURIComponent(profile.username || username);
          fetch(url).then(r=>r.json()).then(d=>{ cachedPosts = Array.isArray(d.posts) ? d.posts : []; cachedPostsUser = (profile.username || username) || ''; }).catch(function(){});
        } catch(_) {}
        
        // Retry tracking audio 10% if it was listened before validation
        if (typeof audioTracked10p !== 'undefined' && audioTracked10p) {
             const trackUser = profile.username || username;
             fetch('/api/track-audio-10p', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ username: trackUser })
             }).catch(e => console.error('Retry audio 10% track error:', e));
        }

        // Update audio progress with username if already tracked
        if (trackedMilestones.size > 0) {
             const trackUser = profile.username || username;
             // Send a "sync" update with the latest tracked state
             const lastMilestone = Array.from(trackedMilestones).pop(); // Simple last one
             fetch('/api/track-audio-progress', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ 
                    username: trackUser,
                    milestone: lastMilestone,
                    // We don't have exact seconds here easily without global state, 
                    // but the backend will just update the username if it matches IP
                })
             }).catch(e => console.error('Update audio progress username error:', e));
        }
        
      } else {
        const msg = String(data.error || 'Falha ao verificar perfil.');
        const isAlreadyTested = (data.code === 'INSTAUSER_ALREADY_USED') || /já foi testado|teste já foi realizado/i.test(msg);
        const isPrivate = (data.code === 'INSTAUSER_PRIVATE') || /perfil\s+é\s+privad|privado/i.test(msg);
        if (isAlreadyTested || isPrivate) {
          const profile = Object.assign({}, data.profile || { username }, { alreadyTested: false });
          if (checkoutProfileImage) checkoutProfileImage.src = profile.profilePicUrl || profile.driveImageUrl || '';
          if (checkoutProfileUsername) checkoutProfileUsername.textContent = (profile.username || username);
          if (typeof profile.followersCount === 'number' && checkoutFollowersCount) {
            checkoutFollowersCount.textContent = String(profile.followersCount);
          }
          if (typeof profile.followingCount === 'number' && checkoutFollowingCount) {
            checkoutFollowingCount.textContent = String(profile.followingCount);
          }
          if (typeof profile.postsCount === 'number' && checkoutPostsCount) {
            checkoutPostsCount.textContent = String(profile.postsCount);
          }
          if (profilePreview) profilePreview.style.display = 'block';
          
          // Atualiza perfil na Etapa 3 (Final)
          const step3Img = document.getElementById('checkoutProfileImageFinal');
          const step3User = document.getElementById('checkoutProfileUsernameFinal');
          const step3Posts = document.getElementById('checkoutPostsCountFinal');
          const step3Followers = document.getElementById('checkoutFollowersCountFinal');
          const step3Following = document.getElementById('checkoutFollowingCountFinal');
          if (step3Img && (profile.profilePicUrl || profile.driveImageUrl)) step3Img.src = profile.profilePicUrl || profile.driveImageUrl;
          if (step3User) step3User.textContent = profile.username || username;
          if (step3Posts && typeof profile.postsCount === 'number') step3Posts.textContent = String(profile.postsCount);
          if (step3Followers && typeof profile.followersCount === 'number') step3Followers.textContent = String(profile.followersCount);
          if (step3Following && typeof profile.followingCount === 'number') step3Following.textContent = String(profile.followingCount);
          
          isInstagramVerified = true;
          isInstagramPrivate = !!isPrivate;
          updatePedidoButtonState();
          showResumoIfAllowed();
          try { updatePromosSummary(); } catch(_) {}
          try { applyCheckoutFlow(); } catch(_) {}
          showStatusMessageCheckout('Perfil verificado com sucesso.', 'success');
          try {
            const trackUrl = '/api/instagram/validet-track';
            // Use same browserId helper
            let bid = '';
            try { bid = getBrowserSessionId(); } catch(_) {}
            
            await fetch(trackUrl, { 
                method: 'POST', 
                headers: { 'Content-Type': 'application/json' }, 
                body: JSON.stringify({ username: profile.username || username, browserId: bid }) 
            });
          } catch (_) {}
          showTutorialStep(5);
          try {
            const url = '/api/instagram/posts?username=' + encodeURIComponent(profile.username || username);
            fetch(url).then(r=>r.json()).then(d=>{ cachedPosts = Array.isArray(d.posts) ? d.posts : []; cachedPostsUser = (profile.username || username) || ''; }).catch(function(){});
          } catch(_) {}
        } else {
          showStatusMessageCheckout(msg, 'error');
        }
      }
    } catch (e) {
      hideLoadingCheckout();
      showStatusMessageCheckout('Erro ao verificar perfil. Tente novamente.', 'error');
    }
  }

  // Avançar de 3 -> 4 quando o usuário digita algo
  if (usernameCheckoutInput) {
    const suppressTip3 = () => {
      tutorial3Suppressed = true;
      const t = document.getElementById('tutorial3Usuario');
      if (t) { t.style.display = 'none'; t.classList.add('hide'); }
      if (grupoUsername) grupoUsername.classList.remove('tutorial-highlight');
    };
    usernameCheckoutInput.addEventListener('focus', () => {
      suppressTip3();
    });
    usernameCheckoutInput.addEventListener('click', () => {
      suppressTip3();
    });
    usernameCheckoutInput.addEventListener('paste', () => { suppressTip3(); });
    usernameCheckoutInput.addEventListener('pointerdown', () => { suppressTip3(); });
    if (grupoUsername) {
      grupoUsername.addEventListener('focusin', () => { suppressTip3(); });
      grupoUsername.addEventListener('pointerdown', () => { suppressTip3(); });
    }
    usernameCheckoutInput.addEventListener('input', () => {
      const hasValue = usernameCheckoutInput.value.trim().length > 0;
      if (hasValue && isFollowersSelected()) {
        showTutorialStep(4);
      } else if (isFollowersSelected()) {
        showTutorialStep(3);
      }
    });
  }

  attachPhoneMask(checkoutPhoneInput);

  // Ocultar 4/5 ao interagir com o campo de telefone (comportamento igual ao 3/5)
  if (checkoutPhoneInput) {
    const suppressTip4 = () => {
      const t = document.getElementById('tutorial4Validar');
      if (t) { t.style.display = 'none'; t.classList.add('hide'); }
      if (checkoutPhoneInput) checkoutPhoneInput.classList.remove('tutorial-highlight');
    };
    checkoutPhoneInput.addEventListener('focus', suppressTip4);
    checkoutPhoneInput.addEventListener('click', suppressTip4);
    checkoutPhoneInput.addEventListener('paste', suppressTip4);
    checkoutPhoneInput.addEventListener('pointerdown', suppressTip4);
    checkoutPhoneInput.addEventListener('input', suppressTip4);
    let prefetchT = null;
    checkoutPhoneInput.addEventListener('input', () => {
      try {
        if (prefetchT) clearTimeout(prefetchT);
        prefetchT = setTimeout(() => {
          prefetchStripeEmbeddedCheckoutSession()
            .then(() => { try { maybeAutoMountStripeEmbeddedCheckout(); } catch(_) {} })
            .catch(() => {});
        }, 500);
      } catch (_) {}
    });
  }

  if (btnPedido) {
    const suppressTip5 = () => {
      const t = document.getElementById('tutorial5Pedido');
      if (t) { t.style.display = 'none'; t.classList.add('hide'); }
      if (btnPedido) btnPedido.classList.remove('tutorial-highlight');
    };
    btnPedido.addEventListener('click', suppressTip5);
    btnPedido.addEventListener('pointerdown', suppressTip5);
  }

  async function criarPixWoovi() {
    // Check Upsell for Followers
    try {
        // Upsell removed
    } catch(e) { console.error(e); }

    try {
      const t = document.getElementById('tutorial5Pedido');
      if (t) { t.style.display = 'none'; t.classList.add('hide'); }
      if (grupoPedido) grupoPedido.classList.remove('tutorial-highlight');
    } catch (_) {}
    try {
      const tipo = tipoSelect.value;
      const qtd = Number(qtdSelect.value);
      const upgradeChecked = !!document.getElementById('orderBumpCheckboxInline')?.checked;
      const getUpgradeAddQtd = (t, base) => {
        try {
          if (!isFollowersTipo(t)) return 0;
          if (t === 'organicos' && Number(base) === 50) return 50;
          if ((t === 'brasileiros' || t === 'organicos') && Number(base) === 1000) {
            return 1000;
          }
          const upsellTargets = { 50: 150, 150: 300, 500: 700, 1000: 2000, 3000: 4000, 5000: 7500, 10000: 15000 };
          const target = upsellTargets[Number(base)];
          if (!target) return 0;
          return Number(target) - Number(base);
        } catch (_) { return 0; }
      };
      const upgradeAdd = upgradeChecked ? getUpgradeAddQtd(tipo, qtd) : 0;
      const qtdEffective = Number(qtd) + Number(upgradeAdd);
      const opt = qtdSelect.options[qtdSelect.selectedIndex];
      const precoStr = opt ? (opt.dataset.preco || '') : '';
      const valueCents = parsePrecoToCents(precoStr);
      
      // Fix: Update global basePriceCents to ensure payment method visibility works
      if (valueCents) {
          basePriceCents = valueCents;
          console.log('✅ basePriceCents updated in criarPixWoovi:', basePriceCents);
      }

      if (!tipo || !qtd || !valueCents) {
        try {
          const hasTipo = !!tipo;
          const hasQtd = !!qtd;
          const followersPackMsg = 'Selecione o pacote de seguidores antes de realizar o pedido.';
          const generalPackMsg = 'Selecione a quantidade/pacote do serviço.';
          const msg = !hasTipo ? 'Selecione o tipo de seguidores antes de realizar o pedido.' : (!hasQtd ? (isFollowersTipo(tipo)? followersPackMsg : generalPackMsg) : 'Selecione o tipo e o pacote antes de realizar o pedido.');
          alert(msg);
          hideAllTutorials();
          if (!hasTipo) {
            const target = document.getElementById('tipoCards') || document.getElementById('grupoTipo');
            if (target) {
              try {
                const rect = target.getBoundingClientRect();
                const top = (window.scrollY || window.pageYOffset || 0) + rect.top - Math.max(80, rect.height * 0.4);
                window.scrollTo({ top, behavior: 'smooth' });
              } catch(_) {
                if (typeof target.scrollIntoView === 'function') target.scrollIntoView({ behavior: 'smooth', block: 'center' });
              }
            }
            try { showTutorialStep(2); } catch(_) {}
            try { const gt = document.getElementById('grupoTipo'); if (gt) gt.classList.add('tutorial-highlight'); } catch(_) {}
          } else if (!hasQtd) {
            const target = document.getElementById('planCards') || document.getElementById('grupoQuantidade');
            if (target) {
              try {
                const rect = target.getBoundingClientRect();
                const top = (window.scrollY || window.pageYOffset || 0) + rect.top - Math.max(80, rect.height * 0.4);
                window.scrollTo({ top, behavior: 'smooth' });
              } catch(_) {
                if (typeof target.scrollIntoView === 'function') target.scrollIntoView({ behavior: 'smooth', block: 'center' });
              }
            }
            try { showTutorialStep(3); } catch(_) {}
            try { if (grupoQuantidade) grupoQuantidade.classList.add('tutorial-highlight'); } catch(_) {}
          }
        } catch(_) {}
        return;
      }
      // Verificação de telefone
      const phoneDigits = onlyDigits((checkoutPhoneInput && checkoutPhoneInput.value) || '');
      if (!phoneDigits || phoneDigits.length < 10) {
        alert('Digite seu telefone antes de realizar o pedido.');
        try {
          hideAllTutorials();
          const tutPhone = document.getElementById('tutorial4Validar');
          if (tutPhone) tutPhone.style.display = 'block';
          if (checkoutPhoneInput && typeof checkoutPhoneInput.scrollIntoView === 'function') {
            checkoutPhoneInput.scrollIntoView({ behavior: 'smooth', block: 'center' });
          }
          if (document.activeElement && typeof document.activeElement.blur === 'function') {
            document.activeElement.blur();
          }
          if (checkoutPhoneInput) checkoutPhoneInput.classList.add('tutorial-highlight');
        } catch (_) {}
        return;
      }
      // Verificação de perfil do Instagram (quando serviço é seguidores)
      if (isFollowersSelected() && !isInstagramVerified) {
        alert('Verifique o perfil do Instagram antes de realizar o pedido.');
        try {
          hideAllTutorials();
          if (perfilCard) perfilCard.style.display = 'block';
          const tutUser = document.getElementById('tutorial3Usuario');
          if (tutUser) tutUser.style.display = 'block';
          if (usernameCheckoutInput && typeof usernameCheckoutInput.scrollIntoView === 'function') {
            usernameCheckoutInput.scrollIntoView({ behavior: 'smooth', block: 'center' });
            usernameCheckoutInput.focus();
          }
          if (grupoUsername) grupoUsername.classList.add('tutorial-highlight');
        } catch (_) {}
        return;
      }
      // sem validação de posts
      btnPedido.disabled = true;
      btnPedido.classList.add('loading');

      const correlationID = (window.crypto && window.crypto.randomUUID) ? window.crypto.randomUUID() : `${Date.now()}-${Math.random().toString(16).slice(2)}`;
      const promos = getSelectedPromos();
      const promosTotalCents = calcPromosTotalCents(promos);
      const totalCents = Math.max(0, Number(valueCents) + Number(promosTotalCents));
      let sckValue = '';
      try {
        const params = new URLSearchParams(window.location.search || '');
        sckValue = params.get('sck') || '';
      } catch (_) {}
      if (!sckValue) {
        try {
          const m2 = document.cookie.match(/(?:^|;\s*)index=([^;]+)/);
          sckValue = m2 && m2[1] ? decodeURIComponent(m2[1]) : '';
        } catch (_) {}
      }
      /*
      // Tracking: Meta Pixel + CAPI (InitiateCheckout)
      const valueBRL = Math.round(Number(totalCents)) / 100;
      const fbpCookie = (document.cookie.match(/_fbp=([^;]+)/)?.[1]) || '';
      try {
        const sendPixel = (name, params, eid) => {
          const trySend = () => {
            try {
              if (typeof fbq === 'function' && window._oppusPixelReady) {
                if (eid) fbq('track', name, params, { eventID: eid });
                else fbq('track', name, params);
                return true;
              }
            } catch (_) {}
            return false;
          };
          if (!trySend()) setTimeout(trySend, 800);
        };
        sendPixel('InitiateCheckout', {
          value: valueBRL,
          currency: 'BRL',
          contents: [{ id: tipo, quantity: qtdEffective }],
          content_name: `${tipo} - ${qtdEffective} ${getUnitForTipo(tipo)}`,
        }, correlationID);
      } catch (_) { }
      try {
        void fetch('/api/meta/track', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            eventName: 'InitiateCheckout',
            value: valueBRL,
            currency: 'BRL',
            contentName: `${tipo} - ${qtdEffective} ${getUnitForTipo(tipo)}`,
            contents: [{ id: tipo, quantity: qtdEffective }],
            phone: phoneFromUrl,
            fbp: fbpCookie,
            correlationID,
            eventSourceUrl: window.location.href,
          })
        });
      } catch (_) { }
      */
      const phoneValue = onlyDigits((checkoutPhoneInput && checkoutPhoneInput.value && checkoutPhoneInput.value.trim()) || phoneFromUrl);
      const usernamePreview = (checkoutProfileUsername && checkoutProfileUsername.textContent && checkoutProfileUsername.textContent.trim()) || '';
      let usernameFromSession = '';
      try { usernameFromSession = sessionStorage.getItem('oppus_instagram_username') || ''; } catch(_) {}
      const usernameInputRaw = (usernameCheckoutInput && usernameCheckoutInput.value && usernameCheckoutInput.value.trim()) || '';
      const usernameInputNorm = normalizeInstagramUsername(usernameInputRaw);
      const instagramUsernameFinal = usernamePreview || usernameFromSession || usernameInputNorm || '';
      const serviceCategory = (function(){
        if (window.__ENG_MODE__ && typeof window.__ENG_MODE__ === 'boolean') {
          return 'seguidores';
        }
        try {
          const path = window.location && window.location.pathname ? window.location.pathname : '';
          if (path.indexOf('/servicos-curtidas') === 0) return 'curtidas';
          if (path.indexOf('/servicos-visualizacoes') === 0) return 'visualizacoes';
        } catch(_) {}
        return 'seguidores';
      })();

      const selectedPixGateway = String((pixGatewaySelect && pixGatewaySelect.value) ? pixGatewaySelect.value : 'woovi').trim().toLowerCase() || 'woovi';
      currentPixGateway = (selectedPixGateway === 'expay' || selectedPixGateway === 'paghiper') ? selectedPixGateway : 'woovi';

      const payload = {
        correlationID,
        value: totalCents,
        comment: 'Checkout OPPUS',
        customer: {
          name: 'Cliente Checkout',
          phone: phoneValue
        },
        additionalInfo: [
          { key: 'tipo_servico', value: tipo },
          { key: 'categoria_servico', value: serviceCategory },
          { key: 'quantidade', value: String(qtdEffective) },
          { key: 'pacote', value: `${qtdEffective} ${getUnitForTipo(tipo)} - ${precoStr}` },
          { key: 'phone', value: phoneValue },
          { key: 'instagram_username', value: instagramUsernameFinal },
          { key: 'pix_gateway', value: currentPixGateway },
          { key: 'order_bumps_total', value: formatCentsToBRL(promosTotalCents) },
          { key: 'order_bumps', value: promos.map(p => `${p.key}:${p.qty ?? 1}`).join(';') }
        ],
        profile_is_private: isInstagramPrivate
      };
      const pushAdditionalInfoIfMissing = (k, v) => {
        const kk = String(k || '').trim();
        const vv = String(v || '').trim();
        if (!kk || !vv) return;
        if (payload.additionalInfo.some(it => it && it.key === kk)) return;
        payload.additionalInfo.push({ key: kk, value: vv });
      };
      try {
        const cpfDigits = onlyDigits(document.getElementById('pixCpfInput')?.value || document.getElementById('cardHolderCpf')?.value || '');
        if (cpfDigits) {
          payload.customer.cpf = cpfDigits;
          pushAdditionalInfoIfMissing('cpf', cpfDigits);
        }
      } catch (_) {}
      try {
        const emailRaw = String(document.getElementById('contactEmailInput')?.value || '').trim();
        if (emailRaw) {
          payload.customer.email = emailRaw;
          pushAdditionalInfoIfMissing('email', emailRaw);
        }
      } catch (_) {}
      if (currentPixGateway === 'expay' && !(window && window.__EXPAY_DEFAULT_CPF_ENABLED)) {
        const cpfDigits = onlyDigits(payload.customer && payload.customer.cpf ? payload.customer.cpf : '');
        if (!cpfDigits || cpfDigits.length !== 11) {
          try {
            const cpfEl = document.getElementById('pixCpfInput') || document.getElementById('cardHolderCpf');
            if (cpfEl) {
              cpfEl.classList.add('input-error');
              cpfEl.classList.add('tutorial-highlight');
              try { cpfEl.focus(); } catch (_) {}
              try { cpfEl.scrollIntoView({ behavior: 'smooth', block: 'center' }); } catch (_) {}
            }
          } catch (_) {}
          throw new Error('Informe seu CPF para pagar via Pix (ExPay).');
        }
      }
      if (currentPixGateway === 'paghiper') {
        const cpfDigits = onlyDigits(payload.customer && payload.customer.cpf ? payload.customer.cpf : '');
        if (!cpfDigits || cpfDigits.length !== 11) {
          try {
            const cpfEl = document.getElementById('pixCpfInput') || document.getElementById('cardHolderCpf');
            if (cpfEl) {
              cpfEl.classList.add('input-error');
              cpfEl.classList.add('tutorial-highlight');
              try { cpfEl.focus(); } catch (_) {}
              try { cpfEl.scrollIntoView({ behavior: 'smooth', block: 'center' }); } catch (_) {}
            }
          } catch (_) {}
          throw new Error('Informe seu CPF para pagar via Pix (PagHiper).');
        }
        const email = String(payload.customer && payload.customer.email ? payload.customer.email : '').trim().toLowerCase();
        const okEmail = /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
        if (!okEmail) {
          try {
            const emailEl = document.getElementById('contactEmailInput');
            if (emailEl) {
              emailEl.classList.add('input-error');
              emailEl.classList.add('tutorial-highlight');
              try { emailEl.focus(); } catch (_) {}
              try { emailEl.scrollIntoView({ behavior: 'smooth', block: 'center' }); } catch (_) {}
            }
          } catch (_) {}
          throw new Error('Informe seu e-mail para pagar via Pix (PagHiper).');
        }
      }
      try {
        const cc = String(window.couponCode || '').trim();
        if (cc) payload.additionalInfo.push({ key: 'cupom', value: cc.toUpperCase() });
      } catch (_) {}
      try {
        const m = document.cookie.match(/(?:^|;\s*)tc_code=([^;]+)/);
        const tc = m && m[1] ? m[1] : '';
        if (tc) { payload.additionalInfo.push({ key: 'tc_code', value: tc }); }
      } catch(_) {}
      try {
        if (sckValue) payload.additionalInfo.push({ key: 'sck', value: sckValue });
      } catch(_) {}

      try {
        if ((serviceCategory === 'curtidas' || serviceCategory === 'visualizacoes') && typeof collectValidPostLinks === 'function') {
          const linksNow = collectValidPostLinks();
          if (Array.isArray(linksNow) && linksNow.length) {
            pushAdditionalInfoIfMissing('post_link', linksNow[0]);
            pushAdditionalInfoIfMissing('post_links', linksNow.join(','));
          }
        }
      } catch (_) {}

      try {
        const selResp = await fetch('/api/instagram/selected-for');
        const selData = await selResp.json();
        const sfor = selData && selData.selectedFor ? selData.selectedFor : {};
        const mapKind = function(k){ const obj = sfor && sfor[k]; const sc = obj && obj.shortcode; return sc ? `https://instagram.com/p/${encodeURIComponent(sc)}/` : ''; };
        const likesLink = mapKind('likes');
        const viewsLink = mapKind('views');
        const commentsLink = mapKind('comments');
        const anyLink = viewsLink || likesLink || commentsLink;
        if (serviceCategory === 'curtidas' || serviceCategory === 'visualizacoes') {
          const mainCandidate = (serviceCategory === 'visualizacoes') ? (viewsLink || anyLink) : (likesLink || anyLink);
          if (mainCandidate) pushAdditionalInfoIfMissing('post_link', mainCandidate);
        }
        
        const hasLikes = promos.some(p => p.key === 'likes');
        const hasViews = promos.some(p => p.key === 'views');
        const hasComments = promos.some(p => p.key === 'comments');
        const kinds = [];
        if (hasLikes) kinds.push('likes');
        if (hasViews) kinds.push('views');
        if (hasComments) kinds.push('comments');

        if (kinds.length === 1) {
          const onlyKind = kinds[0];
          let link = mapKind(onlyKind);
          if (!link && instagramUsernameFinal) {
            try {
              const url = '/api/instagram/posts?username=' + encodeURIComponent(instagramUsernameFinal);
              const pr = await fetch(url);
              const pd = await pr.json();
              const posts = Array.isArray(pd && pd.posts) ? pd.posts : [];
              const isVideo = (p) => !!(p && (p.isVideo || /video|clip/.test(String(p.typename || '').toLowerCase())));
              const candidates = onlyKind === 'views' ? posts.filter(isVideo) : posts;
              const pick = (candidates && candidates[0]) || (posts && posts[0]) || null;
              if (pick && pick.shortcode) link = `https://instagram.com/p/${encodeURIComponent(pick.shortcode)}/`;
            } catch (_) {}
          }
          if (link) payload.additionalInfo.push({ key: `orderbump_post_${onlyKind}`, value: link });
        } else {
          if (hasLikes && anyLink) payload.additionalInfo.push({ key: 'orderbump_post_likes', value: likesLink || anyLink });
          if (hasViews && anyLink) payload.additionalInfo.push({ key: 'orderbump_post_views', value: viewsLink || anyLink });
          if (hasComments && anyLink) payload.additionalInfo.push({ key: 'orderbump_post_comments', value: commentsLink || anyLink });
        }
      } catch(_) {}

      // sem envio de links de posts

      const pixCreateEndpoint = (currentPixGateway === 'expay')
        ? '/api/expay/charge'
        : (currentPixGateway === 'paghiper')
          ? '/api/paghiper/charge'
          : '/api/woovi/charge';
      const resp = await fetch(pixCreateEndpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });

      const parseFetchResponse = async (r) => {
        let data = null;
        let rawText = '';
        try {
          rawText = await r.text();
        } catch (_) {
          rawText = '';
        }
        try {
          data = rawText ? JSON.parse(rawText) : null;
        } catch (_) {
          data = rawText ? { raw: rawText } : null;
        }
        return { data, rawText };
      };

      let { data } = await parseFetchResponse(resp);
      if (!resp.ok) {
        const status = Number(resp && resp.status) || 0;
        const msg = data?.message || data?.details?.message || data?.error || (data?.raw ? String(data.raw).slice(0, 200) : '') || 'Falha ao criar cobrança';
        const allowFallback = (typeof window !== 'undefined' && window && window.__ALLOW_PIX_GATEWAY_FALLBACK !== false);
        const shouldFallbackToWoovi = allowFallback && (currentPixGateway === 'expay' || currentPixGateway === 'paghiper') && (status >= 500 || status === 502);
        if (shouldFallbackToWoovi) {
          try {
            currentPixGateway = 'woovi';
            const gw = payload && Array.isArray(payload.additionalInfo) ? payload.additionalInfo.find(it => it && it.key === 'pix_gateway') : null;
            if (gw) gw.value = 'woovi';
            const wooviResp = await fetch('/api/woovi/charge', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify(payload)
            });
            const parsed = await parseFetchResponse(wooviResp);
            if (!wooviResp.ok) {
              const msg2 = parsed?.data?.message || parsed?.data?.details?.message || parsed?.data?.error || (parsed?.data?.raw ? String(parsed.data.raw).slice(0, 200) : '') || 'Falha ao criar cobrança';
              throw new Error(msg2);
            }
            try {
              const gwLabel = (function () {
                const g = String(payload?.additionalInfo?.find(it => it && it.key === 'pix_gateway')?.value || '').trim().toLowerCase();
                if (g === 'paghiper') return 'PagHiper';
                if (g === 'expay') return 'ExPay';
                return 'Gateway Pix';
              })();
              const detailsMsg = String(msg || '').trim();
              const detailsShort = detailsMsg ? ` (${detailsMsg.slice(0, 140)})` : '';
              if (typeof showToast === 'function') {
                showToast({ title: 'Pix', desc: `${gwLabel} indisponível${detailsShort}. Geramos seu Pix via Woovi.`, platform: 'instagram' });
              } else {
                alert(`${gwLabel} indisponível${detailsShort}. Geramos seu Pix via Woovi.`);
              }
            } catch (_) {}
            data = parsed.data;
          } catch (e) {
            throw new Error(msg);
          }
        } else {
          const alreadyPrefixed = /^ExPay\s+HTTP\s+\d+/i.test(String(msg || '').trim());
          throw new Error((currentPixGateway === 'expay' && status && !alreadyPrefixed) ? `ExPay HTTP ${status}: ${msg}` : msg);
        }
      }
      if (!data || typeof data !== 'object') {
        throw new Error('Resposta inválida ao criar cobrança');
      }

      // Renderização amigável: QR Code e botão de copiar código Pix
      const resolvedGateway = String(data?.gateway || data?.provider || currentPixGateway || 'woovi').trim().toLowerCase();
      currentPixGateway = (resolvedGateway === 'expay' || resolvedGateway === 'paghiper') ? resolvedGateway : 'woovi';
      const charge = data?.charge || data?.data?.charge || data?.data || {};
      const pix = charge?.paymentMethods?.pix || data?.pix || data?.paymentMethods?.pix || {};
      const brCode = pix?.brCode || pix?.br_code || charge?.brCode || data?.brCode || data?.br_code || data?.pix_code || data?.pixCode || '';
      const qrImage = pix?.qrCodeImage || pix?.qr_code_image || charge?.qrCodeImage || data?.qrCodeImage || data?.qr_code_image || data?.qr_code || data?.qrCode || '';

      // Ocultar seções anteriores e Header
      document.querySelectorAll('.section-block').forEach(el => el.style.display = 'none');
      if (document.querySelector('.header')) document.querySelector('.header').style.display = 'none';

      const screen = document.getElementById('paymentSuccessScreen');
      if (screen) {
          screen.style.display = 'flex';
          
          // Força visibilidade baseada no totalCents da transação atual
          // Isso garante que o botão apareça mesmo se calculateTotalCents() falhar
          try {
             const selector = document.getElementById('paymentMethodSelector');
             if (selector) {
                 if (totalCents >= 100) {
                     selector.style.display = 'flex';
                     populateInstallments((typeof window.calculateSubtotalCents === 'function') ? window.calculateSubtotalCents() : totalCents);
                     // Se não houver método selecionado, define Pix (mas o seletor estará visível)
                     if (!currentPaymentMethod) currentPaymentMethod = 'pix';
                 } else {
                     selector.style.display = 'none';
                     if (currentPaymentMethod !== 'pix') selectPaymentMethod('pix');
                 }
             }
          } catch(e) { console.error('Erro ao atualizar visibilidade pagamento (forçado):', e); }
          
          // QR Code
          const qrContainer = document.getElementById('paymentQrImage');
          if (qrContainer && qrImage) {
              qrContainer.innerHTML = `<img src="${qrImage}" alt="QR Code" style="width: 100%; max-width: 200px; display: block; margin: 0 auto;" />`;
          }

          // Values
          const totalFormatted = formatCentsToBRL(totalCents);
          const valEl = document.getElementById('paymentValue');
          if (valEl) valEl.textContent = totalFormatted;

          // Copy Code
          const copyArea = document.getElementById('paymentCopyCode');
          if (copyArea) copyArea.value = brCode;

          // Copy Button Logic
          const copyBtn = document.getElementById('paymentCopyBtn');
          if (copyBtn) {
              copyBtn.replaceWith(copyBtn.cloneNode(true)); // remove old listeners
              const newBtn = document.getElementById('paymentCopyBtn');
              newBtn.addEventListener('click', async () => {
                  try {
                      if (navigator.clipboard?.writeText) {
                          await navigator.clipboard.writeText(brCode);
                      } else {
                          copyArea.select();
                          document.execCommand('copy');
                      }
                      const span = newBtn.querySelector('.button-text');
                      const prev = span ? span.textContent : '';
                      if (span) span.textContent = 'Pix Copiado!';
                      newBtn.classList.add('success');
                      setTimeout(() => {
                          if (span) span.textContent = prev;
                          newBtn.classList.remove('success');
                      }, 2000);
                  } catch(e) { alert('Erro ao copiar'); }
              });
          }

          // Timer
          const timerEl = document.getElementById('pixTimer');
          if (timerEl) {
              let timeLeft = 29 * 60 + 59; // 29:59
              const updateTimer = () => {
                  const m = Math.floor(timeLeft / 60);
                  const s = timeLeft % 60;
                  timerEl.textContent = `${String(m).padStart(2,'0')}:${String(s).padStart(2,'0')}`;
                  if (timeLeft > 0) timeLeft--;
              };
              updateTimer();
              setInterval(updateTimer, 1000);
          }

          // Summary
          const sImg = document.getElementById('summaryItemImage');
          const profileImg = document.getElementById('checkoutProfileImage');
          if (sImg) {
             if (profileImg && profileImg.src && profileImg.style.display !== 'none') {
                 sImg.innerHTML = `<img src="${profileImg.src}" style="width:100%; height:100%; object-fit:cover;" />`;
             } else {
                 sImg.textContent = '📦';
             }
          }
          
          const sName = document.getElementById('summaryItemName');
          if (sName) {
             const tipoLabel = (tipo === 'seguidores_tiktok') ? 'Seguidores TikTok' : 
                              (tipo === 'curtidas_brasileiras' ? 'Curtidas Brasileiras' : 
                               (tipo === 'visualizacoes_reels' ? 'Visualizações Reels' : 'Seguidores Instagram'));
             sName.textContent = tipoLabel;
          }

          const sQty = document.getElementById('summaryItemQty');
          if (sQty) sQty.textContent = `Quantidade: ${qtdEffective}`;

          
          const sPrice = document.getElementById('summaryItemPrice');
          if (sPrice) sPrice.textContent = totalFormatted;
          
          const sTotal = document.getElementById('summaryItemTotal');
          if (sTotal) sTotal.textContent = totalFormatted;
          
          const sSub = document.getElementById('summarySubtotal');
          if (sSub) sSub.textContent = totalFormatted;
          
          const sGrand = document.getElementById('summaryTotal');
          if (sGrand) sGrand.textContent = totalFormatted;
          
          const sPayVal = document.getElementById('summaryPaymentValue');
          if (sPayVal) sPayVal.textContent = `Valor: ${totalFormatted}`;

          const sCustName = document.getElementById('summaryCustomerName');
          if (sCustName) {
              sCustName.innerHTML = `
                  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"></path><circle cx="12" cy="7" r="4"></circle></svg>
                  ${instagramUsernameFinal || 'Cliente'}`;
          }

          const sCustPhone = document.getElementById('summaryCustomerPhone');
          if (sCustPhone) {
             sCustPhone.innerHTML = `
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 16.92v3a2 2 0 0 1-2.18 2 19.79 19.79 0 0 1-8.63-3.07 19.5 19.5 0 0 1-6-6 19.79 19.79 0 0 1-3.07-8.67A2 2 0 0 1 4.11 2h3a2 2 0 0 1 2 1.72 12.84 12.84 0 0 0 .7 2.81 2 2 0 0 1-.45 2.11L8.09 9.91a16 16 0 0 0 6 6l1.27-1.27a2 2 0 0 1 2.11-.45 12.84 12.84 0 0 0 2.81.7A2 2 0 0 1 22 16.92z"></path></svg>
                ${phoneValue}`;
          }

          const sCustEmail = document.getElementById('summaryCustomerEmail');
          if (sCustEmail) {
              sCustEmail.style.display = 'none';
          }
          
          // Scroll to top
          window.scrollTo({ top: 0, behavior: 'smooth' });
      }

      const chargeId = charge?.id || charge?.chargeId || data?.chargeId || data?.id || data?.transaction_id || data?.transactionId || data?.invoice_id || data?.invoiceId || '';
      const identifier = charge?.identifier || (data?.charge && data.charge.identifier) || data?.identifier || data?.transaction_id || data?.transactionId || '';
      const serverCorrelationID = charge?.correlationID || (data?.charge && data.charge.correlationID) || data?.correlationID || data?.correlationId || '';
      if (paymentPollInterval) {
        clearInterval(paymentPollInterval);
        paymentPollInterval = null;
      }
      if (chargeId) {
        const checkPaid = async () => {
          try {
            const statusEndpoint = (currentPixGateway === 'expay')
              ? `/api/expay/charge-status?id=${encodeURIComponent(chargeId)}&identifier=${encodeURIComponent(identifier)}&correlationID=${encodeURIComponent(serverCorrelationID || correlationID)}`
              : (currentPixGateway === 'paghiper')
                ? `/api/paghiper/charge-status?id=${encodeURIComponent(chargeId)}&identifier=${encodeURIComponent(identifier)}&correlationID=${encodeURIComponent(serverCorrelationID || correlationID)}`
                : `/api/woovi/charge-status?id=${encodeURIComponent(chargeId)}`;
            const stResp = await fetch(statusEndpoint);
            const stData = await stResp.json();
            const status = stData?.charge?.status || stData?.status || '';
            const paidFlag = stData?.charge?.paid || stData?.paid || false;
            const isPaid = paidFlag === true || /(paid|approved|aprovado)/i.test(String(status));
            if (isPaid) {
              clearInterval(paymentPollInterval);
              paymentPollInterval = null;
              try { markPaymentConfirmed(); } catch(_) {}
              const qs = new URLSearchParams({ identifier, correlationID: serverCorrelationID || correlationID }).toString();
              await navigateToPedidoOrFallback(identifier, serverCorrelationID || correlationID);
            }
            if (!isPaid) {
              try {
                const dbUrl = `/api/checkout/payment-state?id=${encodeURIComponent(chargeId)}&identifier=${encodeURIComponent(identifier)}&correlationID=${encodeURIComponent(serverCorrelationID || correlationID)}`;
                const dbResp = await fetch(dbUrl);
                const dbData = await dbResp.json();
                if (dbData?.paid === true) {
                  clearInterval(paymentPollInterval);
                  paymentPollInterval = null;
                  try { markPaymentConfirmed(); } catch(_) {}
                  await navigateToPedidoOrFallback(identifier, serverCorrelationID || correlationID);
                }
              } catch(_) {}
            }
          } catch (e) {
            // Silencioso: mantém próximo ciclo
          }
        };
        // Executa imediatamente e depois mais frequente
        checkPaid();
        paymentPollInterval = setInterval(checkPaid, 7000);
      } else {
        const checkPaidDb = async () => {
          try {
            const url = `/api/checkout/payment-state?identifier=${encodeURIComponent(identifier)}&correlationID=${encodeURIComponent(serverCorrelationID || correlationID)}`;
            const stResp = await fetch(url);
            const stData = await stResp.json();
            const isPaid = stData?.paid === true;
            if (isPaid) {
              clearInterval(paymentPollInterval);
              paymentPollInterval = null;
              if (paymentEventSource) { try { paymentEventSource.close(); } catch(_) {} paymentEventSource = null; }
              try { markPaymentConfirmed(); } catch(_) {}
              const qs = new URLSearchParams({ identifier, correlationID: serverCorrelationID || correlationID }).toString();
              await navigateToPedidoOrFallback(identifier, serverCorrelationID || correlationID);
            }
          } catch (e) {}
        };
        checkPaidDb();
        paymentPollInterval = setInterval(checkPaidDb, 3000);
        try {
          if (paymentEventSource) { paymentEventSource.close(); paymentEventSource = null; }
          const sseUrl = `/api/payment/subscribe?identifier=${encodeURIComponent(identifier)}&correlationID=${encodeURIComponent(serverCorrelationID || correlationID)}`;
          paymentEventSource = new EventSource(sseUrl);
          paymentEventSource.addEventListener('paid', async (ev) => {
            try {
              clearInterval(paymentPollInterval);
              paymentPollInterval = null;
              if (paymentEventSource) { paymentEventSource.close(); paymentEventSource = null; }
              try { markPaymentConfirmed(); } catch(_) {}
              const qs = new URLSearchParams({ identifier, correlationID: serverCorrelationID || correlationID }).toString();
              await navigateToPedidoOrFallback(identifier, serverCorrelationID || correlationID);
            } catch(_) {}
          });
        } catch(_) {}
      }
    } catch (err) {
      alert('Erro ao criar PIX: ' + (err?.message || err));
    } finally {
      btnPedido.disabled = false;
      btnPedido.classList.remove('loading');
    }
  }

  // Payment Method Toggles (Click on containers)
  const optionPixToggle = document.getElementById('optionPix');
  const optionCardToggle = document.getElementById('optionCard');

  if (optionPixToggle) optionPixToggle.addEventListener('click', () => selectPaymentMethod('pix'));
  if (optionCardToggle) optionCardToggle.addEventListener('click', () => selectPaymentMethod('credit_card'));

  // Masking Helpers
  function maskCardNumber(v) {
      v = v.replace(/\D/g, "");
      v = v.substring(0, 16);
      return v.replace(/(\d{4})(\d{4})(\d{4})(\d{4})/, "$1 $2 $3 $4").trim();
  }
  function maskDate(v) {
      v = v.replace(/\D/g, "");
      if (v.length > 8) v = v.substring(0, 8); // YYYY-MM-DD usually, but input date handles itself? 
      // Wait, input type="date" doesn't need masking. 
      // But cardExpiry is text.
      return v;
  }
  function maskExpiry(v) {
      v = v.replace(/\D/g, "");
      if (v.length > 4) v = v.substring(0, 4);
      if (v.length > 2) return v.substring(0, 2) + '/' + v.substring(2);
      return v;
  }
  function maskCep(v) {
      v = v.replace(/\D/g, "");
      if (v.length > 8) v = v.substring(0, 8);
      return v.replace(/(\d{5})(\d{3})/, "$1-$2");
  }
  function maskCpf(v) {
      v = String(v || '').replace(/\D/g, "");
      if (v.length > 11) v = v.substring(0, 11);
      if (v.length <= 3) return v;
      if (v.length <= 6) return v.replace(/(\d{3})(\d{1,3})/, "$1.$2");
      if (v.length <= 9) return v.replace(/(\d{3})(\d{3})(\d{1,3})/, "$1.$2.$3");
      return v.replace(/(\d{3})(\d{3})(\d{3})(\d{1,2})/, "$1.$2.$3-$4");
  }
  function maskBirth(v) {
      v = String(v || '').replace(/\D/g, "");
      if (v.length > 8) v = v.substring(0, 8);
      if (v.length <= 2) return v;
      if (v.length <= 4) return v.replace(/(\d{2})(\d{1,2})/, "$1/$2");
      return v.replace(/(\d{2})(\d{2})(\d{1,4})/, "$1/$2/$3");
  }

  const elCardNum = document.getElementById('cardNumber');
  if (elCardNum) elCardNum.addEventListener('input', e => e.target.value = maskCardNumber(e.target.value));
  
  const elCardExp = document.getElementById('cardExpiry');
  if (elCardExp) elCardExp.addEventListener('input', e => e.target.value = maskExpiry(e.target.value));

  const elBillingCep = document.getElementById('billingCep');
  if (elBillingCep) elBillingCep.addEventListener('input', e => e.target.value = maskCep(e.target.value));

  const elCardCpf = document.getElementById('cardHolderCpf');
  if (elCardCpf) elCardCpf.addEventListener('input', e => e.target.value = maskCpf(e.target.value));
  
  const elCardBirth = document.getElementById('cardHolderBirth');
  if (elCardBirth) elCardBirth.addEventListener('input', e => e.target.value = maskBirth(e.target.value));

  if (btnPedido) {
    btnPedido.addEventListener('click', async (e) => {
        e.preventDefault();
        if (typeof currentPaymentMethod !== 'undefined' && currentPaymentMethod === 'credit_card') {
            await handleCardPayment(e);
        } else {
            await criarPixWoovi(e);
        }
    });
  }

  // Lógica de Pagamento Cartão
  const payWithCardBtn = document.getElementById('payWithCardBtn');
  const cardPaymentForm = document.getElementById('cardPaymentForm');

  function getEfiCreditCardLib() {
    try {
      const candidates = [
        window.EfiJs,
        window.EfiPay,
        window.paymentTokenEfi,
        window.PaymentTokenEfi,
        window.PaymentTokenEFI,
        window.payment_token_efi,
        window.paymentToken,
        window.payment_token
      ];
      for (const c of candidates) {
        if (c && c.CreditCard) return c.CreditCard;
      }
    } catch (_) {}
    return null;
  }

  async function ensureEfiPayLoaded() {
    try {
      if (getEfiCreditCardLib()) return true;
    } catch (_) {}

    const urls = (function () {
      const list = [];
      try {
        const a = String(window.EFI_SCRIPT_URL || '').trim();
        const b = String(window.EFI_SCRIPT_FALLBACK_URL || '').trim();
        if (a) list.push(a);
        if (b && b !== a) list.push(b);
      } catch (_) {}
      return list;
    })();
    if (!urls.length) return false;

    const loadOnce = (src) => new Promise((resolve) => {
      try {
        const any = document.querySelector('script[src="' + src.replace(/"/g, '\\"') + '"]');
        if (any) return resolve(true);
      } catch (_) {}
      const s = document.createElement('script');
      s.type = 'text/javascript';
      s.src = src;
      s.async = true;
      s.setAttribute('data-efi-pay', '1');
      s.onload = () => resolve(true);
      s.onerror = () => resolve(false);
      document.head.appendChild(s);
    });

    let loaded = false;
    for (const src of urls) {
      const ok = await loadOnce(src);
      if (ok) {
        loaded = true;
        break;
      }
    }
    if (!loaded) return false;

    const maxWaitMs = 20000;
    const start = Date.now();
    while (Date.now() - start < maxWaitMs) {
      try {
        if (getEfiCreditCardLib()) return true;
      } catch (_) {}
      await new Promise(r => setTimeout(r, 80));
    }
    return false;
  }

  let stripeInstance = null;
  let stripeElements = null;
  let stripeCardNumberEl = null;
  let stripeCardExpiryEl = null;
  let stripeCardCvcEl = null;
  let stripeMounted = false;
  let stripeEmbeddedMounted = false;
  let stripeEmbeddedCheckout = null;
  let stripeEmbeddedMountedKey = '';
  let stripeEmbeddedPrefetchKey = '';
  let stripeEmbeddedPrefetchClientSecret = '';
  let stripeEmbeddedPrefetchAtMs = 0;
  let stripeEmbeddedPrefetchPromise = null;
  let stripeEmbeddedAutoMounting = false;
  let stripeEmbeddedRefreshTimer = null;

  async function ensureStripeMounted() {
    const provider = String(window.CARD_PROVIDER || 'pagarme').trim().toLowerCase();
    if (provider !== 'stripe') return false;
    if (stripeMounted && stripeInstance && stripeElements && stripeCardNumberEl) return true;

    const publishableKey = String(window.STRIPE_PUBLISHABLE_KEY || '').trim();
    if (!publishableKey) throw new Error('Configuração de pagamento inválida (STRIPE_PUBLISHABLE_KEY ausente).');

    const loadStripe = async () => {
      try { if (window.Stripe) return true; } catch (_) {}
      const existing = document.querySelector('script[src="https://js.stripe.com/v3/"]');
      if (!existing) {
        await new Promise((resolve) => {
          const s = document.createElement('script');
          s.src = 'https://js.stripe.com/v3/';
          s.onload = resolve;
          s.onerror = resolve;
          document.head.appendChild(s);
        });
      } else {
        await new Promise((resolve) => {
          if (window.Stripe) return resolve();
          const done = () => resolve();
          existing.addEventListener('load', done, { once: true });
          existing.addEventListener('error', done, { once: true });
          setTimeout(done, 8000);
        });
      }
      return !!window.Stripe;
    };

    const ok = await loadStripe();
    if (!ok) throw new Error('Não foi possível carregar a Stripe. Recarregue a página e tente novamente.');

    stripeInstance = window.Stripe(publishableKey);
    stripeElements = stripeInstance.elements({ locale: 'pt-BR' });
    const style = {
      base: {
        color: '#111827',
        fontSize: '16px',
        fontFamily: 'system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif'
      },
      invalid: { color: '#ef4444' }
    };

    const numberMount = document.getElementById('stripeCardNumber');
    const expMount = document.getElementById('stripeCardExpiry');
    const cvcMount = document.getElementById('stripeCardCvc');
    if (!numberMount || !expMount || !cvcMount) throw new Error('Formulário de cartão da Stripe não encontrado na página.');

    stripeCardNumberEl = stripeElements.create('cardNumber', { style });
    stripeCardExpiryEl = stripeElements.create('cardExpiry', { style });
    stripeCardCvcEl = stripeElements.create('cardCvc', { style });
    stripeCardNumberEl.mount(numberMount);
    stripeCardExpiryEl.mount(expMount);
    stripeCardCvcEl.mount(cvcMount);

    stripeMounted = true;
    return true;
  }

  (function prefetchStripeForFasterCardOpen(){
    try {
      const provider = String(window.CARD_PROVIDER || 'pagarme').trim().toLowerCase();
      const useCheckout = window.STRIPE_USE_CHECKOUT === true || String(window.STRIPE_USE_CHECKOUT || '').toLowerCase() === 'true';
      if (provider !== 'stripe') return;
      if (useCheckout) return;
      const run = () => { ensureStripeMounted().catch(() => {}); };
      if (typeof window.requestIdleCallback === 'function') {
        window.requestIdleCallback(run, { timeout: 2500 });
      } else {
        setTimeout(run, 350);
      }
    } catch (_) {}
  })();

  function getStripeEmbeddedPrefetchKey() {
    try {
      const provider = String(window.CARD_PROVIDER || 'pagarme').trim().toLowerCase();
      const useCheckout = window.STRIPE_USE_CHECKOUT === true || String(window.STRIPE_USE_CHECKOUT || '').toLowerCase() === 'true';
      if (provider !== 'stripe' || !useCheckout) return '';
      const tipo = String((tipoSelect && tipoSelect.value) || '').trim();
      const qtd = String((qtdSelect && qtdSelect.value) || '').trim();
      const phoneDigits = (function(){
        const inputVal = (checkoutPhoneInput && checkoutPhoneInput.value && checkoutPhoneInput.value.trim()) ? String(checkoutPhoneInput.value).trim() : '';
        const urlVal = (function () {
          try {
            const raw = new URLSearchParams(window.location.search).get('phone') || '';
            return String(raw || '').trim();
          } catch (_) {
            return '';
          }
        })();
        let d = onlyDigits(inputVal || urlVal || '');
        d = d.replace(/^0+/, '');
        if (d.startsWith('55') && d.length > 11) d = d.slice(2);
        if (d.length > 11) d = d.slice(-11);
        if (!(d.length === 10 || d.length === 11)) return '';
        if (d.startsWith('0')) return '';
        if (d.length === 11 && d[2] !== '9') {
          const d2 = d.slice(0, 2) + d.slice(3);
          if (/^[1-9]{2}[0-9]{8}$/.test(d2)) d = d2;
        }
        return d;
      })();
      if (!tipo || !qtd || !phoneDigits) return '';
      const totalCents = (typeof window.calculateTotalCents === 'function') ? window.calculateTotalCents() : (window.basePriceCents || 0);
      const usernamePreview = (checkoutProfileUsername && checkoutProfileUsername.textContent && checkoutProfileUsername.textContent.trim()) || '';
      let usernameFromSession = '';
      try { usernameFromSession = sessionStorage.getItem('oppus_instagram_username') || ''; } catch(_) {}
      const usernameInputRaw = (usernameCheckoutInput && usernameCheckoutInput.value && usernameCheckoutInput.value.trim()) || '';
      const instagramUsernameFinal = usernamePreview || usernameFromSession || usernameInputRaw || '';
      const coupon = String(window.couponCode || '').trim().toUpperCase();
      return [tipo, qtd, String(totalCents || 0), phoneDigits, instagramUsernameFinal, coupon].join('|');
    } catch (_) {
      return '';
    }
  }

  async function prefetchStripeEmbeddedCheckoutSession() {
    try {
      const provider = String(window.CARD_PROVIDER || 'pagarme').trim().toLowerCase();
      const useCheckout = window.STRIPE_USE_CHECKOUT === true || String(window.STRIPE_USE_CHECKOUT || '').toLowerCase() === 'true';
      if (provider !== 'stripe' || !useCheckout) return false;
      const key = getStripeEmbeddedPrefetchKey();
      if (!key) return false;
      const now = Date.now();
      if (stripeEmbeddedPrefetchClientSecret && stripeEmbeddedPrefetchKey === key && stripeEmbeddedPrefetchAtMs && (now - stripeEmbeddedPrefetchAtMs) < (10 * 60 * 1000)) {
        return true;
      }
      if (stripeEmbeddedPrefetchPromise && stripeEmbeddedPrefetchKey === key) {
        return await stripeEmbeddedPrefetchPromise;
      }
      stripeEmbeddedPrefetchKey = key;
      stripeEmbeddedPrefetchClientSecret = '';
      stripeEmbeddedPrefetchAtMs = 0;
      stripeEmbeddedPrefetchPromise = (async () => {
        const tipo = String((tipoSelect && tipoSelect.value) || '').trim();
        const qtd = Number((qtdSelect && qtdSelect.value) || 0);
        if (!tipo || !(qtd > 0)) return false;
        const totalCents = (typeof window.calculateTotalCents === 'function') ? window.calculateTotalCents() : (window.basePriceCents || 0);
        const phoneValue = (function(){
          const inputVal = (checkoutPhoneInput && checkoutPhoneInput.value && checkoutPhoneInput.value.trim()) ? String(checkoutPhoneInput.value).trim() : '';
          const urlVal = (function () {
            try {
              const raw = new URLSearchParams(window.location.search).get('phone') || '';
              return String(raw || '').trim();
            } catch (_) {
              return '';
            }
          })();
          let d = onlyDigits(inputVal || urlVal || '');
          d = d.replace(/^0+/, '');
          if (d.startsWith('55') && d.length > 11) d = d.slice(2);
          if (d.length > 11) d = d.slice(-11);
          if (!(d.length === 10 || d.length === 11)) return '';
          if (d.startsWith('0')) return '';
          if (d.length === 11 && d[2] !== '9') {
            const d2 = d.slice(0, 2) + d.slice(3);
            if (/^[1-9]{2}[0-9]{8}$/.test(d2)) d = d2;
          }
          return d;
        })();
        if (!phoneValue) return false;
        const correlationID = (window.crypto && window.crypto.randomUUID) ? window.crypto.randomUUID() : `${Date.now()}-${Math.random().toString(16).slice(2)}`;
        const serviceCategory = (function(){
          if (window.__ENG_MODE__) return 'seguidores';
          try {
            const path = window.location && window.location.pathname ? window.location.pathname : '';
            if (path.indexOf('/servicos-curtidas') === 0) return 'curtidas';
            if (path.indexOf('/servicos-visualizacoes') === 0) return 'visualizacoes';
          } catch(_) {}
          return 'seguidores';
        })();
        const usernamePreview = (checkoutProfileUsername && checkoutProfileUsername.textContent && checkoutProfileUsername.textContent.trim()) || '';
        let usernameFromSession = '';
        try { usernameFromSession = sessionStorage.getItem('oppus_instagram_username') || ''; } catch(_) {}
        const usernameInputRaw = (usernameCheckoutInput && usernameCheckoutInput.value && usernameCheckoutInput.value.trim()) || '';
        const instagramUsernameFinal = usernamePreview || usernameFromSession || usernameInputRaw || '';
        const promos = (typeof getSelectedPromos === 'function') ? getSelectedPromos() : [];
        const promosTotalCents = (function () {
          try {
            const cents = (typeof calcPromosTotalCents === 'function') ? calcPromosTotalCents(promos) : 0;
            return Number.isFinite(Number(cents)) ? Number(cents) : 0;
          } catch (_) {
            return 0;
          }
        })();
        const buildUtmsFromLocation = function () {
          try {
            const sp = new URLSearchParams(window.location.search || '');
            return {
              source: sp.get('utm_source') || '',
              medium: sp.get('utm_medium') || '',
              campaign: sp.get('utm_campaign') || '',
              term: sp.get('utm_term') || '',
              content: sp.get('utm_content') || '',
              gclid: sp.get('gclid') || '',
              fbclid: sp.get('fbclid') || '',
              ref: window.location.href || ''
            };
          } catch (_) {
            return { ref: (window.location && window.location.href) ? String(window.location.href) : '' };
          }
        };
        const totalLabel = (typeof formatCentsToBRL === 'function') ? formatCentsToBRL(totalCents) : String(totalCents);
        const payload = {
          correlationID,
          installments: 1,
          total_cents: totalCents,
          items: [
            { title: `${qtd} ${getUnitForTipo(tipo)}`, quantity: 1, price_cents: totalCents }
          ],
          customer: { phone_number: phoneValue },
          additionalInfo: [
            { key: 'tipo_servico', value: tipo },
            { key: 'categoria_servico', value: serviceCategory },
            { key: 'quantidade', value: String(qtd) },
            { key: 'pacote', value: `${qtd} ${getUnitForTipo(tipo)} - ${totalLabel}` },
            { key: 'phone', value: phoneValue },
            { key: 'instagram_username', value: instagramUsernameFinal },
            { key: 'order_bumps_total', value: (typeof formatCentsToBRL === 'function') ? formatCentsToBRL(promosTotalCents) : String(promosTotalCents) },
            { key: 'order_bumps', value: promos.map(p => `${p.key}:${p.qty ?? 1}`).join(';') },
            { key: 'payment_method', value: 'credit_card' },
            { key: 'installments', value: '1' }
          ],
          profile_is_private: !!window.isInstagramPrivate,
          comment: 'Checkout OPPUS Card',
          utms: buildUtmsFromLocation(),
          checkoutUiMode: 'embedded'
        };
        try {
          const cc = String(window.couponCode || '').trim();
          if (cc) payload.additionalInfo.push({ key: 'cupom', value: cc.toUpperCase() });
        } catch (_) {}
        let checkoutResp = null;
        try {
          checkoutResp = await fetch('/api/stripe/create-checkout-session', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
          });
        } catch (_) {
          return false;
        }
        let checkoutData = null;
        try { checkoutData = await checkoutResp.json(); } catch (_) { checkoutData = {}; }
        if (!checkoutResp.ok) return false;
        const clientSecret = String(checkoutData?.clientSecret || checkoutData?.client_secret || '').trim();
        if (!clientSecret) return false;
        stripeEmbeddedPrefetchClientSecret = clientSecret;
        stripeEmbeddedPrefetchAtMs = Date.now();
        return true;
      })();
      const ok = await stripeEmbeddedPrefetchPromise;
      stripeEmbeddedPrefetchPromise = null;
      return !!ok;
    } catch (_) {
      try { stripeEmbeddedPrefetchPromise = null; } catch (_) {}
      return false;
    }
  }

  async function ensureStripeJsReady() {
    const provider = String(window.CARD_PROVIDER || 'pagarme').trim().toLowerCase();
    if (provider !== 'stripe') return false;
    const publishableKey = String(window.STRIPE_PUBLISHABLE_KEY || '').trim();
    if (!publishableKey) throw new Error('Configuração de pagamento inválida (STRIPE_PUBLISHABLE_KEY ausente).');
    try { if (window.Stripe) return true; } catch (_) {}
    const existing = document.querySelector('script[src="https://js.stripe.com/v3/"]');
    if (!existing) {
      await new Promise((resolve) => {
        const s = document.createElement('script');
        s.src = 'https://js.stripe.com/v3/';
        s.onload = resolve;
        s.onerror = resolve;
        document.head.appendChild(s);
      });
    } else {
      await new Promise((resolve) => {
        if (window.Stripe) return resolve();
        const done = () => resolve();
        existing.addEventListener('load', done, { once: true });
        existing.addEventListener('error', done, { once: true });
        setTimeout(done, 8000);
      });
    }
    return !!window.Stripe;
  }

  async function mountStripeEmbeddedCheckout(clientSecret) {
    const wrapper = document.getElementById('stripeEmbeddedWrapper');
    const mount = document.getElementById('stripeEmbeddedCheckout');
    if (!wrapper || !mount) throw new Error('Área do checkout incorporado não encontrada.');
    wrapper.style.display = 'block';
    mount.innerHTML = '<div style="padding:14px; text-align:center; color:#6b7280; font-size:0.95rem;">Carregando checkout da Stripe...</div>';
    const ok = await ensureStripeJsReady();
    if (!ok) throw new Error('Não foi possível carregar a Stripe. Recarregue a página e tente novamente.');
    const publishableKey = String(window.STRIPE_PUBLISHABLE_KEY || '').trim();
    const stripe = window.Stripe(publishableKey);
    if (!stripe || typeof stripe.initEmbeddedCheckout !== 'function') {
      throw new Error('Sua integração da Stripe não suporta checkout incorporado. Atualize o Stripe.js e tente novamente.');
    }
    if (stripeEmbeddedMounted && stripeEmbeddedCheckout) {
      try { stripeEmbeddedCheckout.destroy(); } catch (_) {}
      stripeEmbeddedCheckout = null;
      stripeEmbeddedMounted = false;
    }
    mount.innerHTML = '';
    const fields = document.getElementById('cardPaymentForm');
    if (fields) fields.style.display = 'none';
    stripeEmbeddedCheckout = await stripe.initEmbeddedCheckout({ clientSecret: String(clientSecret || '').trim() });
    stripeEmbeddedCheckout.mount('#stripeEmbeddedCheckout');
    stripeEmbeddedMounted = true;
    try { stripeEmbeddedMountedKey = getStripeEmbeddedPrefetchKey() || ''; } catch(_) { stripeEmbeddedMountedKey = ''; }
  }

  async function maybeAutoMountStripeEmbeddedCheckout() {
    try {
      if (stripeEmbeddedAutoMounting) return false;
      if (typeof currentPaymentMethod !== 'undefined' && currentPaymentMethod !== 'credit_card') return false;
      const provider = String(window.CARD_PROVIDER || 'pagarme').trim().toLowerCase();
      const useCheckout = window.STRIPE_USE_CHECKOUT === true || String(window.STRIPE_USE_CHECKOUT || '').toLowerCase() === 'true';
      if (provider !== 'stripe' || !useCheckout) return false;
      if (stripeEmbeddedMounted && stripeEmbeddedCheckout) return true;

      const key = getStripeEmbeddedPrefetchKey();
      if (!key) return false;

      stripeEmbeddedAutoMounting = true;
      const ok = await prefetchStripeEmbeddedCheckoutSession();
      if (!ok) return false;
      const cs = String(stripeEmbeddedPrefetchClientSecret || '').trim();
      if (!cs) return false;

      if (typeof currentPaymentMethod !== 'undefined' && currentPaymentMethod !== 'credit_card') return false;
      await mountStripeEmbeddedCheckout(cs);
      return true;
    } catch (_) {
      return false;
    } finally {
      try { stripeEmbeddedAutoMounting = false; } catch (_) {}
    }
  }

  function teardownStripeEmbeddedCheckout(messageHtml) {
    try {
      if (stripeEmbeddedMounted && stripeEmbeddedCheckout) {
        try { stripeEmbeddedCheckout.destroy(); } catch (_) {}
      }
    } catch (_) {}
    try { stripeEmbeddedCheckout = null; } catch (_) {}
    try { stripeEmbeddedMounted = false; } catch (_) {}
    try { stripeEmbeddedMountedKey = ''; } catch (_) {}
    try {
      const wrapper = document.getElementById('stripeEmbeddedWrapper');
      const mount = document.getElementById('stripeEmbeddedCheckout');
      if (wrapper && mount) {
        wrapper.style.display = 'block';
        mount.innerHTML = messageHtml || '';
      }
    } catch (_) {}
    try {
      stripeEmbeddedPrefetchKey = '';
      stripeEmbeddedPrefetchClientSecret = '';
      stripeEmbeddedPrefetchAtMs = 0;
      stripeEmbeddedPrefetchPromise = null;
    } catch (_) {}
  }

  async function refreshStripeEmbeddedCheckoutIfNeeded() {
    try {
      if (typeof currentPaymentMethod !== 'undefined' && currentPaymentMethod !== 'credit_card') return false;
      const provider = String(window.CARD_PROVIDER || 'pagarme').trim().toLowerCase();
      const useCheckout = window.STRIPE_USE_CHECKOUT === true || String(window.STRIPE_USE_CHECKOUT || '').toLowerCase() === 'true';
      if (provider !== 'stripe' || !useCheckout) return false;
      const key = getStripeEmbeddedPrefetchKey();
      if (!key) return false;
      if (stripeEmbeddedMounted && stripeEmbeddedCheckout && stripeEmbeddedMountedKey && stripeEmbeddedMountedKey !== key) {
        teardownStripeEmbeddedCheckout('<div style="padding:14px; text-align:center; color:#6b7280; font-size:0.95rem;">Atualizando valor do checkout da Stripe...</div>');
      }
      return await maybeAutoMountStripeEmbeddedCheckout();
    } catch (_) {
      return false;
    }
  }

  function scheduleStripeEmbeddedCheckoutRefresh() {
    try { if (stripeEmbeddedRefreshTimer) clearTimeout(stripeEmbeddedRefreshTimer); } catch (_) {}
    stripeEmbeddedRefreshTimer = setTimeout(() => {
      refreshStripeEmbeddedCheckoutIfNeeded().catch(() => {});
    }, 350);
  }

  async function handleCardPayment(e) {
    if (e && typeof e.preventDefault === 'function') e.preventDefault();

    // Check Upsell logic (only if relevant for followers)
    try {
        // Upsell removed
    } catch(e) { console.error(e); }

    const payWithCardBtn = document.getElementById('payWithCardBtn');
    if (payWithCardBtn) {
      payWithCardBtn.disabled = true;
      payWithCardBtn.classList.add('loading');
      const span = payWithCardBtn.querySelector('.button-text');
      if (span) {
          if (!span.dataset.original) span.dataset.original = span.textContent;
          span.textContent = 'Processando...';
      }
    }

    try {
      const provider = String(window.CARD_PROVIDER || 'pagarme').trim().toLowerCase();
      const isStripe = provider === 'stripe';
      const useCheckout = window.STRIPE_USE_CHECKOUT === true || String(window.STRIPE_USE_CHECKOUT || '').toLowerCase() === 'true';
      const isStripeCheckout = isStripe && useCheckout;

      // 1. Validação Básica e Highlight de Erros
      const fields = isStripeCheckout
        ? []
        : isStripe
        ? [
          { id: 'cardHolderName', type: 'text' },
          { id: 'cardHolderCpf', type: 'text' }
        ]
        : [
          { id: 'cardNumber', type: 'text' },
          { id: 'cardExpiry', type: 'text' },
          { id: 'cardCvv', type: 'text' },
          { id: 'cardHolderName', type: 'text' },
          { id: 'cardHolderCpf', type: 'text' }
        ];

      let firstError = null;
      let values = {};

      fields.forEach(f => {
          const el = document.getElementById(f.id);
          if (el) {
              el.classList.remove('input-error');
              el.classList.remove('tutorial-highlight'); // Remove highlight anterior
              const val = el.value.trim();
              
              let isValid = true;
              if (!val) isValid = false;
              
              if (!isValid) {
                  el.classList.add('input-error');
                  el.classList.add('tutorial-highlight');
                  if (!firstError) firstError = el;
              }
              values[f.id] = val;
          }
      });

      if (firstError) {
          firstError.focus();
          try { firstError.scrollIntoView({ behavior: 'smooth', block: 'center' }); } catch(_) {}
          throw new Error('Por favor, preencha todos os campos obrigatórios destacados.');
      }

      const cardHolder = String(values.cardHolderName || '').trim();
      const installmentsEl = document.getElementById('cardInstallments');
      let installments = String(installmentsEl?.value || '').trim();
      if (!installments && installmentsEl) {
        const opts = Array.prototype.slice.call(installmentsEl.querySelectorAll('option'));
        const firstNumeric = opts.map(o => String(o.value || '').trim()).find(v => /^\d+$/.test(v));
        installments = firstNumeric || '1';
        try { installmentsEl.value = installments; } catch (_) {}
      }

      let cardNum = '';
      let cardExpiry = '';
      let cardCvv = '';
      let expMonth = '';
      let expYear = '';
      let pagarmePublicKey = '';
      if (!isStripe) {
        cardNum = String(values.cardNumber || '').replace(/\D/g, '');
        cardExpiry = String(values.cardExpiry || '').trim();
        cardCvv = String(values.cardCvv || '').trim();
        pagarmePublicKey = String(window.PAGARME_PUBLIC_KEY || '').trim();
        if (!pagarmePublicKey) {
          throw new Error('Configuração de pagamento inválida (PAGARME_PUBLIC_KEY ausente).');
        }

        if (cardExpiry.includes('/')) {
            [expMonth, expYear] = cardExpiry.split('/');
        } else {
            expMonth = cardExpiry.substring(0, 2);
            expYear = cardExpiry.substring(2);
        }
        if (expYear && expYear.length === 2) expYear = '20' + expYear;
        if (!expMonth || !expYear || expMonth > 12 || expMonth < 1) throw new Error('Data de validade inválida');
      } else {
        if (!useCheckout) await ensureStripeMounted();
      }

      const normalizeDigits = (v) => String(v || '').replace(/\D/g, '');
      const isValidCPF = (cpfRaw) => {
        const cpf = normalizeDigits(cpfRaw);
        if (cpf.length !== 11) return false;
        if (/^(\d)\1+$/.test(cpf)) return false;
        const calc = (base, factor) => {
          let sum = 0;
          for (let i = 0; i < base.length; i++) sum += Number(base[i]) * (factor - i);
          const mod = (sum * 10) % 11;
          return mod === 10 ? 0 : mod;
        };
        const d1 = calc(cpf.slice(0, 9), 10);
        const d2 = calc(cpf.slice(0, 10), 11);
        return d1 === Number(cpf[9]) && d2 === Number(cpf[10]);
      };

      const cardHolderCpf = normalizeDigits(values.cardHolderCpf || '');
      if (!isStripeCheckout) {
        if (!/^\d{11}$/.test(cardHolderCpf) || !isValidCPF(cardHolderCpf)) {
          try {
            const cpfEl = document.getElementById('cardHolderCpf');
            if (cpfEl) {
              cpfEl.classList.add('input-error');
              cpfEl.classList.add('tutorial-highlight');
              try { cpfEl.focus(); } catch (_) {}
              try { cpfEl.scrollIntoView({ behavior: 'smooth', block: 'center' }); } catch(_) {}
            }
          } catch (_) {}
          throw new Error('CPF do titular inválido. Digite um CPF válido (11 dígitos).');
        }
      }

      const cardToken = isStripe ? '' : await (async () => {
        const ctrl = (typeof AbortController !== 'undefined') ? new AbortController() : null;
        const timeoutId = ctrl ? setTimeout(() => { try { ctrl.abort(); } catch (_) {} }, 20000) : null;
        try {
          const tokenUrl = `https://api.pagar.me/core/v5/tokens?appId=${encodeURIComponent(pagarmePublicKey)}`;
          let tokenResp;
          try {
            tokenResp = await fetch(tokenUrl, {
              method: 'POST',
              headers: {
                'Content-Type': 'application/json'
              },
              body: JSON.stringify({
                type: 'card',
                card: {
                  number: cardNum,
                  holder_name: cardHolder,
                  holder_document: cardHolderCpf,
                  exp_month: Number(expMonth),
                  exp_year: Number(expYear),
                  cvv: cardCvv
                }
              }),
              signal: ctrl ? ctrl.signal : undefined
            });
          } catch (e) {
            const origin = (function () {
              try { return String(window.location && window.location.origin || '').trim(); } catch (_) { return ''; }
            })();
            const msg = String(e && (e.message || e.name) || '').toLowerCase();
            const isAbort = (e && e.name === 'AbortError') || msg.includes('abort');
            if (isAbort) {
              throw new Error('Tempo esgotado ao tokenizar o cartão. Verifique sua conexão e tente novamente.');
            }
            const originHint = origin ? `Domínio atual: ${origin}. Cadastre exatamente esse domínio no Pagar.me (CORS/Tokenização) e tente novamente.` : 'Cadastre o domínio do checkout no Pagar.me (CORS/Tokenização) e tente novamente.';
            throw new Error(`Falha ao conectar com o Pagar.me para tokenizar o cartão (CORS/rede). ${originHint}`);
          }
          const tokenData = await tokenResp.json().catch(() => ({}));
          if (!tokenResp.ok) {
            const apiMsg =
              (Array.isArray(tokenData?.errors) && tokenData.errors[0] && tokenData.errors[0].message ? String(tokenData.errors[0].message) : '') ||
              String(tokenData?.message || tokenData?.error || '');
            const trimmed = String(apiMsg || 'Erro ao tokenizar cartão.').trim();
            const origin = (function () {
              try { return String(window.location && window.location.origin || '').trim(); } catch (_) { return ''; }
            })();
            const lower = trimmed.toLowerCase();
            const firstErr = (Array.isArray(tokenData?.errors) && tokenData.errors[0]) ? tokenData.errors[0] : null;
            const firstErrMsg = firstErr && firstErr.message ? String(firstErr.message).trim() : '';
            const looksLikeOriginError = /origin|dom[ií]nio|domain|cors|allowlist|whitelist|unauthorized/i.test(lower) || /origin|dom[ií]nio|domain|cors|allowlist|whitelist|unauthorized/i.test(String(firstErrMsg || '').toLowerCase());
            const statusCode = Number(tokenResp.status || 0);
            const reqId = (function () {
              try {
                const h = tokenResp.headers;
                if (!h || !h.get) return '';
                return String(h.get('x-request-id') || h.get('request-id') || h.get('x-kong-request-id') || '').trim();
              } catch (_) { return ''; }
            })();
            const hintParts = [];
            if (origin) hintParts.push(`Domínio atual: ${origin}`);
            hintParts.push('No Pagar.me, cadastre exatamente esse domínio (sem /caminho) e confirme com Enter/Tab');
            hintParts.push('Se você preencheu IPs permitidos, teste com a lista vazia ou adicione seu IP público atual');
            const hint = hintParts.join('. ') + '.';
            const debugBits = [];
            if (statusCode) debugBits.push(`HTTP ${statusCode}`);
            if (reqId) debugBits.push(`Request-ID ${reqId}`);
            const debug = debugBits.length ? ` (${debugBits.join(', ')})` : '';
            if (looksLikeOriginError) {
              throw new Error(`Tokenização recusada pelo Pagar.me${debug}. ${hint}`);
            }
            if (lower === 'the request is invalid.') {
              const errs = (function () {
                try {
                  if (!Array.isArray(tokenData?.errors)) return '';
                  const parts = tokenData.errors
                    .map(e => {
                      const param = e && e.parameter ? String(e.parameter).trim() : '';
                      const m = e && e.message ? String(e.message).trim() : '';
                      if (!param && !m) return '';
                      return param ? `${param}: ${m || 'inválido'}` : (m || '');
                    })
                    .filter(Boolean);
                  return parts.length ? parts.join(' | ') : '';
                } catch (_) {
                  return '';
                }
              })();
              const errsLower = String((firstErrMsg || errs || '')).toLowerCase();
              if (/(holder_document|holder document|documento|cpf)/i.test(errsLower)) {
                try {
                  const cpfEl = document.getElementById('cardHolderCpf');
                  if (cpfEl) {
                    cpfEl.classList.add('input-error');
                    cpfEl.classList.add('tutorial-highlight');
                    try { cpfEl.focus(); } catch (_) {}
                    try { cpfEl.scrollIntoView({ behavior: 'smooth', block: 'center' }); } catch(_) {}
                  }
                } catch (_) {}
                throw new Error(`CPF do titular inválido no Pagar.me${debug}. Digite um CPF válido e tente novamente.`);
              }
              const msg = firstErrMsg || (errs ? `Requisição inválida no Pagar.me. ${errs}` : 'Requisição inválida no Pagar.me.');
              throw new Error(`${msg}${debug}. Verifique os dados do cartão e do titular e tente novamente.`);
            }
            throw new Error((trimmed || (firstErrMsg ? String(firstErrMsg) : 'Erro ao tokenizar cartão.')) + (debug ? debug : ''));
          }
          const id = String(tokenData?.id || '').trim();
          if (!id) throw new Error('Erro ao tokenizar cartão.');
          return id;
        } finally {
          if (timeoutId) clearTimeout(timeoutId);
        }
      })();

      // 3. Preparar Payload
      const tipo = document.getElementById('tipoSelect')?.value || window.selectedType || 'seguidores';
      const qtdSelect = document.getElementById('quantidadeSelect');
      const qtd = qtdSelect ? Number(qtdSelect.value) : (window.selectedPlan ? window.selectedPlan.q : 1);
      
      // Calculate Price
      let totalCents = 0;
      if (typeof window.calculateTotalCents === 'function') {
          totalCents = window.calculateTotalCents(); // Should return correct total including bumps
      } else {
          totalCents = window.basePriceCents || 0;
      }

      if (totalCents < 500) { // Safety check for min amount (R$ 5,00 typically, but here R$ 1,00 min)
           // alert('Valor mínimo não atingido.');
      }

      const phoneValue = (function(){
        const inputVal = (checkoutPhoneInput && checkoutPhoneInput.value && checkoutPhoneInput.value.trim()) ? String(checkoutPhoneInput.value).trim() : '';
        const urlVal = (function () {
          try {
            const raw = new URLSearchParams(window.location.search).get('phone') || '';
            return String(raw || '').trim();
          } catch (_) {
            return '';
          }
        })();
        let d = onlyDigits(inputVal || urlVal || '');
        d = d.replace(/^0+/, '');
        if (d.startsWith('55') && d.length > 11) d = d.slice(2);
        if (d.length > 11) d = d.slice(-11);
        if (!(d.length === 10 || d.length === 11)) return '';
        if (d.startsWith('0')) return '';
        if (d.length === 11 && d[2] !== '9') {
          const d2 = d.slice(0, 2) + d.slice(3);
          if (/^[1-9]{2}[0-9]{8}$/.test(d2)) d = d2;
        }
        return d;
      })();
      if (!phoneValue) {
        try {
          if (checkoutPhoneInput) {
            checkoutPhoneInput.classList.add('tutorial-highlight');
            try { checkoutPhoneInput.focus(); } catch (_) {}
            try { checkoutPhoneInput.scrollIntoView({ behavior: 'smooth', block: 'center' }); } catch (_) {}
          }
        } catch (_) {}
        throw new Error('Digite seu telefone (DDD + número) para pagar no cartão.');
      }

      const correlationID = (window.crypto && window.crypto.randomUUID) ? window.crypto.randomUUID() : `${Date.now()}-${Math.random().toString(16).slice(2)}`;
      
      // Username logic
      const usernamePreview = (checkoutProfileUsername && checkoutProfileUsername.textContent && checkoutProfileUsername.textContent.trim()) || '';
      let usernameFromSession = '';
      try { usernameFromSession = sessionStorage.getItem('oppus_instagram_username') || ''; } catch(_) {}
      const usernameInputRaw = (usernameCheckoutInput && usernameCheckoutInput.value && usernameCheckoutInput.value.trim()) || '';
      const instagramUsernameFinal = usernamePreview || usernameFromSession || usernameInputRaw || '';
      
      const serviceCategory = (function(){
        if (window.__ENG_MODE__) return 'seguidores';
        try {
          const path = window.location.pathname;
          if (path.includes('curtidas')) return 'curtidas';
          if (path.includes('visualizacoes')) return 'visualizacoes';
        } catch(_) {}
        return 'seguidores';
      })();

      const provider2 = String(window.CARD_PROVIDER || 'pagarme').trim().toLowerCase();
      const isStripe2 = provider2 === 'stripe';
      const useCheckout2 = window.STRIPE_USE_CHECKOUT === true || String(window.STRIPE_USE_CHECKOUT || '').toLowerCase() === 'true';
      const isStripeCheckout2 = isStripe2 && useCheckout2;
      const customerPayload = isStripeCheckout2 ? { phone_number: phoneValue } : { name: cardHolder, cpf: cardHolderCpf, phone_number: phoneValue };
      const buildUtmsFromLocation = function () {
        try {
          const sp = new URLSearchParams(window.location.search || '');
          return {
            source: sp.get('utm_source') || '',
            medium: sp.get('utm_medium') || '',
            campaign: sp.get('utm_campaign') || '',
            term: sp.get('utm_term') || '',
            content: sp.get('utm_content') || '',
            gclid: sp.get('gclid') || '',
            fbclid: sp.get('fbclid') || '',
            ref: window.location.href || ''
          };
        } catch (_) {
          return { ref: (window.location && window.location.href) ? String(window.location.href) : '' };
        }
      };
      const promos = getSelectedPromos();
      const promosTotalCents = (function () {
        try {
          const cents = (typeof calcPromosTotalCents === 'function') ? calcPromosTotalCents(promos) : 0;
          return Number.isFinite(Number(cents)) ? Number(cents) : 0;
        } catch (_) {
          return 0;
        }
      })();
      const totalLabel = (typeof formatCentsToBRL === 'function') ? formatCentsToBRL(totalCents) : String(totalCents);
      const payload = {
        correlationID,
        installments: isStripeCheckout2 ? 1 : (Number(installments) || 1),
        total_cents: totalCents,
        items: [
           { title: `${qtd} ${getUnitForTipo(tipo)}`, quantity: 1, price_cents: totalCents }
        ],
        customer: customerPayload,
        additionalInfo: [
          { key: 'tipo_servico', value: tipo },
          { key: 'categoria_servico', value: serviceCategory },
          { key: 'quantidade', value: String(qtd) },
          { key: 'pacote', value: `${qtd} ${getUnitForTipo(tipo)} - ${totalLabel}` },
          { key: 'phone', value: phoneValue },
          { key: 'instagram_username', value: instagramUsernameFinal },
          { key: 'order_bumps_total', value: (typeof formatCentsToBRL === 'function') ? formatCentsToBRL(promosTotalCents) : String(promosTotalCents) },
          { key: 'order_bumps', value: promos.map(p => `${p.key}:${p.qty ?? 1}`).join(';') },
          { key: 'cupom', value: window.couponCode || '' },
          { key: 'payment_method', value: 'credit_card' },
          { key: 'installments', value: String(Number(installments) || 1) }
        ],
        profile_is_private: !!window.isInstagramPrivate,
        comment: 'Checkout OPPUS Card',
        utms: buildUtmsFromLocation()
      };
      if (!isStripe) payload.card_token = cardToken;
      const pushAdditionalInfoIfMissing = (k, v) => {
        const kk = String(k || '').trim();
        const vv = String(v || '').trim();
        if (!kk || !vv) return;
        if (payload.additionalInfo.some(it => it && it.key === kk)) return;
        payload.additionalInfo.push({ key: kk, value: vv });
      };
      try {
        const cc = String(window.couponCode || '').trim();
        if (cc) {
          for (let i = payload.additionalInfo.length - 1; i >= 0; i--) {
            if (payload.additionalInfo[i] && payload.additionalInfo[i].key === 'cupom') payload.additionalInfo.splice(i, 1);
          }
          payload.additionalInfo.push({ key: 'cupom', value: cc.toUpperCase() });
        }
      } catch (_) {}

      try {
        const m = document.cookie.match(/(?:^|;\s*)tc_code=([^;]+)/);
        const tc = m && m[1] ? m[1] : '';
        if (tc) payload.additionalInfo.push({ key: 'tc_code', value: tc });
      } catch(_) {}
      try {
        let sckValue = '';
        try {
          const params = new URLSearchParams(window.location.search || '');
          sckValue = params.get('sck') || '';
        } catch (_) {}
        if (!sckValue) {
          try {
            const m2 = document.cookie.match(/(?:^|;\s*)index=([^;]+)/);
            sckValue = m2 && m2[1] ? decodeURIComponent(m2[1]) : '';
          } catch (_) {}
        }
        if (sckValue) payload.additionalInfo.push({ key: 'sck', value: sckValue });
      } catch(_) {}
      
      try {
        for (let i = payload.additionalInfo.length - 1; i >= 0; i--) {
          const k = payload.additionalInfo[i] && payload.additionalInfo[i].key ? String(payload.additionalInfo[i].key) : '';
          if (k === 'order_bumps' || k === 'order_bumps_total') payload.additionalInfo.splice(i, 1);
        }
        payload.additionalInfo.push({ key: 'order_bumps_total', value: (typeof formatCentsToBRL === 'function') ? formatCentsToBRL(promosTotalCents) : String(promosTotalCents) });
        payload.additionalInfo.push({ key: 'order_bumps', value: promos.map(p => `${p.key}:${p.qty ?? 1}`).join(';') });
      } catch (_) {}

      try {
        if ((serviceCategory === 'curtidas' || serviceCategory === 'visualizacoes') && typeof collectValidPostLinks === 'function') {
          const linksNow = collectValidPostLinks();
          if (Array.isArray(linksNow) && linksNow.length) {
            pushAdditionalInfoIfMissing('post_link', linksNow[0]);
            pushAdditionalInfoIfMissing('post_links', linksNow.join(','));
          }
        }
      } catch (_) {}

      const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
      const fetchRetry = async (url, opts, attempts) => {
        let lastErr = null;
        const max = Number(attempts) || 1;
        for (let i = 0; i < max; i++) {
          try {
            return await fetch(url, opts);
          } catch (e) {
            lastErr = e;
            if (i >= max - 1) throw e;
            await sleep(500 * (i + 1));
          }
        }
        throw lastErr || new Error('fetch_failed');
      };

      try {
        const selResp = await fetchRetry('/api/instagram/selected-for', { headers: { 'Accept': 'application/json' } }, 3);
        const selData = await selResp.json();
        const sfor = selData && selData.selectedFor ? selData.selectedFor : {};
        const normalizeIgShortcode = function (sc) {
          const v = String(sc || '').trim();
          if (!v) return '';
          const m = v.match(/^[A-Za-z0-9_-]+/);
          const code = m ? String(m[0] || '') : '';
          if (!code) return '';
          return code.length > 15 ? code.slice(0, 11) : code;
        };
        const buildIgMediaLink = function (k, sc) {
          const code = normalizeIgShortcode(sc);
          if (!code) return '';
          const kindPath = (k === 'views') ? 'reel' : 'p';
          return `https://www.instagram.com/${kindPath}/${encodeURIComponent(code)}/`;
        };
        const mapKind = function (k) {
          const obj = sfor && sfor[k];
          const sc = obj && obj.shortcode;
          return sc ? buildIgMediaLink(k, sc) : '';
        };
        const likesLink = mapKind('likes');
        const viewsLink = mapKind('views');
        const commentsLink = mapKind('comments');
        const anyLink = viewsLink || likesLink || commentsLink;
        if (serviceCategory === 'curtidas' || serviceCategory === 'visualizacoes') {
          const mainCandidate = (serviceCategory === 'visualizacoes') ? (viewsLink || anyLink) : (likesLink || anyLink);
          if (mainCandidate) pushAdditionalInfoIfMissing('post_link', mainCandidate);
        }

        const hasLikes = promos.some(p => p.key === 'likes');
        const hasViews = promos.some(p => p.key === 'views');
        const hasComments = promos.some(p => p.key === 'comments');
        const kinds = [];
        if (hasLikes) kinds.push('likes');
        if (hasViews) kinds.push('views');
        if (hasComments) kinds.push('comments');

        if (kinds.length === 1) {
          const onlyKind = kinds[0];
          let link = mapKind(onlyKind);
          if (!link && instagramUsernameFinal) {
            try {
              const url = '/api/instagram/posts?username=' + encodeURIComponent(instagramUsernameFinal);
              const pr = await fetchRetry(url, { headers: { 'Accept': 'application/json' } }, 2);
              const pd = await pr.json();
              const posts = Array.isArray(pd && pd.posts) ? pd.posts : [];
              const isVideo = (p) => !!(p && (p.isVideo || /video|clip/.test(String(p.typename || '').toLowerCase())));
              const candidates = onlyKind === 'views' ? posts.filter(isVideo) : posts;
              const pick = (candidates && candidates[0]) || (posts && posts[0]) || null;
              if (pick && pick.shortcode) link = buildIgMediaLink(onlyKind, pick.shortcode);
            } catch (_) {}
          }
          if (link) payload.additionalInfo.push({ key: `orderbump_post_${onlyKind}`, value: link });
        } else {
          if (hasLikes && anyLink) payload.additionalInfo.push({ key: 'orderbump_post_likes', value: likesLink || anyLink });
          if (hasViews && anyLink) payload.additionalInfo.push({ key: 'orderbump_post_views', value: viewsLink || anyLink });
          if (hasComments && anyLink) payload.additionalInfo.push({ key: 'orderbump_post_comments', value: commentsLink || anyLink });
        }
      } catch (_) {}

      // 4. Enviar para Backend
      if (isStripe) {
        const stripePayload = Object.assign({}, payload);
        try { delete stripePayload.card_token; } catch (_) {}
        const useCheckout = window.STRIPE_USE_CHECKOUT === true || String(window.STRIPE_USE_CHECKOUT || '').toLowerCase() === 'true';

        if (useCheckout) {
          stripePayload.checkoutUiMode = 'embedded';
          try { showStatusMessageCheckout('Carregando checkout da Stripe...', 'info'); } catch (_) {}
          try {
            const key = getStripeEmbeddedPrefetchKey();
            const now = Date.now();
            if (key && stripeEmbeddedPrefetchClientSecret && stripeEmbeddedPrefetchKey === key && stripeEmbeddedPrefetchAtMs && (now - stripeEmbeddedPrefetchAtMs) < (10 * 60 * 1000)) {
              await mountStripeEmbeddedCheckout(stripeEmbeddedPrefetchClientSecret);
              return;
            }
            if (key && stripeEmbeddedPrefetchPromise && stripeEmbeddedPrefetchKey === key) {
              const ok = await stripeEmbeddedPrefetchPromise;
              stripeEmbeddedPrefetchPromise = null;
              if (ok && stripeEmbeddedPrefetchClientSecret) {
                await mountStripeEmbeddedCheckout(stripeEmbeddedPrefetchClientSecret);
                return;
              }
            }
          } catch (_) {}
          const ctrl = (typeof AbortController !== 'undefined') ? new AbortController() : null;
          const timeoutId = ctrl ? setTimeout(() => { try { ctrl.abort(); } catch (_) {} }, 45000) : null;
          let checkoutResp = null;
          try {
            checkoutResp = await fetch('/api/stripe/create-checkout-session', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify(stripePayload),
              signal: ctrl ? ctrl.signal : undefined
            });
          } catch (_) {
            throw new Error('Falha ao conectar no servidor. Recarregue a página e tente novamente.');
          } finally {
            if (timeoutId) clearTimeout(timeoutId);
          }
          let checkoutData = null;
          try { checkoutData = await checkoutResp.json(); } catch (_) { checkoutData = {}; }
          if (!checkoutResp.ok) {
            const errCode = String(checkoutData?.error || '').trim().toLowerCase();
            if (errCode === 'invalid_cpf') {
              try {
                const cpfEl = document.getElementById('cardHolderCpf');
                if (cpfEl) {
                  cpfEl.classList.add('input-error');
                  cpfEl.classList.add('tutorial-highlight');
                  try { cpfEl.focus(); } catch (_) {}
                  try { cpfEl.scrollIntoView({ behavior: 'smooth', block: 'center' }); } catch(_) {}
                }
              } catch (_) {}
            }
            if (errCode === 'missing_phone' || errCode === 'invalid_phone') {
              try {
                if (checkoutPhoneInput) {
                  checkoutPhoneInput.classList.add('input-error');
                  checkoutPhoneInput.classList.add('tutorial-highlight');
                  try { checkoutPhoneInput.focus(); } catch (_) {}
                  try { checkoutPhoneInput.scrollIntoView({ behavior: 'smooth', block: 'center' }); } catch(_) {}
                }
              } catch (_) {}
            }
            const baseMsg = checkoutData?.message || checkoutData?.error || 'Falha ao iniciar checkout';
            throw new Error(String(baseMsg).trim() || 'Falha ao iniciar checkout.');
          }
          const url = String(checkoutData?.url || checkoutData?.checkoutUrl || '').trim();
          const clientSecret = String(checkoutData?.clientSecret || checkoutData?.client_secret || '').trim();
          if (clientSecret) {
            await mountStripeEmbeddedCheckout(clientSecret);
            return;
          }
          throw new Error('Checkout incorporado não retornou os dados necessários. Verifique a criação da sessão com ui_mode=embedded.');
        }

        const ctrl = (typeof AbortController !== 'undefined') ? new AbortController() : null;
        const timeoutId = ctrl ? setTimeout(() => { try { ctrl.abort(); } catch (_) {} }, 45000) : null;
        let createResp = null;
        try {
          createResp = await fetch('/api/stripe/create-intent', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(stripePayload),
            signal: ctrl ? ctrl.signal : undefined
          });
        } catch (e) {
          throw new Error('Falha ao conectar no servidor. Recarregue a página e tente novamente.');
        } finally {
          if (timeoutId) clearTimeout(timeoutId);
        }
        let createData = null;
        try { createData = await createResp.json(); } catch (_) { createData = {}; }
        if (!createResp.ok) {
          const errCode = String(createData?.error || '').trim().toLowerCase();
          if (errCode === 'invalid_cpf') {
            try {
              const cpfEl = document.getElementById('cardHolderCpf');
              if (cpfEl) {
                cpfEl.classList.add('input-error');
                cpfEl.classList.add('tutorial-highlight');
                try { cpfEl.focus(); } catch (_) {}
                try { cpfEl.scrollIntoView({ behavior: 'smooth', block: 'center' }); } catch(_) {}
              }
            } catch (_) {}
          }
          if (errCode === 'missing_phone' || errCode === 'invalid_phone') {
            try {
              if (checkoutPhoneInput) {
                checkoutPhoneInput.classList.add('input-error');
                checkoutPhoneInput.classList.add('tutorial-highlight');
                try { checkoutPhoneInput.focus(); } catch (_) {}
                try { checkoutPhoneInput.scrollIntoView({ behavior: 'smooth', block: 'center' }); } catch(_) {}
              }
            } catch (_) {}
          }
          const baseMsg = createData?.message || createData?.error || 'Falha ao iniciar pagamento';
          throw new Error(String(baseMsg).trim() || 'Falha ao iniciar pagamento.');
        }

        const clientSecret = String(createData?.clientSecret || '').trim();
        const identifierServer = String(createData?.identifier || createData?.paymentIntentId || '').trim();
        const correlationIDServer = String(createData?.correlationID || correlationID || '').trim();
        if (!clientSecret) throw new Error('Pagamento não iniciou corretamente (clientSecret ausente).');

        await ensureStripeMounted();
        const confirmResult = await stripeInstance.confirmCardPayment(clientSecret, {
          payment_method: {
            card: stripeCardNumberEl,
            billing_details: { name: String(cardHolder || '').trim(), phone: String(phoneValue || '').trim() }
          }
        });

        if (confirmResult && confirmResult.error) {
          const m = String(confirmResult.error.message || '').trim();
          throw new Error(m || 'Pagamento não aprovado.');
        }
        const pi = confirmResult && confirmResult.paymentIntent ? confirmResult.paymentIntent : null;
        const piId = String(pi?.id || identifierServer || '').trim();
        const piStatus = String(pi?.status || '').trim().toLowerCase();
        if (!piId) throw new Error('Pagamento não retornou identificador.');

        let finalizeResp = null;
        try {
          finalizeResp = await fetch('/api/stripe/confirm-intent', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ payment_intent_id: piId, identifier: identifierServer, correlationID: correlationIDServer })
          });
        } catch (_) {
          finalizeResp = null;
        }
        let finalizeData = null;
        try { if (finalizeResp) finalizeData = await finalizeResp.json(); } catch (_) { finalizeData = {}; }

        const paid = finalizeData?.paid === true || piStatus === 'succeeded';
        if (!paid && piStatus && piStatus !== 'succeeded') {
          alert('Pagamento em processamento. Vamos te levar para o seu pedido.');
        } else {
          alert('Pagamento realizado com sucesso!');
        }

        if (typeof navigateToPedidoOrFallback === 'function') {
          await navigateToPedidoOrFallback(String(finalizeData?.identifier || piId || identifierServer || ''), String(finalizeData?.correlationID || correlationIDServer || correlationID || ''));
        } else {
          window.location.href = '/pedido';
        }
        return;
      }

      let resp = null;
      let lastNetErr = null;
      for (let attempt = 0; attempt < 3; attempt++) {
        const ctrl = (typeof AbortController !== 'undefined') ? new AbortController() : null;
        const timeoutId = ctrl ? setTimeout(() => { try { ctrl.abort(); } catch (_) {} }, 45000) : null;
        try {
          resp = await fetch('/api/pagarme/card-charge', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
            signal: ctrl ? ctrl.signal : undefined
          });
          lastNetErr = null;
          break;
        } catch (e) {
          lastNetErr = e;
          if (attempt >= 2) break;
          await sleep(800 * (attempt + 1));
        } finally {
          if (timeoutId) clearTimeout(timeoutId);
        }
      }
      if (!resp) throw (lastNetErr || new Error('Falha ao conectar no servidor. Recarregue a página e tente novamente.'));

      const data = await resp.json();
      if (!resp.ok) {
        const errCode = String(data?.error || '').trim().toLowerCase();
        if (errCode === 'invalid_cpf') {
          try {
            const cpfEl = document.getElementById('cardHolderCpf');
            if (cpfEl) {
              cpfEl.classList.add('input-error');
              cpfEl.classList.add('tutorial-highlight');
              try { cpfEl.focus(); } catch (_) {}
              try { cpfEl.scrollIntoView({ behavior: 'smooth', block: 'center' }); } catch(_) {}
            }
          } catch (_) {}
        }
        if (errCode === 'missing_phone' || errCode === 'invalid_phone') {
          try {
            if (checkoutPhoneInput) {
              checkoutPhoneInput.classList.add('input-error');
              checkoutPhoneInput.classList.add('tutorial-highlight');
              try { checkoutPhoneInput.focus(); } catch (_) {}
              try { checkoutPhoneInput.scrollIntoView({ behavior: 'smooth', block: 'center' }); } catch(_) {}
            }
          } catch (_) {}
        }
        const baseMsg = data?.message || data?.error || 'Falha ao processar pagamento';
        const pf = data && data.pagarme_failure ? data.pagarme_failure : null;
        const extra = (pf && (pf.acquirer_message || pf.gateway_message || pf.acquirer_return_code || pf.refusal_reason))
          ? ` Motivo: ${String(pf.acquirer_message || pf.gateway_message || pf.refusal_reason || '').trim()}${(pf.acquirer_return_code || pf.gateway_response_code) ? ` (código: ${String(pf.acquirer_return_code || pf.gateway_response_code).trim()})` : ''}`
          : '';
        const identifierErr = String(data?.identifier || data?.pagarme?.order_id || data?.order?.id || '').trim();
        const idPart = identifierErr ? ` Pedido: ${identifierErr}.` : '';
        throw new Error(`${String(baseMsg)}${extra}${idPart}`);
      }

      const paid = data?.paid === true || data?.success === true;
      const identifierServer = String(data?.identifier || data?.pagarme?.order_id || data?.order?.id || '').trim();
      const correlationIDServer = String(data?.correlationID || correlationID || '').trim();

      if (!paid) {
        const txStatus = String(data?.pagarme?.transaction_status || data?.pagarme?.charge_status || data?.pagarme?.order_status || '').trim();
        const idLabel = identifierServer ? ` Pedido: ${identifierServer}.` : '';
        throw new Error((data?.message && String(data.message).trim()) || (`Pagamento não confirmado no Pagar.me${txStatus ? ` (${txStatus})` : ''}.${idLabel}`));
      }

      alert('Pagamento realizado com sucesso!');
      if (typeof navigateToPedidoOrFallback === 'function') {
        await navigateToPedidoOrFallback(identifierServer || identifier || '', correlationIDServer);
      } else {
        window.location.href = '/pedido';
      }

    } catch (err) {
      console.error(err);
      const msg = (function () {
        try {
          if (!err) return '';
          if (typeof err === 'string') return err;
          if (typeof err === 'object') {
            const m = String(err.message || err.error || '').trim();
            if (m) return m;
            try { return JSON.stringify(err); } catch (_) {}
            return String(err);
          }
          return String(err);
        } catch (_) {
          return '';
        }
      })();
      const finalMsg = msg || 'Erro desconhecido ao processar cartão.';
      try {
        if (typeof showStatusMessageCheckout === 'function') showStatusMessageCheckout(finalMsg, 'error');
        else alert(finalMsg);
      } catch (_) {
        alert(finalMsg);
      }
    } finally {
      if (payWithCardBtn) {
        payWithCardBtn.disabled = false;
        payWithCardBtn.classList.remove('loading');
        const span = payWithCardBtn.querySelector('.button-text');
        if (span && span.dataset.original) span.textContent = span.dataset.original;
      }
    }
  }

  if (cardPaymentForm) {
    cardPaymentForm.addEventListener('submit', handleCardPayment);
  }

  const isEngajamentoNovoPage = (function () {
    try { return String(window.location && window.location.pathname || '').indexOf('/engajamento-novo') === 0; } catch (_) { return false; }
  })();
  if (checkCheckoutButton && !isEngajamentoNovoPage) {
    checkCheckoutButton.addEventListener('click', checkInstagramProfileCheckout);
  }

  // Tracking: Audio Progress (3s, 10%, 50%, 75%, 100%)
  const audioMilestones = [
    { pct: 0, sec: 3, id: '3s' },
    { pct: 0.10, sec: 0, id: '10%' },
    { pct: 0.50, sec: 0, id: '50%' },
    { pct: 0.75, sec: 0, id: '75%' },
    { pct: 1.00, sec: 0, id: '100%' }
  ];
  let trackedMilestones = new Set();

  async function trackAudioProgress(milestoneId, seconds, percentage) {
    if (trackedMilestones.has(milestoneId)) return;
    trackedMilestones.add(milestoneId);
    
    const username = document.getElementById('usernameCheckoutInput')?.value;
    const browserId = getBrowserSessionId();
    try {
      await fetch('/api/track-audio-progress', { 
        method: 'POST', 
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ 
          username,
          seconds,
          percentage: Math.floor(percentage * 100),
          milestone: milestoneId,
          browserId
        }) 
      });
    } catch (e) { console.error('Audio progress track error:', e); }
  }

  // Legacy/Specific 10% tracking for User Profile
  let audioTracked10p = false;
  async function trackAudio10p() {
    audioTracked10p = true;
    const username = document.getElementById('usernameCheckoutInput')?.value;
    if (!username) return;
    try {
      await fetch('/api/track-audio-10p', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username })
      });
    } catch (e) { console.error('Audio 10% track error:', e); }
  }

  // Hook into validation success to retry sending 10% track if it happened before validation
  const originalCheckProfile = window.checkInstagramProfileCheckout; // If defined globally, or we just rely on the event listener
  // Since checkInstagramProfileCheckout is defined in this scope or globally? 
  // It seems defined in this file (line 63 listener adds it). 
  // I need to find where checkInstagramProfileCheckout is defined to hook into it.
  
  const guideAudio = document.getElementById('guideAudio');
  const audioSpeed15x = document.getElementById('audioSpeed15x');
  const audioSpeed2x = document.getElementById('audioSpeed2x');
  const audioPlayBtn = document.getElementById('audioPlayBtn');
  const audioProgress = document.getElementById('audioProgress');
  const audioCurrent = document.getElementById('audioCurrent');
  const audioDuration = document.getElementById('audioDuration');
  function setAudioRate(rate) {
    if (!guideAudio) return;
    guideAudio.playbackRate = rate;
    if (audioSpeed15x) audioSpeed15x.classList.toggle('active', rate === 1.5);
    if (audioSpeed2x) audioSpeed2x.classList.toggle('active', rate === 2);
  }
  if (audioSpeed15x) audioSpeed15x.addEventListener('click', () => {
    if (!guideAudio) return;
    const isActive = guideAudio.playbackRate === 1.5;
    setAudioRate(isActive ? 1 : 1.5);
  });
  if (audioSpeed2x) audioSpeed2x.addEventListener('click', () => {
    if (!guideAudio) return;
    const isActive = guideAudio.playbackRate === 2;
    setAudioRate(isActive ? 1 : 2);
  });
  function fmt(t) { const m = Math.floor(t/60); const s = Math.floor(t%60); return `${String(m).padStart(2,'0')}:${String(s).padStart(2,'0')}`; }
  if (guideAudio) {
    guideAudio.addEventListener('loadedmetadata', () => {
      if (audioDuration) audioDuration.textContent = fmt(guideAudio.duration || 0);
    });
    guideAudio.addEventListener('timeupdate', () => {
      if (audioCurrent) audioCurrent.textContent = fmt(guideAudio.currentTime || 0);
      if (audioProgress && guideAudio.duration) audioProgress.value = String(Math.floor((guideAudio.currentTime / guideAudio.duration) * 100));
      
      // Tracking logic
      const current = guideAudio.currentTime;
      const duration = guideAudio.duration;
      const pct = (duration > 0) ? (current / duration) : 0;
      
      // Progress Tracking (Milestones)
      audioMilestones.forEach(m => {
        if ((m.sec > 0 && current >= m.sec) || (m.pct > 0 && pct >= m.pct)) {
           trackAudioProgress(m.id, current, pct);
        }
      });
      
      // Specific 10% Profile tracking
      if (pct >= 0.10) trackAudio10p();
    });
  }
  if (audioPlayBtn) audioPlayBtn.addEventListener('click', async () => {
    if (!guideAudio) return;
    if (guideAudio.paused) { 
      await guideAudio.play(); 
      audioPlayBtn.textContent = 'Pause'; 
    } else { 
      guideAudio.pause(); 
      audioPlayBtn.textContent = 'Ouvir Áudio'; 
    }
    try { const ta = document.getElementById('tutorialAudio'); if (ta) ta.style.display = 'none'; } catch(_) {}
    showTutorialStep(2);
  });
  if (audioProgress) audioProgress.addEventListener('input', () => {
    if (!guideAudio || !guideAudio.duration) return;
    const pct = Number(audioProgress.value) / 100;
    guideAudio.currentTime = guideAudio.duration * pct;
  });
  setAudioRate(1);

  const platformToggle = document.querySelector('.platform-toggle');
  if (platformToggle && !window.__ENG_MODE__) {
    platformToggle.addEventListener('click', (e) => {
      const target = e.target.closest('.platform-btn');
      if (!target) return;
      if (target.classList.contains('instagram')) setPlatform('instagram');
      if (target.classList.contains('tiktok')) setPlatform('tiktok');
      try {
        const tp = document.getElementById('tutorialPlatform');
        if (tp) { tp.style.display = 'none'; tp.classList.add('hide'); }
      } catch(_) {}
    });
  }

  if (!window.__ENG_MODE__ && btnInstagram) {
    try { setPlatform('instagram'); } catch(_) {}
  }

  

  const testimonialsCarousel = document.getElementById('testimonialsCarousel');
  if (testimonialsCarousel) {
    let idx = 0;
    const items = Array.from(testimonialsCarousel.querySelectorAll('.carousel-item'));
    const prev = testimonialsCarousel.querySelector('.prev');
    const next = testimonialsCarousel.querySelector('.next');
    let autoTimer = null;
    function render() {
      items.forEach((it, i) => {
        it.classList.remove('active', 'pos-left', 'pos-right', 'pos-hidden-left', 'pos-hidden-right');
        if (i === idx) {
          it.classList.add('active');
          it.setAttribute('aria-hidden', 'false');
        } else {
          it.classList.add('pos-hidden-right');
          it.setAttribute('aria-hidden', 'true');
        }
      });
    }
    function startAuto() {
      stopAuto();
      autoTimer = setInterval(() => {
        idx = (idx + 1) % items.length;
        render();
      }, 45000);
    }
    function stopAuto() { if (autoTimer) { clearInterval(autoTimer); autoTimer = null; } }
    if (prev) prev.addEventListener('click', () => { idx = (idx - 1 + items.length) % items.length; render(); });
    if (next) next.addEventListener('click', () => { idx = (idx + 1) % items.length; render(); });
    const viewport = testimonialsCarousel.querySelector('.carousel-viewport');
    if (viewport) {
      let touchStartX = 0;
      let touchLastX = 0;
      viewport.addEventListener('touchstart', (e) => {
        if (!e.touches || !e.touches.length) return;
        touchStartX = e.touches[0].clientX;
        touchLastX = touchStartX;
        stopAuto();
      }, { passive: true });
      viewport.addEventListener('touchmove', (e) => {
        if (!e.touches || !e.touches.length) return;
        touchLastX = e.touches[0].clientX;
      }, { passive: true });
      viewport.addEventListener('touchend', () => {
        const delta = touchStartX - touchLastX;
        const threshold = 40;
        if (Math.abs(delta) > threshold) {
          if (delta > 0) {
            idx = (idx + 1) % items.length;
          } else {
            idx = (idx - 1 + items.length) % items.length;
          }
          render();
        }
        startAuto();
      });
    }
    testimonialsCarousel.addEventListener('mouseenter', stopAuto);
    testimonialsCarousel.addEventListener('mouseleave', startAuto);
    render();
    startAuto();
  }

  // Navegação do carrossel
  // carrossel removido

  // Inicializar visibilidade do card de perfil
  updatePerfilVisibility();
  updatePedidoButtonState();
  clearResumo();
  renderPromoPrices();
  try { updatePromosSummary(); } catch(_) {}
  showTutorialStep(1);
  // sem carrossel de posts

  // sem carrossel de posts
  
  (function initHeaderTicker(){
    const el = document.getElementById('headerTicker');
    const span = el ? el.querySelector('.ticker-item') : null;
    const msgs = ['Preços Justos', 'Transparencia total', 'Empresa regularizada', 'Mais de 20 mil clientes'];
    let i = 0;
    function step(){
      if (!span) return;
      span.classList.remove('enter');
      span.classList.add('leave');
      setTimeout(()=>{
        span.textContent = msgs[i];
        span.classList.remove('leave');
        span.classList.add('enter');
        i = (i + 1) % msgs.length;
      }, 600);
    }
    if (span) {
      span.textContent = msgs[0];
      span.classList.add('enter');
      i = 1;
      setInterval(step, 3200);
    }
  })();

  function enableTipDrag(){
    const tip = document.getElementById('tutorialAudio');
    const parent = tip ? tip.closest('.audio-controls') : null;
    if (!tip || !parent) return;
    let dragging = false;
    let startX = 0, startY = 0, initLeft = 0, initTop = 0;
    function onDown(e){
      dragging = true;
      tip.classList.add('dragging');
      startX = e.clientX;
      startY = e.clientY;
      initLeft = parseFloat(tip.style.left || '0') || 0;
      initTop = parseFloat(tip.style.top || '0') || 0;
      document.addEventListener('pointermove', onMove);
      document.addEventListener('pointerup', onUp);
    }
    function onMove(e){
      if (!dragging) return;
      const dx = e.clientX - startX;
      const dy = e.clientY - startY;
      const pw = parent.clientWidth;
      const ph = parent.clientHeight;
      const bw = tip.offsetWidth;
      const bh = tip.offsetHeight;
      let left = Math.max(0, Math.min(pw - bw, initLeft + dx));
      let top = Math.max(0, Math.min(ph - bh, initTop + dy));
      tip.style.left = left + 'px';
      tip.style.top = top + 'px';
    }
    function onUp(){
      dragging = false;
      tip.classList.remove('dragging');
      document.removeEventListener('pointermove', onMove);
      document.removeEventListener('pointerup', onUp);
      const left = parseFloat(tip.style.left || '0') || 0;
      const top = parseFloat(tip.style.top || '0') || 0;
      tip.setAttribute('data-tip-left', String(left));
      tip.setAttribute('data-tip-top', String(top));
      try { localStorage.setItem('oppus_tip_audio_pos', JSON.stringify({ left, top })); } catch(_) {}
    }
    tip.addEventListener('pointerdown', onDown);
    try {
      const saved = JSON.parse(localStorage.getItem('oppus_tip_audio_pos') || 'null');
      if (saved && typeof saved.left === 'number' && typeof saved.top === 'number') {
        tip.style.left = saved.left + 'px';
        tip.style.top = saved.top + 'px';
        tip.setAttribute('data-tip-left', String(saved.left));
        tip.setAttribute('data-tip-top', String(saved.top));
      }
    } catch(_) {}
  }

  (function initClientHeader(){
    const fetchBtn = document.getElementById('clientFetchBtn');
    const clientPage = document.getElementById('clientPage');
    const phoneInputPage = document.getElementById('clientPhoneInputPage');
    const consultBtn = document.getElementById('clientPageConsultBtn');
    const backBtn = document.getElementById('clientPageBackBtn');
    const ordersBox = document.getElementById('clientPageOrders');
    function applyPhone(v) {
      phoneFromUrl = v;
      try { localStorage.setItem('oppus_client_phone', v); } catch (_) {}
    }
    async function fetchOrders(v){
      const digits = String(v || '').replace(/\D/g, '');
      if (!digits) { if (ordersBox) { ordersBox.style.display = 'block'; ordersBox.textContent = 'Digite seu telefone ou número de pedido.'; } return; }
      try {
        if (digits.length >= 5 && digits.length <= 10) {
          const r = await fetch(`/api/order?orderID=${encodeURIComponent(digits)}`);
          const d = await r.json();
          const o = d && d.order ? d.order : null;
          if (ordersBox) {
            ordersBox.style.display = 'block';
            if (!o) {
              ordersBox.textContent = 'Pedido não encontrado.';
            } else {
              try {
                const oid = (o && o.fama24h && o.fama24h.orderId) ? String(o.fama24h.orderId) : ((o && o.fornecedor_social && o.fornecedor_social.orderId) ? String(o.fornecedor_social.orderId) : String(o._id || ''));
                if (oid) {
                  try { await fetch('/pedido/select', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ orderID: oid }) }); } catch(_){ }
                  window.location.href = '/pedido?orderID=' + encodeURIComponent(String(oid));
                  return;
                }
              } catch (_) {}
              const oid = (o && o.fama24h && o.fama24h.orderId) ? String(o.fama24h.orderId) : ((o && o.fornecedor_social && o.fornecedor_social.orderId) ? String(o.fornecedor_social.orderId) : String(o._id || ''));
              const status = String(o.status || o.woovi?.status || '-');
              const tipo = String(o.tipo || o.tipoServico || '-');
              const qtd = String(o.quantidade || o.qtd || '-');
              const user = String(o.instagramUsername || o.instauser || '-');
              const paid = (o.woovi && o.woovi.paidAt) || o.paidAt || null;
              let paidStr = '-';
              if (paid) {
                try {
                  const d0 = new Date(paid);
                  const sp = d0;
                  const dd = String(sp.getUTCDate()).padStart(2,'0');
                  const mm = String(sp.getUTCMonth()+1).padStart(2,'0');
                  const yyyy = sp.getUTCFullYear();
                  const hh = String(sp.getUTCHours()).padStart(2,'0');
                  const mn = String(sp.getUTCMinutes()).padStart(2,'0');
                  paidStr = `${dd}/${mm}/${yyyy} as ${hh}:${mn}`;
                } catch(_) {}
              }
              const fama = o && o.fama24h && o.fama24h.statusPayload ? o.fama24h.statusPayload : null;
              const rawF = String((fama && (fama.status || fama.Status || fama.status_text || fama.statusText || fama.StatusText)) || '').trim();
              const tF = rawF.toLowerCase();
              const stF = tF ? (/cancel/.test(tF) ? 'Cancelado' : (/partial/.test(tF) ? 'Parcial' : (/pend/.test(tF) ? 'Pendente' : (/process|progress|start|running/.test(tF) ? 'Em andamento' : (/complete|success|finished|done/.test(tF) ? 'Concluído' : rawF))))) : '-';
              const clsF = stF==='Concluído' ? 'status-green' : (stF==='Cancelado' ? 'status-red' : (stF==='Em andamento' ? 'status-yellow' : (stF==='Pendente' ? 'status-blue' : '')));
              ordersBox.innerHTML = `<div style="padding:10px;border:1px solid var(--border-color);border-radius:10px;margin:6px auto;max-width:620px;color:var(--text-primary);">
                <div><strong>Status:</strong> <span class="${(String(status).toLowerCase()==='pago'?'status-green':(String(status).toLowerCase()==='pendente'?'status-yellow':''))}">${status}</span></div>
                <div><strong>Serviço:</strong> <span>${tipo}</span></div>
                <div><strong>Quantidade:</strong> <span>${qtd}</span></div>
                <div><strong>Instagram:</strong> <span>${user}</span></div>
                <div><strong>Pago em:</strong> <span>${paidStr}</span></div>
                <div><strong>Número do pedido:</strong> <span>${oid || '-'}</span></div>
                <div><strong>Status do serviço:</strong> <span id="famaStatus_${oid}" class="status-text ${clsF}">${stF}</span></div>
                <div style="margin-top:8px;">${oid ? `<button type="button" class="continue-button small open-pedido-btn" data-orderid="${encodeURIComponent(oid)}">Detalhes do pedido</button>` : ''}</div>
              </div>`;
            }
          }
          return;
        }
        const resp = await fetch(`/api/checkout-orders?phone=${encodeURIComponent(digits)}`);
        const data = await resp.json();
        const list = Array.isArray(data.orders) ? data.orders : [];
        if (ordersBox) {
          ordersBox.style.display = 'block';
          if (!list.length) {
            ordersBox.textContent = 'Nenhum pedido encontrado.';
          } else {
            if (list.length === 1) {
              try {
                const only = list[0];
                const onlyOid = (only && only.fama24h && only.fama24h.orderId) ? String(only.fama24h.orderId) : ((only && only.fornecedor_social && only.fornecedor_social.orderId) ? String(only.fornecedor_social.orderId) : '');
                if (onlyOid) {
                  try { await fetch('/pedido/select', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ orderID: onlyOid }) }); } catch(_){ }
                  window.location.href = '/pedido?oid=' + encodeURIComponent(String(onlyOid));
                  return;
                }
              } catch(_) {}
            }
            ordersBox.innerHTML = list.map((o) => {
              const providerOid = (o && o.fama24h && o.fama24h.orderId) ? String(o.fama24h.orderId) : ((o && o.fornecedor_social && o.fornecedor_social.orderId) ? String(o.fornecedor_social.orderId) : null);
              const functionalOid = providerOid || String(o._id || '');
              let displayOid = providerOid || '';


              const status = String(o.status || o.woovi?.status || '-');
              const tipo = String(o.tipo || o.tipoServico || '-');
              const qtd = String(o.quantidade || o.qtd || '-');
              const user = String(o.instagramUsername || o.instauser || '-');
              const paid = (o.woovi && o.woovi.paidAt) || o.paidAt || null;
              let paidStr = '-';
              if (paid) {
                try {
                  const d0 = new Date(paid);
                  const sp = d0;
                  const dd = String(sp.getUTCDate()).padStart(2,'0');
                  const mm = String(sp.getUTCMonth()+1).padStart(2,'0');
                  const yyyy = sp.getUTCFullYear();
                  const hh = String(sp.getUTCHours()).padStart(2,'0');
                  const mn = String(sp.getUTCMinutes()).padStart(2,'0');
                  paidStr = `${dd}/${mm}/${yyyy} as ${hh}:${mn}`;
                } catch(_) {}
              }
              const fama = o && o.fama24h && o.fama24h.statusPayload ? o.fama24h.statusPayload : null;
              const rawF = String((fama && (fama.status || fama.Status || fama.status_text || fama.statusText || fama.StatusText)) || '').trim();
              const tF = rawF.toLowerCase();
              const stF = tF ? (/cancel/.test(tF) ? 'Cancelado' : (/partial/.test(tF) ? 'Parcial' : (/pend/.test(tF) ? 'Pendente' : (/process|progress|start|running/.test(tF) ? 'Em andamento' : (/complete|success|finished|done/.test(tF) ? 'Concluído' : rawF))))) : '-';
              const clsF = stF==='Concluído' ? 'status-green' : (stF==='Cancelado' ? 'status-red' : (stF==='Em andamento' ? 'status-yellow' : (stF==='Pendente' ? 'status-blue' : '')));
              return `<div style="padding:10px;border:1px solid var(--border-color);border-radius:10px;margin:6px auto;max-width:620px;color:var(--text-primary);">
                <div><strong>Status:</strong> <span class="${(String(status).toLowerCase()==='pago'?'status-green':(String(status).toLowerCase()==='pendente'?'status-yellow':''))}">${status}</span></div>
                <div><strong>Serviço:</strong> <span>${tipo}</span></div>
                <div><strong>Quantidade:</strong> <span>${qtd}</span></div>
                <div><strong>Instagram:</strong> <span>${user}</span></div>
                <div><strong>Pago em:</strong> <span>${paidStr}</span></div>
                <div><strong>Número do pedido:</strong> <span>${displayOid}</span></div>
                <div><strong>Status do serviço:</strong> <span id="famaStatus_${functionalOid}" class="status-text ${clsF}">${stF}</span></div>
                <div style="margin-top:8px;">${functionalOid ? `<button type="button" class="continue-button small open-pedido-btn" data-orderid="${encodeURIComponent(functionalOid)}">Detalhes do pedido</button>` : ''}</div>
              </div>`;
            }).join('');
          }
        }
      } catch (_) {
        if (ordersBox) { ordersBox.style.display = 'block'; ordersBox.textContent = 'Erro ao buscar pedidos.'; }
      }
    }
    function showClientPage(){ if (clientPage) { clientPage.style.display = 'block'; } }
    function hideClientPage(){ if (clientPage) { clientPage.style.display = 'none'; } }
    if (fetchBtn) {
      fetchBtn.addEventListener('click', (e) => {
        if (clientPage) {
          e.preventDefault();
          showClientPage();
        } else {
          window.location.href = '/cliente';
        }
      });
    }
    attachPhoneMask(phoneInputPage);
    // Fallback de delegação caso o botão não esteja disponível no momento do carregamento
    document.addEventListener('click', (ev) => {
      const t = ev.target;
      if (t && (t.id === 'clientFetchBtn' || (t.closest && t.closest('#clientFetchBtn')))) {
        if (clientPage) {
          ev.preventDefault();
          showClientPage();
        } else {
          window.location.href = '/cliente';
        }
      }
    });
    if (backBtn) backBtn.addEventListener('click', hideClientPage);
    if (consultBtn) {
      consultBtn.addEventListener('click', async () => {
        const raw = (phoneInputPage && phoneInputPage.value && phoneInputPage.value.trim()) || '';
        const v = onlyDigits(raw);
        if (!v) { alert('Digite seu telefone ou número do pedido.'); return; }
        try { localStorage.setItem('oppus_client_phone', v); } catch (_) {}
        const digits = v;
        if (digits.length >= 5 && digits.length <= 10) {
          try {
            const r = await fetch(`/api/order?orderID=${encodeURIComponent(digits)}`);
            const d = await r.json();
            const o = d && d.order ? d.order : null;
            const oid = (o && o.fama24h && o.fama24h.orderId) ? String(o.fama24h.orderId) : ((o && o.fornecedor_social && o.fornecedor_social.orderId) ? String(o.fornecedor_social.orderId) : String(o._id || ''));
            if (oid) {
              try { await fetch('/pedido/select', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ orderID: oid }) }); } catch(_){ }
              window.location.href = '/pedido?orderID=' + encodeURIComponent(String(oid));
              return;
            }
          } catch(_) {}
        }
        fetchOrders(v);
      });
    }
  async function openPedido(orderID) {
      try { await fetch('/pedido/select', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ orderID }) }); } catch(_) {}
      try { await fetch('/api/fama/status', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ order: String(orderID) }) }); } catch(_) {}
      try { window.location.href = '/pedido?orderID=' + encodeURIComponent(String(orderID)); } catch(_) {}
  }
  try { window.openPedido = openPedido; } catch(_) {}
    document.addEventListener('click', (ev) => {
      const t = ev.target;
      const btn = t && (t.classList && t.classList.contains('open-pedido-btn')) ? t : (t.closest && t.closest('.open-pedido-btn'));
      if (btn) {
        ev.preventDefault();
        const oid = btn.getAttribute('data-orderid') || '';
        if (oid) { openPedido(oid); }
      }
    });
    try {
      const stored = localStorage.getItem('oppus_client_phone');
      if (stored && phoneInputPage) phoneInputPage.value = stored;
    } catch (_) {}
  // Termos de uso
  const termsLink = document.getElementById('termsLink');
  const termsPage = document.getElementById('termsPage');
  const termsCloseBtn = document.getElementById('termsCloseBtn');
  if (termsLink && termsPage) {
    termsLink.addEventListener('click', (e)=>{
      try {
        const href = termsLink.getAttribute('href') || '';
        if (href === '#' || href === '') {
          e.preventDefault();
          termsPage.style.display='block';
        }
      } catch (_) {}
    });
  }
  if (termsCloseBtn && termsPage) {
    termsCloseBtn.addEventListener('click', ()=>{ termsPage.style.display='none'; });
  }
  const warrantyModal = document.getElementById('warranty60Modal');
  const warrantyInfoBtn = document.getElementById('warranty60InfoBtn');
  const warrantyInfoBtn30 = document.getElementById('warranty30InfoBtn');
  const warrantyInfoBtnLifetime = document.getElementById('warrantyLifetimeInfoBtn');
  const warrantyCloseBtn = document.getElementById('warranty60CloseBtn');
  if (warrantyInfoBtn && warrantyModal) {
    warrantyInfoBtn.addEventListener('click', function(){
      try {
        if (warrantyModal.parentNode !== document.body) {
          document.body.appendChild(warrantyModal);
        }
      } catch(_) {}
      warrantyModal.style.display = 'flex';
    });
  }
  if (warrantyInfoBtn30 && warrantyModal) {
    warrantyInfoBtn30.addEventListener('click', function(){
      try {
        if (warrantyModal.parentNode !== document.body) {
          document.body.appendChild(warrantyModal);
        }
      } catch(_) {}
      warrantyModal.style.display = 'flex';
    });
  }
  if (warrantyInfoBtnLifetime && warrantyModal) {
    warrantyInfoBtnLifetime.addEventListener('click', function(){
      try {
        if (warrantyModal.parentNode !== document.body) {
          document.body.appendChild(warrantyModal);
        }
      } catch(_) {}
      warrantyModal.style.display = 'flex';
    });
  }
  if (warrantyCloseBtn && warrantyModal) {
    warrantyCloseBtn.addEventListener('click', function(){ warrantyModal.style.display = 'none'; });
  }
  const warrantyCloseBtn2 = document.getElementById('warranty60CloseBtn2');
  if (warrantyCloseBtn2 && warrantyModal) {
    warrantyCloseBtn2.addEventListener('click', function(){ warrantyModal.style.display = 'none'; });
  }
  if (warrantyModal) {
    warrantyModal.addEventListener('click', function(e){ if (e.target === warrantyModal) { warrantyModal.style.display = 'none'; } });
  }
  const commentsModal = document.getElementById('commentsExampleModal');
  const commentsBtn = document.getElementById('commentsExampleBtn');
  const commentsCloseBtn = document.getElementById('commentsExampleCloseBtn');
  const commentsCloseBtn2 = document.getElementById('commentsExampleCloseBtn2');
  const commentsVideo = document.getElementById('commentsVideoPlayer');

  if (commentsBtn && commentsModal) {
    commentsBtn.addEventListener('click', function(e){
      try { e.stopPropagation(); } catch(_) {}
      suppressOpenPostModalOnce = true;
      setTimeout(function(){ suppressOpenPostModalOnce = false; }, 500);
      try {
        if (commentsModal.parentNode !== document.body) {
          document.body.appendChild(commentsModal);
        }
      } catch(_) {}
      commentsModal.style.display = 'flex';
      if (commentsVideo) {
        commentsVideo.currentTime = 0;
        try { commentsVideo.play(); } catch(e) { console.log('Video play failed', e); }
      }
    });
  }
  function closeCommentsModal() {
    if (commentsModal) commentsModal.style.display = 'none';
    if (commentsVideo) commentsVideo.pause();
  }
  if (commentsCloseBtn && commentsModal) {
    commentsCloseBtn.addEventListener('click', closeCommentsModal);
  }
  if (commentsCloseBtn2 && commentsModal) {
    commentsCloseBtn2.addEventListener('click', closeCommentsModal);
  }
  if (commentsModal) {
    commentsModal.addEventListener('click', function(e){ if (e.target === commentsModal) { closeCommentsModal(); } });
  }

  // Novo modal: Explicação da Ferramenta
  const toolModal = document.getElementById('toolExplanationModal');
  const toolBtn = document.getElementById('toolExplanationBtn');
  const toolCloseBtn = document.getElementById('toolExplanationCloseBtn');
  const toolCloseBtn2 = document.getElementById('toolExplanationCloseBtn2');
  const toolVideo = document.getElementById('toolVideoPlayer');

  if (toolBtn && toolModal) {
    toolBtn.addEventListener('click', function(e){
      try { e.stopPropagation(); } catch(_) {}
      try {
        if (toolModal.parentNode !== document.body) {
          document.body.appendChild(toolModal);
        }
      } catch(_) {}
      toolModal.style.display = 'flex';
      if (toolVideo) {
        toolVideo.currentTime = 0;
        try { toolVideo.play(); } catch(e) { console.log('Video play failed', e); }
      }
    });
  }
  function closeToolModal() {
    if (toolModal) toolModal.style.display = 'none';
    if (toolVideo) toolVideo.pause();
  }
  if (toolCloseBtn && toolModal) {
    toolCloseBtn.addEventListener('click', closeToolModal);
  }
  if (toolCloseBtn2 && toolModal) {
    toolCloseBtn2.addEventListener('click', closeToolModal);
  }
  if (toolModal) {
    toolModal.addEventListener('click', function(e){ if (e.target === toolModal) { closeToolModal(); } });
  }
  })();
  (function initSaleToasts(){
    const isCheckout = !!document.querySelector('.checkout-page');
    if (!isCheckout) return;
    const parent = document.querySelector('.checkout-page');
    let container = document.getElementById('toastContainer');
    if (!container) {
      container = document.createElement('div');
      container.id = 'toastContainer';
      container.className = 'toast-container';
    }
    if (container.parentNode !== parent) {
      parent.appendChild(container);
    }
    const minutesCycle = [20, 12, 10, 6, 3, 1];
    let minutesIdx = 0;
    function nextMinutes(){ const m = minutesCycle[minutesIdx]; minutesIdx = (minutesIdx + 1) % minutesCycle.length; return m; }
    function getPlatformIcon(pl){
      if (pl === 'tiktok') {
        return '<svg class="toast-platform" viewBox="0 0 24 24" fill="currentColor" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><path d="M14 3c.3 1.9 1.5 3.6 3.2 4.5 1 .6 2.1.9 3.2.9v3a8.6 8.6 0 01-3.2-.6 7.8 7.8 0 01-2.2-1.3v6.6a5.9 5.9 0 11-5.8-5.9c.4 0 .9.1 1.3.2v3a2.9 2.9 0 00-1.3-.3 2.9 2.9 0 102.9 2.9V3h2z"/></svg>';
      }
      return '<svg class="toast-platform" viewBox="0 0 24 24" fill="currentColor" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><path d="M7 2h10a5 5 0 015 5v10a5 5 0 01-5 5H7a5 5 0 01-5-5V7a5 5 0 015-5zm0 2a3 3 0 00-3 3v10a3 3 0 003 3h10a3 3 0 003-3V7a3 3 0 00-3-3H7zm5 3a5 5 0 110 10 5 5 0 010-10zm0 2a3 3 0 100 6 3 3 0 000-6zm5.5-3a1.5 1.5 0 110 3 1.5 1.5 0 010-3z"/></svg>';
    }
    function showToast(message){
      if (!container) return;
      const t = document.createElement('div');
      t.className = 'toast';
      const icon = getPlatformIcon(message.platform || selectedPlatform);
      const timeText = message.time || `há ${nextMinutes()} minutos`;
      t.innerHTML = `<button class="toast-close" aria-label="Fechar">×</button>${icon}<div class="toast-body"><div class="toast-title"></div><div class="toast-desc"></div><div class="toast-meta"><span class="toast-dot"></span><span class="toast-time">${timeText}</span></div></div>`;
      const titleEl = t.querySelector('.toast-title');
      const descEl = t.querySelector('.toast-desc');
      if (titleEl) titleEl.textContent = message.title || '';
      if (descEl) descEl.textContent = message.desc || '';
      container.appendChild(t);
      const btn = t.querySelector('.toast-close');
      if (btn) {
        btn.addEventListener('click', function(){
          t.style.opacity='0';
          t.style.transform='translateX(100%)';
          setTimeout(()=>{ if(t.parentNode){ t.parentNode.removeChild(t);} },700);
        });
      }
      setTimeout(()=>{ t.style.opacity='0'; t.style.transform='translateX(100%)'; setTimeout(()=>{ if(t.parentNode){ t.parentNode.removeChild(t);} },700); },4000);
    }
    const combos = [];
    const tiposIG = ['mistos','brasileiros','organicos'];
    tiposIG.forEach(tp=>{ (tabela[tp]||[]).forEach(it=>{ combos.push({ q: it.q, tipo: tp }); }); });
    function pickIG(){ const c = combos[Math.floor(Math.random()*combos.length)] || { q: 150, tipo: 'mistos' }; return c; }
    const nomes = [
      'Marcos','Carlos','João','Paulo','Rodrigo','Bruno','Ricardo','André','Felipe','Gustavo','Eduardo','Thiago','Diego','Leandro','Rafael','Daniel','Fábio','Alexandre','Roberto','Sérgio',
      'Ana','Juliana','Patrícia','Fernanda','Renata','Adriana','Marcela','Camila','Luciana','Vanessa','Aline','Raquel','Sabrina','Simone','Carolina','Priscila','Bianca','Monique','Cristiane','Michele'
    ];
    const sobrenomes = ['Silva','Souza','Almeida','Araujo','Ferreira','Costa','Oliveira','Santos','Ribeiro','Gomes','Barbosa','Medeiros','Prado','Peixoto','Matos','Nogueira','Queiroz','Amaral','Correia'];
    let lastToastName = '';
    function randNome(){ return nomes[Math.floor(Math.random()*nomes.length)]; }
    function randSobrenomeInicial(){ const s = sobrenomes[Math.floor(Math.random()*sobrenomes.length)] || 'S'; return s.charAt(0); }
    function makeNomeUnico(){
      let attempt = 0; let nome;
      do {
        nome = `${randNome()} ${randSobrenomeInicial()}.`;
        attempt++;
      } while (nome === lastToastName && attempt < 10);
      lastToastName = nome;
      return nome;
    }
    function makeToast(platform){
      const nome = makeNomeUnico();
      if (platform === 'tiktok') {
        return;
      } else {
        const c = pickIG();
        const unit = getUnitForTipo(c.tipo);
        const label = getLabelForTipo(c.tipo);
        showToast({ title: `${nome} confirmou compra`, desc: `Adquiriu ${c.q} ${unit} — ${label}`, platform: 'instagram' });
      }
    }
    const platformCycle = ['instagram','instagram','tiktok'];
    let cycleIdx = 0;
    function cycle(){
      makeToast(platformCycle[cycleIdx]);
      cycleIdx = (cycleIdx + 1) % platformCycle.length;
      setTimeout(cycle, 15000);
    }
    setTimeout(cycle, 7000);
  })();
})();

// Garantir utilitários acessíveis globalmente para funções fora do escopo do IIFE
(function(){
  if (typeof window.parsePrecoToCents !== 'function') {
    window.parsePrecoToCents = function(precoStr){
      if (!precoStr) return 0;
      const cleaned = String(precoStr).replace(/[^\d,]/g, '').replace(',', '.');
      const value = Math.round(parseFloat(cleaned) * 100);
      return isNaN(value) ? 0 : value;
    };
  }
  if (typeof window.formatCentsToBRL !== 'function') {
    window.formatCentsToBRL = function(cents){
      const valor = Math.max(0, Number(cents) || 0);
      const reais = Math.floor(valor / 100);
      const centavos = valor % 100;
      return `R$ ${reais.toLocaleString('pt-BR')},${String(centavos).padStart(2, '0')}`;
    };
  }
  if (typeof window.calcPromosTotalCents !== 'function') {
    window.calcPromosTotalCents = function(promos){
      try { return (Array.isArray(promos) ? promos : []).reduce((acc, p) => acc + (Number(p.priceCents) || 0), 0); } catch (_) { return 0; }
    };
  }
  if (typeof window.getSelectedPromos !== 'function') {
    window.getSelectedPromos = function(){
      const promos = [];
      try {
        const likesChecked = !!document.getElementById('promoLikes')?.checked;
        const viewsChecked = !!document.getElementById('promoViews')?.checked;
        const commentsChecked = !!document.getElementById('promoComments')?.checked;
        const warrantyChecked = !!document.getElementById('promoWarranty60')?.checked;
        const upgradeChecked = !!document.getElementById('orderBumpCheckboxInline')?.checked;
        if (likesChecked) {
          const qty = Number(document.getElementById('likesQty')?.textContent || 150);
          let priceStr = document.querySelector('.promo-prices[data-promo="likes"] .new-price')?.textContent || '';
          if (!priceStr) priceStr = (window.promoPricing && window.promoPricing.likes ? window.promoPricing.likes.price : '') || '';
          const tipo = String((document.getElementById('tipoSelect') && document.getElementById('tipoSelect').value) || '').toLowerCase();
          const label = (function(t){
            if (t === 'organicos') return `Curtidas orgânicas (${qty})`;
            if (t === 'brasileiros' || t === 'curtidas_brasileiras') return `Curtidas brasileiras (${qty})`;
            if (t === 'mistos') return `Curtidas mistas (${qty})`;
            return `Curtidas (${qty})`;
          })(tipo);
          promos.push({ key: 'likes', qty, label, priceCents: window.parsePrecoToCents(priceStr) });
        }
        if (viewsChecked) {
          const qty = Number(document.getElementById('viewsQty')?.textContent || 1000);
          let priceStr = document.querySelector('.promo-prices[data-promo="views"] .new-price')?.textContent || '';
          if (!priceStr) priceStr = (window.promoPricing && window.promoPricing.views ? window.promoPricing.views.price : '') || '';
          promos.push({ key: 'views', qty, label: `Visualizações (${qty})`, priceCents: window.parsePrecoToCents(priceStr) });
        }
        if (commentsChecked) {
          const qty = Number(document.getElementById('commentsQty')?.textContent || 1);
          const priceCents = qty * 150;
          promos.push({ key: 'comments', qty, label: `Comentários (${qty})`, priceCents });
        }
        if (warrantyChecked) {
          const priceStr = 'R$ 19,90';
          const label = 'Reposição por 6 meses';
          promos.push({ key: 'warranty_6m', qty: 1, label, priceCents: window.parsePrecoToCents(priceStr) });
        }
        if (upgradeChecked) {
          let priceStr = document.querySelector('.promo-prices[data-promo="upgrade"] .new-price')?.textContent || '';
          const highlight = document.getElementById('orderBumpHighlight')?.textContent || '';
          promos.push({ key: 'upgrade', qty: 1, label: `Upgrade de pacote ${highlight ? `(${highlight})` : ''}`.trim(), priceCents: window.parsePrecoToCents(priceStr) });
        }
      } catch (_) {}
      return promos;
    };
  }
})();

  Array.from(document.querySelectorAll('.promo-item input[type="checkbox"]')).forEach(inp => {
    inp.addEventListener('change', updatePromosSummary);
    inp.addEventListener('click', updatePromosSummary);
  });
  Array.from(document.querySelectorAll('.promo-item')).forEach(function(node){
    node.addEventListener('click', function(){ setTimeout(function(){ try { updatePromosSummary(); } catch(_) {} }, 0); });
  });
  const inlineUpgradeCheckbox = document.getElementById('orderBumpCheckboxInline');
  if (inlineUpgradeCheckbox) {
    inlineUpgradeCheckbox.addEventListener('change', function(){ try { updatePromosSummary(); } catch(_) {} });
  }
  // Reforço: qualquer interação na área de promoções recalcula o resumo
  (function(){
    const promoContainer = document.getElementById('orderBumpInline');
    if (!promoContainer) return;
    ['change','input','click'].forEach(evt => {
      promoContainer.addEventListener(evt, function(){
        try { updatePromosSummary(); } catch(_) {}
      });
    });
  })();
  Array.from(document.querySelectorAll('.promo-prices[data-promo] .new-price')).forEach(function(el){
    var parent = el.closest('.promo-item');
    if (!parent) return;
    parent.addEventListener('click', function(){ try { updatePromosSummary(); } catch(_) {} });
  });
  (function(){
    const phoneEl = document.getElementById('checkoutPhoneInput');
    if (!phoneEl) return;
    phoneEl.addEventListener('focus', ()=>{ showTutorialStep(5); });
    phoneEl.addEventListener('input', ()=>{ showTutorialStep(5); });
  })();
  
  function showResumoIfAllowed(){
    try {
      // Ignorar verificação na página engajamento-novo
      if (window.location.pathname.indexOf('/engajamento-novo') !== -1) return;

      const allow = (!isFollowersSelected()) || !!isInstagramVerified;
      if (!resumo) return;
      resumo.hidden = !allow;
      resumo.style.display = allow ? 'block' : 'none';
    } catch(_) {}
  }

  function updatePromosSummary() {
    const resPromos = document.getElementById('resPromos');
    const resPromosContainer = document.getElementById('resPromosContainer');
    
    showResumoIfAllowed();
    // Base: prioriza o card de plano ativo; depois texto do resumo; por fim base armazenada
    let baseCents = 0;
    try {
      const activeCard = planCards?.querySelector('.service-card[data-role="plano"].active');
      if (activeCard && activeCard.dataset && activeCard.dataset.preco) {
        baseCents = parsePrecoToCents(activeCard.dataset.preco);
      }
    } catch(_) {}
    if (!baseCents) {
      // Se não achou no card ativo, tenta pegar do resPreco (mas cuidado pois pode ser o total antigo)
      // Melhor usar basePriceCents global
      baseCents = basePriceCents || 0;
    }
    
    const promos = (typeof window.getSelectedPromos === 'function') ? window.getSelectedPromos() : [];
    
    // Renderiza lista de promoções
    if (resPromos) {
        if (promos.length > 0) {
            const html = promos.map(p => {
                const val = formatCentsToBRL(Number(p.priceCents) || 0);
                return `<div style="display:flex; justify-content:space-between; margin-bottom:0.2rem;">
                          <span>+ ${p.label}</span>
                          <span style="font-weight:600;">${val}</span>
                        </div>`;
            }).join('');
            resPromos.innerHTML = html;
            if (resPromosContainer) resPromosContainer.style.display = 'block';
        } else {
            resPromos.innerHTML = '';
            if (resPromosContainer) resPromosContainer.style.display = 'none';
        }
    }

    const resPrecoEl = document.getElementById('resPreco');
    const resTotalFinal = document.getElementById('resTotalFinal');
    const promosTotal = Number(window.calcPromosTotalCents ? window.calcPromosTotalCents(promos) : 0);
    const totalCents = Math.max(0, Number(baseCents) + promosTotal);
    
    // Atualiza preço do item principal (sem promos)
    if (resPrecoEl) resPrecoEl.textContent = formatCentsToBRL(baseCents);
    
    // Atualiza Total Final
    if (resTotalFinal) resTotalFinal.textContent = formatCentsToBRL(totalCents);
    
    // Atualiza metas no card de perfil (Etapa 3)
    const step3TargetGain = document.getElementById('checkoutTargetGain');
    const step3TargetTotal = document.getElementById('checkoutTargetTotal');
    const step3Followers = document.getElementById('checkoutFollowersCountFinal');
    
    if (step3TargetGain && step3TargetTotal) {
       let gain = 0;
       try { 
          // Re-fetch element to ensure freshness
          const freshQtdSelect = document.getElementById('quantidadeSelect');
          if (freshQtdSelect) gain = parseInt(freshQtdSelect.value) || 0; 
       } catch(_) {}

       // Adicionar ganho do upgrade se selecionado
       try {
         const upgradePromo = promos.find(p => p.key === 'upgrade');
         if (upgradePromo) {
             // Tenta extrair do texto de destaque ou usar lógica do tipo
             const highlight = document.getElementById('orderBumpHighlight')?.textContent || '';
             const match = highlight.match(/\+\s*([\d\.]+)/);
             if (match && match[1]) {
                 gain += parseInt(match[1].replace(/\./g, '')) || 0;
             }
         }
       } catch(_) {}
       
       let current = 0;
       if (step3Followers && step3Followers.textContent && step3Followers.textContent !== '-') {
          current = parseInt(step3Followers.textContent.replace(/\./g, '').replace(/,/g, '')) || 0;
       }
       
       step3TargetGain.textContent = gain.toLocaleString('pt-BR');
       step3TargetTotal.textContent = (current + gain).toLocaleString('pt-BR');
       
       // Update New Header Fields (Step 3)
       const headerQty = document.getElementById('headerSelectedQty');
       
       // Campos de Review (Novo - Etapa 3)
       const revImg = document.getElementById('reviewProfileImage');
       const revUser = document.getElementById('reviewProfileUsername');
       const revFoll = document.getElementById('reviewProfileFollowers');
       
       const srcImg = document.getElementById('checkoutProfileImage');
       const srcUser = document.getElementById('checkoutProfileUsername');
       const srcFoll = document.getElementById('checkoutFollowersCount');
       
       if (revImg) {
           // Tenta pegar a imagem do perfil validado, fallback para placeholder
           let finalSrc = "https://upload.wikimedia.org/wikipedia/commons/a/ac/Default_pfp.jpg";
           if (srcImg && srcImg.src && srcImg.src !== '' && !srcImg.src.endsWith('undefined')) {
               finalSrc = srcImg.src;
           }
           revImg.src = finalSrc;
       }
       if (revUser && srcUser) revUser.textContent = srcUser.textContent;
       if (revFoll && srcFoll) revFoll.textContent = srcFoll.textContent;

       if (headerQty) headerQty.textContent = `+ ${gain.toLocaleString('pt-BR')} Seguidores`;
       
       /* Antigo headerUser removido da visualização principal mas mantido lógica se necessário */
       const headerUser = document.getElementById('headerSelectedUsername');
       if (headerUser) {
           // Prefer validated username from profile preview, fallback to input
           const validatedUser = document.getElementById('checkoutProfileUsername');
           const inputUser = document.getElementById('usernameCheckoutInput');
           let userTxt = (validatedUser && validatedUser.textContent) ? validatedUser.textContent : (inputUser ? inputUser.value : '');
           // Clean @ if present
           userTxt = userTxt.replace('@', '').trim();
           headerUser.textContent = userTxt;
       }
    }
    try { updatePaymentMethodVisibility(); } catch(_) {}
    try { scheduleStripeEmbeddedCheckoutRefresh(); } catch(_) {}
  }

  window.goToStep = function(step) {
    // Esconder todos os containers principais
    // Identificar containers
    const grid1 = document.getElementById('tipoSelectCard')?.closest('.cards-grid'); // Step 1 container
    const card2 = document.getElementById('perfilCard');
    const container3 = document.getElementById('step3Container');

    // Atualizar stepper UI
    document.querySelectorAll('.step').forEach((el, idx) => {
        if (idx + 1 === step) el.classList.add('active');
        else if (idx + 1 < step) el.classList.add('completed'); // Opcional, se tiver estilo
        else el.classList.remove('active');
    });

    // Mostrar/Esconder
    if (step === 1) {
        if (grid1) {
             grid1.parentElement.style.display = 'block';
             // Forçar visibilidade do grid caso tenha sido alterado
             if (grid1.style.display === 'none') grid1.style.display = 'grid';
             
             // Forçar visibilidade dos cards internos
             const cards = grid1.querySelectorAll('.service-card');
             cards.forEach(c => c.style.display = 'block');

             // RESTAURAR ESTADO VISUAL
             try {
                 const tipoCardsContainer = document.getElementById('tipoCards');
                 // Se os cards de tipo sumiram ou precisam ser re-marcados
                 if (tipoCardsContainer && (!tipoCardsContainer.innerHTML.trim() || tipoCardsContainer.style.display === 'none')) {
                    if (typeof renderTipoCards === 'function') renderTipoCards();
                 }
                 
                 if (tipoSelect && tipoSelect.value) {
                    // Marcar tipo ativo
                    const tCards = tipoCardsContainer?.querySelectorAll('.service-card[data-role="tipo"]');
                    tCards?.forEach(c => c.classList.toggle('active', c.dataset.tipo === tipoSelect.value));
                    
                    // Restaurar descrição
                    if (typeof renderTipoDescription === 'function') renderTipoDescription(tipoSelect.value);
                    
                    // Restaurar planos
                    if (typeof renderPlanCards === 'function') {
                       // Verifica se precisa renderizar (se estiver vazio) ou se forçamos para garantir o estado
                       const planCardsContainer = document.getElementById('planCards');
                       renderPlanCards(tipoSelect.value); // Recria os cards
                       
                       // Marcar plano ativo
                       if (qtdSelect && qtdSelect.value) {
                          const pCards = planCardsContainer?.querySelectorAll('.service-card[data-role="plano"]');
                          pCards?.forEach(c => {
                             const isActive = String(c.dataset.qtd) === String(qtdSelect.value);
                             c.classList.toggle('active', isActive);
                          });
                       }
                    }
                 }
             } catch(e) { console.error('Erro ao restaurar step 1:', e); }
        }
        if (card2) card2.style.display = 'none';
        if (container3) container3.style.display = 'none';
        window.scrollTo({ top: 0, behavior: 'smooth' });
    } else if (step === 2) {
        if (grid1) grid1.parentElement.style.display = 'none';
        if (card2) {
            card2.style.display = 'block';
            // Garante que o input de username esteja visível
            const usernameInput = document.getElementById('usernameCheckoutInput');
            if (usernameInput && !usernameInput.value) usernameInput.focus();
        }
        if (container3) container3.style.display = 'none';
        window.scrollTo({ top: 0, behavior: 'smooth' });
    } else if (step === 3) {
        if (grid1) grid1.parentElement.style.display = 'none';
        if (card2) card2.style.display = 'none';
        if (container3) container3.style.display = 'block';
        window.scrollTo({ top: 0, behavior: 'smooth' });
        try { updatePromosSummary(); } catch(_) {}
    }
  };

  async function navigateToPedidoOrFallback(identifier, correlationID) {
    let targetUrl = '';
    try {
      try { await fetch('/session/mark-paid', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ identifier, correlationID }) }); } catch(_) {}
      const apiUrl = `/api/order?identifier=${encodeURIComponent(identifier)}&correlationID=${encodeURIComponent(correlationID)}`;
      const resp = await fetch(apiUrl);
      const data = await resp.json();
      const oid = (data && data.order && data.order.fama24h && data.order.fama24h.orderId) || (data && data.order && data.order.fornecedor_social && data.order.fornecedor_social.orderId) || null;
      if (oid) {
        try { await fetch('/pedido/select', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ orderID: String(oid) }) }); } catch(_) {}
        targetUrl = `/pedido?oid=${encodeURIComponent(String(oid))}`;
      } else {
        targetUrl = `/pedido?t=${encodeURIComponent(identifier)}&ref=${encodeURIComponent(correlationID)}`;
      }
    } catch(_) {
      targetUrl = `/pedido?t=${encodeURIComponent(identifier)}&ref=${encodeURIComponent(correlationID)}`;
    }
    try { window.location.assign(targetUrl || '/pedido'); } catch(_) {}
    try {
      setTimeout(async () => {
        try { if (location && location.pathname === '/pedido') return; } catch(_) {}
        try {
          const r = await fetch(targetUrl || '/pedido', { method: 'GET', headers: { 'Accept': 'text/html' } });
          if (r && r.ok) { window.location.href = (targetUrl || '/pedido'); return; }
        } catch(_) {}
        try { markPaymentConfirmed(); } catch(_) {}
        try { showStatusMessageCheckout('Pagamento confirmado. Exibindo resumo abaixo.', 'success'); } catch(_) {}
        try {
          const checkUrl = `/api/order?identifier=${encodeURIComponent(identifier)}&correlationID=${encodeURIComponent(correlationID)}`;
          const resp2 = await fetch(checkUrl);
          const data2 = await resp2.json();
          if (data2 && data2.order) { showResumoIfAllowed(); }
        } catch(_) { showResumoIfAllowed(); }
      }, 2500);
    } catch(_) {}
  }

  function markPaymentConfirmed() {
    try {
      if (pixResultado) {
        pixResultado.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;color:#22C55E;font-weight:700;font-size:1rem;"><span class="price-new">Pagamento confirmado</span></div>';
      }
    } catch(_) {}
    try { showStatusMessageCheckout('Pagamento confirmado. Exibindo resumo abaixo.', 'success'); } catch(_) {}
    try { showResumoIfAllowed(); } catch(_) {}
  }
  try {
    const audioBtn = document.getElementById('audioPlayBtn');
    const audioTip = document.getElementById('tutorialAudio');
    if (audioBtn && audioTip) {
      audioBtn.addEventListener('click', () => { audioTip.classList.add('hide'); });
    }
  } catch(_) {}

  (function adjustValidateBtn(){
    try {
      const btn = document.getElementById('checkCheckoutButton');
      if (!btn) return;
      const apply = () => {
        const w = window.innerWidth || document.documentElement.clientWidth;
        if (w <= 480) {
          btn.style.padding = '0.26rem 0.48rem';
          btn.style.fontSize = '12px';
        } else if (w <= 768) {
          btn.style.padding = '0.32rem 0.52rem';
          btn.style.fontSize = '12.5px';
        }
      };
      apply();
      window.addEventListener('resize', apply);
      window.addEventListener('orientationchange', apply);
    } catch(_) {}
  })();
  /* initFaqMover disabled */
  /* (function initFaqMover(){ ... })(); */
  (function initFaqAccordion(){
    const faq = document.getElementById('faqSection');
    if (!faq) return;
    const buttons = faq.querySelectorAll('.faq-card .faq-question');
    buttons.forEach(function(btn){
      const card = btn.closest('.faq-card');
      const ans = card ? card.querySelector('.faq-answer') : null;
      btn.addEventListener('click', function(){
        const expanded = btn.getAttribute('aria-expanded') === 'true';
        btn.setAttribute('aria-expanded', expanded ? 'false' : 'true');
        if (card) card.classList.toggle('open', !expanded);
        if (ans) ans.hidden = expanded;
      });
    });
  })();
  (function disableDoubleTapZoom(){
    var mq = window.matchMedia('(max-width: 640px)');
    if (!mq || !mq.matches) return;
    var last = 0;
    document.addEventListener('touchend', function(e){
      var now = Date.now();
      try {
        var t = e && e.target ? e.target : null;
        if (t && typeof t.closest === 'function') {
          var hit = t.closest('input,textarea,select,button,[contenteditable="true"]');
          if (hit) { last = now; return; }
        }
      } catch (_) {}
      if (now - last <= 300) { e.preventDefault(); }
      last = now;
    }, { passive: false });
  })();
