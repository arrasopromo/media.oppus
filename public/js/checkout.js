(() => {
  /* try { if (typeof fbq === 'function' && window._oppusPixelReady) fbq('track', 'PageView'); } catch(e) {} */
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
  const btnInstagram = document.querySelector('.platform-btn.instagram');
  const btnTikTok = document.querySelector('.platform-btn.tiktok');
  let selectedPlatform = (btnInstagram && btnInstagram.getAttribute('aria-pressed') === 'true') ? 'instagram' : 'tiktok';
  let basePriceCents = 0;
  let paymentPollInterval = null;
  let paymentEventSource = null;
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
  let phoneFromUrl = new URLSearchParams(window.location.search).get('phone') || '11111111';

  const tabela = {
    mistos: [
      { q: 150, p: 'R$ 7,90' },
      { q: 300, p: 'R$ 14,90' },
      { q: 500, p: 'R$ 32,90' },
      { q: 700, p: 'R$ 39,90' },
      { q: 1000, p: 'R$ 49,90' },
      { q: 2000, p: 'R$ 79,90' },
      { q: 3000, p: 'R$ 109,90' },
      { q: 4000, p: 'R$ 139,90' },
      { q: 5000, p: 'R$ 159,90' },
      { q: 7500, p: 'R$ 199,90' },
      { q: 10000, p: 'R$ 269,90' },
      { q: 15000, p: 'R$ 399,90' },
    ],
    brasileiros: [
      { q: 150, p: 'R$ 19,90' },
      { q: 300, p: 'R$ 29,90' },
      { q: 500, p: 'R$ 54,90' },
      { q: 700, p: 'R$ 69,90' },
      { q: 1000, p: 'R$ 99,90' },
      { q: 2000, p: 'R$ 169,90' },
      { q: 3000, p: 'R$ 229,90' },
      { q: 4000, p: 'R$ 299,90' },
      { q: 5000, p: 'R$ 329,90' },
      { q: 7500, p: 'R$ 459,90' },
      { q: 10000, p: 'R$ 599,90' },
      { q: 15000, p: 'R$ 999,90' },
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

  // Garantia única com slider: 30 dias (R$ 9,90) <-> Vitalícia (R$ 19,90)
  let warrantyMode = '30';
  try { window.warrantyMode = warrantyMode; } catch(_) {}
  const wDec = document.getElementById('warrantyModeDec');
  const wInc = document.getElementById('warrantyModeInc');
  const wLabel = document.getElementById('warrantyModeLabel');
  const wHighlight = document.getElementById('warrantyHighlight');
  const wOld = document.getElementById('warrantyOldPrice');
  const wNew = document.getElementById('warrantyNewPrice');
  const wDisc = document.getElementById('warrantyDiscount');
  function applyWarrantyMode(){
    const isLife = warrantyMode === 'life';
    if (wLabel) wLabel.textContent = isLife ? 'Vitalícia' : '30 dias';
    if (wHighlight) wHighlight.textContent = isLife ? 'GARANTIA VITALÍCIA' : '+ 30 DIAS DE REPOSIÇÃO';
    if (wOld) wOld.textContent = isLife ? 'R$ 129,90' : 'R$ 39,90';
    if (wNew) wNew.textContent = isLife ? 'R$ 19,90' : 'R$ 9,90';
    if (wDisc) wDisc.textContent = isLife ? '85% OFF' : '75% OFF';
    try { updatePromosSummary(); } catch(_) {}
  }
  function stepWarranty(delta){
    const next = (warrantyMode === '30' && delta > 0) ? 'life' : (warrantyMode === 'life' && delta < 0) ? '30' : warrantyMode;
    if (next !== warrantyMode) { warrantyMode = next; try { window.warrantyMode = warrantyMode; } catch(_) {} applyWarrantyMode(); }
  }
  if (wDec) wDec.addEventListener('click', () => stepWarranty(-1));
  if (wInc) wInc.addEventListener('click', () => stepWarranty(1));
  applyWarrantyMode();

  tabela.seguidores_tiktok = tabela.mistos;
  function getAllowedQuantities(tipo) {
    const without50 = [150, 500, 1000, 3000, 5000, 10000];
    const base = [50, 150, 500, 1000, 3000, 5000, 10000];
    if (tipo === 'mistos') {
      return base;
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

  function findPrice(tipo, qtd) {
    const arr = tabela[tipo] || [];
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
    if (!isFollowersTipo(tipo) || !baseQtd) { orderInline.style.display = 'none'; return; }
    // Sempre mostrar o card de Promoções para serviços de seguidores
    orderInline.style.display = 'block';
    if (checkbox) checkbox.checked = false;

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

    // Promo específica: organicos 50 -> +50 (total 100) para teste
    /* if (tipo === 'organicos' && Number(baseQtd) === 50) {
      const addQtd = 50;
      const diffStr = findPrice('organicos', 50) || 'R$ 0,10';
      if (labelSpan) labelSpan.textContent = `Por mais ${diffStr}, adicione ${addQtd} seguidores e atualize para 100.`;
      if (upHighlight) upHighlight.textContent = `+ ${addQtd} seguidores`;
      if (upOld) upOld.textContent = '—';
      if (upNew) upNew.textContent = diffStr;
      if (upDisc) upDisc.textContent = 'OFERTA';
      return;
    } */

    // Upgrade genérico para demais pacotes
    const upsellTargets = { 50: 150, 150: 300, 500: 700, 1000: 2000, 3000: 4000, 5000: 7500, 10000: 15000 };
    const targetQtd = upsellTargets[Number(baseQtd)];
    if (!targetQtd) {
      if (labelSpan) labelSpan.textContent = 'Nenhum upgrade disponível para este pacote.';
      if (upOld) upOld.textContent = '—';
      if (upNew) upNew.textContent = '—';
      if (upDisc) upDisc.textContent = 'OFERTA';
      return;
    }
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
    const tipos = selectedPlatform === 'tiktok'
      ? [
          { key: 'seguidores_tiktok', label: 'Seguidores' }
        ]
      : [
          { key: 'mistos', label: 'Seguidores Mistos' },
          { key: 'brasileiros', label: 'Seguidores Brasileiros' },
          { key: 'organicos', label: 'Seguidores Brasileiros Orgânicos' }
        ];
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
    for (const t of tipos) {
      const el = document.createElement('div');
      el.className = 'service-card' + (selectedPlatform === 'tiktok' ? ' disabled' : '');
      el.dataset.role = 'tipo';
      el.dataset.tipo = t.key;
      el.innerHTML = `<div class="card-content"><div class="card-title" style="text-align:center;">${t.label}</div><div class="card-desc" style="text-align:center;">${selectedPlatform === 'tiktok' ? '<span class="status-warning"><svg class="status-maint-icon" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><path fill="currentColor" d="M3 12a9 9 0 1118 0 9 9 0 01-18 0zm9-4a1 1 0 011 1v3.586l2.293 2.293a1 1 0 11-1.414 1.414l-2.586-2.586A2 2 0 0110 12V9a1 1 0 011-1z"/></svg> Serviço em manutenção</span>' : ''}</div></div>`;
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
    let plans = tabela[tipo] || [];
    if (isFollowersTipo(tipo)) {
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
      card.innerHTML = `<div class="card-content"><div class="card-title">${item.q} ${unit}</div><div class="card-desc"><span class="price-old">${increasedText}</span> <span class="price-new">${baseText}</span></div></div>`;
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
            if (target && typeof target.scrollIntoView === 'function') {
              target.scrollIntoView({ behavior: 'smooth', block: 'start' });
            }
          }
        } catch(_) {}
      });
      planCards.appendChild(card);
    }
  }

  function getLabelForTipo(t) {
    switch (t) {
      case 'mistos': return 'Seguidores Mistos';
      case 'brasileiros': return 'Seguidores Brasileiros';
      case 'organicos': return 'Seguidores Brasileiros Orgânicos';
      case 'seguidores_tiktok': return 'Seguidores';
      case 'curtidas_reais': return 'Curtidas reais';
      case 'visualizacoes_reels': return 'Visualizações Reels';
      default: return String(t).replace(/_/g, ' ');
    }
  }

  function getTipoDescription(tipo) {
    switch (tipo) {
      case 'mistos':
        return `
          <p>Este serviço entrega seguidores mistos, podendo conter tanto brasileiros quanto estrangeiros. Perfis de diversas regiões do mundo, com nomes variados e níveis diferentes de atividade. Alguns perfis internacionais são reais. Ideal para quem busca crescimento rápido, com ótima estabilidade e excelente custo-benefício.</p>
          <ul>
            <li>✨ <strong>Qualidade garantida:</strong> Trabalhamos somente com serviços bons e estáveis, que não ficam caindo.</li>
            <li>📉 <strong>Queda estimada:</strong> Em média 5% a 10%; caso ocorra — nós repomos tudo gratuitamente.</li>
            <li>✅ <strong>Vantagem:</strong> Melhor custo-benefício para quem quer crescer rápido.</li>
            <li>ℹ️ <strong>Observação:</strong> Parte dos seguidores pode ser internacional.</li>
          </ul>
        `;
      case 'brasileiros':
        return `
          <p>🇧🇷 Entrega composta exclusivamente por perfis com nomes brasileiros, garantindo uma base com aparência nacional. Perfis com nomes e características locais, podendo variar em frequência de postagem ou interação. Perfeito para quem busca credibilidade nacional, com serviço estável e de qualidade.</p>
          <ul>
            <li>✨ <strong>Qualidade garantida:</strong> Todos os nossos serviços são bons e estáveis, não caem facilmente, e têm suporte completo de reposição.</li>
            <li>📉 <strong>Queda estimada:</strong> Em média 5% a 10%; repomos automaticamente caso aconteça.</li>
            <li>✅ <strong>Vantagem:</strong> Perfis brasileiros com nomes e fotos locais.</li>
            <li>ℹ️ <strong>Observação:</strong> Interações e stories podem variar entre os perfis.</li>
          </ul>
        `;
      case 'organicos':
        return `
          <p>Serviço premium com seguidores 100% brasileiros, ativos e filtrados, com interações, stories recentes e até perfis verificados. Os seguidores são cuidadosamente selecionados para entregar credibilidade máxima e engajamento real. Perfeito para quem busca autoridade e resultados duradouros, com a melhor estabilidade do mercado.</p>
          <ul>
            <li>✨ <strong>Qualidade garantida:</strong> Trabalhamos somente com serviços premium, estáveis e seguros, que não sofrem quedas significativas.</li>
            <li>📉 <strong>Queda estimada:</strong> Em média 1% a 2%; caso ocorra — garantimos a reposição total.</li>
            <li>✅ <strong>Vantagem:</strong> Seguidores reais, engajados e 100% brasileiros.</li>
            <li>ℹ️ <strong>Observação:</strong> A entrega é gradual para manter a naturalidade e segurança do perfil.</li>
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
    switch (tipo) {
      case 'mistos':
      case 'brasileiros':
      case 'organicos':
      case 'seguidores_tiktok':
        return 'seguidores';
      case 'curtidas_reais':
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

  (function initBuyFollowersBtn(){
    const btn = document.getElementById('buyFollowersBtn');
    if (!btn) return;
    btn.addEventListener('click', function(e){
      try { e.preventDefault(); } catch(_) {}
      try {
        const targetPlat = document.getElementById('plataformaCard');
        if (targetPlat && typeof targetPlat.scrollIntoView === 'function') targetPlat.scrollIntoView({ behavior: 'smooth', block: 'start' });
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
        try { positionTutorials(); } catch(_) {}
        setTimeout(() => { try { positionTutorials(); } catch(_) {} }, 120);
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
    if (!tipo || !tabela[tipo]) {
      qtdSelect.disabled = true;
      const opt = document.createElement('option');
      opt.value = '';
      opt.textContent = 'Selecione o tipo primeiro...';
      qtdSelect.appendChild(opt);
      clearResumo();
      return;
    }
    qtdSelect.disabled = false;
    let opts = tabela[tipo];
    if (isFollowersTipo(tipo)) {
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
    if (tipo === 'curtidas_reais') {
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
        if (target && typeof target.scrollIntoView === 'function') {
          target.scrollIntoView({ behavior: 'smooth', block: 'start' });
        }
      }
    } catch(_) {}
  });

  const likesTable = [
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
  ];
  const likesQtyEl = document.getElementById('likesQty');
  const likesDec = document.getElementById('likesDec');
  const likesInc = document.getElementById('likesInc');
  const likesPrices = document.querySelector('.promo-prices[data-promo="likes"]');
  function formatCurrencyBR(n) { return `R$ ${n.toFixed(2).replace('.', ',')}`; }
  function parseCurrencyBR(s) { const cleaned = String(s).replace(/[R$\s]/g, '').replace('.', '').replace(',', '.'); const val = parseFloat(cleaned); return isNaN(val) ? 0 : val; }
  function updateLikesPrice(q) {
    const entry = likesTable.find(e => e.q === q);
    const newEl = likesPrices ? likesPrices.querySelector('.new-price') : null;
    const oldEl = likesPrices ? likesPrices.querySelector('.old-price') : null;
    if (newEl && entry) newEl.textContent = entry.price;
    if (oldEl && entry) { const newVal = parseCurrencyBR(entry.price); const oldVal = newVal * 1.70; oldEl.textContent = formatCurrencyBR(oldVal); }
    const hl = document.querySelector('.promo-item.likes .promo-highlight');
    if (hl) hl.textContent = `+ ${q} CURTIDAS`;
  }
  function stepLikes(dir) {
    const current = Number(likesQtyEl?.textContent || 150);
    const idx = likesTable.findIndex(e => e.q === current);
    let nextIdx = idx >= 0 ? idx + dir : 0;
    if (nextIdx < 0) nextIdx = 0;
    if (nextIdx >= likesTable.length) nextIdx = likesTable.length - 1;
    const next = likesTable[nextIdx].q;
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
      refs.postModalGrid.innerHTML = html || (isInstagramPrivate ? '<div style="grid-column:1/-1;color:#ef4444;">Deixe o perfil no modo público para selecionar o post</div>' : '<div style="grid-column:1/-1">Nenhum post encontrado.</div>');
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
        promos.push({ key: 'likes', qty, label: `Curtidas (${qty})`, priceCents: parsePrecoToCents(priceStr) });
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
        const mode = (typeof window.warrantyMode === 'string') ? window.warrantyMode : '30';
        const priceStr = (mode === 'life') ? 'R$ 19,90' : 'R$ 9,90';
        const label = (mode === 'life') ? 'Garantia vitalícia' : '+30 dias de reposição';
        promos.push({ key: (mode === 'life') ? 'warranty_lifetime' : 'warranty30', qty: 1, label, priceCents: parsePrecoToCents(priceStr) });
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
      const resp = await fetch('/api/check-instagram-profile', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username })
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
          await fetch(trackUrl, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ username: profile.username || username }) });
        } catch (_) {}
        // Avança para o passo final
        showTutorialStep(5);
        try {
          const url = '/api/instagram/posts?username=' + encodeURIComponent(profile.username || username);
          fetch(url).then(r=>r.json()).then(d=>{ cachedPosts = Array.isArray(d.posts) ? d.posts : []; cachedPostsUser = (profile.username || username) || ''; }).catch(function(){});
        } catch(_) {}
        
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
          isInstagramVerified = true;
          isInstagramPrivate = !!isPrivate;
          updatePedidoButtonState();
          showResumoIfAllowed();
          try { updatePromosSummary(); } catch(_) {}
          try { applyCheckoutFlow(); } catch(_) {}
          showStatusMessageCheckout('Perfil verificado com sucesso.', 'success');
          try {
            const trackUrl = '/api/instagram/validet-track';
            await fetch(trackUrl, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ username: profile.username || username }) });
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
      const payload = {
        correlationID,
        value: totalCents,
        comment: 'Checkout OPPUS',
        customer: {
          name: 'Cliente Checkout',
          phone: phoneValue
        },
        // Sanitiza e evita emojis/Unicode não permitido
        additionalInfo: [
          { key: 'tipo_servico', value: tipo },
          { key: 'quantidade', value: String(qtdEffective) },
          { key: 'pacote', value: `${qtdEffective} ${getUnitForTipo(tipo)} - ${precoStr}` },
          { key: 'phone', value: phoneValue },
          { key: 'instagram_username', value: instagramUsernameFinal },
          { key: 'order_bumps_total', value: formatCentsToBRL(promosTotalCents) },
          { key: 'order_bumps', value: promos.map(p => `${p.key}:${p.qty ?? 1}`).join(';') }
        ],
        profile_is_private: isInstagramPrivate
      };
      try {
        const m = document.cookie.match(/(?:^|;\s*)tc_code=([^;]+)/);
        const tc = m && m[1] ? m[1] : '';
        if (tc) { payload.additionalInfo.push({ key: 'tc_code', value: tc }); }
      } catch(_) {}

      try {
        const selResp = await fetch('/api/instagram/selected-for');
        const selData = await selResp.json();
        const sfor = selData && selData.selectedFor ? selData.selectedFor : {};
        const mapKind = function(k){ const obj = sfor && sfor[k]; const sc = obj && obj.shortcode; return sc ? `https://instagram.com/p/${encodeURIComponent(sc)}/` : ''; };
        const likesLink = mapKind('likes');
        const viewsLink = mapKind('views');
        const commentsLink = mapKind('comments');
        
        const hasLikes = promos.some(p => p.key === 'likes');
        const hasViews = promos.some(p => p.key === 'views');
        const hasComments = promos.some(p => p.key === 'comments');

        if (likesLink && hasLikes) payload.additionalInfo.push({ key: 'orderbump_post_likes', value: likesLink });
        if (viewsLink && hasViews) payload.additionalInfo.push({ key: 'orderbump_post_views', value: viewsLink });
        if (commentsLink && hasComments) payload.additionalInfo.push({ key: 'orderbump_post_comments', value: commentsLink });
      } catch(_) {}

      // sem envio de links de posts

      const resp = await fetch('/api/woovi/charge', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });

      const data = await resp.json();
      if (!resp.ok) {
        throw new Error(data?.details?.message || 'Falha ao criar cobrança');
      }

      // Renderização amigável: QR Code e botão de copiar código Pix
      const charge = data?.charge || {};
      const pix = charge?.paymentMethods?.pix || {};
      const brCode = pix?.brCode || charge?.brCode || data?.brCode || '';
      const qrImage = pix?.qrCodeImage || charge?.qrCodeImage || data?.qrCodeImage || '';

      const copyButtonId = 'copyPixBtn';
      const inputId = 'pixBrCodeInput';

      const imgHtml = qrImage
        ? `<img src="${qrImage}" alt="QR Code Pix" style="width: 180px; height: 180px; border-radius: 8px; display: block; margin: 0 auto 0.75rem; background: #fff;" />`
        : '';

      const codeFieldHtml = brCode
        ? `<div style="margin-bottom: 0.5rem; text-align: center;">
             <input id="${inputId}" type="text" readonly value="${brCode}" style="width: 100%; padding: 0.5rem; font-size: 0.9rem; border-radius: 6px; border: 1px solid rgba(255,255,255,0.3); background: rgba(255,255,255,0.85); color: #111827; text-align: center;" />
           </div>`
        : '<div style="color:#fff;">Não foi possível exibir o código Pix.</div>';

      const copyBtnHtml = brCode
        ? `<div class="button-container" style="margin-bottom: 0.5rem;">
             <button id="${copyButtonId}" class="continue-button">
               <span class="button-text">Copiar código Pix</span>
             </button>
           </div>`
        : '';

      const textColor = document.body.classList.contains('theme-light') ? '#000' : '#fff';
      const waitingHtml = `
        <div style="display:flex; align-items:center; justify-content:center; gap:0.5rem; color:${textColor};">
          <svg width="18" height="18" viewBox="0 0 50 50" xmlns="http://www.w3.org/2000/svg">
            <circle cx="25" cy="25" r="20" stroke="${textColor}" stroke-width="4" fill="none" stroke-dasharray="31.4 31.4">
              <animateTransform attributeName="transform" type="rotate" from="0 25 25" to="360 25 25" dur="1s" repeatCount="indefinite" />
            </circle>
          </svg>
          <span>Aguardando pagamento...</span>
        </div>`;

      pixResultado.innerHTML = `${imgHtml}${codeFieldHtml}${copyBtnHtml}${waitingHtml}`;
      pixResultado.style.display = 'block';

      const copyBtn = document.getElementById(copyButtonId);
      try {
        const isMobile = window.innerWidth <= 640;
        if (isMobile) {
          const target = copyBtn || document.getElementById('paymentCard') || pixResultado;
          if (target) {
            const rect = target.getBoundingClientRect();
            const top = (window.scrollY || window.pageYOffset || 0) + rect.top - 80;
            window.scrollTo({ top, behavior: 'smooth' });
          }
        }
      } catch(_) {}

      if (copyBtn && brCode) {
        copyBtn.addEventListener('click', async () => {
          try {
            if (navigator.clipboard?.writeText) {
              await navigator.clipboard.writeText(brCode);
            } else {
              const input = document.getElementById(inputId);
              input?.select();
              document.execCommand('copy');
            }
            const span = copyBtn.querySelector('.button-text');
            const prev = span ? span.textContent : '';
            if (span) span.textContent = 'Pix copiado';
            try { showStatusMessageCheckout('Código Pix copiado', 'success'); } catch(_) {}
            copyBtn.disabled = true;
            copyBtn.classList.add('loading');
            setTimeout(() => {
              copyBtn.disabled = false;
              copyBtn.classList.remove('loading');
              if (span) span.textContent = prev || 'Copiar código Pix';
            }, 1200);
          } catch (e) {
            alert('Não foi possível copiar o código Pix.');
          }
        });
      }

      const chargeId = charge?.id || charge?.chargeId || data?.chargeId || '';
      const identifier = charge?.identifier || (data?.charge && data.charge.identifier) || '';
      const serverCorrelationID = charge?.correlationID || (data?.charge && data.charge.correlationID) || '';
      if (paymentPollInterval) {
        clearInterval(paymentPollInterval);
        paymentPollInterval = null;
      }
      if (chargeId) {
        const checkPaid = async () => {
          try {
            const stResp = await fetch(`/api/woovi/charge-status?id=${encodeURIComponent(chargeId)}`);
            const stData = await stResp.json();
            const status = stData?.charge?.status || stData?.status || '';
            const paidFlag = stData?.charge?.paid || stData?.paid || false;
            const isPaid = paidFlag === true || /paid/i.test(String(status));
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
        // Executa imediatamente e depois a cada 30s
        checkPaid();
        paymentPollInterval = setInterval(checkPaid, 30000);
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
        paymentPollInterval = setInterval(checkPaidDb, 12000);
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

  if (btnPedido) {
    btnPedido.addEventListener('click', criarPixWoovi);
  }

  if (checkCheckoutButton) {
    checkCheckoutButton.addEventListener('click', checkInstagramProfileCheckout);
  }

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
    });
  }
  if (audioPlayBtn) audioPlayBtn.addEventListener('click', async () => {
    if (!guideAudio) return;
    if (guideAudio.paused) { await guideAudio.play(); audioPlayBtn.textContent = 'Pause'; } else { guideAudio.pause(); audioPlayBtn.textContent = 'Ouvir Áudio'; }
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
  if (platformToggle) {
    platformToggle.addEventListener('click', (e) => {
      const target = e.target.closest('.platform-btn');
      if (!target) return;
      if (window.__ENG_MODE__ && target.classList.contains('instagram')) {
        try { e.preventDefault(); } catch(_) {}
        try { e.stopPropagation(); } catch(_) {}
        window.location.href = '/servicos';
        return;
      }
      if (target.classList.contains('instagram')) setPlatform('instagram');
      if (target.classList.contains('tiktok')) setPlatform('tiktok');
      try {
        const tp = document.getElementById('tutorialPlatform');
        if (tp) { tp.style.display = 'none'; tp.classList.add('hide'); }
      } catch(_) {}
    });
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
                  window.location.href = '/pedido?orderID=' + encodeURIComponent(String(onlyOid));
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
    });
  }
  if (commentsCloseBtn && commentsModal) {
    commentsCloseBtn.addEventListener('click', function(){ commentsModal.style.display = 'none'; });
  }
  if (commentsCloseBtn2 && commentsModal) {
    commentsCloseBtn2.addEventListener('click', function(){ commentsModal.style.display = 'none'; });
  }
  if (commentsModal) {
    commentsModal.addEventListener('click', function(e){ if (e.target === commentsModal) { commentsModal.style.display = 'none'; } });
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
          promos.push({ key: 'likes', qty, label: `Curtidas (${qty})`, priceCents: window.parsePrecoToCents(priceStr) });
        }
        if (viewsChecked) {
          const qty = Number(document.getElementById('viewsQty')?.textContent || 1000);
          let priceStr = document.querySelector('.promo-prices[data-promo="views"] .new-price')?.textContent || '';
          if (!priceStr) priceStr = (window.promoPricing && window.promoPricing.views ? window.promoPricing.views.price : '') || '';
          promos.push({ key: 'views', qty, label: `Visualizações (${qty})`, priceCents: window.parsePrecoToCents(priceStr) });
        }
        if (commentsChecked) {
          let priceStr = document.querySelector('.promo-prices[data-promo="comments"] .new-price')?.textContent || '';
          if (!priceStr) priceStr = (window.promoPricing && window.promoPricing.comments ? window.promoPricing.comments.price : '') || '';
          promos.push({ key: 'comments', qty: 1, label: 'Comentário promocional', priceCents: window.parsePrecoToCents(priceStr) });
        }
        if (warrantyChecked) {
          const mode = (typeof window.warrantyMode === 'string') ? window.warrantyMode : '30';
          const priceStr = (mode === 'life') ? 'R$ 19,90' : 'R$ 9,90';
          const label = (mode === 'life') ? 'Garantia vitalícia' : '+30 dias de reposição';
          promos.push({ key: (mode === 'life') ? 'warranty_lifetime' : 'warranty30', qty: 1, label, priceCents: window.parsePrecoToCents(priceStr) });
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
      const allow = (!isFollowersSelected()) || !!isInstagramVerified;
      if (!resumo) return;
      resumo.hidden = !allow;
      resumo.style.display = allow ? 'block' : 'none';
    } catch(_) {}
  }

  function updatePromosSummary() {
    const resPromos = document.getElementById('resPromos');
    if (!resPromos) return;
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
      const resTxt = document.getElementById('resPreco')?.textContent || '';
      baseCents = parsePrecoToCents(resTxt);
    }
    if (!baseCents) {
      baseCents = basePriceCents || 0;
    }
    const promos = (typeof window.getSelectedPromos === 'function') ? window.getSelectedPromos() : [];
    const labels = promos.map(p => {
      const val = formatCentsToBRL(Number(p.priceCents) || 0);
      return `${p.label} (${val})`;
    }).filter(Boolean);
    const resPrecoEl = document.getElementById('resPreco');
    const totalCents = Math.max(0, Number(baseCents) + Number(window.calcPromosTotalCents ? window.calcPromosTotalCents(promos) : 0));
    const bullets = labels.length ? labels.map(s => `• ${s}`).join('\n') : 'Nenhuma';
    if (resPromos) resPromos.textContent = bullets;
    if (resPrecoEl) resPrecoEl.textContent = formatCentsToBRL(totalCents);
  }

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
        targetUrl = `/pedido?orderID=${encodeURIComponent(String(oid))}`;
      } else {
        targetUrl = `/pedido?identifier=${encodeURIComponent(identifier)}&correlationID=${encodeURIComponent(correlationID)}`;
      }
    } catch(_) {
      targetUrl = `/pedido?identifier=${encodeURIComponent(identifier)}&correlationID=${encodeURIComponent(correlationID)}`;
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
  (function initFaqMover(){
    const isCheckout = !!document.querySelector('.checkout-page');
    if (!isCheckout) return;
    let moved = false;
    function move(){
      if (moved) return;
      const faq = document.getElementById('faqSection');
      const grid = document.querySelector('.cards-grid.checkout-grid');
      if (!faq || !grid) return;
      try { grid.appendChild(faq); moved = true; } catch(_) {}
    }
    document.addEventListener('click', function(){ move(); });
    window.addEventListener('scroll', function(){ if (!moved && (window.scrollY || document.documentElement.scrollTop || 0) > 100) { move(); } }, { passive: true });
    const tipoSel = document.getElementById('tipoSelect');
    if (tipoSel) tipoSel.addEventListener('change', function(){ move(); });
  })();
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
      if (now - last <= 300) { e.preventDefault(); }
      last = now;
    }, { passive: false });
  })();
