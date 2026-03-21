
document.addEventListener('DOMContentLoaded', function() {
  const isCurtidasContext = window.location.pathname.startsWith('/servicos-curtidas');
  const isViewsContext = window.location.pathname.startsWith('/servicos-visualizacoes');

  function getBrowserSessionId() {
      let bid = '';
      try { bid = localStorage.getItem('oppus_browser_id'); } catch(_) {}
      if (!bid) {
          bid = 'bid_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
          try { localStorage.setItem('oppus_browser_id', bid); } catch(_) {}
      }
      return bid;
  }
  try { getBrowserSessionId(); } catch(_) {}

  // Coupon State
  window.couponCode = '';
  window.couponDiscount = 0;
  (function(){
    function getUrlCoupon(){
      try {
        const p = new URLSearchParams(window.location.search);
        let code = String(p.get('cupom') || p.get('coupon') || '').trim();
        if (!code) {
          const m = String(window.location.pathname || '').match(/\/cupom=([^\/]+)/i);
          if (m && m[1]) code = decodeURIComponent(m[1]);
        }
        return String(code || '').trim().toUpperCase();
      } catch(_) { return ''; }
    }
    const pre = getUrlCoupon();
    if (pre) {
      try { sessionStorage.setItem('oppus_coupon_code', pre); } catch(_) {}
      const input = document.getElementById('couponInput');
      const msg = document.getElementById('couponMessage');
      if (input) input.value = pre;
      const applyNow = function(){
        const usernameEl = document.getElementById('usernameCheckoutInput');
        const instagram_username = usernameEl ? usernameEl.value.trim().replace(/^@+/, '') : '';
        fetch('/api/validate-coupon', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ code: pre, instagram_username })
        })
        .then(function(res){ return res.json(); })
        .then(function(data){
          if (data && data.valid) {
            window.couponCode = data.code;
            window.couponDiscount = data.discount || 0;
            if (msg) {
              const percent = Math.round((Number(data.discount||0)) * 100);
              msg.textContent = 'Cupom aplicado! (' + percent + '% OFF)';
              msg.style.color = '#22c55e';
              msg.style.display = 'block';
            }
            if (input) input.disabled = true;
            if (typeof updatePromosSummary === 'function') updatePromosSummary();
          } else {
            window.couponCode = '';
            window.couponDiscount = 0;
            if (msg) {
              msg.textContent = (data && data.error) ? data.error : 'Cupom inválido.';
              msg.style.color = '#ef4444';
              msg.style.display = 'block';
            }
            if (typeof updatePromosSummary === 'function') updatePromosSummary();
          }
        }).catch(function(){});
      };
      const ue = document.getElementById('usernameCheckoutInput');
      if (ue && ue.value && ue.value.trim()) {
        applyNow();
      } else if (ue) {
        let done = false;
        ue.addEventListener('change', function(){
          if (!done && ue.value && ue.value.trim()) { done = true; applyNow(); }
        });
        ue.addEventListener('blur', function(){
          if (!done && ue.value && ue.value.trim()) { done = true; applyNow(); }
        });
      } else {
        applyNow();
      }
    }
  })();

  const applyCouponBtn = document.getElementById('applyCouponBtn');
  if (applyCouponBtn) {
      applyCouponBtn.addEventListener('click', function() {
          const input = document.getElementById('couponInput');
          const msg = document.getElementById('couponMessage');
          if (!input || !msg) return;
          
          const code = input.value.trim().toUpperCase();
          if (!code) {
              msg.textContent = 'Digite um cupom.';
              msg.style.color = '#ef4444';
              msg.style.display = 'block';
              return;
          }
          
          // Validation Logic via API
          this.disabled = true;
          this.textContent = 'Verificando...';
          
          const usernameEl = document.getElementById('usernameCheckoutInput');
          const instagram_username = usernameEl ? usernameEl.value.trim().replace(/^@+/, '') : '';

          fetch('/api/validate-coupon', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ code, instagram_username })
          })
          .then(res => res.json())
          .then(data => {
              if (data.valid) {
                  window.couponCode = data.code;
                  window.couponDiscount = data.discount; // decimal, e.g. 0.10
                  
                  const percent = Math.round(data.discount * 100);
                  msg.textContent = `Cupom aplicado! (${percent}% OFF)`;
                  msg.style.color = '#22c55e';
                  msg.style.display = 'block';
                  
                  input.disabled = true;
                  this.disabled = true;
                  this.textContent = 'Aplicado';
              } else {
                  msg.textContent = data.error || 'Cupom inválido.';
                  msg.style.color = '#ef4444';
                  msg.style.display = 'block';
                  window.couponCode = '';
                  window.couponDiscount = 0;
                  
                  this.disabled = false;
                  this.textContent = 'Aplicar';
              }
              if (typeof updatePromosSummary === 'function') updatePromosSummary();
          })
          .catch(err => {
              console.error('Erro ao validar cupom:', err);
              msg.textContent = 'Erro ao validar cupom.';
              msg.style.color = '#ef4444';
              msg.style.display = 'block';
              
              this.disabled = false;
              this.textContent = 'Aplicar';
          });
      });
  }

  const tabelaSeguidores = {
    mistos: [
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
  };

  // Upsell: desconto de 25% em seguidores (mundiais, brasileiros, brasileiros reais) a partir de 500
  let isUpsellFollowers = false;
  try {
    const paramsUpsell = new URLSearchParams(window.location.search || '');
    const u = String(paramsUpsell.get('upsell_followers') || paramsUpsell.get('upsell') || '').toLowerCase();
    if (u === '1' || u === 'seguidores_25' || u === 'followers_25') {
      isUpsellFollowers = true;
    }
  } catch(_) {}
  try { window.isUpsellFollowers = isUpsellFollowers; } catch(_) {}


  if (isUpsellFollowers && !isCurtidasContext && !isViewsContext) {
    ['mistos', 'brasileiros', 'organicos'].forEach(function(tipoKey){
      const arr = tabelaSeguidores[tipoKey] || [];
      arr.forEach(function(item){
        if (Number(item.q) >= 500) {
          const cents = (typeof parsePrecoToCents === 'function') ? parsePrecoToCents(item.p) : 0;
          if (cents > 0) {
            const newCents = Math.round(cents * 0.75);
            if (typeof formatCentsToBRL === 'function') {
              item.p = formatCentsToBRL(newCents);
            }
          }
        }
      });
    });
  }

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
    brasileiros: tabelaSeguidores.brasileiros,
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

  const tabela = isViewsContext ? tabelaVisualizacoes : (isCurtidasContext ? tabelaCurtidas : tabelaSeguidores);

  const promoPricing = {
    likes: { old: 'R$ 49,90', price: 'R$ 9,90', discount: 80 },
    views: { old: 'R$ 89,90', price: 'R$ 19,90', discount: 78 },
    comments: { old: 'R$ 29,90', price: 'R$ 9,90', discount: 67 },
    warranty: { old: 'R$ 39,90', price: 'R$ 14,90', discount: 63 },
    warranty60: { old: 'R$ 39,90', price: 'R$ 9,90', discount: 75 },
  };
  try { window.promoPricing = promoPricing; } catch(_) {}

  let selectedPlatform = 'instagram';
  let basePriceCents = 0;
  let isInstagramVerified = false;
  let isInstagramPrivate = false;
  let warrantyMode = '30';
  try {
    const initialWarrantyLabel = (document.getElementById('warrantyModeLabel')?.textContent || '').toLowerCase();
    const initialWarrantyHighlight = (document.getElementById('warrantyHighlight')?.textContent || '').toLowerCase();
    if (initialWarrantyLabel.includes('vital') || initialWarrantyHighlight.includes('vital')) {
      warrantyMode = 'life';
    }
    window.warrantyMode = warrantyMode;
  } catch(_) {}

  let paymentPollInterval = null;
  let paymentEventSource = null;

  // --- UTM Tracking Persistence ---
  try {
    const p = new URLSearchParams(window.location.search);
    const utms = {};
    ['utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content', 'gclid', 'fbclid'].forEach(k => {
       const v = p.get(k);
       if(v) utms[k] = v;
    });
    if (Object.keys(utms).length > 0) {
        sessionStorage.setItem('oppus_utms', JSON.stringify(utms));
    }
  } catch(_) {}

  // Elementos UI Principais
  const tipoSelect = document.getElementById('tipoSelect');
  const qtdSelect = document.getElementById('quantidadeSelect');
  const tipoCards = document.getElementById('tipoCards');
  const planCards = document.getElementById('planCards');
  const perfilCard = document.getElementById('perfilCard');
  const grupoPedido = document.getElementById('grupoPedido'); // Pode não existir
  const orderInline = document.getElementById('orderBumpInline');
  const paymentCard = document.getElementById('paymentCard'); // Pode não existir
  const resumo = document.getElementById('resumo');
  const resTipo = document.getElementById('resTipo');
  const resQtd = document.getElementById('resQtd');
  const resPreco = document.getElementById('resPreco');
  const resTotalFinal = document.getElementById('resTotalFinal');
  const btnPedido = document.getElementById('realizarPedidoBtn');

  // Perfil UI
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

  // Inputs de contato
  const contactPhoneInput = document.getElementById('contactPhoneInput');
  const contactEmailInput = document.getElementById('contactEmailInput');

  // --- Helpers ---
  function parsePrecoToCents(precoStr) {
    if (!precoStr) return 0;
    const cleaned = precoStr.replace(/[^\d,]/g, '').replace(',', '.');
    const value = Math.round(parseFloat(cleaned) * 100);
    return isNaN(value) ? 0 : value;
  }

  function formatCentsToBRL(cents) {
    const valor = Math.max(0, Number(cents) || 0);
    const reais = Math.floor(valor / 100);
    const centavos = valor % 100;
    return `R$ ${reais.toLocaleString('pt-BR')},${String(centavos).padStart(2, '0')}`;
  }

  function onlyDigits(v) { return String(v || '').replace(/\D+/g, ''); }

  function maskBrPhone(v) {
    const s = onlyDigits(v).slice(0, 11);
    if (!s) return '';
    const ddd = s.slice(0, 2);
    const first = s.slice(2, 3);
    const mid = s.slice(3, 7);
    const end = s.slice(7, 11);
    let out = '';
    if (ddd.length < 2) {
      out = `(${ddd}`;
    } else {
      out = `(${ddd})`;
    }
    if (first) out += ` ${first}`;
    if (mid) out += mid;
    if (end) out += `-${end}`;
    return out;
  }

  function attachPhoneMask(input) {
    if (!input) return;
    input.addEventListener('input', () => { input.value = maskBrPhone(input.value); });
    input.addEventListener('keydown', (e) => {
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
    input.addEventListener('paste', (e) => {
      const txt = (e.clipboardData || window.clipboardData)?.getData('text');
      if (txt) { e.preventDefault(); input.value = maskBrPhone(txt); }
    });
  }

  function normalizeInstagramUsername(input) {
    let username = input.trim();
    if (username.includes('instagram.com/')) {
      const parts = username.split('instagram.com/');
      if (parts[1]) {
        username = parts[1].split(/[/?#]/)[0];
      }
    }
    username = username.replace(/^@/, '');
    username = username.replace(/[^a-zA-Z0-9_.]/g, '');
    return username;
  }

  function isValidInstagramUsername(username) {
    const regex = /^[a-zA-Z0-9._]{1,30}$/;
    return regex.test(username) && !username.startsWith('.') && !username.endsWith('.');
  }

  function getLabelForTipo(tipo) {
    if (isViewsContext) {
      const mapViews = {
        visualizacoes_reels: 'Visualizações Reels'
      };
      return mapViews[tipo] || tipo;
    }
    if (isCurtidasContext) {
        const map = {
          'mistos': 'Curtidas Mistas',
          'brasileiros': 'Curtidas Brasileiras',
          'organicos': 'Curtidas Brasileiras'
        };
        return map[tipo] || tipo;
    }
    const map = {
      'mistos': 'Seguidores Mistos',
      'brasileiros': 'Seguidores Brasileiros',
      'organicos': 'Seguidores Brasileiros Reais'
    };
    return map[tipo] || tipo;
  }

  function getUnitForTipo(tipo) {
    if (isViewsContext || tipo === 'visualizacoes_reels') return 'visualizações';
    return isCurtidasContext ? 'curtidas' : 'seguidores';
  }

  function isFollowersTipo(tipo) {
    return ['mistos', 'brasileiros', 'organicos'].includes(tipo);
  }

  function findPrice(tipo, qtd) {
    const arr = tabela[tipo] || [];
    const item = arr.find(i => Number(i.q) === Number(qtd));
    return item ? item.p : null;
  }

  // --- Stepper Logic (Checkout Reference) ---

  window.goToStep = function(step) {
    if (step === 2) {
      const activePlan = planCards ? planCards.querySelector('.service-card[data-role="plano"].active') : null;
      if (!activePlan) {
        alert('Por favor, selecione um pacote antes de prosseguir.');
        return;
      }
    }
    
    if (step === 3) {
      if (!isInstagramVerified) {
        alert('Por favor, verifique o perfil na etapa 2 antes de prosseguir.');
        if (window.goToStep) window.goToStep(2);
        return;
      }

      const email = contactEmailInput ? contactEmailInput.value.trim() : '';
      const phone = contactPhoneInput ? contactPhoneInput.value.trim() : '';
      const emailErrorMsg = document.getElementById('emailErrorMsg');

      if (!email || !email.includes('@')) {
        if (emailErrorMsg) emailErrorMsg.style.display = 'block';
        else showStatusMessageCheckout('Por favor, informe um email válido.', 'error');
        if (window.goToStep) window.goToStep(2);

        setTimeout(() => {
             if (contactEmailInput) {
                 contactEmailInput.focus();
                 contactEmailInput.scrollIntoView({ behavior: 'smooth', block: 'center' });
             }
        }, 300);
        return;
      } else {
        if (emailErrorMsg) emailErrorMsg.style.display = 'none';
      }

      if (!phone || phone.length < 10) {
        showStatusMessageCheckout('Por favor, informe um telefone válido.', 'error');
        if (window.goToStep) window.goToStep(2);

        setTimeout(() => {
             if (contactPhoneInput) {
                 contactPhoneInput.focus();
                 contactPhoneInput.scrollIntoView({ behavior: 'smooth', block: 'center' });
             }
        }, 300);
        return;
      }

      if (!isInstagramPrivate) {
        if (isCurtidasContext) {
          if (!curtidasSelectedPost || !curtidasSelectedPost.shortcode || curtidasSelectedPost.kind !== 'likes') {
            openPostModal('likes');
            return;
          }
        }
        if (isViewsContext) {
          if (!curtidasSelectedPost || !curtidasSelectedPost.shortcode || curtidasSelectedPost.kind !== 'views') {
            openPostModal('views');
            return;
          }
        }
      }
    }

    // UI Elements
    const step1Container = document.getElementById('step1Container');
    const step2Container = document.getElementById('perfilCard');
    const step3Container = document.getElementById('step3Container');

    // Stepper Indicators
    document.querySelectorAll('.step').forEach((el, idx) => {
        if (idx + 1 === step) el.classList.add('active');
        else if (idx + 1 < step) el.classList.add('completed');
        else el.classList.remove('active', 'completed');
    });

    // Visibility
    if (step === 1) {
        if (step1Container) step1Container.style.display = 'grid'; // or block/flex depending on css
        if (step2Container) step2Container.style.display = 'none';
        if (step3Container) step3Container.style.display = 'none';
        window.scrollTo({ top: 0, behavior: 'smooth' });
        
        // Remove hash when leaving checkout step
         if (window.location.hash === '#checkout') {
              const cleanUrl = window.location.pathname + window.location.search;
              history.replaceState(null, null, cleanUrl);
              // Dispatch events for GTM
              try { window.dispatchEvent(new Event('hashchange')); } catch(e){}
              try { window.dispatchEvent(new Event('popstate')); } catch(e){}
         }

    } else if (step === 2) {
        if (step1Container) step1Container.style.display = 'none';
        if (step2Container) step2Container.style.display = 'block';
        if (step3Container) step3Container.style.display = 'none';
        
        // Focus on username input
        if (usernameCheckoutInput && !usernameCheckoutInput.value) {
            setTimeout(() => usernameCheckoutInput.focus(), 100);
        }
        window.scrollTo({ top: 0, behavior: 'smooth' });

        // Remove hash when leaving checkout step
        if (window.location.hash === '#checkout') {
             const cleanUrl = window.location.pathname + window.location.search;
             history.replaceState(null, null, cleanUrl);
             // Dispatch events for GTM
             try { window.dispatchEvent(new Event('hashchange')); } catch(e){}
             try { window.dispatchEvent(new Event('popstate')); } catch(e){}
        }

    } else if (step === 3) {
        if (step1Container) step1Container.style.display = 'none';
        if (step2Container) step2Container.style.display = 'none';
        if (step3Container) step3Container.style.display = 'block';
        window.scrollTo({ top: 0, behavior: 'smooth' });
        try { updatePromosSummary(); } catch(_) {}

        if (isCurtidasContext || isViewsContext) {
          const srcContainer = document.getElementById('selectedPostPreview');
          const srcContent = document.getElementById('selectedPostPreviewContent');
          const dst = document.getElementById('step3PostPreview');
          const dstContent = document.getElementById('step3PostPreviewContent');
          const hasPreview = srcContainer && srcContent && srcContainer.style.display !== 'none' && srcContent.innerHTML.trim();
          if (dst && dstContent) {
            if (hasPreview && !isInstagramPrivate) {
              dst.style.display = 'block';
              dstContent.innerHTML = srcContent.innerHTML;
            } else {
              dst.style.display = 'none';
              dstContent.innerHTML = '';
            }
          }
        }

        // URL hash removed per request
        /*
        if (window.location.hash !== '#checkout') {
             history.pushState(null, null, '#checkout');
             // Dispatch explicit event for GTM as backup
             try { window.dispatchEvent(new Event('hashchange')); } catch(e){}
             try { window.dispatchEvent(new Event('popstate')); } catch(e){}
        }
        */
    }
  };

  // --- Renderização dos Cards ---

  function renderTipoCards() {
    if (!tipoCards) return;
    tipoCards.innerHTML = '';
    // Garantir visibilidade (pois vem oculto do HTML)
    tipoCards.style.display = 'grid';
    
    const tipos = Object.keys(tabela).filter(t => {
      if (t === 'seguidores_tiktok') return false;
      if (isCurtidasContext && t === 'brasileiros') return false;
      return true;
    });

    // Fallback de segurança: garantir que organicos esteja presente se disponível na tabela
    if (!isCurtidasContext && !isViewsContext && !tipos.includes('organicos') && tabela.organicos) {
       tipos.push('organicos');
    }
    
    tipos.forEach(tipo => {
      const card = document.createElement('div');
      card.className = 'service-card option-card';
      card.setAttribute('data-role', 'tipo'); // Alinhado com checkout
      card.setAttribute('data-tipo', tipo);
      
      const label = getLabelForTipo(tipo);
      // Layout idêntico ao checkout.js (centralizado)
      card.innerHTML = `<div class="card-content"><div class="card-title" style="text-align:center;">${label}</div></div>`;
      
      card.addEventListener('click', () => {
        // Atualizar select oculto
        if (tipoSelect) {
          tipoSelect.value = tipo;
          tipoSelect.dispatchEvent(new Event('change'));
        }
        // Atualizar UI visual
        const all = tipoCards.querySelectorAll('.option-card');
        all.forEach(c => c.classList.remove('active'));
        card.classList.add('active');
      });
      
      tipoCards.appendChild(card);
    });
  }

  function getAllowedQuantities(tipo) {
    const base = [50, 150, 300, 500, 700, 1000, 2000, 3000, 4000, 5000, 7500, 10000, 15000];
    if (tipo === 'mistos' || tipo === 'brasileiros' || tipo === 'organicos' || tipo === 'seguidores_tiktok') {
      if (isCurtidasContext) {
        // Para curtidas mantemos apenas a partir de 150
        return base.filter(function(q){ return q >= 150; }).slice(0, 6);
      }
      return base;
    }
    return base;
  }

  const quantityBadges = {
    50: 'PACOTE TESTE',
    150: 'PACOTE INICIAL',
    500: 'PACOTE BÁSICO',
    1000: 'MAIS PEDIDO',
    3000: 'EXCLUSIVO',
    5000: 'VIP',
    10000: 'ELITE'
  };

  function renderPlanCards(tipo) {
    if (!planCards) return;
    planCards.innerHTML = '';
    // Garantir visibilidade
    planCards.style.display = '';

    let arr = tabela[tipo] || [];
    const unit = getUnitForTipo(tipo);
    
    if (isFollowersTipo(tipo)) {
      const allowed = getAllowedQuantities(tipo);
      if (isCurtidasContext) {
        arr = arr.filter(x => quantityBadges.hasOwnProperty(Number(x.q)));
      } else {
        arr = arr
          .filter(x => allowed.includes(Number(x.q)))
          .filter(x => quantityBadges.hasOwnProperty(Number(x.q)));
      }
    }

    if (isViewsContext && tipo === 'visualizacoes_reels') {
      const allowedViews = [1000, 5000, 25000, 100000, 200000, 500000];
      arr = arr.filter(x => allowedViews.includes(Number(x.q)));
    }
    
    arr.forEach(item => {
      const card = document.createElement('div');
      card.className = 'service-card plan-card';
      card.setAttribute('data-role', 'plano');
      card.setAttribute('data-qtd', item.q);
      card.setAttribute('data-preco', item.p);
      
      // Cálculo de preço "antigo" (estética checkout)
      const baseText = String(item.p);
      const baseStr = baseText.replace(/[^0-9,\.]/g, '');
      let base = 0;
      try { base = parseFloat(baseStr.replace('.', '').replace(',', '.')); } catch(_) {}
      const inc = base * 1.15;
      const ceilInt = Math.ceil(inc);
      const increasedRounded = (ceilInt - 0.10);
      const increasedText = `R$ ${increasedRounded.toFixed(2).replace('.', ',')}`;

      const qNum = Number(item.q);
      let badgeHtml = '';
      let badgeText = '';

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

      if (!badgeText && isFollowersTipo(tipo) && quantityBadges[qNum]) {
        badgeText = quantityBadges[qNum];
      }

      if (badgeText) {
        badgeHtml = `<div class="plan-badge">${badgeText}</div>`;
      }

      const qtyFormatted = qNum.toLocaleString('pt-BR');
      card.innerHTML = `${badgeHtml}<div class="card-content"><div class="card-title">${qtyFormatted} ${unit}</div><div class="card-desc"><span class="price-old">${increasedText}</span> <span class="price-new">${baseText}</span></div></div>`;
      
      card.addEventListener('click', () => {
        // Atualizar estado
        const baseText = item.p;
        
        // Atualizar select oculto
        const opt = Array.from(qtdSelect.options).find(o => o.value === String(item.q));
        if (opt) opt.selected = true;
        
        // Atualizar resumo
        if (resTipo) resTipo.textContent = getLabelForTipo(tipo);
        if (resQtd) resQtd.textContent = `${item.q} ${unit}`;
        if (resPreco) resPreco.textContent = baseText;
        try { basePriceCents = parsePrecoToCents(baseText); } catch(_) { basePriceCents = 0; }
        
        // Update Order Bump e Promos
        updateOrderBump(tipo, Number(item.q));
        updatePromosSummary();
        
        // Marcar ativo
        const cards = planCards.querySelectorAll('.service-card[data-role="plano"]');
        cards.forEach(c => c.classList.toggle('active', c === card));
        
        // Ir para Step 2
        if (window.goToStep) window.goToStep(2);
      });
      planCards.appendChild(card);
    });
  }

  function getTipoDescription(tipo) {
    let html = '';
    switch (tipo) {
      case 'visualizacoes_reels':
        html = `
          <p>Pacotes de visualizações reais para impulsionar o alcance dos seus vídeos e Reels. Ideal para quem quer ganhar mais entrega, engajamento e prova social em conteúdos estratégicos.</p>
          <ul>
            <li>🚀 <strong>Mais alcance:</strong> aumenta as visualizações dos seus Reels de forma rápida.</li>
            <li>🎯 <strong>Foco em resultados:</strong> pensado para ajudar vídeos a performarem melhor no algoritmo.</li>
            <li>✅ <strong>Entrega segura:</strong> serviço estável, com acompanhamento e suporte.</li>
          </ul>
        `;
        break;
      case 'mistos':
        html = isCurtidasContext ? `
          <p>Curtidas com entrega rápida e estável para impulsionar suas publicações.</p>
          <ul>
            <li>✅ 100% seguro e confidencial, sem precisar da sua senha.</li>
            <li>🌍 Alcance global para melhorar a prova social do post.</li>
            <li>📈 Ideal para dar força inicial em conteúdos estratégicos.</li>
          </ul>
        ` : `
          <p>Perfis variados com entrega rápida e estável, com seguidores reais de vários países.</p>
          <ul>
            <li>✅ 100% seguro e confidencial, sem precisar da sua senha.</li>
            <li>🌍 Seguidores reais de vários países para crescer sua base.</li>
            <li>🛠 Ferramenta de reposição de seguidores: não perca nenhum seguidor.</li>
          </ul>
        `;
        break;
      case 'brasileiros':
        html = `
          <p>Base nacional com nomes locais e seguidores brasileiros.</p>
          <ul>
            <li>✅ 100% seguro e confidencial, sem precisar da sua senha.</li>
            <li>🇧🇷 Foco total no público brasileiro e mais credibilidade.</li>
            <li>🛠 Ferramenta de reposição de seguidores: não perca nenhum seguidor.</li>
          </ul>
        `;
        break;
      case 'organicos':
        html = isCurtidasContext ? `
          <p>Curtidas focadas no público brasileiro para impulsionar suas publicações.</p>
          <ul>
            <li>✅ 100% seguro e confidencial, sem precisar da sua senha.</li>
            <li>🇧🇷 Foco no Brasil para reforçar prova social no seu público.</li>
            <li>📈 Ideal para posts que você quer destacar.</li>
          </ul>
        ` : `
          <p>Brasileiros e reais: perfis ativos e selecionados, com maior credibilidade.</p>
          <ul>
            <li>✅ 100% seguro e confidencial, sem precisar da sua senha.</li>
            <li>✨ Perfis mais qualificados para reforçar autoridade do perfil.</li>
            <li>📉 Serviço com baixa queda de seguidores.</li>
          </ul>
        `;
        break;
      default:
        return '';
    }

    if (isCurtidasContext) { return html.replace(/seguidores/g, 'curtidas').replace(/Seguidores/g, 'Curtidas'); }
    return html;
  }

  function renderTipoDescription(tipo) {
    const descCard = document.getElementById('tipoDescCard');
    const titleEl = document.getElementById('tipoDescTitle');
    const contentEl = document.getElementById('tipoDescContent');
    if (!descCard || !titleEl || !contentEl) return;

    titleEl.textContent = 'Descrição do serviço';
    contentEl.innerHTML = getTipoDescription(tipo);
    descCard.style.display = 'block';
  }

  // --- Lógica de Promoções (Checkout Reference) ---

  function renderPromoPrices() {
    const blocks = document.querySelectorAll('.promo-prices');
    blocks.forEach(b => {
      const key = b.getAttribute('data-promo');
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

  function applyWarrantyMode() {
    const isLife = warrantyMode === 'life';
    const wLabel = document.getElementById('warrantyModeLabel');
    const wHighlight = document.getElementById('warrantyHighlight');
    const wOld = document.getElementById('warrantyOldPrice');
    const wNew = document.getElementById('warrantyNewPrice');
    const wDisc = document.getElementById('warrantyDiscount');

    if (wLabel) wLabel.textContent = isLife ? 'Vitalícia' : '30 dias';
    if (wHighlight) wHighlight.textContent = isLife ? 'GARANTIA VITALÍCIA' : '+ 30 DIAS DE REPOSIÇÃO';
    if (wOld) wOld.textContent = 'R$ 39,90';
    if (wNew) wNew.textContent = 'R$ 9,90';
    if (wDisc) wDisc.textContent = '75% OFF';
    updatePromosSummary();
  }

  function stepWarranty(delta) {
    const next = (warrantyMode === '30' && delta > 0) ? 'life' : (warrantyMode === 'life' && delta < 0) ? '30' : warrantyMode;
    if (next !== warrantyMode) { 
        warrantyMode = next; 
        try { window.warrantyMode = warrantyMode; } catch(_) {} 
        applyWarrantyMode(); 
    }
  }

  const wDec = document.getElementById('warrantyModeDec');
  const wInc = document.getElementById('warrantyModeInc');
  if (wDec) wDec.addEventListener('click', () => stepWarranty(-1));
  if (wInc) wInc.addEventListener('click', () => stepWarranty(1));

  function updateWarrantyVisibility(tipo) {
    const warrantyItem = document.querySelector('.promo-item.warranty60');
    if (!warrantyItem) return;
    
    // Mostrar apenas para seguidores mistos (mundiais) e brasileiros
    if (tipo === 'mistos' || tipo === 'brasileiros') {
        warrantyItem.style.display = '';
    } else {
        warrantyItem.style.display = 'none';
        const cb = document.getElementById('promoWarranty60');
        if (cb && cb.checked) {
             cb.checked = false;
             updatePromosSummary();
        }
    }
  }

  function updateOrderBump(tipo, baseQtd) {
    updateWarrantyVisibility(tipo);
    if (!orderInline) return;
    const unit = getUnitForTipo(tipo);
    const labelSpan = document.getElementById('orderBumpText');
    const checkbox = document.getElementById('orderBumpCheckboxInline');
    const upgradePrices = document.querySelector('.promo-prices[data-promo="upgrade"]');
    const upOld = upgradePrices ? upgradePrices.querySelector('.old-price') : null;
    const upNew = upgradePrices ? upgradePrices.querySelector('.new-price') : null;
    const upDisc = upgradePrices ? upgradePrices.querySelector('.discount-badge') : null;
    const upHighlight = document.getElementById('orderBumpHighlight');

    // Upgrades específicos para visualizações de Reels
    if (tipo === 'visualizacoes_reels' && baseQtd) {
      orderInline.style.display = 'block';
      if (checkbox) checkbox.checked = false;

      const upsellViewsTargets = {
        1000: 2500,
        5000: 10000,
        25000: 50000,
        100000: 150000,
        200000: 250000,
        500000: 1000000
      };

      const targetQtdViews = upsellViewsTargets[Number(baseQtd)];
      if (!targetQtdViews) {
        if (labelSpan) labelSpan.textContent = 'Nenhum upgrade disponível para este pacote.';
        if (upOld) upOld.textContent = '—';
        if (upNew) upNew.textContent = '—';
        if (upDisc) upDisc.textContent = 'OFERTA';
        return;
      }

      const basePriceViews = findPrice(tipo, baseQtd);
      const targetPriceViews = findPrice(tipo, targetQtdViews);

      if (!basePriceViews || !targetPriceViews) {
        if (labelSpan) labelSpan.textContent = 'Nenhum upgrade disponível para este pacote.';
        if (upOld) upOld.textContent = '—';
        if (upNew) upNew.textContent = '—';
        if (upDisc) upDisc.textContent = 'OFERTA';
        return;
      }

      const diffCentsViews = parsePrecoToCents(targetPriceViews) - parsePrecoToCents(basePriceViews);
      const addQtdViews = targetQtdViews - baseQtd;
      const diffStrViews = formatCentsToBRL(diffCentsViews);

      if (labelSpan) labelSpan.textContent = `Por mais ${diffStrViews}, adicione ${addQtdViews} ${unit} e atualize para ${targetQtdViews}.`;
      if (upHighlight) upHighlight.textContent = `+ ${addQtdViews} ${unit}`;
      if (upOld) upOld.textContent = targetPriceViews || '—';
      if (upNew) upNew.textContent = diffStrViews;
      if (upDisc) {
        const targetCentsViews = parsePrecoToCents(targetPriceViews);
        const pctViews = targetCentsViews ? Math.round(((targetCentsViews - diffCentsViews) / targetCentsViews) * 100) : 0;
        upDisc.textContent = `${pctViews}% OFF`;
      }
      return;
    }

    if (!isFollowersTipo(tipo) || !baseQtd) { orderInline.style.display = 'none'; return; }
    orderInline.style.display = 'block';
    if (checkbox) checkbox.checked = false;

    // Promos específicas: 1000 -> 2000 com extras para brasileiros/organicos
    if ((tipo === 'brasileiros' || tipo === 'organicos') && Number(baseQtd) === 1000) {
      const targetQtd = 2000;
      const basePrice = findPrice(tipo, 1000);
      const targetPrice = findPrice(tipo, 2000);
      const diffCents = parsePrecoToCents(targetPrice) - parsePrecoToCents(basePrice);
      const diffStr = formatCentsToBRL(diffCents);
      if (labelSpan) labelSpan.textContent = `Por mais ${diffStr}, atualize para ${targetQtd} ${unit}.`;
      if (upHighlight) upHighlight.textContent = `+ ${targetQtd - 1000} ${unit}`;
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
    const upsellTargets = { 
      150: 300, 300: 500, 500: 700, 700: 1000, 
      1000: 2000, 1200: 2000, 2000: 3000, 3000: 4000, 4000: 5000, 
      5000: 7500, 7500: 10000, 10000: 15000 
    };
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
    if (labelSpan) labelSpan.textContent = `Por mais ${diffStr}, adicione ${addQtd} ${unit} e atualize para ${targetQtd}.`;
    if (upHighlight) upHighlight.textContent = `+ ${addQtd} ${unit}`;
    if (upOld) upOld.textContent = targetPrice || '—';
    if (upNew) upNew.textContent = diffStr;
    if (upDisc) {
      const targetCents = parsePrecoToCents(targetPrice);
      const pct = targetCents ? Math.round(((targetCents - diffCents) / targetCents) * 100) : 0;
      upDisc.textContent = `${pct}% OFF`;
    }
  }

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
  function updateViewsPrice(q) {
    const entry = viewsTable.find(e => e.q === q);
    const newEl = viewsPrices ? viewsPrices.querySelector('.new-price') : null;
    const oldEl = viewsPrices ? viewsPrices.querySelector('.old-price') : null;
    if (newEl && entry) newEl.textContent = entry.price;
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

  const commentsQtyEl = document.getElementById('commentsQty');
  const commentsDec = document.getElementById('commentsDec');
  const commentsInc = document.getElementById('commentsInc');
  const commentsPrices = document.querySelector('.promo-prices[data-promo="comments"]');

  function updateCommentsPrice(q) {
    const newEl = commentsPrices ? commentsPrices.querySelector('.new-price') : null;
    const oldEl = commentsPrices ? commentsPrices.querySelector('.old-price') : null;
    
    // Formatação BRL direta com toFixed(2)
    const format = (cents) => {
        const val = cents / 100;
        return `R$ ${val.toFixed(2).replace('.', ',')}`;
    };

    if (newEl) newEl.textContent = format(q * 150); // q * 1.50 * 100
    if (oldEl) { const oldCents = (q * 150) * 1.7; oldEl.textContent = format(oldCents); }
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
        const priceCents = qty * 150; // R$ 1,50 (150 cents)
        promos.push({ key: 'comments', qty, label: `Comentários (${qty})`, priceCents });
      }
      if (warrantyChecked) {
        const mode = (typeof window.warrantyMode === 'string') ? window.warrantyMode : '30';
        let priceStr = (document.getElementById('warrantyNewPrice')?.textContent || '').trim();
        if (!priceStr) priceStr = promoPricing.warranty60?.price || 'R$ 9,90';
        const label = (mode === 'life') ? 'Garantia vitalícia' : '+30 dias de reposição';
        promos.push({ key: (mode === 'life') ? 'warranty60' : 'warranty30', qty: 1, label, priceCents: parsePrecoToCents(priceStr) });
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

  function updatePromosSummary() {
    showResumoIfAllowed();
    
    // Atualiza header de quantidade (Bug fix)
    const headerQty = document.getElementById('headerSelectedQty');
    if (headerQty && resQtd && resQtd.textContent) {
      headerQty.textContent = resQtd.textContent;
    }
    
    try { updateReviewMath(); } catch(_) {}

    let baseCents = basePriceCents || 0;
    
    // Calcula preço base original (com margem para dar desconto)
    // No renderPlanCards usamos base * 1.15. Vamos recalcular.
    const baseVal = baseCents / 100;
    const inc = baseVal * 1.15;
    const ceilInt = Math.ceil(inc);
    const increasedRounded = (ceilInt - 0.10);
    let baseOriginalCents = Math.round(increasedRounded * 100);

    const promos = getSelectedPromos();
    
    // Renderiza lista de order bumps
    const resPromosContainer = document.getElementById('resPromosContainer');
    const resPromos = document.getElementById('resPromos');
    if (resPromos && resPromosContainer) {
        if (promos.length > 0) {
            resPromosContainer.style.display = 'block';
            
            // Header "Promoções selecionadas:"
            let html = '<div style="font-weight:600; margin-bottom:-4px; padding-bottom:0; color:var(--text-primary); line-height:1.2; margin-top:0.5rem;">Promoções selecionadas:</div>';
            
            html += promos.map((p, index) => {
                // Tenta achar preço original do promo
                let oldPriceCents = 0;
                if (p.key === 'upgrade') {
                    // Tenta pegar do DOM
                    const upOld = document.querySelector('.promo-prices[data-promo="upgrade"] .old-price');
                    if (upOld) oldPriceCents = parsePrecoToCents(upOld.textContent);
                    else oldPriceCents = p.priceCents * 1.5; 
                } else if (p.key === 'comments') {
                   // Comments old = current * 1.7
                   oldPriceCents = p.priceCents * 1.7;
                } else {
                   // Likes, Views, Warranty
                   const conf = promoPricing[p.key === 'warranty30' ? 'warranty' : (p.key === 'warranty_lifetime' ? 'warranty' : p.key)];
                   if (conf) oldPriceCents = parsePrecoToCents(conf.old);
                   else if (p.key === 'warranty_lifetime') oldPriceCents = 12990; // R$ 129,90
                   else if (p.key === 'warranty30') oldPriceCents = 3990; // R$ 39,90
                }
                // Adiciona ao total original
                baseOriginalCents += (oldPriceCents || p.priceCents);
                
                const marginTop = index === 0 ? '0' : '0.1rem';
                return `
                <div class="resumo-row" style="margin-top:${marginTop}; margin-bottom:0.1rem; line-height:1.4; display: flex; justify-content: space-between; align-items: center;">
                    <span>• ${p.label}</span>
                    <span>${formatCentsToBRL(p.priceCents)}</span>
                </div>`;
            }).join('');
            
            resPromos.innerHTML = html;
        } else {
            resPromosContainer.style.display = 'none';
            resPromos.innerHTML = '';
        }
    }

    const promosTotal = calcPromosTotalCents(promos);
    let totalCents = Math.max(0, Number(baseCents) + promosTotal);

    // Apply Coupon (Display)
    if (window.couponDiscount && window.couponDiscount > 0) {
        // Recalculate based on total
        const discountVal = Math.round(totalCents * window.couponDiscount);
        totalCents -= discountVal;
    }
    
    // Atualiza Total Final com Desconto
    if (resTotalFinal) {
        const totalOriginal = baseOriginalCents; // Soma de todos os originais
        const totalCurrent = totalCents;
        
        let discountPct = 0;
        if (totalOriginal > totalCurrent) {
            discountPct = Math.round(((totalOriginal - totalCurrent) / totalOriginal) * 100);
        }
        
        // HTML Rico
        const isMobile = window.innerWidth <= 640;
        const totalOriginalBrl = formatCentsToBRL(totalOriginal);
        const totalCurrentBrl = formatCentsToBRL(totalCurrent);
        
        if (isMobile) {
            // Mobile: Alinhado à esquerda, em duas linhas
            resTotalFinal.innerHTML = `
                <div class="promo-prices" style="flex-direction: column; align-items: flex-start; gap: 0;">
                    <div style="display:flex; gap: 0.5rem; align-items: center;">
                        <span class="old-price" style="text-decoration: line-through; color: #9ca3af;">${totalOriginalBrl}</span>
                        <span class="discount-badge">${discountPct}% OFF</span>
                    </div>
                    <span class="new-price">${totalCurrentBrl}</span>
                </div>
            `;
        } else {
            // Desktop: Layout original (uma linha, flex-end)
            resTotalFinal.innerHTML = `
                <div class="promo-prices" style="justify-content: flex-end; display: flex; align-items: center; gap: 0.5rem;">
                    <span class="old-price">${totalOriginalBrl}</span>
                    <span class="discount-badge">${discountPct}% OFF</span>
                    <span class="new-price">${totalCurrentBrl}</span>
                </div>
            `;
        }
    }
  }

  // --- Funções de Post Select Modal ---

  function getPostModalRefs() {
    return {
      postModal: document.getElementById('postSelectModal'),
      postModalGrid: document.getElementById('postModalGrid'),
      postModalTitle: document.getElementById('postModalTitle'),
      postModalClose: document.getElementById('postModalClose'),
    };
  }

  function ensureSpinnerCSS() {
    if (document.getElementById('oppusSpinnerStyles')) return;
    const style = document.createElement('style');
    style.id = 'oppusSpinnerStyles';
    style.textContent = "@keyframes oppusSpin{from{transform:rotate(0deg)}to{transform:rotate(360deg)}} .oppus-spinner{width:32px;height:32px;border:4px solid rgba(255,255,255,0.25);border-top-color:#7c3aed;border-radius:50%;animation:oppusSpin 1s linear infinite} .oppus-spinner-wrap{grid-column:1/-1;display:flex;justify-content:center;align-items:center;gap:8px;padding:24px;color:var(--text-secondary)}";
    document.head.appendChild(style);
  }

  function spinnerHTML() { ensureSpinnerCSS(); return '<div class="oppus-spinner-wrap"><div class="oppus-spinner"></div><span>Carregando...</span></div>'; }

  let cachedPosts = null;
  let cachedPostsUser = '';
  let postModalOpenLock = false;
  let suppressOpenPostModalOnce = false;

  function openPostModal(kind) {
    if (postModalOpenLock) return;
    if (isInstagramPrivate && ((isCurtidasContext && kind === 'likes') || (isViewsContext && kind === 'views'))) return;
    postModalOpenLock = true;
    setTimeout(function(){ postModalOpenLock = false; }, 600);
    const refs = getPostModalRefs();
    if (!refs.postModal || !refs.postModalGrid) return;
    
    const user = (checkoutProfileUsername && checkoutProfileUsername.textContent && checkoutProfileUsername.textContent.trim()) || '';
    if (!user) {
        showStatusMessageCheckout('Valide seu perfil primeiro.', 'error');
        return;
    }

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

    refs.postModalGrid.innerHTML = spinnerHTML();

    const renderFrom = function(arr) {
      const items = (Array.isArray(arr) ? arr : []).filter(p => {
        // Relax filter for views to allow any video-like content
        if (kind === 'views') {
             const isVid = !!p.isVideo || (String(p.typename||'').toLowerCase().includes('video') || String(p.typename||'').toLowerCase().includes('clip'));
             // Fallback: se não tiver isVideo mas tiver media_type == 2 (GraphVideo)
             const isMediaTypeVideo = (p.media_type === 2);
             return isVid || isMediaTypeVideo;
        }
        return true;
      }).slice(0, 8); // Checkout uses 8

      let headerHtml = '';
      if (isCurtidasContext && kind === 'likes') {
        headerHtml = '<div style="grid-column:1/-1; text-align:center; padding:0.5rem 0 1rem; font-weight:600; color:var(--text-primary);">Selecione o post que deseja receber as curtidas</div>';
      } else if (isViewsContext && kind === 'views') {
        headerHtml = '<div style="grid-column:1/-1; text-align:center; padding:0.5rem 0 1rem; font-weight:600; color:var(--text-primary);">Selecione o Reels que deseja receber as visualizações</div>';
      }

      const html = items.map(function(p){
        const dsrc = p.displayUrl ? ('/image-proxy?url=' + encodeURIComponent(p.displayUrl)) : null;
        const vsrc = p.videoUrl ? ('/image-proxy?url=' + encodeURIComponent(p.videoUrl)) : null;
        const isVid = p.isVideo || (p.media_type === 2);
        
        const media = (dsrc)
          ? ('<div class="media-frame"><img src="'+dsrc+'" loading="lazy" decoding="async"/></div>')
          : (isVid && vsrc
            ? ('<div class="media-frame"><video data-src="'+vsrc+'" muted playsinline preload="none"></video></div>')
            : ('<div class="media-frame"><iframe src="https://www.instagram.com/p/'+p.shortcode+'/embed" loading="lazy" allowtransparency="true" allow="encrypted-media; picture-in-picture" scrolling="no"></iframe></div>'));
        return '<div class="service-card"><div class="card-content pick-post-card" data-kind="'+kind+'" data-shortcode="'+p.shortcode+'">'+media+'<div class="inline-msg" style="margin-top:6px">'+(p.takenAt? new Date(Number(p.takenAt)*1000).toLocaleString('pt-BR') : '-')+'</div><div style="margin-top:8px;display:flex;justify-content:center;align-items:center;"><button type="button" class="continue-button select-post-btn" style="width:100%; text-align:center;" data-shortcode="'+p.shortcode+'" data-kind="'+kind+'">Selecionar</button></div></div></div>';
      }).join('');
      
      if (!html) {
          const manualHtml = `
            <div style="grid-column:1/-1; text-align:center; padding: 1rem;">
                <p style="margin-bottom:0.5rem; color:var(--text-secondary);">Não encontramos posts recentes compatíveis automaticamente.</p>
                <div style="display:flex; gap:0.5rem; max-width:400px; margin:0 auto;">
                    <input type="text" id="manualPostLinkInput" placeholder="${kind === 'views' ? 'Cole o link do Reels/Vídeo aqui...' : 'Cole o link do post aqui...'}" style="flex:1; padding:0.6rem; border-radius:6px; border:1px solid var(--border-color); background:var(--bg-primary); color:var(--text-primary);" />
                    <button type="button" id="manualPostLinkBtn" class="continue-button" style="padding:0.6rem 1rem;">Usar Link</button>
                </div>
                <div id="manualLinkMsg" style="margin-top:0.5rem; font-size:0.9rem;"></div>
            </div>
          `;
          refs.postModalGrid.innerHTML = headerHtml + manualHtml;
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
                            if (typeof updateSelectedPostPreview === 'function') {
                                try { updateSelectedPostPreview(kind, sc); } catch(_) {}
                            }
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
          refs.postModalGrid.innerHTML = headerHtml + html;
      }

      const highlightSelected = function(kind, sc){ try{ const cards = Array.from(refs.postModalGrid.querySelectorAll('.card-content')); cards.forEach(function(c){ c.classList.remove('selected-mark'); }); const target = refs.postModalGrid.querySelector('.card-content[data-shortcode="'+sc+'"]'); if (target) target.classList.add('selected-mark'); }catch(_){} };
      
      Array.from(refs.postModalGrid.querySelectorAll('.select-post-btn')).forEach(function(btn){
        btn.addEventListener('click', function(){
          const sc = this.getAttribute('data-shortcode');
          const k = this.getAttribute('data-kind');
          const user2 = (checkoutProfileUsername && checkoutProfileUsername.textContent && checkoutProfileUsername.textContent.trim()) || '';
          fetch('/api/instagram/select-post-for', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ username: user2, shortcode: sc, kind: k }) })
            .then(r=>r.json())
            .then(function(){ 
              highlightSelected(k, sc); 
              if (typeof updateSelectedPostPreview === 'function') {
                  try { updateSelectedPostPreview(k, sc); } catch(_) {}
                  try { 
                      const refs2 = getPostModalRefs();
                      if (refs2.postModal) refs2.postModal.style.display = 'none';
                      document.body.style.overflow = '';
                  } catch(_) {}
              }
            });
        });
      });
      
      Array.from(refs.postModalGrid.querySelectorAll('.pick-post-card')).forEach(function(card){
        card.addEventListener('click', function(){
          const sc = this.getAttribute('data-shortcode');
          const k = this.getAttribute('data-kind');
          const user2 = (checkoutProfileUsername && checkoutProfileUsername.textContent && checkoutProfileUsername.textContent.trim()) || '';
          fetch('/api/instagram/select-post-for', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ username: user2, shortcode: sc, kind: k }) })
            .then(r=>r.json())
            .then(function(){ 
              highlightSelected(k, sc); 
              if (typeof updateSelectedPostPreview === 'function') {
                  try { updateSelectedPostPreview(k, sc); } catch(_) {}
                  try { 
                      const refs2 = getPostModalRefs();
                      if (refs2.postModal) refs2.postModal.style.display = 'none';
                      document.body.style.overflow = '';
                  } catch(_) {}
              }
            });
        });
      });
      
      try { fetch('/api/instagram/selected-for').then(r=>r.json()).then(function(d){ const obj = d && d.selectedFor ? d.selectedFor : {}; const cur = obj[kind]; if (cur && cur.shortcode) { highlightSelected(kind, cur.shortcode); } }); } catch(_) {}
    };

    const useCache = !!cachedPosts && cachedPostsUser === user;
    if (useCache) {
      renderFrom(cachedPosts);
    } else {
      const url = '/api/instagram/posts?username=' + encodeURIComponent(user);
      refs.postModalGrid.innerHTML = spinnerHTML();
      fetch(url).then(r=>r.json()).then(d=>{
        const arr = Array.isArray(d.posts) ? d.posts : [];
        cachedPosts = arr; cachedPostsUser = user;
        renderFrom(arr);
      }).catch(function(){
        renderFrom([]);
      });
    }
  }

  // --- Inicialização de Listeners de Promos e Modal ---

  function initPromoListeners() {
    const promoLikes = document.getElementById('promoLikes');
    const promoViews = document.getElementById('promoViews');
    const promoComments = document.getElementById('promoComments');
    
    if (promoLikes) promoLikes.addEventListener('change', function() { if (this.checked) openPostModal('likes'); updatePromosSummary(); });
    if (promoViews) promoViews.addEventListener('change', function() { if (this.checked) openPostModal('views'); updatePromosSummary(); });
    if (promoComments) promoComments.addEventListener('change', function() { if (this.checked) openPostModal('comments'); updatePromosSummary(); });

    // Step Controls - REMOVIDO PARA EVITAR CONFLITO COM LISTENERS DE TABELA
    // Os listeners de stepLikes, stepViews e stepComments já foram definidos anteriormente
    
    // Modal Close
    const refs = getPostModalRefs();
    if (refs.postModalClose) refs.postModalClose.addEventListener('click', () => { if(refs.postModal) refs.postModal.style.display = 'none'; });
    if (document.getElementById('postModalClose2')) document.getElementById('postModalClose2').addEventListener('click', () => { if(refs.postModal) refs.postModal.style.display = 'none'; });
    
    // Checkbox Order Bump
    const obCheck = document.getElementById('orderBumpCheckboxInline');
    if (obCheck) obCheck.addEventListener('change', updatePromosSummary);
    
    // Checkbox Warranty
    const wCheck = document.getElementById('promoWarranty60');
    if (wCheck) wCheck.addEventListener('change', updatePromosSummary);
  }

  // --- Lógica de Verificação de Perfil ---

  // --- Post Modal & Preview Logic ---

  let curtidasSelectedPost = null;

  function updateSelectedPostPreview(kind, sc) {
      const container = document.getElementById('selectedPostPreview');
      const slot = document.getElementById('selectedPostPreviewContent');
      if (!container || !slot) return;
      
      const arr = Array.isArray(cachedPosts) ? cachedPosts : [];
      let p = arr.find(x => x && x.shortcode === sc);
      
      if (!p) {
        if (!sc) {
          container.style.display = 'none';
          return;
        }
        const media = '<iframe src="https://www.instagram.com/p/'+sc+'/embed" allowtransparency="true" allow="encrypted-media; picture-in-picture" scrolling="no" style="width:100%;border-radius:12px;"></iframe>';
        slot.innerHTML = '<div style="background:var(--bg-secondary);border-radius:12px;padding:0.75rem;display:flex;flex-direction:column;gap:0.5rem;"><div style="width:100%;max-width:260px;margin:0 auto;">'+media+'</div></div>';
        container.style.display = 'block';
        curtidasSelectedPost = { kind, shortcode: sc };
        return;
      }

      const dsrc = p.displayUrl ? ('/image-proxy?url=' + encodeURIComponent(p.displayUrl)) : null;
      const vsrc = p.videoUrl ? ('/image-proxy?url=' + encodeURIComponent(p.videoUrl)) : null;
      const isVid = p.isVideo || (p.media_type === 2);
      
      let media = '';
      if (dsrc) {
        media = '<img src="'+dsrc+'" style="width:100%;height:auto;border-radius:12px;object-fit:cover;" loading="lazy" decoding="async"/>';
      } else if (isVid && vsrc) {
        media = '<video src="'+vsrc+'" style="width:100%;border-radius:12px;" muted playsinline preload="none"></video>';
      } else {
        media = '<iframe src="https://www.instagram.com/p/'+p.shortcode+'/embed" allowtransparency="true" allow="encrypted-media; picture-in-picture" scrolling="no" style="width:100%;border-radius:12px;"></iframe>';
      }
      
      const dateText = p.takenAt ? new Date(Number(p.takenAt) * 1000).toLocaleString('pt-BR') : '';
      let extra = '';
      if (dateText) extra = '<div style="font-size:0.8rem;color:var(--text-secondary);text-align:center;">'+dateText+'</div>';

      slot.innerHTML = '<div style="background:var(--bg-secondary);border-radius:12px;padding:0.75rem;display:flex;flex-direction:column;gap:0.5rem;"><div style="width:100%;max-width:260px;margin:0 auto;">'+media+'</div>'+extra+'</div>';
      container.style.display = 'block';
      curtidasSelectedPost = { kind, shortcode: p.shortcode };
  }

  async function checkInstagramProfileCheckout() {
    if (!usernameCheckoutInput) return;
    const rawInput = usernameCheckoutInput.value.trim();
    if (!rawInput) {
      showStatusMessageCheckout('Digite o usuário ou URL do Instagram.', 'error');
      return;
    }
    
    const username = normalizeInstagramUsername(rawInput);
    if (!isValidInstagramUsername(username)) {
      showStatusMessageCheckout('Nome de usuário inválido.', 'error');
      return;
    }
    if (username !== rawInput) usernameCheckoutInput.value = username;
    
    hideStatusMessageCheckout();
    const helpLink = document.getElementById('howToGetLinkContainer');
    if (helpLink) helpLink.style.display = 'none';

    clearProfilePreview();
    showLoadingCheckout();
    
    try {
      const params = new URLSearchParams(window.location.search);
      let utms = {
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

      // Merge with sessionStorage if empty
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

        if (checkoutProfileImage && profile.profilePicUrl) checkoutProfileImage.src = profile.profilePicUrl;
        if (checkoutProfileUsername) checkoutProfileUsername.textContent = profile.username || username;
        if (checkoutFollowersCount) checkoutFollowersCount.textContent = String(profile.followersCount || '-');
        if (checkoutFollowingCount) checkoutFollowingCount.textContent = String(profile.followingCount || '-');
        if (checkoutPostsCount) checkoutPostsCount.textContent = String(profile.postsCount || '-');
        
        if (profilePreview) profilePreview.style.display = 'block';
        
        // Show contact fields
        const contactArea = document.getElementById('contactFieldsArea');
        if (contactArea) {
            contactArea.style.display = 'block';
            // Scroll automático para a parte de digitar o email
            setTimeout(() => {
                const emailInput = document.getElementById('contactEmailInput');
                if (emailInput) {
                    emailInput.scrollIntoView({ behavior: 'smooth', block: 'center' });
                    // Opcional: focar no campo
                    // emailInput.focus(); 
                } else {
                    contactArea.scrollIntoView({ behavior: 'smooth', block: 'center' });
                }
            }, 300);
        }
        
        const revImg = document.getElementById('reviewProfileImage');
        const revUser = document.getElementById('reviewProfileUsername');
        const revFoll = document.getElementById('reviewProfileFollowers');
        if (revImg) revImg.src = profile.profilePicUrl || '';
        if (revUser) revUser.textContent = profile.username || username;
        if (revFoll) revFoll.textContent = String(profile.followersCount || '-');
        
        isInstagramVerified = true;
        try { isInstagramPrivate = !!(profile.isPrivate || profile.is_private); } catch(_) { isInstagramPrivate = false; }
        
        // Pré-carregar posts se vierem na verificação ou buscar em background
        if (profile.latestPosts && Array.isArray(profile.latestPosts) && profile.latestPosts.length > 0) {
            cachedPosts = profile.latestPosts;
            cachedPostsUser = profile.username || username;
        } else {
             // Tentar buscar em background para agilizar o modal
             try {
                // Verificar se já não estamos buscando para este usuário
                if (cachedPostsUser !== (profile.username || username)) {
                    const url = '/api/instagram/posts?username=' + encodeURIComponent(profile.username || username);
                    fetch(url).then(r=>r.json()).then(d=>{ 
                        if(d.posts && Array.isArray(d.posts)) {
                            cachedPosts = d.posts; 
                            cachedPostsUser = (profile.username || username); 
                        }
                    }).catch(function(){});
                }
             } catch(_) {}
        }

        // Após validar perfil, abrir o modal de seleção de post:
        // - Curtidas  -> seleção de post (likes)
        // - Visualizações -> seleção de Reels (views)
        if (isCurtidasContext || isViewsContext) {
          openPostModal(isCurtidasContext ? 'likes' : 'views');
        }
        
        updatePedidoButtonState();
        showResumoIfAllowed();
        updatePromosSummary();
        applyCheckoutFlow();
        showStatusMessageCheckout('Perfil verificado com sucesso.', 'success');
        
        try {
          const bid = getBrowserSessionId();
          fetch('/api/instagram/validet-track', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ username: profile.username || username, browserId: bid })
          }).catch(() => {});
        } catch (_) {}
        
      } else {
        const msg = (data && data.error) ? String(data.error) : '';
        if (/não\s+localizad|não\s+encontrad|inexist|username_invalid|user_not_found/i.test(msg)) {
          showStatusMessageCheckout('Usuário não encontrado. configra o nome digitado e tente novamente.', 'error');
        } else if (/erro\s+na\s+verifica[cç][aã]o\s+do\s+perfil/i.test(msg)) {
          showStatusMessageCheckout('Erro de usuário. configra o nome digitado e tente novamente.', 'error');
        } else {
          showStatusMessageCheckout(msg || 'Falha ao verificar perfil.', 'error');
        }
        const helpLink = document.getElementById('howToGetLinkContainer');
        if (helpLink) helpLink.style.display = 'block';
      }
    } catch (e) {
      hideLoadingCheckout();
      showStatusMessageCheckout('Erro ao conectar com o servidor.', 'error');
      const helpLink = document.getElementById('howToGetLinkContainer');
      if (helpLink) helpLink.style.display = 'block';
    }
  }

  // --- Funções Auxiliares UI ---

  function showStatusMessageCheckout(msg, type) {
    if (!statusCheckoutMessage) return;
    statusCheckoutMessage.textContent = msg;
    statusCheckoutMessage.className = 'status-message ' + (type === 'error' ? 'error' : 'success');
    statusCheckoutMessage.style.display = 'block';
  }

  function hideStatusMessageCheckout() {
    if (!statusCheckoutMessage) return;
    statusCheckoutMessage.style.display = 'none';
  }

  function showLoadingCheckout() {
    if (loadingCheckoutSpinner) loadingCheckoutSpinner.style.display = 'block';
  }

  function hideLoadingCheckout() {
    if (loadingCheckoutSpinner) loadingCheckoutSpinner.style.display = 'none';
  }

  function clearProfilePreview() {
    if (profilePreview) profilePreview.style.display = 'none';
    isInstagramVerified = false;
  }

  function updatePedidoButtonState() {
    if (btnPedido) btnPedido.disabled = false;
  }

  function showResumoIfAllowed() {
    const isFollowers = isFollowersTipo(tipoSelect.value);
    const allow = (!isFollowers) || !!isInstagramVerified;
    if (resumo) {
        resumo.hidden = !allow;
        resumo.style.display = allow ? 'block' : 'none';
    }
  }

  function updatePerfilVisibility() {
    // Controlled by goToStep
  }
  
  function updateWarrantyVisibility() {
    const tipo = tipoSelect.value;
    const inp = document.getElementById('promoWarranty60');
    if (!inp) return;
    const item = inp.closest('.promo-item');
    const show = (tipo === 'mistos' || tipo === 'brasileiros');
    if (item) item.style.display = show ? '' : 'none';
    if (!show && inp.checked) inp.checked = false;
    try { updatePromosSummary(); } catch(_) {}
  }

  function applyCheckoutFlow() {
    const tipo = tipoSelect.value;
    const isFollowers = isFollowersTipo(tipo);
    const verified = !!isInstagramVerified;
    
    // Controlled by goToStep. We only manage internal visibility of Step 3 elements here if needed.
    if (verified || !isFollowers) {
        if (orderInline) orderInline.style.display = 'block';
        if (grupoPedido) grupoPedido.style.display = 'block';
        if (paymentCard) paymentCard.style.display = 'block';
        
        const headerQty = document.getElementById('headerSelectedQty');
        if (headerQty && qtdSelect.value) {
            const unit = getUnitForTipo(tipo);
            headerQty.textContent = `+ ${qtdSelect.value} ${unit}`;
        }
        
        if (!isCurtidasContext && !isViewsContext) {
            updateReviewMath();
        }
    } else {
        if (orderInline) orderInline.style.display = 'none';
        if (grupoPedido) grupoPedido.style.display = 'none';
        if (paymentCard) paymentCard.style.display = 'none';
    }
  }

  function updateReviewMath() {
      const reviewSelectedQty = document.getElementById('reviewSelectedQty');
      const reviewTotalFollowers = document.getElementById('reviewTotalFollowers');
      const reviewProfileFollowers = document.getElementById('reviewProfileFollowers');
      const qtdSelect = document.getElementById('quantidadeSelect');
      
      try {
          // Get current followers
          const currentText = reviewProfileFollowers ? reviewProfileFollowers.textContent : '0';
          const current = (currentText === '-' || !currentText) ? 0 : parseInt(currentText.replace(/\D/g, '') || '0', 10);
          
          // Get selected quantity
          let selected = 0;
          if (qtdSelect && qtdSelect.value) {
             selected = parseInt(qtdSelect.value.replace(/\D/g, '') || '0', 10);
          }
          
          const total = current + selected;
          
          // Format numbers
          const fmt = (n) => n.toLocaleString('pt-BR');

          if (reviewSelectedQty) reviewSelectedQty.textContent = `+${fmt(selected)}`;
          if (reviewTotalFollowers) reviewTotalFollowers.textContent = fmt(total);
          
          // Ensure current is formatted too if it's a number
          if (reviewProfileFollowers && current > 0 && reviewProfileFollowers.textContent !== fmt(current)) {
              reviewProfileFollowers.textContent = fmt(current);
          }
      } catch (e) {
          console.error('Error updating review math:', e);
      }
  }

  // --- Funções de Pagamento (PIX) ---

  function markPaymentConfirmed() {
    const pixResultado = document.getElementById('pixResultado');
    try {
      if (pixResultado) {
        pixResultado.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;color:#22C55E;font-weight:700;font-size:1rem;"><span class="price-new">Pagamento confirmado</span></div>';
      }
    } catch(_) {}
    try { showStatusMessageCheckout('Pagamento confirmado. Exibindo resumo abaixo.', 'success'); } catch(_) {}
    try { showResumoIfAllowed(); } catch(_) {}
  }

  async function navigateToPedidoOrFallback(identifier, correlationID, chargeId) {
    try {
      try { await fetch('/session/mark-paid', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ identifier, correlationID }) }); } catch(_) {}
      const apiUrl = `/api/order?identifier=${encodeURIComponent(identifier)}&correlationID=${encodeURIComponent(correlationID)}&id=${encodeURIComponent(chargeId||'')}`;
      const extractProviderOid = function(orderObj){
        if (!orderObj || typeof orderObj !== 'object') return '';
        var o = orderObj;
        var oidF = (o && o.fama24h && o.fama24h.orderId) ? String(o.fama24h.orderId) : '';
        var oidFS = (o && o.fornecedor_social && o.fornecedor_social.orderId) ? String(o.fornecedor_social.orderId) : '';
        return oidF || oidFS || '';
      };
      let data = null;
      try {
        const resp = await fetch(apiUrl);
        data = await resp.json();
      } catch(_) {}
      let providerOid = data && data.order ? extractProviderOid(data.order) : '';
      if (!data || !data.order || !providerOid) {
        showStatusMessageCheckout('Pagamento recebido! Processando pedido...', 'success');
        let attempts = 0;
        const maxAttempts = 10;
        while (attempts < maxAttempts && !providerOid) {
          attempts++;
          try {
            const respLoop = await fetch(apiUrl);
            const dataLoop = await respLoop.json();
            if (dataLoop && dataLoop.order) {
              providerOid = extractProviderOid(dataLoop.order);
              if (providerOid) {
                try { localStorage.setItem('oppus_selected_oid', String(providerOid)); } catch(_) {}
                break;
              }
            }
          } catch(_) {}
          await new Promise(function(resolve){ setTimeout(resolve, 1500); });
        }
      }
      const finalOid = providerOid || (chargeId ? String(chargeId) : '');
      window.location.href = `/pedido?t=${encodeURIComponent(identifier)}&ref=${encodeURIComponent(correlationID||'')}&oid=${encodeURIComponent(finalOid||'')}`;
    } catch(_) {
        showStatusMessageCheckout('Pagamento confirmado! Verifique seu email.', 'success');
    }
  }

  async function criarPixWoovi() {
    if (btnPedido) {
        btnPedido.disabled = true;
        btnPedido.classList.add('loading');
    }
    
    // Ocultar elementos estáticos do PIX se existirem, para usar o render dinâmico
    const staticPixElements = ['pixQrcode', 'pixLoader', 'pixCopiaCola', 'copyPixBtn'];
    staticPixElements.forEach(id => {
        const el = document.getElementById(id);
        if (el) el.parentElement.style.display = 'none'; // Oculta o container pai desses elementos
    });
    // Garantir que pixResultado esteja visível e limpo
    const pixResultado = document.getElementById('pixResultado');
    if (pixResultado) {
        pixResultado.innerHTML = '';
        pixResultado.style.display = 'block';
        // Se o pai estava oculto (caso dos elementos estáticos estarem no mesmo container), reexibir o container principal
        const pixContainer = document.getElementById('pixContainer');
        if (pixContainer) {
            pixContainer.style.display = 'block'; // Ensure container is visible
            // Reexibir apenas o necessário
            Array.from(pixContainer.children).forEach(c => {
                if (c.id === 'pixResultado' || c.tagName === 'H4' || c.tagName === 'P') c.style.display = 'block';
                else if (staticPixElements.includes(c.id) || c.querySelector('#pixQrcode')) c.style.display = 'none';
            });
        }
    }

    try {
      const tipo = tipoSelect ? tipoSelect.value : 'mistos';
      const qtdSelectVal = qtdSelect ? qtdSelect.value : '0';
      const qtd = parseInt(qtdSelectVal, 10);
      const precoText = resPreco ? resPreco.textContent : '';
      const precoStr = precoText; 
      
      let baseCents = basePriceCents || 0;
      const promos = getSelectedPromos();
      const promosTotalCents = calcPromosTotalCents(promos);
      let totalCents = Math.max(0, Number(baseCents) + promosTotalCents);

      if (window.couponDiscount && window.couponDiscount > 0) {
        const discountVal = Math.round(totalCents * window.couponDiscount);
        totalCents -= discountVal;
      }

      const valueBRL = totalCents / 100;
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
      
      // Quantidade efetiva (considerando upgrade)
      const upgradeItem = promos.find(p => p.key === 'upgrade');
      // Se tiver upgrade, a quantidade base já foi dobrada visualmente? 
      // Não, no servicos-instagram.js o updateOrderBump apenas mostra o texto.
      // A lógica de quantidade real deve ser ajustada aqui.
      // Se houver upgrade, a quantidade entregue é maior, mas para o checkout (registro)
      // usamos a quantidade base + info de upgrade.
      const qtdEffective = qtd; 

      const correlationID = 'InstagramService_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
      
      // Phone
      const phoneInput = contactPhoneInput || document.getElementById('checkoutPhoneInput');
      const phoneValue = onlyDigits(phoneInput ? phoneInput.value : '');

      const emailValue = contactEmailInput ? contactEmailInput.value.trim() : '';
      
      // Username
      const usernamePreview = (checkoutProfileUsername && checkoutProfileUsername.textContent && checkoutProfileUsername.textContent.trim()) || '';
      const usernameInputRaw = (usernameCheckoutInput && usernameCheckoutInput.value && usernameCheckoutInput.value.trim()) || '';
      const usernameInputNorm = normalizeInstagramUsername(usernameInputRaw);
      const instagramUsernameFinal = usernamePreview || usernameInputNorm || '';

      if (!instagramUsernameFinal) {
        throw new Error('Nome de usuário do Instagram não identificado.');
      }

      const serviceCategory = isViewsContext ? 'visualizacoes' : (isCurtidasContext ? 'curtidas' : 'seguidores');

      const payload = {
        correlationID,
        value: totalCents,
        comment: 'Checkout OPPUS Instagram',
        customer: {
          name: 'Cliente Instagram',
          phone: phoneValue,
          email: emailValue
        },
        additionalInfo: [
          { key: 'tipo_servico', value: tipo },
          { key: 'categoria_servico', value: serviceCategory },
          { key: 'quantidade', value: String(qtdEffective) },
          { key: 'pacote', value: `${qtdEffective} ${getUnitForTipo(tipo)} - ${precoStr}` },
          { key: 'phone', value: phoneValue },
          { key: 'instagram_username', value: instagramUsernameFinal },
          { key: 'order_bumps_total', value: formatCentsToBRL(promosTotalCents) },
          { key: 'order_bumps', value: promos.map(p => `${p.key}:${p.qty ?? 1}`).join(';') },
          { key: 'cupom', value: window.couponCode || '' }
        ],
        profile_is_private: isInstagramPrivate
      };
      try {
        if (sckValue) payload.additionalInfo.push({ key: 'sck', value: sckValue });
      } catch (_) {}

      // Tentar pegar posts selecionados (simulado ou via cache/session se tivesse implementado full)
      // Aqui vamos apenas verificar se tem promos que precisam de posts
      // No código anterior do modal, não salvamos no backend. 
      // Se for necessário, deveríamos ter salvo. 
      // Assumindo que o modal apenas seleciona visualmente por enquanto ou falta implementar a persistência.
      // Vou manter simplificado como no checkout.js que busca de /api/instagram/selected-for
      
      try {
        const selResp = await fetch('/api/instagram/selected-for');
        if (selResp.ok) {
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
                if (pick && pick.shortcode) link = buildIgMediaLink(onlyKind, pick.shortcode);
              } catch (_) {}
            }
            if (link) payload.additionalInfo.push({ key: `orderbump_post_${onlyKind}`, value: link });
          } else {
            if (hasLikes && anyLink) payload.additionalInfo.push({ key: 'orderbump_post_likes', value: likesLink || anyLink });
            if (hasViews && anyLink) payload.additionalInfo.push({ key: 'orderbump_post_views', value: viewsLink || anyLink });
            if (hasComments && anyLink) payload.additionalInfo.push({ key: 'orderbump_post_comments', value: commentsLink || anyLink });
          }

          // Para serviços principais de curtidas/visualizações, salvar também o post selecionado
          if (serviceCategory === 'curtidas' && likesLink) {
            payload.additionalInfo.push({ key: 'post_link', value: likesLink });
          }
          if (serviceCategory === 'visualizacoes' && viewsLink) {
            payload.additionalInfo.push({ key: 'post_link', value: viewsLink });
          }
        }
      } catch(_) {}

      const resp = await fetch('/api/woovi/charge', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });

      const data = await resp.json();
      if (!resp.ok) {
        throw new Error(data?.details?.message || 'Falha ao criar cobrança');
      }

      // Renderização do PIX
      const charge = data?.charge || data || {};
      const pix = charge?.paymentMethods?.pix || charge?.pix || {};
      const brCode = pix?.brCode || charge?.brCode || data?.brCode || '';
      const qrImage = pix?.qrCodeImage || charge?.qrCodeImage || data?.qrCodeImage || '';

      const copyButtonId = 'copyPixBtnDynamic';
      const inputId = 'pixBrCodeInputDynamic';

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

      const textColor = (document.body.classList.contains('theme-light') || true) ? '#000' : '#fff'; // Forçando escuro se necessário ou detectando tema
      
      const waitingHtml = `
        <div style="display:flex; align-items:center; justify-content:center; gap:0.5rem; color:${textColor};">
          <svg width="18" height="18" viewBox="0 0 50 50" xmlns="http://www.w3.org/2000/svg">
            <circle cx="25" cy="25" r="20" stroke="${textColor}" stroke-width="4" fill="none" stroke-dasharray="31.4 31.4">
              <animateTransform attributeName="transform" type="rotate" from="0 25 25" to="360 25 25" dur="1s" repeatCount="indefinite" />
            </circle>
          </svg>
          <span>Aguardando pagamento...</span>
        </div>`;

      if (pixResultado) {
          pixResultado.innerHTML = `${imgHtml}${codeFieldHtml}${copyBtnHtml}${waitingHtml}`;
          pixResultado.style.display = 'block';
      }

      // Scroll para o PIX
      try {
        const isMobile = window.innerWidth <= 640;
        if (isMobile && pixResultado) {
            const rect = pixResultado.getBoundingClientRect();
            const top = (window.scrollY || window.pageYOffset || 0) + rect.top - 80;
            window.scrollTo({ top, behavior: 'smooth' });
        }
      } catch(_) {}

      // Listener do botão copiar e verificar
      setTimeout(() => {
          const copyBtn = document.getElementById(copyButtonId);
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
                setTimeout(() => {
                  copyBtn.disabled = false;
                  if (span) span.textContent = prev || 'Copiar código Pix';
                }, 1200);
              } catch (e) {
                alert('Não foi possível copiar o código Pix.');
              }
            });
          }
      }, 100);

      // Polling de Status (Lógica idêntica ao checkout.js)
      const chargeId = charge?.id || charge?.chargeId || data?.chargeId || '';
      const identifier = charge?.identifier || (data?.charge && data.charge.identifier) || '';
      const serverCorrelationID = charge?.correlationID || (data?.charge && data.charge.correlationID) || '';
      
      if (paymentPollInterval) {
        clearInterval(paymentPollInterval);
        paymentPollInterval = null;
      }
      
      const doCheckDb = async () => {
         try {
           const dbUrl = `/api/checkout/payment-state?id=${encodeURIComponent(chargeId)}&identifier=${encodeURIComponent(identifier)}&correlationID=${encodeURIComponent(serverCorrelationID || correlationID)}`;
           const dbResp = await fetch(dbUrl);
           const dbData = await dbResp.json();
           if (dbData?.paid === true) {
             clearInterval(paymentPollInterval);
             paymentPollInterval = null;
             try { markPaymentConfirmed(); } catch(_) {}
             await navigateToPedidoOrFallback(identifier, serverCorrelationID || correlationID, chargeId);
             return true;
           }
         } catch(e) { console.error('DB Check error:', e); }
         return false;
      };

      if (chargeId || identifier || serverCorrelationID) {
        const checkPaid = async () => {
          if (!chargeId) { await doCheckDb(); return; }
          try {
            const stResp = await fetch(`/api/woovi/charge-status?id=${encodeURIComponent(chargeId)}`);
            const stData = await stResp.json();
            const status = stData?.charge?.status || stData?.status || '';
            const paidFlag = stData?.charge?.paid || stData?.paid || false;
            const isPaid = paidFlag === true || /paid/i.test(String(status)) || /completed/i.test(String(status));
            
            if (isPaid) {
              clearInterval(paymentPollInterval);
              paymentPollInterval = null;
              try { markPaymentConfirmed(); } catch(_) {}
              await navigateToPedidoOrFallback(identifier, serverCorrelationID || correlationID, chargeId);
            } else {
               // Fallback imediato ao DB
               await doCheckDb();
            }
          } catch (e) {
            // Se falhar Woovi, tenta DB
            await doCheckDb();
          }
        };

        // Fallback Polling (DB Check)
         const checkPaidDb = async () => {
           await doCheckDb();
         };
 
         // Inicia polling primário
         if (chargeId) checkPaid();
         else checkPaidDb();

         paymentPollInterval = setInterval(checkPaidDb, 5000); 
         
         // SSE Listener (Real-time)
         try {
             if (window.paymentEventSource) { window.paymentEventSource.close(); window.paymentEventSource = null; }
             const sseUrl = `/api/payment/subscribe?identifier=${encodeURIComponent(identifier)}&correlationID=${encodeURIComponent(serverCorrelationID || correlationID)}`;
             window.paymentEventSource = new EventSource(sseUrl);
             window.paymentEventSource.addEventListener('paid', async (ev) => {
               try {
                 if (paymentPollInterval) { clearInterval(paymentPollInterval); paymentPollInterval = null; }
                 if (window.paymentEventSource) { window.paymentEventSource.close(); window.paymentEventSource = null; }
                 try { markPaymentConfirmed(); } catch(_) {}
                 await navigateToPedidoOrFallback(identifier, serverCorrelationID || correlationID, chargeId);
               } catch(_) {}
             });
         } catch(_) {}
         
         // Polling Woovi a cada 15s como backup (apenas se tiver chargeId)
         if (chargeId) {
             const checkPaidWoovi = async () => {
                await checkPaid();
             };
             setInterval(checkPaidWoovi, 15000);
         }
       }

    } catch (err) {
      alert('Erro ao criar PIX: ' + (err?.message || err));
    } finally {
      if (btnPedido) {
        btnPedido.disabled = false;
        btnPedido.classList.remove('loading');
      }
    }
  }

  // --- Inicialização ---

  function scrollToCardsMobile() {
    try {
      const isMobile = window.innerWidth <= 640;
      if (isMobile) {
        setTimeout(() => {
          const pCards = document.getElementById('planCards');
          if (pCards && pCards.style.display !== 'none') {
            const rect = pCards.getBoundingClientRect();
            const scrollTop = window.pageYOffset || document.documentElement.scrollTop;
            // Deixa uma beirada da descrição (aprox 120px acima do topo dos cards)
            const targetTop = (rect.top + scrollTop) - 120;
            window.scrollTo({ top: targetTop, behavior: 'smooth' });
          }
        }, 500);
      }
    } catch (_) {}
  }

  if (tipoSelect) {
    tipoSelect.addEventListener('change', () => {
      const tipo = tipoSelect.value;
      popularQuantidades(tipo);
      renderPlanCards(tipo);
      renderTipoDescription(tipo);
      updatePerfilVisibility();
      updateWarrantyVisibility();
      
      // Update visual active state of type cards
      if (tipoCards) {
        const all = tipoCards.querySelectorAll('.option-card');
        all.forEach(c => {
          if (c.getAttribute('data-tipo') === tipo) {
            c.classList.add('active');
          } else {
            c.classList.remove('active');
          }
        });
      }

      // Scroll Mobile para os cards (deixando beirada da descrição)
      scrollToCardsMobile();
    });
  }

  function popularQuantidades(tipo) {
    if (!qtdSelect) return;
    qtdSelect.innerHTML = '';
    const arr = tabela[tipo] || [];
    arr.forEach(item => {
      const opt = document.createElement('option');
      opt.value = item.q;
      opt.textContent = `${item.q} - ${item.p}`;
      qtdSelect.appendChild(opt);
    });
    qtdSelect.disabled = false;
  }

  if (checkCheckoutButton) {
    checkCheckoutButton.addEventListener('click', checkInstagramProfileCheckout);
  }

  if (btnPedido) {
    btnPedido.addEventListener('click', criarPixWoovi);
  }
  
  if (usernameCheckoutInput) {
    usernameCheckoutInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
            e.preventDefault();
            checkInstagramProfileCheckout();
        }
    });

    // Hide tutorial balloon — apenas quando o usuário realmente interagir (digitar ou clicar no balão)
    const hideBalloon = () => {
      const balloon = document.getElementById('tutorial3Usuario');
      if (balloon) {
        balloon.style.display = 'none';
        balloon.classList.add('hide');
      }
    };
    
    // Esconder somente quando começar a digitar ou colar
    usernameCheckoutInput.addEventListener('input', hideBalloon);
    usernameCheckoutInput.addEventListener('paste', hideBalloon);

    // Listener no próprio balão: permitir fechar com um toque/clique
    const balloonElement = document.getElementById('tutorial3Usuario');
    if (balloonElement) {
        balloonElement.addEventListener('click', hideBalloon);
    }
    
    // Removido: esconder em cliques/focus globais para evitar sumir imediatamente ao carregar
  }

  if (contactPhoneInput) attachPhoneMask(contactPhoneInput);
  if (document.getElementById('checkoutPhoneInput')) attachPhoneMask(document.getElementById('checkoutPhoneInput'));

  // --- Step Navigation Listeners ---
  const backToStep1Btn = document.getElementById('backToStep1Btn');
  if (backToStep1Btn) {
      backToStep1Btn.addEventListener('click', () => {
          if (window.goToStep) window.goToStep(1);
      });
  }

  const confirmContactDataBtn = document.getElementById('btnActionProceed');
  if (confirmContactDataBtn) {
      confirmContactDataBtn.addEventListener('click', (e) => {
          if (e) e.preventDefault(); // Prevent link navigation
          const email = contactEmailInput ? contactEmailInput.value.trim() : '';
          const phone = contactPhoneInput ? contactPhoneInput.value.trim() : '';
          
          const emailErrorMsg = document.getElementById('emailErrorMsg');
          
          if (!email || !email.includes('@')) {
              if (emailErrorMsg) emailErrorMsg.style.display = 'block';
              else showStatusMessageCheckout('Por favor, informe um email válido.', 'error');
              
              if (contactEmailInput) contactEmailInput.focus();
              return;
          } else {
              if (emailErrorMsg) emailErrorMsg.style.display = 'none';
          }
          
          if (!phone || phone.length < 10) {
              showStatusMessageCheckout('Por favor, informe um telefone válido.', 'error');
              if (contactPhoneInput) contactPhoneInput.focus();
              return;
          }
          
          if (window.goToStep) window.goToStep(3);
      });
  }

  // Init
  renderPromoPrices();
  applyWarrantyMode();
  renderTipoCards();
  initPromoListeners();
  
  // Default selection (Mistos)
  if (tipoSelect) {
    if (!tipoSelect.value) {
      tipoSelect.value = 'mistos';
    }
    // Always dispatch change to ensure cards are rendered and scroll logic runs
    setTimeout(() => {
        tipoSelect.dispatchEvent(new Event('change'));
    }, 100);
  }

  // Initial Step
  if (window.goToStep) window.goToStep(1);
  
  // Expor função para o EJS se necessário (mas tentamos evitar scripts inline)
  window.checkInstagramProfileCheckout = checkInstagramProfileCheckout;

  // --- Modals Logic (Warranty, Comments, Tools) ---
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
});
