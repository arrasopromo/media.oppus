(() => {
  const tipoSelect = document.getElementById('tipoSelect');
  const qtdSelect = document.getElementById('quantidadeSelect');
  const resumo = document.getElementById('resumo');
  const resTipo = document.getElementById('resTipo');
  const resQtd = document.getElementById('resQtd');
  const resPreco = document.getElementById('resPreco');
  const btnPedido = document.getElementById('realizarPedidoBtn');
  const pixResultado = document.getElementById('pixResultado');
  let paymentPollInterval = null;
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
  const grupoPedido = document.getElementById('grupoPedido');
  // carrossel removido
  let isInstagramVerified = false;
  // Captura phone da URL: /checkout?phone=... (default 11111111)
  const phoneFromUrl = new URLSearchParams(window.location.search).get('phone') || '11111111';

  const tabela = {
    mistos: [
      { q: 150, p: 'R$ 7,90' },
      { q: 300, p: 'R$ 14,90' },
      { q: 500, p: 'R$ 22,90' },
      { q: 700, p: 'R$ 29,90' },
      { q: 1200, p: 'R$ 39,90' },
      { q: 2000, p: 'R$ 49,90' },
      { q: 3000, p: 'R$ 69,90' },
      { q: 4000, p: 'R$ 79,90' },
      { q: 5000, p: 'R$ 99,90' },
      { q: 7500, p: 'R$ 129,90' },
      { q: 10000, p: 'R$ 159,90' },
      { q: 15000, p: 'R$ 219,90' },
    ],
    brasileiros: [
      { q: 150, p: 'R$ 12,90' },
      { q: 300, p: 'R$ 29,90' },
      { q: 500, p: 'R$ 39,90' },
      { q: 700, p: 'R$ 49,90' },
      { q: 1000, p: 'R$ 59,90' },
      { q: 2000, p: 'R$ 79,90' },
      { q: 3000, p: 'R$ 99,90' },
      { q: 4000, p: 'R$ 129,90' },
      { q: 5000, p: 'R$ 159,90' },
      { q: 7500, p: 'R$ 199,90' },
      { q: 10000, p: 'R$ 299,90' },
      { q: 15000, p: 'R$ 399,90' },
    ],
    organicos: [
      { q: 150, p: 'R$ 39,90' },
      { q: 300, p: 'R$ 49,90' },
      { q: 500, p: 'R$ 69,90' },
      { q: 700, p: 'R$ 89,90' },
      { q: 1000, p: 'R$ 129,90' },
      { q: 2000, p: 'R$ 229,90' },
      { q: 3000, p: 'R$ 259,90' },
      { q: 4000, p: 'R$ 329,90' },
      { q: 5000, p: 'R$ 399,90' },
      { q: 7500, p: 'R$ 539,90' },
      { q: 10000, p: 'R$ 699,90' },
      { q: 15000, p: 'R$ 999,90' },
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

  function getUnitForTipo(tipo) {
    switch (tipo) {
      case 'mistos':
      case 'brasileiros':
      case 'organicos':
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
    statusCheckoutMessage.style.color = type === 'error' ? '#ffb4b4' : (type === 'success' ? '#b8ffb8' : '#ffffff');
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
    const selectedOption = tipoSelect.options[tipoSelect.selectedIndex];
    const text = selectedOption ? selectedOption.textContent.toLowerCase() : '';
    const isFollowersService = /seguidores/i.test(text);
    perfilCard.style.display = isFollowersService && tipoSelect.value ? 'block' : 'none';
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
    if (isFollowersSelected()) {
      btnPedido.disabled = !isInstagramVerified;
    } else {
      btnPedido.disabled = false;
    }
  }

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
  }

  function showTutorialStep(step) {
    hideAllTutorials();
    switch (step) {
      case 1:
        if (tutorial1Tipo) tutorial1Tipo.style.display = 'block';
        if (grupoTipo) grupoTipo.classList.add('tutorial-highlight');
        break;
      case 2:
        if (tutorial2Pacote) tutorial2Pacote.style.display = 'block';
        if (grupoQuantidade) grupoQuantidade.classList.add('tutorial-highlight');
        break;
      case 3:
        if (isFollowersSelected()) {
          if (tutorial3Usuario) tutorial3Usuario.style.display = 'block';
          if (grupoUsername) grupoUsername.classList.add('tutorial-highlight');
        } else {
          // Para serviços que não exigem perfil, avançar direto para o pedido
          showTutorialStep(5);
        }
        break;
      case 4:
        if (isFollowersSelected()) {
          if (tutorial4Validar) tutorial4Validar.style.display = 'block';
          if (grupoUsername) grupoUsername.classList.add('tutorial-highlight');
        } else {
          showTutorialStep(5);
        }
        break;
      case 5:
        if (tutorial5Pedido) tutorial5Pedido.style.display = 'block';
        if (grupoPedido) grupoPedido.classList.add('tutorial-highlight');
        break;
      default:
        break;
    }
  }

  function clearResumo() {
    resumo.hidden = true;
    resTipo.textContent = '';
    resQtd.textContent = '';
    resPreco.textContent = '';
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
    const opts = tabela[tipo];
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

  tipoSelect.addEventListener('change', () => {
    const tipo = tipoSelect.value;
    popularQuantidades(tipo);
    clearResumo();
    updatePerfilVisibility();
    if (tipo) {
      showTutorialStep(2);
    } else {
      showTutorialStep(1);
    }
  });

  qtdSelect.addEventListener('change', () => {
    const tipo = tipoSelect.value;
    const qtd = qtdSelect.value;
    const opt = qtdSelect.options[qtdSelect.selectedIndex];
    const preco = opt ? (opt.dataset.preco || '') : '';
    if (!tipo || !qtd) {
      clearResumo();
      return;
    }
    // Remove underscores do tipo no resumo
    resTipo.textContent = String(tipo).replace(/_/g, ' ');
    resQtd.textContent = `${qtd} ${getUnitForTipo(tipo)}`;
    resPreco.textContent = preco;
    resumo.hidden = false;
    updatePedidoButtonState();
    // sem carrossel de posts
    // Tutorial: após escolher pacote, ir para usuário (ou pular direto para pedido)
    showTutorialStep(3);
  });

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
        updatePedidoButtonState();
        showStatusMessageCheckout('Perfil verificado com sucesso.', 'success');
        // Avança para o passo final
        showTutorialStep(5);
        // Mobile: rolar suavemente até a área de realizar pedido
        try {
          const isMobile = window.innerWidth <= 768;
          if (isMobile) {
            setTimeout(() => {
              const target = document.getElementById('grupoPedido') || document.getElementById('resumo');
              if (target && typeof target.scrollIntoView === 'function') {
                target.scrollIntoView({ behavior: 'smooth', block: 'start' });
              }
            }, 300);
          }
        } catch (e) { /* silencioso */ }
      } else {
        const msg = data.error || 'Falha ao verificar perfil.';
        showStatusMessageCheckout(msg, 'error');
      }
    } catch (e) {
      hideLoadingCheckout();
      showStatusMessageCheckout('Erro ao verificar perfil. Tente novamente.', 'error');
    }
  }

  // Avançar de 3 -> 4 quando o usuário digita algo
  if (usernameCheckoutInput) {
    usernameCheckoutInput.addEventListener('input', () => {
      const hasValue = usernameCheckoutInput.value.trim().length > 0;
      if (hasValue && isFollowersSelected()) {
        showTutorialStep(4);
      } else if (isFollowersSelected()) {
        showTutorialStep(3);
      }
    });
  }

  async function criarPixWoovi() {
    try {
      const tipo = tipoSelect.value;
      const qtd = Number(qtdSelect.value);
      const opt = qtdSelect.options[qtdSelect.selectedIndex];
      const precoStr = opt ? (opt.dataset.preco || '') : '';
      const valueCents = parsePrecoToCents(precoStr);
      if (!tipo || !qtd || !valueCents) {
        alert('Selecione o tipo e o pacote antes de realizar o pedido.');
        return;
      }
      if (isFollowersSelected() && !isInstagramVerified) {
        alert('Verifique o perfil do Instagram antes de realizar o pedido.');
        return;
      }
      // sem validação de posts
      btnPedido.disabled = true;
      btnPedido.classList.add('loading');

      const correlationID = (window.crypto && window.crypto.randomUUID) ? window.crypto.randomUUID() : `${Date.now()}-${Math.random().toString(16).slice(2)}`;
      // Tracking: Meta Pixel + CAPI (InitiateCheckout)
      const valueBRL = Math.round(Number(valueCents)) / 100;
      const fbpCookie = (document.cookie.match(/_fbp=([^;]+)/)?.[1]) || '';
      try {
        if (typeof fbq === 'function') {
          fbq('track', 'InitiateCheckout', {
            value: valueBRL,
            currency: 'BRL',
            contents: [{ id: tipo, quantity: qtd }],
            content_name: `${tipo} - ${qtd} ${getUnitForTipo(tipo)}`,
          }, { eventID: correlationID });
        }
      } catch (_) { /* silencioso */ }
      try {
        void fetch('/api/meta/track', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            eventName: 'InitiateCheckout',
            value: valueBRL,
            currency: 'BRL',
            contentName: `${tipo} - ${qtd} ${getUnitForTipo(tipo)}`,
            contents: [{ id: tipo, quantity: qtd }],
            phone: phoneFromUrl,
            fbp: fbpCookie,
            correlationID,
            eventSourceUrl: window.location.href,
          })
        });
      } catch (_) { /* silencioso */ }
      const payload = {
        correlationID,
        value: valueCents,
        comment: 'Checkout OPPUS',
        customer: {
          name: 'Cliente Checkout',
          phone: phoneFromUrl
        },
        // Sanitiza e evita emojis/Unicode não permitido
        additionalInfo: [
          { key: 'tipo_servico', value: tipo },
          { key: 'quantidade', value: String(qtd) },
          { key: 'pacote', value: `${qtd} ${getUnitForTipo(tipo)} - ${precoStr}` },
          { key: 'phone', value: phoneFromUrl },
          { key: 'instagram_username', value: (sessionStorage.getItem('oppus_instagram_username') || '') }
        ]
      };

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

      const waitingHtml = `
        <div style="display:flex; align-items:center; justify-content:center; gap:0.5rem; color:#fff;">
          <svg width="18" height="18" viewBox="0 0 50 50" xmlns="http://www.w3.org/2000/svg">
            <circle cx="25" cy="25" r="20" stroke="#fff" stroke-width="4" fill="none" stroke-dasharray="31.4 31.4">
              <animateTransform attributeName="transform" type="rotate" from="0 25 25" to="360 25 25" dur="1s" repeatCount="indefinite" />
            </circle>
          </svg>
          <span>Aguardando pagamento...</span>
        </div>`;

      pixResultado.innerHTML = `${imgHtml}${codeFieldHtml}${copyBtnHtml}${waitingHtml}`;
      pixResultado.style.display = 'block';

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
            copyBtn.disabled = true;
            copyBtn.classList.add('loading');
            setTimeout(() => {
              copyBtn.disabled = false;
              copyBtn.classList.remove('loading');
            }, 1000);
          } catch (e) {
            alert('Não foi possível copiar o código Pix.');
          }
        });
      }

      // Inicia polling de pagamento a cada 30 segundos
      const chargeId = charge?.id || charge?.chargeId || data?.chargeId || '';
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
              // Atualiza UI para pagamento confirmado
              pixResultado.innerHTML = `
                ${imgHtml}${codeFieldHtml}${copyBtnHtml}
                <div style="display:flex; align-items:center; justify-content:center; gap:0.5rem; color:#b8ffb8; font-weight:600;">
                  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                    <path d="M9 16.17L4.83 12l-1.42 1.41L9 19 21 7l-1.41-1.41z" stroke="#b8ffb8" stroke-width="2"/>
                  </svg>
                  <span>Pagamento confirmado!</span>
                </div>`;
            }
          } catch (e) {
            // Silencioso: mantém próximo ciclo
          }
        };
        // Executa imediatamente e depois a cada 30s
        checkPaid();
        paymentPollInterval = setInterval(checkPaid, 30000);
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

  // Navegação do carrossel
  // carrossel removido

  // Inicializar visibilidade do card de perfil
  updatePerfilVisibility();
  updatePedidoButtonState();
  // sem carrossel de posts

  // sem carrossel de posts
  // Inicializar tutorial no passo 1
  showTutorialStep(1);
})();