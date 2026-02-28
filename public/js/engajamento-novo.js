
document.addEventListener('DOMContentLoaded', function() {
  // --- Estado Global ---
  window.currentService = 'followers'; // followers, likes, views
  window.selectedType = ''; // Começa vazio para forçar seleção no dropdown
  window.selectedPlan = null; // { q: 100, p: 'R$ 9,90', id: ... }
  window.selectedPost = null; // { shortcode: '...', thumb: '...' }
  window.fetchedPosts = []; // Cache de posts buscados
  window.currentStep = 1;
  
  // Cache de Elementos
  const serviceBtns = document.querySelectorAll('.service-selector-btn');
  const tipoSelect = document.getElementById('tipoSelect');
  const quantidadeSelect = document.getElementById('quantidadeSelect');
  
  // Elementos do Modal de Posts
  const postsModal = document.getElementById('postsModal');
  const postsGrid = document.getElementById('postsGrid');
  const closePostsModalBtn = document.getElementById('closePostsModal');
  
  // Elementos de Navegação
  const steps = [1, 2, 3];
  
  // --- Tabelas de Preços ---
  const tabelaSeguidores = {
    mistos: [
      { q: 150, p: 'R$ 7,90' }, { q: 300, p: 'R$ 12,90' }, { q: 500, p: 'R$ 19,90' },
      { q: 700, p: 'R$ 24,90' }, { q: 1000, p: 'R$ 29,90' }, { q: 2000, p: 'R$ 54,90' },
      { q: 3000, p: 'R$ 89,90' }, { q: 4000, p: 'R$ 109,90' }, { q: 5000, p: 'R$ 129,90' },
      { q: 7500, p: 'R$ 169,90' }, { q: 10000, p: 'R$ 199,90' }, { q: 15000, p: 'R$ 329,90' }
    ],
    organicos: [
      { q: 150, p: 'R$ 39,90' }, { q: 300, p: 'R$ 49,90' }, { q: 500, p: 'R$ 69,90' },
      { q: 700, p: 'R$ 89,90' }, { q: 1000, p: 'R$ 129,90' }, { q: 2000, p: 'R$ 199,90' },
      { q: 3000, p: 'R$ 249,90' }, { q: 4000, p: 'R$ 329,90' }, { q: 5000, p: 'R$ 499,90' },
      { q: 7500, p: 'R$ 599,90' }, { q: 10000, p: 'R$ 899,90' }, { q: 15000, p: 'R$ 1.299,90' }
    ]
  };

  const tabelaCurtidas = {
    mistos: [
      { q: 150, p: 'R$ 5,90' }, { q: 300, p: 'R$ 7,90' }, { q: 500, p: 'R$ 9,90' },
      { q: 700, p: 'R$ 14,90' }, { q: 1000, p: 'R$ 19,90' }, { q: 2000, p: 'R$ 24,90' },
      { q: 3000, p: 'R$ 29,90' }, { q: 4000, p: 'R$ 34,90' }, { q: 5000, p: 'R$ 39,90' },
      { q: 7500, p: 'R$ 49,90' }, { q: 10000, p: 'R$ 69,90' }, { q: 15000, p: 'R$ 89,90' }
    ],
    organicos: [
      { q: 150, p: 'R$ 4,90' }, { q: 300, p: 'R$ 9,90' }, { q: 500, p: 'R$ 14,90' },
      { q: 700, p: 'R$ 29,90' }, { q: 1000, p: 'R$ 39,90' }, { q: 2000, p: 'R$ 49,90' },
      { q: 3000, p: 'R$ 59,90' }, { q: 4000, p: 'R$ 69,90' }, { q: 5000, p: 'R$ 79,90' },
      { q: 7500, p: 'R$ 109,90' }, { q: 10000, p: 'R$ 139,90' }, { q: 15000, p: 'R$ 199,90' }
    ]
  };

  const tabelaVisualizacoes = {
    visualizacoes_reels: [
      { q: 1000, p: 'R$ 4,90' }, { q: 2500, p: 'R$ 9,90' }, { q: 5000, p: 'R$ 14,90' },
      { q: 10000, p: 'R$ 19,90' }, { q: 25000, p: 'R$ 24,90' }, { q: 50000, p: 'R$ 34,90' },
      { q: 100000, p: 'R$ 49,90' }, { q: 150000, p: 'R$ 59,90' }, { q: 200000, p: 'R$ 69,90' },
      { q: 250000, p: 'R$ 89,90' }, { q: 500000, p: 'R$ 109,90' }, { q: 1000000, p: 'R$ 159,90' }
    ]
  };

  // --- Funções de Renderização (Selects) ---

  function getLabelForTipo(tipo) {
    // Labels atualizadas conforme solicitação
    if (window.currentService === 'followers') {
      if (tipo === 'mistos') return 'Comprar Seguidores';
      if (tipo === 'organicos') return 'Comprar Seguidores Reais';
    }
    if (window.currentService === 'likes') {
      if (tipo === 'mistos') return 'Comprar Curtidas';
      if (tipo === 'organicos') return 'Comprar Curtidas Reais';
    }
    if (window.currentService === 'views') {
      return 'Visualizações Reels';
    }
    
    // Fallback
    const labels = {
      'mistos': 'Mistos (Mundiais)',
      'organicos': 'Reais (Orgânicos)',
      'visualizacoes_reels': 'Visualizações Reels'
    };
    return labels[tipo] || tipo;
  }

  function renderTipoOptions() {
    const tipoCards = document.getElementById('tipoCards');
    const quantidadeSelectCard = document.getElementById('quantidadeSelectCard');
    
    if (!tipoCards) return;
    tipoCards.innerHTML = '';
    
    let tipos = [];
    if (window.currentService === 'followers') {
      tipos = ['mistos', 'organicos']; // Brasileiros removidos
    } else if (window.currentService === 'likes') {
      tipos = ['mistos', 'organicos']; // Brasileiros removidos
    } else if (window.currentService === 'views') {
      tipos = ['visualizacoes_reels'];
    }

    // Se houver apenas 1 tipo, seleciona automaticamente? 
    // Melhor deixar o usuário clicar para consistência visual de "botões"
    
    tipos.forEach(tipo => {
      const btn = document.createElement('button');
      btn.className = 'service-selector-btn type-btn'; // Reutilizando estilo de botão
      if (window.selectedType === tipo) {
        btn.classList.add('active');
      }
      
      // Ícone opcional
      let icon = '';
      if (tipo === 'mistos') icon = '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"></path><circle cx="9" cy="7" r="4"></circle><path d="M23 21v-2a4 4 0 0 0-3-3.87"></path><path d="M16 3.13a4 4 0 0 1 0 7.75"></path></svg>';
      if (tipo === 'organicos') icon = '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z"></path></svg>';
      
      btn.innerHTML = `${icon} ${getLabelForTipo(tipo)}`;
      
      btn.onclick = () => {
        window.selectedType = tipo;
        
        // Atualiza visual dos botões
        document.querySelectorAll('.type-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        
        // Mostra card de quantidade
        if (quantidadeSelectCard) quantidadeSelectCard.style.display = 'block';
        
        renderPlanOptions();
        updateSummary();
        
        // Scroll suave para o próximo passo (quantidade)
        if (quantidadeSelectCard) {
             setTimeout(() => {
                 quantidadeSelectCard.scrollIntoView({ behavior: 'smooth', block: 'center' });
             }, 100);
        }
      };
      
      tipoCards.appendChild(btn);
    });
    
    // Se não tiver tipo selecionado, esconde a quantidade
    if (!window.selectedType && quantidadeSelectCard) {
        quantidadeSelectCard.style.display = 'none';
    } else if (window.selectedType && quantidadeSelectCard) {
        quantidadeSelectCard.style.display = 'block';
    }
  }

  function renderPlanOptions() {
    if (!quantidadeSelect) return;
    quantidadeSelect.innerHTML = '<option value="" selected>Selecione a quantidade...</option>';
    
    if (!window.selectedType) {
      quantidadeSelect.disabled = true;
      quantidadeSelect.innerHTML = '<option value="">Selecione o tipo primeiro...</option>';
      return;
    }
    
    quantidadeSelect.disabled = false;
    
    let tabela = {};
    if (window.currentService === 'followers') tabela = tabelaSeguidores;
    else if (window.currentService === 'likes') tabela = tabelaCurtidas;
    else if (window.currentService === 'views') tabela = tabelaVisualizacoes;
    
    const planos = tabela[window.selectedType] || [];
    
    planos.forEach((plano, index) => {
      const option = document.createElement('option');
      option.value = index; // Usamos o index para recuperar o objeto plano depois
      const unit = window.currentService === 'followers' ? 'seguidores' : 
                   (window.currentService === 'likes' ? 'curtidas' : 'visualizações');
      option.textContent = `${plano.q} ${unit} - ${plano.p}`;
      quantidadeSelect.appendChild(option);
    });
  }

  function updateSummary() {
    const resTipo = document.getElementById('resTipo');
    const resQtd = document.getElementById('resQtd');
    const resPreco = document.getElementById('resPreco');
    
    if (resTipo) resTipo.textContent = getLabelForTipo(window.selectedType);
    if (resQtd && window.selectedPlan) resQtd.textContent = window.selectedPlan.q;
    if (resPreco && window.selectedPlan) resPreco.textContent = window.selectedPlan.p;
  }

  // Event Listeners para Selects
  if (tipoSelect) {
    tipoSelect.addEventListener('change', (e) => {
      window.selectedType = e.target.value;
      renderPlanOptions();
      updateSummary();
    });
  }

  if (quantidadeSelect) {
    quantidadeSelect.addEventListener('change', (e) => {
      const index = e.target.value;
      if (index !== '') {
        let tabela = {};
        if (window.currentService === 'followers') tabela = tabelaSeguidores;
        else if (window.currentService === 'likes') tabela = tabelaCurtidas;
        else if (window.currentService === 'views') tabela = tabelaVisualizacoes;
        
        const planos = tabela[window.selectedType] || [];
        window.selectedPlan = planos[index];
        updateSummary();
        window.goToStep(2); // Avança automaticamente
      } else {
        window.selectedPlan = null;
      }
    });
  }

  // --- Lógica de Seleção de Serviço (Botões do Topo) ---

  window.selectService = function(service) {
    window.currentService = service;
    
    serviceBtns.forEach(btn => {
      if (btn.dataset.service === service) btn.classList.add('active');
      else btn.classList.remove('active');
    });
    
    window.selectedType = ''; // Reset type
    window.selectedPlan = null;
    window.selectedPost = null;
    
    renderTipoOptions();
    updateSummary();
    
    // Reset para Step 1 se mudar serviço
    window.goToStep(1);
  };

  // --- Lógica de Posts (Modal e Fetch) ---

  async function fetchPosts(username) {
    if (!postsGrid) return;
    
    postsGrid.innerHTML = '<div style="grid-column:1/-1;text-align:center;padding:2rem;">Carregando posts...</div>';
    
    try {
      const response = await fetch(`/api/instagram/posts?username=${encodeURIComponent(username)}`);
      const data = await response.json();
      
      if (data && data.posts && Array.isArray(data.posts)) {
        window.fetchedPosts = data.posts;
        renderPosts(data.posts);
      } else {
        postsGrid.innerHTML = '<div style="grid-column:1/-1;text-align:center;padding:2rem;">Nenhum post encontrado ou perfil privado.</div>';
      }
    } catch (error) {
      console.error('Erro ao buscar posts:', error);
      postsGrid.innerHTML = '<div style="grid-column:1/-1;text-align:center;padding:2rem;color:red;">Erro ao carregar posts. Tente novamente.</div>';
    }
  }

  function renderPosts(posts) {
    if (!postsGrid) return;
    postsGrid.innerHTML = '';
    
    if (posts.length === 0) {
        postsGrid.innerHTML = '<div style="grid-column:1/-1;text-align:center;padding:2rem;">Nenhum post disponível.</div>';
        return;
    }
    
    posts.forEach(post => {
      const div = document.createElement('div');
      div.className = 'pick-post-card';
      div.style.cursor = 'pointer';
      div.style.position = 'relative';
      div.style.aspectRatio = '1';
      div.style.overflow = 'hidden';
      div.style.borderRadius = '4px';
      
      // Determine image URL (proxy if needed)
      const imgUrl = post.displayUrl || post.thumbnail_src || '';
      const proxyUrl = imgUrl ? `/image-proxy?url=${encodeURIComponent(imgUrl)}` : '';
      
      div.innerHTML = `
        <img src="${proxyUrl}" style="width:100%;height:100%;object-fit:cover;" alt="Post">
        ${window.selectedPost && window.selectedPost.shortcode === post.shortcode ? 
          '<div style="position:absolute;inset:0;background:rgba(37,99,235,0.4);display:flex;align-items:center;justify-content:center;color:white;font-weight:bold;font-size:2rem;">✓</div>' : ''}
      `;
      
      div.onclick = () => selectPost(post);
      postsGrid.appendChild(div);
    });
  }

  function selectPost(post) {
    window.selectedPost = post;
    renderPosts(window.fetchedPosts); // Re-render to show selection
    
    // Fechar modal e avançar após pequena pausa visual
    setTimeout(() => {
        closePostsModal();
        // Atualizar UI de seleção no Step 2 (opcional: mostrar thumb do post escolhido)
        const postPreview = document.getElementById('selectedPostPreview');
        if (postPreview) {
            postPreview.style.display = 'block';
            postPreview.innerHTML = `Post selecionado: ${post.shortcode}`;
        }
        
        // Se já estamos no step 2 e validamos, podemos tentar ir pro 3
        validateAndAdvanceFromStep2();
    }, 300);
  }

  function openPostsModal() {
    if (postsModal) {
        postsModal.style.display = 'flex';
        // Se já temos posts cacheados para este usuário, usa. Senão, busca.
        const username = document.getElementById('usernameCheckoutInput').value;
        if (window.fetchedPosts.length === 0) {
            fetchPosts(username);
        } else {
            renderPosts(window.fetchedPosts);
        }
    }
  }

  function closePostsModal() {
    if (postsModal) postsModal.style.display = 'none';
  }
  
  if (closePostsModalBtn) {
      closePostsModalBtn.onclick = closePostsModal;
  }

  // --- Navegação (Steps) ---

  window.goToStep = function(step) {
    // Validação Step 1 -> 2
    if (step > 1) {
        if (!window.selectedPlan) {
            alert('Por favor, selecione um pacote primeiro.');
            return;
        }
    }
    
    // Validação Step 2 -> 3
    if (step > 2) {
        const usernameInput = document.getElementById('usernameCheckoutInput');
        const username = usernameInput ? usernameInput.value.trim() : '';
        
        if (!username) {
            alert('Por favor, informe o usuário do Instagram.');
            if (step === 3) window.goToStep(2); // Volta pro 2 se tentou pular
            return;
        }

        // Validar Email e Telefone
        const emailInput = document.getElementById('contactEmailInput');
        const phoneInput = document.getElementById('checkoutPhoneInput');
        
        if (emailInput && !emailInput.value.trim().includes('@')) {
             alert('Por favor, informe um e-mail válido.');
             if (step === 3) window.goToStep(2);
             return;
        }
        
        if (phoneInput && phoneInput.value.trim().length < 10) {
             alert('Por favor, informe um telefone válido.');
             if (step === 3) window.goToStep(2);
             return;
        }
        
        // Se for Likes ou Views, EXIGE post selecionado
        if ((window.currentService === 'likes' || window.currentService === 'views') && !window.selectedPost) {
            // Se tentou ir pro 3 mas não tem post, abre o modal
            openPostsModal();
            return; 
        }
    }

    // Atualizar Estado Visual
    window.currentStep = step;
    
    // Mostrar/Esconder Containers
    const container1 = document.getElementById('step1Container'); // Seleção
    const container2 = document.getElementById('step2Container'); // Dados/Perfil
    const container3 = document.getElementById('step3Container'); // Pagamento
    
    if (container1) container1.style.display = step === 1 ? 'grid' : 'none';
    if (container2) container2.style.display = step === 2 ? 'block' : 'none';
    if (container3) container3.style.display = step === 3 ? 'block' : 'none';
    
    // Se entrou no Step 3, preparar resumo final e pagamento
    if (step === 3) {
        // Sincronizar email de contato para o pagamento
        const contactEmail = document.getElementById('contactEmailInput');
        const cardEmail = document.getElementById('cardHolderEmail');
        if (contactEmail && cardEmail && !cardEmail.value) {
             cardEmail.value = contactEmail.value;
        }

        preparePaymentStep();
        // Trigger Tutorial Step 5 (Payment)
        if (window.showTutorialStep) window.showTutorialStep(5);
    }

    // Se entrou no Step 2, mostrar tutorial de usuário
    if (step === 2) {
        if (window.showTutorialStep) window.showTutorialStep(3);
    }
  };

  // --- Inicialização ---
  // Render inicial
  renderTipoOptions();
  
  // Se houver hash na URL, tenta navegar
  if (window.location.hash === '#step2') {
      // Precisa ter plano selecionado, então talvez não funcione direto
  }

  // --- Helpers de Validação e Pagamento (Mantidos do original) ---
  
  function validateAndAdvanceFromStep2() {
      // Chamado após selecionar post ou validar perfil
      if (window.currentService === 'followers') {
          window.goToStep(3);
      } else {
          // Likes/Views
          if (window.selectedPost) {
              window.goToStep(3);
          } else {
              openPostsModal();
          }
      }
  }

  // Hook no botão de validar perfil
  const checkProfileBtn = document.getElementById('checkCheckoutButton');
  const usernameInput = document.getElementById('usernameCheckoutInput');
  const statusMsg = document.getElementById('statusCheckoutMessage');
  const loadingSpinner = document.getElementById('loadingCheckoutSpinner');
  const profilePreview = document.getElementById('profilePreview');
  const contactFields = document.getElementById('contactFieldsArea');

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

  async function checkProfile() {
      const rawInput = usernameInput.value.trim();
      if (!rawInput) return;

      // Reset UI
      if (statusMsg) statusMsg.style.display = 'none';
      if (profilePreview) profilePreview.style.display = 'none';
      if (contactFields) contactFields.style.display = 'none';
      
      // Validação Básica
      const username = normalizeInstagramUsername(rawInput);
      if (!isValidInstagramUsername(username)) {
          if (statusMsg) {
              statusMsg.textContent = 'Nome de usuário inválido.';
              statusMsg.style.display = 'block';
              statusMsg.style.color = 'red';
          }
          return;
      }

      // Show Loading
      if (loadingSpinner) loadingSpinner.style.display = 'flex';
      if (checkProfileBtn) checkProfileBtn.disabled = true;

      try {
          // Check Profile API
          const response = await fetch(`/api/instagram/posts?username=${encodeURIComponent(username)}`);
          const data = await response.json();

          if (data.success && data.user) {
              // Success!
              const user = data.user;
              window.fetchedUser = user; // Salvar user globalmente para o Step 3
              
              // Populate Profile Preview
              const pImg = document.getElementById('checkoutProfileImage');
              const pUser = document.getElementById('checkoutProfileUsername');
              const pPosts = document.getElementById('checkoutPostsCount');
              const pFollowers = document.getElementById('checkoutFollowersCount');
              const pFollowing = document.getElementById('checkoutFollowingCount');

              if (pImg) {
                  // Use proxy for image
                  const imgUrl = user.profilePicUrl || '';
                  pImg.src = imgUrl ? `/image-proxy?url=${encodeURIComponent(imgUrl)}` : '';
              }
              if (pUser) pUser.textContent = user.username;
              if (pPosts) pPosts.textContent = user.mediaCount || '-';
              if (pFollowers) pFollowers.textContent = user.followersCount || '-';
              if (pFollowing) pFollowing.textContent = user.followsCount || '-';

              // Show Preview
              if (profilePreview) profilePreview.style.display = 'block';
              
              // Show Contact Fields (Next Step Trigger)
              if (contactFields) {
                  contactFields.style.display = 'block';
                  // Trigger Tutorial Step 4 (Phone/Contact)
                  if (window.showTutorialStep) window.showTutorialStep(4);
              }

              // Store fetched posts if available
              if (data.posts && Array.isArray(data.posts)) {
                  window.fetchedPosts = data.posts;
              }

              // Auto-fetch posts for selection if needed
              if (window.currentService === 'likes' || window.currentService === 'views') {
                  renderPosts(window.fetchedPosts); // Pre-render grid
              }

          } else {
              // Error (User not found or private)
              if (statusMsg) {
                  statusMsg.textContent = 'Perfil não encontrado ou privado.';
                  statusMsg.style.display = 'block';
                  statusMsg.style.color = 'red';
              }
          }
      } catch (error) {
          console.error('Erro ao verificar perfil:', error);
          if (statusMsg) {
              statusMsg.textContent = 'Erro ao verificar perfil. Tente novamente.';
              statusMsg.style.display = 'block';
              statusMsg.style.color = 'red';
          }
      } finally {
          if (loadingSpinner) loadingSpinner.style.display = 'none';
          if (checkProfileBtn) checkProfileBtn.disabled = false;
      }
  }

  if (checkProfileBtn) {
      checkProfileBtn.onclick = checkProfile;
  }
  
  // Enter key support
  if (usernameInput) {
      usernameInput.addEventListener('keypress', (e) => {
          if (e.key === 'Enter') checkProfile();
      });
  }

  // --- Pagamento (Step 3) ---
  
  function preparePaymentStep() {
      // Populate review data
      const reviewImg = document.getElementById('reviewProfileImage');
      const reviewUser = document.getElementById('reviewProfileUsername');
      
      if (window.fetchedUser) {
          if (reviewImg) {
              const imgUrl = window.fetchedUser.profilePicUrl || '';
              reviewImg.src = imgUrl ? `/image-proxy?url=${encodeURIComponent(imgUrl)}` : '';
          }
          if (reviewUser) reviewUser.textContent = window.fetchedUser.username;
      }
      
      // Post Preview (if any)
      const postPreview = document.getElementById('step3PostPreview');
      const postContent = document.getElementById('step3PostPreviewContent');
      
      if (window.selectedPost && postPreview && postContent) {
          postPreview.style.display = 'block';
          const imgUrl = window.selectedPost.displayUrl || window.selectedPost.thumbnail_src || '';
          const proxyUrl = imgUrl ? `/image-proxy?url=${encodeURIComponent(imgUrl)}` : '';
          postContent.innerHTML = `<img src="${proxyUrl}" style="width:80px; height:80px; object-fit:cover; border-radius:8px;">`;
      } else if (postPreview) {
          postPreview.style.display = 'none';
      }
      
      // Update Payment Summary
      updateSummary();
  }

});
