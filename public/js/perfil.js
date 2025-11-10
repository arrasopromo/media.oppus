// Normalizar username do Instagram
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

let currentProfile = null;
let isChecking = false;
let timerInterval = null;
let timeRemaining = 300; // 5 minutos

const usernameInput = document.getElementById('usernameInput');
const checkButton = document.getElementById('checkButton');
const confirmButton = document.getElementById('confirmButton');
const profileImageContainer = document.getElementById('profileImageContainer');
const profileImage = document.getElementById('profileImage');
const profileUsername = document.getElementById('profileUsername');
const profileStats = document.getElementById('profileStats');
const verifiedBadge = document.getElementById('verifiedBadge');
const statusMessage = document.getElementById('statusMessage');
const loadingSpinner = document.getElementById('loadingSpinner');
const loadingOverlay = document.getElementById('loadingOverlay');
const toastContainer = document.getElementById('toastContainer');
const timerElement = document.getElementById('timer');
const continueBtn = document.getElementById('continueBtn');
const postEmbedContainer = document.getElementById('postEmbedContainer');
// Pop-ups interativos
const tutorialPop1 = document.getElementById('tutorialPop1');
const tutorialPop2 = document.getElementById('tutorialPop2');
const tutorialPop3 = document.getElementById('tutorialPop3');
const tutorialPop1Text = document.getElementById('tutorialPop1Text');
const tutorialPop2Text = document.getElementById('tutorialPop2Text');

// Timer de 5 minutos para expirar a sessão e redirecionar
const MAX_SESSION_TIME = 5 * 60 * 1000; // 5 minutos

function setupEventListeners() {
    usernameInput.addEventListener('input', handleUsernameInput);
    usernameInput.addEventListener('keydown', handleUsernameKeydown);
    usernameInput.addEventListener('focus', () => {
        // Ao clicar no campo, o primeiro pop desaparece
        hideTutorial();
    });
    document.addEventListener('keydown', handleGlobalKeydown);
}

function initializePage() {
    initializeTimer();
    
    // Verificar se há pedido já realizado nesta sessão
    checkOrderStatus();
    
    // Verificar se o link atual já foi usado (verificação adicional)
    checkLinkStatus();

    // Ajustar UI quando serviço for baseado em post (curtidas/visualizações)
    const selectedService = (sessionStorage.getItem('oppus_servico') || new URLSearchParams(window.location.search).get('servico') || '').toLowerCase();
    if (isPostService(selectedService)) {
        const titleEl = document.querySelector('.title');
        const subtitleEl = document.querySelector('.subtitle');
        if (titleEl) titleEl.textContent = 'Buscar Post';
        if (subtitleEl) subtitleEl.textContent = 'Cole seu link do post';
        usernameInput.placeholder = 'Cole o link do post (instagram.com/p/SHORTCODE ou /reel/SHORTCODE)';
        if (tutorialPop1Text) tutorialPop1Text.textContent = 'Cole seu link do post aqui';
        if (tutorialPop2Text) tutorialPop2Text.textContent = 'Após digitar ou colar link do perfil clique no V para validar';
    } else {
        // Seguidores: manter padrão "perfil"
        if (tutorialPop1Text) tutorialPop1Text.textContent = 'Digite seu usuário do Instagram';
        if (tutorialPop2Text) tutorialPop2Text.textContent = 'Após digitar ou colar link do perfil clique no V para validar';
    }

    // Mostrar o primeiro pop inicialmente
    showTutorialStep(1);
}

function handleUsernameInput(event) {
    const rawValue = event.target.value;
    const selectedService = (sessionStorage.getItem('oppus_servico') || new URLSearchParams(window.location.search).get('servico') || '').toLowerCase();
    // Para serviços de post (curtidas/visualizações), NÃO normalizar o link
    let value = rawValue.trim();
    if (!isPostService(selectedService)) {
        const normalizedValue = normalizeInstagramUsername(rawValue);
        if (normalizedValue !== rawValue) {
            event.target.value = normalizedValue;
        }
        value = normalizedValue.trim();
    }
    if (currentProfile && currentProfile.username !== value) {
        clearProfileState();
    }
    
    // Se há um pedido realizado na sessão, manter bloqueio independente do username
    const orderCompleted = localStorage.getItem('oppus_order_completed');
    const orderId = localStorage.getItem('oppus_order_id');
    
    if (orderCompleted === 'true' && orderId) {
        // Manter botão bloqueado se há pedido realizado
        confirmButton.disabled = true;
        confirmButton.textContent = 'Pedido Realizado';
        confirmButton.style.opacity = '0.6';
    }
    
    if (isPostService(selectedService)) {
        // Para curtidas, habilitar o botão quando houver um shortcode válido
        const shortcode = extractShortcodeFromInput(value);
        checkButton.disabled = !shortcode;
        if (shortcode) {
            checkButton.classList.add('pulse-effect');
            // Após digitar o primeiro caractere (shortcode válido), mostrar etapa 2
            showTutorialStep(2);
        } else {
            checkButton.classList.remove('pulse-effect');
        }
    } else {
        checkButton.disabled = value.length < 1;
        if (value.length >= 1) {
            checkButton.classList.add('pulse-effect');
            // Após digitar o primeiro caractere, mostrar etapa 2
            showTutorialStep(2);
        } else {
            checkButton.classList.remove('pulse-effect');
        }
    }
}

function handleUsernameKeydown(event) {
    if (event.key === 'Enter') {
        event.preventDefault();
        if (!checkButton.disabled && !isChecking) {
            checkProfile();
        }
    }
}

function handleGlobalKeydown(event) {
    if (event.key === 'Enter' && currentProfile && !confirmButton.disabled) {
        event.preventDefault();
        confirmProfile();
    }
    if (event.key === 'Escape') {
        goBack();
    }
}

async function checkProfile() {
    const rawInput = usernameInput.value.trim();
    if (!rawInput || isChecking) {
        return;
    }
    const selectedService = (sessionStorage.getItem('oppus_servico') || new URLSearchParams(window.location.search).get('servico') || '').toLowerCase();
        if (isPostService(selectedService)) {
            const shortcode = extractShortcodeFromInput(rawInput);
            if (!shortcode) {
                showStatusMessage('Link do post inválido. Use instagram.com/p/SHORTCODE ou /reel/SHORTCODE', 'error');
                return;
            }
            // Detecta tipo do post (reel ou p) apenas para o embed
            const postType = /\/reel\//i.test(rawInput) ? 'reel' : 'p';
            renderPostEmbed(shortcode, postType);
        showStatusMessage('Post carregado. Visualize o embed abaixo.', 'success');
        // Mostrar etapa 3 após carregar o post
        showTutorialStep(3);
        // Habilitar confirmação para serviços baseados em post
        enableConfirmButton();
        return;
    }
    const username = normalizeInstagramUsername(rawInput);
    if (!isValidInstagramUsername(username)) {
        showStatusMessage('Nome de usuário inválido. Use apenas letras, números, pontos e underscores.', 'error');
        return;
    }
    if (username !== rawInput) {
        usernameInput.value = username;
        showToast(`Username normalizado para: ${username}`, 'warning');
    }
    isChecking = true;
    showLoading();
    hideStatusMessage();
    clearProfileState();
    checkButton.disabled = true;
    try {
        const response = await fetch('/api/check-instagram-profile', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ username })
        });
        const data = await response.json();
        hideLoading();
        if (data.success) {
            currentProfile = data.profile;
            showProfileSuccess(data.profile);
            // Mostrar etapa 3 após sucesso
            showTutorialStep(3);
        } else {
            if (data.code === 'INSTAUSER_ALREADY_USED') {
                showStatusMessage('Este perfil já foi testado anteriormente. O serviço de teste já foi realizado para este usuário.', 'error');
            } else {
                showStatusMessage(data.error, 'error');
            }
        }
    } catch (error) {
        hideLoading();
        showStatusMessage('Erro ao verificar perfil. Tente novamente.', 'error');
    } finally {
        isChecking = false;
        checkButton.disabled = false;
    }
}

function extractShortcodeFromInput(input) {
    if (!input) return '';
    const trimmed = input.trim();

    // 1) Tratar links de redirecionamento (l.instagram.com/?u=...)
    try {
        const candidate = trimmed.startsWith('http') ? trimmed : `https://${trimmed}`;
        const candidateUrl = new URL(candidate);
        if (candidateUrl.hostname.endsWith('l.instagram.com')) {
            const uParam = candidateUrl.searchParams.get('u');
            if (uParam) {
                const decoded = decodeURIComponent(uParam);
                const innerMatch = decoded.match(/^(?:https?:\/\/)?(?:www\.)?instagram\.com\/(?:p|reel|tv)\/([A-Za-z0-9_-]+)/i);
                if (innerMatch) return innerMatch[1];
            }
        }
    } catch (e) {
        // Ignorar erros de URL inválida
    }

    // 2) Aceitar URLs de post/reel/tv com ou sem protocolo, e com parâmetros extras
    const urlMatch = trimmed.match(/^(?:https?:\/\/)?(?:www\.)?instagram\.com\/(?:p|reel|tv)\/([A-Za-z0-9_-]+)/i);
    if (urlMatch) return urlMatch[1];

    // 3) Aceitar apenas o SHORTCODE (copiado do IG)
    const shortcodeMatch = trimmed.match(/^([A-Za-z0-9_-]{5,})$/);
    return shortcodeMatch ? shortcodeMatch[1] : '';
}

function renderPostEmbed(shortcode, type = 'p') {
    if (!postEmbedContainer) return;
    const segment = (type === 'reel') ? 'reel' : 'p';
    const embedUrl = `https://www.instagram.com/${segment}/${shortcode}/embed`;
    postEmbedContainer.style.display = 'block';
    postEmbedContainer.innerHTML = `<iframe src="${embedUrl}" allowtransparency="true" allow="encrypted-media; picture-in-picture" scrolling="no"></iframe>`;
}

function isPostService(service) {
    return ['curtidas', 'curtidas_brasileiras', 'visualizacoes_reels'].includes(service);
}

function showProfileSuccess(profile) {
    profileImageContainer.style.display = 'flex';
    
    profileImage.onerror = function() {
        console.warn('Erro ao carregar imagem do perfil');
    };
    
    profileImage.src = profile.profilePicUrl || '';
    profileUsername.textContent = '@' + profile.username;
    profileStats.textContent = `Seguidores: ${profile.followersCount}`;
    verifiedBadge.style.display = profile.isVerified ? 'block' : 'none';
    
    // Verificar se há pedido realizado na sessão primeiro
    const orderCompleted = localStorage.getItem('oppus_order_completed');
    const orderId = localStorage.getItem('oppus_order_id');
    
    if (orderCompleted === 'true' && orderId) {
        // Se há pedido realizado na sessão, manter bloqueio
        confirmButton.disabled = true;
        confirmButton.textContent = 'Pedido Realizado';
        confirmButton.style.opacity = '0.6';
        showStatusMessage('Sessão bloqueada. Você já realizou um pedido.', 'info');
    } else if (profile.alreadyTested) {
        // Se o perfil já foi testado anteriormente
        confirmButton.disabled = true;
        confirmButton.textContent = 'Perfil Já Testado';
        confirmButton.style.opacity = '0.6';
        showStatusMessage('O serviço de teste já foi realizado para este usuário.', 'error');
    } else {
        enableConfirmButton();
    }
}

function showLoading() {
    loadingSpinner.style.display = 'flex';
}
function hideLoading() {
    loadingSpinner.style.display = 'none';
}
function showStatusMessage(message, type = 'success') {
    statusMessage.textContent = message;
    statusMessage.className = 'status-message ' + type;
    statusMessage.style.display = 'block';
}
function hideStatusMessage() {
    statusMessage.style.display = 'none';
}
function clearProfileState() {
    profileImageContainer.style.display = 'none';
    profileImage.src = '';
    profileUsername.textContent = '';
    profileStats.textContent = '';
    verifiedBadge.style.display = 'none';
    disableConfirmButton();
}
function enableConfirmButton() {
    confirmButton.disabled = false;
}
function disableConfirmButton() {
    confirmButton.disabled = true;
}
function showLoadingOverlay(message = 'Processando...') {
    const loadingContent = loadingOverlay.querySelector('.loading-content p');
    if (loadingContent) {
        loadingContent.textContent = message;
    }
    loadingOverlay.style.display = 'flex';
}
function hideLoadingOverlay() {
    loadingOverlay.style.display = 'none';
}
function showToast(message, type = 'success') {
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.textContent = message;
    toastContainer.appendChild(toast);
    setTimeout(() => {
        toast.style.opacity = '0';
        toast.style.transform = 'translateX(100%)';
        setTimeout(() => {
            if (toast.parentNode) {
                toast.parentNode.removeChild(toast);
            }
        }, 300);
    }, 5000);
}
function showSuccessMessage(message) {
    const successDiv = document.createElement('div');
    successDiv.className = 'success-modal';
    successDiv.innerHTML = `
        <div class="success-content">
            <div class="success-icon">✅</div>
            <h2>Sucesso!</h2>
            <p>${message}</p>
            <button onclick="goBack()" class="success-button">Voltar ao Início</button>
        </div>
    `;
    document.body.appendChild(successDiv);
}
function goBack() {
    // Tentar voltar para a URL original se disponível
    const sessionData = localStorage.getItem('oppus_session');
    if (sessionData) {
        try {
            const data = JSON.parse(sessionData);
            if (data.originalUrl) {
                window.location.href = data.originalUrl;
                return;
            }
        } catch (e) {
            // Ignorar erro de parsing
        }
    }
    
    // Se não tem dados de sessão, voltar para /teste123 como fallback
    window.location.href = '/teste123';
}
function initializeTimer() {
    // Timer direto de 5 minutos
    let seconds = 300; // 5 minutos direto
    
    function updateTimer() {
        const min = String(Math.floor(seconds / 60)).padStart(2, '0');
        const sec = String(seconds % 60).padStart(2, '0');
        timerElement.textContent = `${min}:${sec}`;
        
        if (seconds > 0) {
            seconds--;
        } else {
            // Tempo expirado
            clearInterval(timerInterval);
            timerElement.textContent = '00:00';
            // Bloquear campo de texto e mostrar mensagem
            usernameInput.disabled = true;
            checkButton.disabled = true;
            showStatusMessage('Tempo de teste expirado. Recarregue a página para tentar novamente.', 'error');
        }
    }
    
    updateTimer();
    timerInterval = setInterval(updateTimer, 1000);
}

// Função confirmProfile já ajustada para o pedido ggram
async function confirmProfile() {
    // Verificar se já há um pedido realizado na sessão
    const orderCompleted = localStorage.getItem('oppus_order_completed');
    const orderId = localStorage.getItem('oppus_order_id');
    const urlParams = new URLSearchParams(window.location.search);
    const selectedService = (sessionStorage.getItem('oppus_servico') || urlParams.get('servico') || 'seguidores_mistos').toLowerCase();
    
    if (confirmButton.disabled || (orderCompleted === 'true' && orderId)) {
        console.log('🔒 Sessão bloqueada - pedido já realizado');
        showToast('❌ Sessão bloqueada. Você já realizou um pedido.', 'error');
        return;
    }
    
    // Para seguidores, continuar exigindo perfil válido; para post, validaremos o link
    if (!isPostService(selectedService)) {
        if (!currentProfile) {
            showToast('❌ Primeiro valide o perfil antes de confirmar', 'error');
            return;
        }
        if (currentProfile.alreadyTested) {
            showToast('❌ Este perfil já foi testado anteriormente', 'error');
            showStatusMessage('O serviço de teste já foi realizado para este usuário.', 'error');
            return;
        }
    }
    showLoadingOverlay('Enviando pedido ao serviço...');
    try {
        // Obter o id da URL (pode ser /perfil/:id ou /perfil?id=)
        const pathParts = window.location.pathname.split('/');
        const idFromPath = pathParts[2]; // /perfil/:id -> pathParts[2] é o id
        const idFromQuery = urlParams.get('id');
        // Para serviços de post (curtidas/visualizações), não usar idParam
        const idParam = isPostService(selectedService) ? null : (idFromPath || idFromQuery);
        
        // Montar link conforme tipo de serviço
        let requestBody;
        if (isPostService(selectedService)) {
            const rawInput = usernameInput.value.trim();
            const shortcode = extractShortcodeFromInput(rawInput);
            if (!shortcode) {
                hideLoadingOverlay();
                showStatusMessage('Link do post inválido. Cole o link do Instagram (p/reel/tv) ou apenas o código do post.', 'error');
                return;
            }
            // Forçar uso de caminho /p/ na requisição do serviço e garantir barra final
            const postLink = `https://www.instagram.com/p/${shortcode}/`;
            // Não incluir id para serviços de post
            requestBody = { link: postLink, servico: selectedService };
        } else {
            // Para seguidores, enviar sempre o campo 'link' com o valor digitado normalizado
            const rawInput = usernameInput.value.trim();
            const fallbackUsername = normalizeInstagramUsername(rawInput);
            const valueToSend = (currentProfile && currentProfile.username) ? currentProfile.username : fallbackUsername;
            if (!isValidInstagramUsername(valueToSend)) {
                hideLoadingOverlay();
                showStatusMessage('Nome de usuário inválido. Use apenas letras, números, pontos e underscores.', 'error');
                return;
            }
            // Enviar como 'link' conforme especificação do provedor
            requestBody = { link: valueToSend, id: idParam, servico: selectedService };
        }

        const response = await fetch(`/api/ggram-order${idParam ? `?id=${idParam}` : ''}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(requestBody)
        });
        const result = await response.json();
        hideLoadingOverlay();
        
        if (response.ok && result.order) {
            showToast('✅ Pedido de teste realizado com sucesso', 'success');
            showSuccessMessage('Pedido de teste realizado com sucesso');
            
            // Bloquear botão após confirmação e salvar no localStorage
            confirmButton.disabled = true;
            confirmButton.textContent = 'Pedido Realizado';
            confirmButton.style.opacity = '0.6';
            
            // Salvar estado no localStorage para persistir após refresh (BLOQUEAR SESSÃO INTEIRA)
            localStorage.setItem('oppus_order_completed', 'true');
            localStorage.setItem('oppus_order_id', result.order);
            localStorage.setItem('oppus_order_timestamp', Date.now().toString());
            
            // Mostrar mensagem de bloqueio da sessão
            showStatusMessage('Sessão bloqueada. Você já realizou um pedido.', 'info');
            redirectToWhatsAppWithDelay();
        } else {
            let errorMsg = result.error || 'Erro ao processar pedido.';
            const selectedServiceErr = (sessionStorage.getItem('oppus_servico') || urlParams.get('servico') || 'seguidores_mistos').toLowerCase();
            
            // Tratar erro específico do ggram.me
            if (!isPostService(selectedServiceErr) && result.error === 'link_duplicate') {
                errorMsg = 'Você acabou de realizar um pedido para este perfil. Aguarde alguns minutos antes de tentar novamente.';
            } else if (result.error === 'link_blocked') {
                errorMsg = 'Este link temporário já foi usado para um pedido. Links são válidos apenas para um pedido.';
                // Bloquear o botão permanentemente
                confirmButton.disabled = true;
                confirmButton.textContent = 'Link Já Usado';
                confirmButton.style.opacity = '0.6';
                // Salvar no localStorage para persistir o bloqueio
                localStorage.setItem('oppus_order_completed', 'true');
                localStorage.setItem('oppus_order_id', 'link_blocked');
                localStorage.setItem('oppus_order_timestamp', Date.now().toString());
            } else if (!isPostService(selectedServiceErr) && result.error === 'session_blocked') {
                errorMsg = 'Você já realizou um pedido nesta sessão. Não é permitido fazer múltiplos pedidos.';
                // Bloquear o botão permanentemente
                confirmButton.disabled = true;
                confirmButton.textContent = 'Pedido Realizado';
                confirmButton.style.opacity = '0.6';
                // Salvar no localStorage para persistir o bloqueio
                localStorage.setItem('oppus_order_completed', 'true');
                localStorage.setItem('oppus_order_id', 'blocked');
                localStorage.setItem('oppus_order_timestamp', Date.now().toString());
            } else if (!isPostService(selectedServiceErr) && result.error && result.error.includes('duplicate')) {
                errorMsg = 'Este perfil já foi processado recentemente. Tente novamente em alguns minutos.';
            }
            
            showToast('❌ ' + errorMsg, 'error');
            showStatusMessage(errorMsg, 'error');
        }
    } catch (error) {
        hideLoadingOverlay();
        showToast('Erro ao enviar pedido. Tente novamente.', 'error');
        showStatusMessage('Erro ao enviar pedido. Tente novamente.', 'error');
    }
}

async function checkUsageBlock() {
    try {
        const response = await fetch('/api/check-usage', { method: 'POST' });
        const data = await response.json();
        if (data.used) {
            usernameInput.disabled = true;
            checkButton.disabled = true;
            showStatusMessage(data.message || 'Já há registro de utilização para este IP e navegador.', 'error');
        }
    } catch (e) {
        // Ignorar erro silenciosamente
    }
}

// Verificar se o link atual já foi usado
async function checkLinkStatus() {
    try {
        // Obter o id da URL atual
        const pathParts = window.location.pathname.split('/');
        const idFromPath = pathParts[2]; // /perfil/:id -> pathParts[2] é o id
        const urlParams = new URLSearchParams(window.location.search);
        const idFromQuery = urlParams.get('id');
        const linkId = idFromPath || idFromQuery;
        
        if (linkId && linkId !== 'teste123') {
            const response = await fetch(`/api/check-link-status?id=${linkId}`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' }
            });
            const data = await response.json();
            
            if (data.blocked) {
                console.log('🔒 Link já foi usado para um pedido:', linkId);
                confirmButton.disabled = true;
                confirmButton.textContent = 'Link Já Usado';
                confirmButton.style.opacity = '0.6';
                showStatusMessage('Este link temporário já foi usado para um pedido. Links são válidos apenas para um pedido.', 'error');
                
                // Salvar no localStorage
                localStorage.setItem('oppus_order_completed', 'true');
                localStorage.setItem('oppus_order_id', 'link_blocked');
                localStorage.setItem('oppus_order_timestamp', Date.now().toString());
            }
        }
    } catch (error) {
        console.log('Erro ao verificar status do link:', error);
    }
}

// Verificar se há pedido já realizado nesta sessão
function checkOrderStatus() {
    const orderCompleted = localStorage.getItem('oppus_order_completed');
    const orderId = localStorage.getItem('oppus_order_id');
    const orderTimestamp = localStorage.getItem('oppus_order_timestamp');
    
    if (orderCompleted === 'true' && orderId) {
        console.log('🔒 Sessão bloqueada - pedido já realizado:', { orderId, orderTimestamp });
        
        // Se há um pedido realizado, bloquear o botão permanentemente
        confirmButton.disabled = true;
        if (orderId === 'link_blocked') {
            confirmButton.textContent = 'Link Já Usado';
            showStatusMessage('Este link temporário já foi usado para um pedido. Links são válidos apenas para um pedido.', 'error');
        } else {
            confirmButton.textContent = 'Pedido Realizado';
            showStatusMessage('Sessão bloqueada. Você já realizou um pedido.', 'info');
        }
        confirmButton.style.opacity = '0.6';
    } else {
        // Limpar dados antigos se não há pedido válido
        localStorage.removeItem('oppus_order_completed');
        localStorage.removeItem('oppus_order_username');
        localStorage.removeItem('oppus_order_id');
        localStorage.removeItem('oppus_order_timestamp');
    }
}

// Função para redirecionar automaticamente para o WhatsApp após pedido, com temporizador de 10 segundos
function redirectToWhatsAppWithDelay() {
    let seconds = 10;
    showStatusMessage(`Redirecionando para o WhatsApp em ${seconds} segundos...`, 'info');
    const interval = setInterval(() => {
        seconds--;
        showStatusMessage(`Redirecionando para o WhatsApp em ${seconds} segundos...`, 'info');
        if (seconds <= 0) {
            clearInterval(interval);
            window.location.href = 'http://wa.me/47997086876';
        }
    }, 1000);
}

// Função para obter fingerprint do navegador (igual backend)
function getBrowserFingerprint() {
    // Gere um hash base64 simples do userAgent
    return btoa(unescape(encodeURIComponent(navigator.userAgent))).substr(0, 20);
}

document.addEventListener('DOMContentLoaded', function() {
    console.log('Página de perfil carregada - Agência OPPUS');
    initializePage();
    setupEventListeners();
    const selectedServiceInit = (sessionStorage.getItem('oppus_servico') || new URLSearchParams(window.location.search).get('servico') || '').toLowerCase();
    if (!isPostService(selectedServiceInit)) {
        checkUsageBlock();
    }

    // Timer de 5 minutos para expirar a sessão e redirecionar
    setTimeout(() => {
        window.location.href = '/used.html';
    }, MAX_SESSION_TIME);
});
// Tutorial: controle de etapas
function showTutorialStep(step) {
    hideTutorial();
    const inputGroup = document.querySelector('.input-group');
    if (step === 1 && tutorialPop1) {
        tutorialPop1.style.display = 'flex';
        if (inputGroup) inputGroup.classList.add('tutorial-highlight');
    } else if (step === 2 && tutorialPop2) {
        tutorialPop2.style.display = 'flex';
        checkButton.classList.add('tutorial-highlight');
    } else if (step === 3 && tutorialPop3) {
        tutorialPop3.style.display = 'flex';
        confirmButton.classList.add('tutorial-highlight');
    }
}

function hideTutorial() {
    const inputGroup = document.querySelector('.input-group');
    if (tutorialPop1) tutorialPop1.style.display = 'none';
    if (tutorialPop2) tutorialPop2.style.display = 'none';
    if (tutorialPop3) tutorialPop3.style.display = 'none';
    if (inputGroup) inputGroup.classList.remove('tutorial-highlight');
    checkButton.classList.remove('tutorial-highlight');
    confirmButton.classList.remove('tutorial-highlight');
}
