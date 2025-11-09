// Elementos DOM
const helpModal = document.getElementById('helpModal');
const loadingOverlay = document.getElementById('loadingOverlay');
const toastContainer = document.getElementById('toastContainer');

// Inicialização
document.addEventListener('DOMContentLoaded', function() {
    initializePage();
    setupEventListeners();
    logErrorAccess();
    console.log('❌ Página de Erro - Link Expirado/Inválido');
});

// Configurar event listeners
function setupEventListeners() {
    // Teclas globais
    document.addEventListener('keydown', handleGlobalKeydown);
    
    // Clique fora do modal para fechar
    helpModal.addEventListener('click', function(event) {
        if (event.target === helpModal) {
            closeHelp();
        }
    });
    
    // Prevenir scroll do body quando modal estiver aberto
    helpModal.addEventListener('show', function() {
        document.body.style.overflow = 'hidden';
    });
    
    helpModal.addEventListener('hide', function() {
        document.body.style.overflow = '';
    });
}

// Inicializar página
function initializePage() {
    // Verificar se há informações sobre o erro na URL
    const urlParams = new URLSearchParams(window.location.search);
    const reason = urlParams.get('reason');
    const linkId = urlParams.get('id');
    
    if (reason) {
        showErrorReason(reason);
    }
    
    if (linkId) {
        console.log(`Link ID inválido: ${linkId}`);
    }
    
    // Animar entrada da página
    setTimeout(() => {
        document.body.classList.add('loaded');
    }, 100);
}

// Manipular teclas globais
function handleGlobalKeydown(event) {
    // Escape para fechar modal
    if (event.key === 'Escape') {
        if (helpModal.style.display !== 'none') {
            closeHelp();
        }
    }
    
    // Enter para solicitar novo link
    if (event.key === 'Enter' && !helpModal.classList.contains('show')) {
        requestNewLink();
    }
    
    // H para mostrar ajuda
    if (event.key.toLowerCase() === 'h' && !helpModal.classList.contains('show')) {
        showHelp();
    }
    
    // Ctrl/Cmd + R para recarregar
    if ((event.ctrlKey || event.metaKey) && event.key === 'r') {
        event.preventDefault();
        showToast('Recarregar a página não resolverá o problema. Solicite um novo link.', 'warning');
    }
}

// Mostrar razão específica do erro
function showErrorReason(reason) {
    const reasons = {
        'expired': 'O link expirou após 10 minutos de inatividade.',
        'used': 'Este link já foi utilizado anteriormente.',
        'invalid_ip': 'O link foi acessado de um IP diferente do original.',
        'invalid_browser': 'O link foi acessado de um navegador diferente do original.',
        'not_found': 'O link não foi encontrado no sistema.',
        'malformed': 'O formato do link está incorreto.'
    };
    
    const message = reasons[reason] || 'Erro desconhecido ao validar o link.';
    
    setTimeout(() => {
        showToast(message, 'error');
    }, 1000);
}

// Solicitar novo link
async function requestNewLink() {
    console.log('🔗 Solicitando novo link...');
    
    showLoading();
    
    try {
        // Simular solicitação de novo link
        const response = await fetch('/generate', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                reason: 'link_expired',
                timestamp: new Date().toISOString()
            })
        });
        
        if (!response.ok) {
            throw new Error(`Erro na solicitação: ${response.status}`);
        }
        
        const data = await response.json();
        
        if (data.success) {
            hideLoading();
            showNewLinkModal(data);
        } else {
            throw new Error(data.error || 'Erro desconhecido');
        }
        
    } catch (error) {
        console.error('Erro ao solicitar novo link:', error);
        hideLoading();
        showToast('Erro ao gerar novo link. Tente novamente ou entre em contato.', 'error');
    }
}

// Mostrar modal com novo link
function showNewLinkModal(linkData) {
    const modal = document.createElement('div');
    modal.className = 'modal-overlay show';
    modal.innerHTML = `
        <div class="modal-content">
            <div class="modal-header">
                <h3>Novo Link Gerado</h3>
                <button class="modal-close" onclick="closeNewLinkModal()">
                    <svg width="24" height="24" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                        <line x1="18" y1="6" x2="6" y2="18"></line>
                        <line x1="6" y1="6" x2="18" y2="18"></line>
                    </svg>
                </button>
            </div>
            <div class="modal-body">
                <div class="new-link-content">
                    <div class="success-icon">
                        <svg width="48" height="48" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                            <path d="M9 12L11 14L15 10M21 12C21 16.9706 16.9706 21 12 21C7.02944 21 3 16.9706 3 12C3 7.02944 7.02944 3 12 3C16.9706 3 21 7.02944 21 12Z" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
                        </svg>
                    </div>
                    <h4>Link criado com sucesso!</h4>
                    <p>Seu novo link foi gerado e é válido por <strong>10 minutos</strong>.</p>
                    
                    <div class="link-info">
                        <div class="link-display">
                            <input type="text" id="newLinkInput" value="${window.location.origin}${linkData.url}" readonly>
                            <button onclick="copyNewLink()" class="copy-button">
                                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                                    <rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect>
                                    <path d="M5 15H4C3.46957 15 2.96086 14.7893 2.58579 14.4142C2.21071 14.0391 2 13.5304 2 13V4C2 3.46957 2.21071 2.96086 2.58579 2.58579C2.96086 2.21071 3.46957 2 4 2H13C13.5304 2 14.0391 2.21071 14.4142 2.58579C14.7893 2.96086 15 3.46957 15 4V5"></path>
                                </svg>
                                Copiar
                            </button>
                        </div>
                        
                        <div class="link-details">
                            <div class="detail">
                                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                                    <circle cx="12" cy="12" r="10"></circle>
                                    <polyline points="12,6 12,12 16,14"></polyline>
                                </svg>
                                <span>Expira em: ${linkData.expiresIn}</span>
                            </div>
                            <div class="detail">
                                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                                    <path d="M9 12L11 14L15 10M21 12C21 16.9706 16.9706 21 12 21C7.02944 21 3 16.9706 3 12C3 7.02944 7.02944 3 12 3C16.9706 3 21 7.02944 21 12Z"></path>
                                </svg>
                                <span>ID: ${linkData.id}</span>
                            </div>
                        </div>
                    </div>
                    
                    <div class="modal-actions">
                        <button onclick="accessNewLink('${linkData.url}')" class="primary-action">
                            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                                <path d="M18 13V19C18 19.5304 17.7893 20.0391 17.4142 20.4142C17.0391 20.7893 16.5304 21 16 21H5C4.46957 21 3.96086 20.7893 3.58579 20.4142C3.21071 20.0391 3 19.5304 3 19V8C3 7.46957 3.21071 6.96086 3.58579 6.58579C3.96086 6.21071 4.46957 6 5 6H11M15 3H21V9M10 14L21 3"></path>
                            </svg>
                            Acessar Agora
                        </button>
                        <button onclick="closeNewLinkModal()" class="secondary-action">Fechar</button>
                    </div>
                </div>
            </div>
        </div>
    `;
    
    modal.id = 'newLinkModal';
    document.body.appendChild(modal);
    
    // Focar no input do link
    setTimeout(() => {
        const input = document.getElementById('newLinkInput');
        if (input) {
            input.select();
        }
    }, 300);
    
    showToast('Novo link gerado com sucesso!', 'success');
}

// Copiar novo link
function copyNewLink() {
    const input = document.getElementById('newLinkInput');
    if (input) {
        input.select();
        document.execCommand('copy');
        showToast('Link copiado para a área de transferência!', 'success');
    }
}

// Acessar novo link
function accessNewLink(url) {
    showLoading();
    setTimeout(() => {
        window.location.href = url;
    }, 500);
}

// Fechar modal do novo link
function closeNewLinkModal() {
    const modal = document.getElementById('newLinkModal');
    if (modal) {
        modal.classList.remove('show');
        setTimeout(() => {
            modal.remove();
        }, 300);
    }
}

// Ir para página inicial
function goToHome() {
    console.log('🏠 Redirecionando para página inicial...');
    showLoading();
    
    setTimeout(() => {
        window.location.href = '/';
    }, 500);
}

// Mostrar ajuda
function showHelp() {
    console.log('❓ Mostrando ajuda...');
    helpModal.style.display = 'flex';
    setTimeout(() => {
        helpModal.classList.add('show');
    }, 50);
    
    // Focar no botão de fechar para acessibilidade
    setTimeout(() => {
        const closeButton = helpModal.querySelector('.modal-close');
        if (closeButton) {
            closeButton.focus();
        }
    }, 300);
}

// Fechar ajuda
function closeHelp() {
    helpModal.classList.remove('show');
    setTimeout(() => {
        helpModal.style.display = 'none';
    }, 300);
}

// Mostrar loading overlay
function showLoading() {
    loadingOverlay.classList.add('active');
}

// Esconder loading overlay
function hideLoading() {
    loadingOverlay.classList.remove('active');
}

// Registrar acesso à página de erro
function logErrorAccess() {
    const errorData = {
        timestamp: new Date().toISOString(),
        userAgent: navigator.userAgent,
        url: window.location.href,
        referrer: document.referrer,
        screenResolution: `${screen.width}x${screen.height}`,
        language: navigator.language
    };
    
    console.log('📊 Acesso à página de erro registrado:', errorData);
    
    // Enviar dados para analytics (opcional)
    // sendErrorAnalytics(errorData);
}

// Enviar dados de analytics (implementação futura)
async function sendErrorAnalytics(data) {
    try {
        await fetch('/api/analytics/error', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(data)
        });
    } catch (error) {
        console.log('Analytics não disponível:', error);
    }
}

// Mostrar toast notification
function showToast(message, type = 'info') {
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    
    const icons = {
        success: '<svg class="toast-icon" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg"><path d="M9 12L11 14L15 10M21 12C21 16.9706 16.9706 21 12 21C7.02944 21 3 16.9706 3 12C3 7.02944 7.02944 3 12 3C16.9706 3 21 7.02944 21 12Z" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>',
        error: '<svg class="toast-icon" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg"><circle cx="12" cy="12" r="10" stroke="currentColor" stroke-width="2"/><line x1="15" y1="9" x2="9" y2="15" stroke="currentColor" stroke-width="2"/><line x1="9" y1="9" x2="15" y2="15" stroke="currentColor" stroke-width="2"/></svg>',
        warning: '<svg class="toast-icon" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg"><path d="M10.29 3.86L1.82 18C1.64 18.37 1.54 18.78 1.54 19.2C1.54 20.22 2.36 21.04 3.38 21.04H20.62C21.64 21.04 22.46 20.22 22.46 19.2C22.46 18.78 22.36 18.37 22.18 18L13.71 3.86C13.32 3.12 12.68 2.75 12 2.75C11.32 2.75 10.68 3.12 10.29 3.86Z" stroke="currentColor" stroke-width="2"/><line x1="12" y1="9" x2="12" y2="13" stroke="currentColor" stroke-width="2"/><line x1="12" y1="17" x2="12.01" y2="17" stroke="currentColor" stroke-width="2"/></svg>',
        info: '<svg class="toast-icon" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg"><circle cx="12" cy="12" r="10" stroke="currentColor" stroke-width="2"/><path d="M12 16V12" stroke="currentColor" stroke-width="2"/><path d="M12 8H12.01" stroke="currentColor" stroke-width="2"/></svg>'
    };
    
    toast.innerHTML = `
        ${icons[type] || icons.info}
        <span>${message}</span>
    `;
    
    toastContainer.appendChild(toast);
    
    // Mostrar toast
    setTimeout(() => {
        toast.classList.add('show');
    }, 50);
    
    // Remover toast após 5 segundos
    setTimeout(() => {
        toast.classList.remove('show');
        setTimeout(() => {
            if (toast.parentElement) {
                toast.parentElement.removeChild(toast);
            }
        }, 300);
    }, 5000);
}

// Adicionar estilos para o modal do novo link
function addNewLinkModalStyles() {
    const style = document.createElement('style');
    style.textContent = `
        .new-link-content {
            text-align: center;
        }
        
        .success-icon {
            margin-bottom: 1rem;
        }
        
        .success-icon svg {
            color: #10b981;
        }
        
        .new-link-content h4 {
            font-size: 1.3rem;
            color: var(--text-primary);
            margin-bottom: 0.5rem;
        }
        
        .new-link-content p {
            color: var(--text-secondary);
            margin-bottom: 1.5rem;
        }
        
        .link-info {
            background: rgba(15, 15, 35, 0.6);
            border: 1px solid rgba(107, 70, 193, 0.2);
            border-radius: 12px;
            padding: 1.5rem;
            margin-bottom: 1.5rem;
        }
        
        .link-display {
            display: flex;
            gap: 0.5rem;
            margin-bottom: 1rem;
        }
        
        .link-display input {
            flex: 1;
            background: rgba(26, 26, 46, 0.8);
            border: 1px solid rgba(107, 70, 193, 0.3);
            border-radius: 8px;
            padding: 0.75rem;
            color: var(--text-primary);
            font-family: monospace;
            font-size: 0.9rem;
        }
        
        .copy-button {
            background: var(--primary-purple);
            border: none;
            border-radius: 8px;
            padding: 0.75rem 1rem;
            color: white;
            cursor: pointer;
            display: flex;
            align-items: center;
            gap: 0.5rem;
            transition: all 0.3s ease;
        }
        
        .copy-button:hover {
            background: var(--light-purple);
            transform: translateY(-2px);
        }
        
        .link-details {
            display: flex;
            justify-content: space-between;
            gap: 1rem;
        }
        
        .detail {
            display: flex;
            align-items: center;
            gap: 0.5rem;
            color: var(--text-secondary);
            font-size: 0.9rem;
        }
        
        .detail svg {
            color: var(--primary-purple);
        }
        
        .modal-actions {
            display: flex;
            gap: 1rem;
            justify-content: center;
        }
        
        .modal-actions .primary-action,
        .modal-actions .secondary-action {
            padding: 0.875rem 1.5rem;
            border-radius: 12px;
            font-weight: 500;
            cursor: pointer;
            transition: all 0.3s ease;
            border: none;
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }
        
        .modal-actions .primary-action {
            background: var(--instagram-gradient);
            color: white;
        }
        
        .modal-actions .primary-action:hover {
            transform: translateY(-2px);
            box-shadow: 0 8px 25px rgba(240, 148, 51, 0.3);
        }
        
        .modal-actions .secondary-action {
            background: rgba(107, 70, 193, 0.1);
            border: 1px solid rgba(107, 70, 193, 0.3);
            color: var(--text-primary);
        }
        
        .modal-actions .secondary-action:hover {
            background: rgba(107, 70, 193, 0.2);
            transform: translateY(-2px);
        }
        
        @media (max-width: 768px) {
            .link-display {
                flex-direction: column;
            }
            
            .link-details {
                flex-direction: column;
                gap: 0.5rem;
            }
            
            .modal-actions {
                flex-direction: column;
            }
        }
    `;
    document.head.appendChild(style);
}

// Inicializar estilos adicionais
addNewLinkModalStyles();

// Debug: Informações no console
console.log('🚫 Agência OPPUS - Página de Erro');
console.log('⌨️ Atalhos: Enter (novo link), H (ajuda), Esc (fechar modal)');
console.log('🔗 Use "Solicitar Novo Link" para gerar um novo acesso');

// Exportar funções para uso global
window.oppusError = {
    requestNewLink,
    goToHome,
    showHelp,
    closeHelp,
    showToast
};

