// Landing page sem vídeo: navegação por cards de serviço
document.addEventListener('DOMContentLoaded', function() {
    console.log('Página carregada - Agência OPPUS');

    // Navegação por cards (landing)
    const cards = document.querySelectorAll('.service-card');
    cards.forEach(card => {
        card.addEventListener('click', function(e) {
            if (card.classList.contains('disabled')) {
                e.preventDefault();
                return false;
            }
            try {
                const servico = card.dataset.servico || new URL(card.href, window.location.origin).searchParams.get('servico');
                if (servico) {
                    sessionStorage.setItem('oppus_servico', servico);
                }
            } catch (err) {
                // Ignora erros de URL
            }
        });
    });

    // Tema escuro/claro com persistência
    const btn = document.getElementById('themeToggleBtn');
    const applyTheme = (theme) => {
        const isLight = theme === 'light';
        document.body.classList.toggle('theme-light', isLight);
        if (btn) {
            btn.textContent = isLight ? 'Tema: Escuro' : 'Tema: Claro';
            btn.setAttribute('aria-pressed', String(isLight));
        }
    };
    applyTheme('light');
    if (btn) {
        btn.addEventListener('click', () => {
            const next = document.body.classList.contains('theme-light') ? 'dark' : 'light';
            localStorage.setItem('oppus_theme', next);
            applyTheme(next);
        });
    }
});