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
            btn.setAttribute('aria-pressed', String(isLight));
            const label = btn.querySelector('.theme-label');
            if (label) label.textContent = isLight ? 'Tema: Escuro' : 'Tema: Claro';
        }
    };
    
    // Ler do localStorage ou usar light como default
    const savedTheme = localStorage.getItem('oppus_theme') || 'light';
    applyTheme(savedTheme);

    if (btn) {
        btn.addEventListener('click', () => {
            const currentTheme = document.body.classList.contains('theme-light') ? 'light' : 'dark';
            const next = currentTheme === 'light' ? 'dark' : 'light';
            localStorage.setItem('oppus_theme', next);
            applyTheme(next);
        });
    }
});