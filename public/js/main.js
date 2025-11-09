// Landing page sem vídeo: navegação por cards de serviço
document.addEventListener('DOMContentLoaded', function() {
    console.log('Página carregada - Agência OPPUS (landing com cards)');

    const cards = document.querySelectorAll('.service-card');
    cards.forEach(card => {
        card.addEventListener('click', function(e) {
            // Bloquear clique em cards desabilitados
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
});