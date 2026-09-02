document.addEventListener('DOMContentLoaded', () => {
  const track = document.getElementById('whyUsTrack');
  const dots = document.querySelectorAll('.why-us-dot');
  
  if (!track || dots.length === 0) return;

  let isScrolling = false;
  let autoPlayInterval;

  // Update active dot on scroll
  const updateActiveDot = () => {
    if (isScrolling) return;

    const scrollLeft = track.scrollLeft;
    const cardWidth = track.firstElementChild.offsetWidth;
    // Calculate index based on center of view
    const center = scrollLeft + (track.offsetWidth / 2);
    const index = Math.floor(center / (cardWidth + 24)); // 24 is gap (1.5rem)

    dots.forEach((dot, i) => {
      if (i === index) {
        dot.classList.add('active');
      } else {
        dot.classList.remove('active');
      }
    });
  };

  track.addEventListener('scroll', () => {
    window.requestAnimationFrame(updateActiveDot);
  });

  // Dot click handling
  dots.forEach((dot, index) => {
    dot.addEventListener('click', () => {
      isScrolling = true;
      const cardWidth = track.firstElementChild.offsetWidth;
      const gap = 24; // 1.5rem
      
      track.scrollTo({
        left: index * (cardWidth + gap),
        behavior: 'smooth'
      });

      dots.forEach(d => d.classList.remove('active'));
      dot.classList.add('active');

      setTimeout(() => {
        isScrolling = false;
      }, 500);
      
      // Reset autoplay on interaction
      stopAutoPlay();
      startAutoPlay();
    });
  });

  // Auto-play functionality
  const startAutoPlay = () => {
    autoPlayInterval = setInterval(() => {
      const cardWidth = track.firstElementChild.offsetWidth;
      const gap = 24;
      const maxScroll = track.scrollWidth - track.clientWidth;
      
      let nextScroll = track.scrollLeft + cardWidth + gap;
      
      if (nextScroll > maxScroll + 50) { // Buffer
        nextScroll = 0;
      }

      track.scrollTo({
        left: nextScroll,
        behavior: 'smooth'
      });
      
    }, 6000); // 6 seconds
  };

  const stopAutoPlay = () => {
    clearInterval(autoPlayInterval);
  };

  // Start autoplay
  startAutoPlay();

  // Pause on hover/touch
  track.addEventListener('mouseenter', stopAutoPlay);
  track.addEventListener('mouseleave', startAutoPlay);
  track.addEventListener('touchstart', stopAutoPlay, { passive: true });
  track.addEventListener('touchend', startAutoPlay);
});
