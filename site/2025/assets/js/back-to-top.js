/**
 * Back to Top Button functionality
 *
 * Shows/hides the back-to-top button based on scroll position
 * and smoothly scrolls to the top when clicked.
 */

(function() {
    'use strict';

    // Get the button element
    const backToTopButton = document.getElementById('back-to-top');

    if (!backToTopButton) {
        console.warn('Back to top button not found');
        return;
    }

    // Scroll threshold in pixels
    const SCROLL_THRESHOLD = 300;

    /**
     * Show or hide the button based on scroll position
     */
    function toggleButtonVisibility() {
        const scrollTop = window.pageYOffset || document.documentElement.scrollTop;

        if (scrollTop > SCROLL_THRESHOLD) {
            backToTopButton.classList.add('visible');
        } else {
            backToTopButton.classList.remove('visible');
        }
    }

    /**
     * Scroll to the top of the page smoothly
     */
    function scrollToTop() {
        window.scrollTo({
            top: 0,
            behavior: 'smooth'
        });
    }

    // Add scroll event listener with throttling
    let scrollTimeout;
    window.addEventListener('scroll', function() {
        if (scrollTimeout) {
            window.cancelAnimationFrame(scrollTimeout);
        }

        scrollTimeout = window.requestAnimationFrame(function() {
            toggleButtonVisibility();
        });
    }, { passive: true });

    // Add click event listener
    backToTopButton.addEventListener('click', scrollToTop);

    // Check initial scroll position
    toggleButtonVisibility();

    // Support keyboard navigation
    backToTopButton.addEventListener('keydown', function(event) {
        if (event.key === 'Enter' || event.key === ' ') {
            event.preventDefault();
            scrollToTop();
        }
    });
})();
