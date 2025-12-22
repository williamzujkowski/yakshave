/**
 * Keyboard shortcuts for gh-year-end reports.
 * Provides navigation and interaction shortcuts.
 */

(function() {
    'use strict';

    /**
     * Show keyboard shortcuts help modal.
     */
    function showShortcutsModal() {
        const modal = document.getElementById('shortcuts-modal');
        if (modal) {
            modal.classList.add('open');
            modal.setAttribute('aria-hidden', 'false');

            // Focus the close button for accessibility
            const closeBtn = modal.querySelector('.modal-close');
            if (closeBtn) {
                closeBtn.focus();
            }
        }
    }

    /**
     * Close keyboard shortcuts help modal.
     */
    function closeShortcutsModal() {
        const modal = document.getElementById('shortcuts-modal');
        if (modal) {
            modal.classList.remove('open');
            modal.setAttribute('aria-hidden', 'true');
        }
    }

    /**
     * Handle keyboard shortcuts.
     */
    function handleKeydown(e) {
        // Don't trigger shortcuts if user is typing in an input field
        if (e.target.matches('input, textarea, select')) {
            return;
        }

        switch(e.key) {
            case '/':
                e.preventDefault();
                // Focus search input (try contributor search first, then repo search)
                const searchInput = document.querySelector('#contributor-search, #repo-search');
                if (searchInput) {
                    searchInput.focus();
                }
                break;

            case 'Escape':
                // Close any open modal
                const openModal = document.querySelector('.modal.open');
                if (openModal) {
                    openModal.classList.remove('open');
                    openModal.setAttribute('aria-hidden', 'true');
                }
                break;

            case '?':
                e.preventDefault();
                showShortcutsModal();
                break;

            case 'd':
                // Toggle dark mode
                const themeToggle = document.querySelector('.theme-toggle');
                if (themeToggle) {
                    themeToggle.click();
                }
                break;

            case 't':
            case 'Home':
                // Scroll to top
                window.scrollTo({ top: 0, behavior: 'smooth' });
                break;
        }
    }

    /**
     * Initialize keyboard shortcuts functionality.
     */
    function init() {
        // Setup keyboard event listener
        document.addEventListener('keydown', handleKeydown);

        // Setup modal close handlers
        const modal = document.getElementById('shortcuts-modal');
        if (modal) {
            // Close button
            const closeBtn = modal.querySelector('.modal-close');
            if (closeBtn) {
                closeBtn.addEventListener('click', closeShortcutsModal);
            }

            // Close on backdrop click
            modal.addEventListener('click', (e) => {
                if (e.target === modal) {
                    closeShortcutsModal();
                }
            });
        }
    }

    // Initialize when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }

    // Export for other modules
    window.keyboardShortcuts = {
        showHelp: showShortcutsModal,
        closeHelp: closeShortcutsModal
    };
})();
