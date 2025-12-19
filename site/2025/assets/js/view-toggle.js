/**
 * View toggle management for gh-year-end reports.
 * Handles switching between Executive and Engineer views.
 */

(function() {
    'use strict';

    /**
     * Get the stored view preference.
     */
    function getStoredView() {
        return localStorage.getItem('gh-year-end-view') || 'engineer';
    }

    /**
     * Apply view mode to the document.
     */
    function applyView(view) {
        document.body.setAttribute('data-view', view);
        localStorage.setItem('gh-year-end-view', view);

        // Update button states
        const viewButtons = document.querySelectorAll('.view-btn');
        viewButtons.forEach(btn => {
            const btnView = btn.getAttribute('data-view');
            if (btnView === view) {
                btn.classList.add('active');
            } else {
                btn.classList.remove('active');
            }
        });

        // Toggle visibility of view-specific elements
        const execElements = document.querySelectorAll('.exec-only');
        const engineerElements = document.querySelectorAll('.engineer-only');

        execElements.forEach(el => {
            el.style.display = view === 'exec' ? '' : 'none';
        });

        engineerElements.forEach(el => {
            el.style.display = view === 'engineer' ? '' : 'none';
        });
    }

    /**
     * Initialize view toggle functionality.
     */
    function init() {
        // Apply stored view
        const initialView = getStoredView();
        applyView(initialView);

        // Setup view toggle buttons
        const viewButtons = document.querySelectorAll('.view-btn');
        viewButtons.forEach(btn => {
            btn.addEventListener('click', () => {
                const view = btn.getAttribute('data-view');
                if (view) {
                    applyView(view);
                }
            });
        });
    }

    // Initialize when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }

    // Export for other modules
    window.viewManager = {
        getView: getStoredView,
        setView: applyView
    };
})();
