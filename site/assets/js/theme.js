/**
 * Theme management for gh-year-end reports.
 * Handles dark/light mode toggle and persistence.
 */

(function() {
    'use strict';

    /**
     * Get the stored theme preference or detect from system.
     */
    function getInitialTheme() {
        const stored = localStorage.getItem('gh-year-end-theme');
        if (stored) {
            return stored;
        }

        // Detect system preference
        if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
            return 'dark';
        }

        return 'light';
    }

    /**
     * Apply theme to the document.
     */
    function applyTheme(theme) {
        document.documentElement.setAttribute('data-theme', theme);
        localStorage.setItem('gh-year-end-theme', theme);

        // Update toggle button appearance
        const toggleBtn = document.querySelector('.theme-toggle');
        if (toggleBtn) {
            const sunIcon = toggleBtn.querySelector('.sun-icon');
            const moonIcon = toggleBtn.querySelector('.moon-icon');

            if (theme === 'dark') {
                if (sunIcon) sunIcon.style.display = 'block';
                if (moonIcon) moonIcon.style.display = 'none';
            } else {
                if (sunIcon) sunIcon.style.display = 'none';
                if (moonIcon) moonIcon.style.display = 'block';
            }
        }
    }

    /**
     * Toggle between light and dark themes.
     */
    function toggleTheme() {
        const currentTheme = document.documentElement.getAttribute('data-theme') || 'light';
        const newTheme = currentTheme === 'light' ? 'dark' : 'light';
        applyTheme(newTheme);
    }

    /**
     * Initialize theme functionality.
     */
    function init() {
        // Apply initial theme
        const initialTheme = getInitialTheme();
        applyTheme(initialTheme);

        // Setup toggle button
        const toggleBtn = document.querySelector('.theme-toggle');
        if (toggleBtn) {
            toggleBtn.addEventListener('click', toggleTheme);
        }

        // Listen for system preference changes
        if (window.matchMedia) {
            window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', (e) => {
                // Only auto-switch if user hasn't set a preference
                if (!localStorage.getItem('gh-year-end-theme')) {
                    applyTheme(e.matches ? 'dark' : 'light');
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
    window.themeManager = {
        getTheme: () => document.documentElement.getAttribute('data-theme') || 'light',
        setTheme: applyTheme,
        toggle: toggleTheme
    };
})();
