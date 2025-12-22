/**
 * Global search functionality across contributors and repositories.
 *
 * Handles:
 * - Building search index from page data
 * - Real-time search filtering
 * - Search results dropdown
 * - Keyboard navigation
 */

let searchIndex = [];
let searchInput = null;
let searchResults = null;

/**
 * Build search index from contributors and repos data.
 */
async function buildSearchIndex() {
    try {
        const dataPromises = [];

        // Try to fetch contributors data
        dataPromises.push(
            fetch('contributors.json')
                .then(res => res.ok ? res.json() : null)
                .catch(() => null)
        );

        // Try to fetch repos data
        dataPromises.push(
            fetch('repos.json')
                .then(res => res.ok ? res.json() : null)
                .catch(() => null)
        );

        const [contributorsData, reposData] = await Promise.all(dataPromises);

        // Add contributors to search index
        if (contributorsData && Array.isArray(contributorsData)) {
            contributorsData.forEach(contributor => {
                searchIndex.push({
                    type: 'contributor',
                    name: contributor.login || contributor.name || '',
                    url: `engineers.html?search=${encodeURIComponent(contributor.login || contributor.name || '')}`,
                    meta: `${contributor.total_prs || 0} PRs`,
                    searchText: (contributor.login || contributor.name || '').toLowerCase()
                });
            });
        }

        // Add repos to search index
        if (reposData && Array.isArray(reposData)) {
            reposData.forEach(repo => {
                const repoName = repo.repo_full_name || repo.repo || repo.name || '';
                searchIndex.push({
                    type: 'repository',
                    name: repoName,
                    url: `repos.html?search=${encodeURIComponent(repoName)}`,
                    meta: `${repo.prs_merged || repo.pr_count || 0} PRs`,
                    searchText: repoName.toLowerCase()
                });
            });
        }

        console.log(`Global search index built: ${searchIndex.length} items`);
    } catch (error) {
        console.error('Failed to build search index:', error);
    }
}

/**
 * Perform search and display results.
 */
function performSearch(query) {
    const trimmedQuery = query.toLowerCase().trim();

    if (trimmedQuery.length < 2) {
        searchResults.style.display = 'none';
        return;
    }

    // Filter search index
    const matches = searchIndex
        .filter(item => item.searchText.includes(trimmedQuery))
        .slice(0, 5);

    // Render results
    if (matches.length === 0) {
        searchResults.innerHTML = '<div class="search-no-results">No results found</div>';
    } else {
        searchResults.innerHTML = matches.map(match => `
            <a href="${match.url}" class="search-result-item">
                <span class="result-type">${match.type}</span>
                <span class="result-name">${escapeHtml(match.name)}</span>
                <span class="result-meta">${escapeHtml(match.meta)}</span>
            </a>
        `).join('');
    }

    searchResults.style.display = 'block';
}

/**
 * Escape HTML to prevent XSS.
 */
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

/**
 * Close search results dropdown.
 */
function closeSearchResults() {
    if (searchResults) {
        searchResults.style.display = 'none';
    }
}

/**
 * Initialize global search.
 */
function initGlobalSearch() {
    searchInput = document.getElementById('global-search');
    searchResults = document.getElementById('global-search-results');

    if (!searchInput || !searchResults) {
        return;
    }

    // Input event for real-time search
    searchInput.addEventListener('input', (e) => {
        performSearch(e.target.value);
    });

    // Focus event
    searchInput.addEventListener('focus', (e) => {
        if (e.target.value.trim().length >= 2) {
            performSearch(e.target.value);
        }
    });

    // Click outside to close
    document.addEventListener('click', (e) => {
        if (!searchInput.contains(e.target) && !searchResults.contains(e.target)) {
            closeSearchResults();
        }
    });

    // Keyboard shortcuts
    document.addEventListener('keydown', (e) => {
        // Focus search on '/'
        if (e.key === '/' && document.activeElement !== searchInput) {
            e.preventDefault();
            searchInput.focus();
        }

        // Close on Escape
        if (e.key === 'Escape' && document.activeElement === searchInput) {
            searchInput.blur();
            closeSearchResults();
        }
    });
}

// Initialize on DOM ready
document.addEventListener('DOMContentLoaded', async function() {
    await buildSearchIndex();
    initGlobalSearch();
});
