/**
 * URL State Management - Shareable Deep Links
 *
 * Enables sharing links to specific filtered views using URL query parameters.
 * Examples:
 * - engineers.html?search=william
 * - repos.html?filter=healthy
 * - leaderboards.html?tab=prs
 */

/**
 * Read URL parameters on page load.
 * @returns {Object} Object containing all URL query parameters
 */
function getUrlParams() {
  return Object.fromEntries(new URLSearchParams(window.location.search));
}

/**
 * Update URL parameter without page reload.
 * @param {string} key - Parameter key
 * @param {string|null} value - Parameter value (null to delete)
 */
function updateUrlParam(key, value) {
  const url = new URL(window.location);
  if (value) {
    url.searchParams.set(key, value);
  } else {
    url.searchParams.delete(key);
  }
  window.history.replaceState({}, '', url);
}

/**
 * Apply URL state on page load.
 * Restores search filters, dropdowns, and tab selections from URL parameters.
 */
document.addEventListener('DOMContentLoaded', function() {
  const params = getUrlParams();

  // Apply search param for contributors page
  if (params.search) {
    const searchInput = document.querySelector('#contributor-search, #repo-search');
    if (searchInput) {
      searchInput.value = params.search;
      searchInput.dispatchEvent(new Event('input'));
    }
  }

  // Apply filter param for repos page
  if (params.filter) {
    const filterSelect = document.querySelector('#health-filter');
    if (filterSelect) {
      filterSelect.value = params.filter;
      filterSelect.dispatchEvent(new Event('change'));
    }
  }

  // Apply tab param for leaderboards page
  if (params.tab) {
    const tabButton = document.querySelector(`[data-tab="${params.tab}"]`);
    if (tabButton) {
      tabButton.click();
    }
  }
});

/**
 * Copy current URL to clipboard (for share functionality).
 * @returns {Promise<boolean>} Success status
 */
async function copyShareLink() {
  try {
    await navigator.clipboard.writeText(window.location.href);
    return true;
  } catch (err) {
    console.error('Failed to copy link:', err);
    return false;
  }
}
