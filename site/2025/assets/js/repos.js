/**
 * Repositories page functionality.
 *
 * Handles:
 * - Repository search and filtering
 * - Table sorting
 * - Repository detail modal
 * - Hygiene charts
 */

/**
 * Initialize repository table features.
 */
function initRepoTable(tableSelector, reposData) {
    const table = document.querySelector(tableSelector);
    if (!table) return;

    const tbody = table.querySelector('tbody');
    const searchInput = document.getElementById('repo-search');
    const healthFilter = document.getElementById('health-filter');
    const sortSelect = document.getElementById('sort-repos');

    let filteredData = [...reposData];

    // Search functionality
    if (searchInput) {
        searchInput.addEventListener('input', (e) => {
            const query = e.target.value.toLowerCase();
            filteredData = reposData.filter(r =>
                r.name.toLowerCase().includes(query) ||
                (r.full_name && r.full_name.toLowerCase().includes(query))
            );
            if (typeof updateUrlParam === 'function') {
                updateUrlParam('search', e.target.value || null);
            }
            renderTable();
        });
    }

    // Health filter
    if (healthFilter) {
        healthFilter.addEventListener('change', (e) => {
            const filterValue = e.target.value;
            if (filterValue === 'all') {
                filteredData = [...reposData];
            } else {
                filteredData = reposData.filter(r => r.health_status === filterValue);
            }
            if (typeof updateUrlParam === 'function') {
                updateUrlParam('filter', filterValue === 'all' ? null : filterValue);
            }
            renderTable();
        });
    }

    // Sort functionality
    if (sortSelect) {
        sortSelect.addEventListener('change', (e) => {
            const sortBy = e.target.value;
            filteredData.sort((a, b) => {
                const aVal = a[sortBy] || 0;
                const bVal = b[sortBy] || 0;
                return bVal - aVal;
            });
            renderTable();
        });
    }

    // Column sorting
    const sortableHeaders = table.querySelectorAll('th.sortable');
    sortableHeaders.forEach(header => {
        header.style.cursor = 'pointer';
        header.addEventListener('click', () => {
            const sortKey = header.getAttribute('data-sort');
            if (!sortKey) return;

            filteredData.sort((a, b) => {
                const aVal = a[sortKey] || 0;
                const bVal = b[sortKey] || 0;
                return bVal - aVal;
            });
            renderTable();
        });
    });

    function renderTable() {
        if (!tbody) return;

        tbody.innerHTML = '';
        filteredData.forEach(repo => {
            const row = document.createElement('tr');
            row.setAttribute('data-repo-id', repo.repo_id);

            const hygieneCategory = repo.hygiene_score_category || 'medium';
            const healthStatus = repo.health_status || 'healthy';

            row.innerHTML = `
                <td class="repo-name">
                    <div class="repo-info">
                        <a href="https://github.com/${repo.full_name}"
                           target="_blank"
                           rel="noopener noreferrer"
                           class="repo-link">
                            ${repo.name}
                        </a>
                        ${repo.is_private ? '<span class="badge badge-private">Private</span>' : ''}
                        ${repo.language ? `<span class="badge badge-language">${repo.language}</span>` : ''}
                    </div>
                </td>
                <td>
                    <div class="hygiene-score-cell">
                        <span class="score-badge score-${hygieneCategory}">
                            ${repo.hygiene_score || 0}
                        </span>
                        <div class="score-bar">
                            <div class="score-fill" style="width: ${repo.hygiene_score || 0}%"></div>
                        </div>
                    </div>
                </td>
                <td>${repo.prs_merged || 0}</td>
                <td>${repo.active_contributors_365d || 0}</td>
                <td>
                    <div class="progress-bar">
                        <div class="progress-fill" style="width: ${repo.review_coverage || 0}%"></div>
                        <span class="progress-text">${repo.review_coverage || 0}%</span>
                    </div>
                </td>
                <td>${repo.median_time_to_merge || 'N/A'}</td>
                <td>
                    <span class="status-badge status-${healthStatus}">
                        ${healthStatus.charAt(0).toUpperCase() + healthStatus.slice(1)}
                    </span>
                </td>
                <td>
                    <button class="btn-icon" onclick="showRepoDetails('${repo.repo_id}')" aria-label="View details">
                        <svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
                            <path d="M8 15A7 7 0 1 1 8 1a7 7 0 0 1 0 14zm0 1A8 8 0 1 0 8 0a8 8 0 0 0 0 16z"/>
                            <path d="M5.255 5.786a.237.237 0 0 0 .241.247h.825c.138 0 .248-.113.266-.25.09-.656.54-1.134 1.342-1.134.686 0 1.314.343 1.314 1.168 0 .635-.374.927-.965 1.371-.673.489-1.206 1.06-1.168 1.987l.003.217a.25.25 0 0 0 .25.246h.811a.25.25 0 0 0 .25-.25v-.105c0-.718.273-.927 1.01-1.486.609-.463 1.244-.977 1.244-2.056 0-1.511-1.276-2.241-2.673-2.241-1.267 0-2.655.59-2.75 2.286z"/>
                        </svg>
                    </button>
                </td>
            `;
            tbody.appendChild(row);
        });
    }

    renderTable();
}

/**
 * Show repository details modal.
 */
function showRepoDetails(repoId) {
    const modal = document.getElementById('repo-modal');
    const modalContent = document.getElementById('modal-repo-content');

    if (!modal || !modalContent) return;

    // Find repository data
    const repoRow = document.querySelector(`tr[data-repo-id="${repoId}"]`);
    if (!repoRow) return;

    const repoName = repoRow.querySelector('.repo-link')?.textContent || repoId;
    const repoLink = repoRow.querySelector('.repo-link')?.href || '';

    // Build modal content
    modalContent.innerHTML = `
        <div class="repo-detail-header">
            <h2>${repoName}</h2>
            <a href="${repoLink}" target="_blank" rel="noopener noreferrer">
                View on GitHub
            </a>
        </div>
        <div class="repo-detail-content">
            <p>Detailed repository metrics and analysis would appear here.</p>
            <p>This is a placeholder for future implementation.</p>
        </div>
    `;

    modal.style.display = 'block';
}

/**
 * Close repository details modal.
 */
function closeRepoModal() {
    const modal = document.getElementById('repo-modal');
    if (modal) {
        modal.style.display = 'none';
    }
}

/**
 * Render security policies chart.
 */
function renderSecurityPoliciesChart(selector, data) {
    if (!data) return;

    const container = document.querySelector(selector);
    if (!container) return;

    const theme = document.documentElement.getAttribute('data-theme') || 'light';
    const chart = new DonutChart(selector.replace('#', ''), {
        theme,
        height: 200,
        showLegend: false
    });

    const chartData = [
        { label: 'With SECURITY.md', value: data.security_md_count || 0 },
        { label: 'Without', value: (data.total_repos || 0) - (data.security_md_count || 0) }
    ].filter(d => d.value > 0);

    chart.render(chartData);
}

/**
 * Render CODEOWNERS chart.
 */
function renderCodeownersChart(selector, data) {
    if (!data) return;

    const container = document.querySelector(selector);
    if (!container) return;

    const theme = document.documentElement.getAttribute('data-theme') || 'light';
    const chart = new DonutChart(selector.replace('#', ''), {
        theme,
        height: 200,
        showLegend: false
    });

    const chartData = [
        { label: 'With CODEOWNERS', value: data.codeowners_count || 0 },
        { label: 'Without', value: (data.total_repos || 0) - (data.codeowners_count || 0) }
    ].filter(d => d.value > 0);

    chart.render(chartData);
}

/**
 * Render CI workflows chart.
 */
function renderCIWorkflowsChart(selector, data) {
    if (!data) return;

    const container = document.querySelector(selector);
    if (!container) return;

    const theme = document.documentElement.getAttribute('data-theme') || 'light';
    const chart = new DonutChart(selector.replace('#', ''), {
        theme,
        height: 200,
        showLegend: false
    });

    const chartData = [
        { label: 'With Workflows', value: data.ci_workflows_count || 0 },
        { label: 'Without', value: (data.total_repos || 0) - (data.ci_workflows_count || 0) }
    ].filter(d => d.value > 0);

    chart.render(chartData);
}

/**
 * Render documentation chart.
 */
function renderDocumentationChart(selector, data) {
    if (!data) return;

    const container = document.querySelector(selector);
    if (!container) return;

    const theme = document.documentElement.getAttribute('data-theme') || 'light';
    const chart = new DonutChart(selector.replace('#', ''), {
        theme,
        height: 200,
        showLegend: false
    });

    const chartData = [
        { label: 'With README', value: data.readme_count || 0 },
        { label: 'Without', value: (data.total_repos || 0) - (data.readme_count || 0) }
    ].filter(d => d.value > 0);

    chart.render(chartData);
}

/**
 * Render repository activity chart.
 */
function renderRepoActivity(selector, data) {
    if (!data || data.length === 0) return;

    const container = document.querySelector(selector);
    if (!container) return;

    const theme = document.documentElement.getAttribute('data-theme') || 'light';
    const chart = new TimeSeriesChart(selector.replace('#', ''), {
        theme,
        height: 300,
        showArea: true,
        xAxisLabel: 'Date',
        yAxisLabel: 'Activity'
    });

    // Transform data to expected format
    const chartData = data.map(d => ({
        date: new Date(d.date || d.week || d.month),
        value: d.count || d.value || 0
    }));

    chart.render(chartData);
}
