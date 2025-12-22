/**
 * Contributors page functionality.
 *
 * Handles:
 * - Contributor search and filtering
 * - Table sorting
 * - Contributor detail modal
 * - Sparkline rendering
 */

/**
 * Initialize contributor table features.
 */
function initContributorTable(tableSelector, contributorsData) {
    const table = document.querySelector(tableSelector);
    if (!table) return;

    const tbody = table.querySelector('tbody');
    const searchInput = document.getElementById('contributor-search');
    const metricFilter = document.getElementById('metric-filter');
    const sortBySelect = document.getElementById('sort-by');

    let filteredData = [...contributorsData];

    // Search functionality
    if (searchInput) {
        searchInput.addEventListener('input', (e) => {
            const query = e.target.value.toLowerCase();
            filteredData = contributorsData.filter(c =>
                c.login.toLowerCase().includes(query)
            );
            if (typeof updateUrlParam === 'function') {
                updateUrlParam('search', e.target.value || null);
            }
            renderTable();
        });
    }

    // Metric filter
    if (metricFilter) {
        metricFilter.addEventListener('change', () => {
            renderTable();
        });
    }

    // Sort functionality
    if (sortBySelect) {
        sortBySelect.addEventListener('change', (e) => {
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
        filteredData.forEach((contributor, index) => {
            const row = document.createElement('tr');
            row.setAttribute('data-user-id', contributor.user_id);

            row.innerHTML = `
                <td class="rank-cell">${index + 1}</td>
                <td class="contributor-cell">
                    <div class="contributor-mini">
                        <img src="${contributor.avatar_url || 'https://github.com/identicons/' + contributor.login + '.png'}"
                             alt="${contributor.login}"
                             class="mini-avatar"
                             loading="lazy">
                        <a href="https://github.com/${contributor.login}"
                           target="_blank"
                           rel="noopener noreferrer">
                            ${contributor.login}
                        </a>
                    </div>
                </td>
                <td>${contributor.prs_merged || 0}</td>
                <td>${contributor.prs_opened || 0}</td>
                <td>${contributor.reviews_submitted || 0}</td>
                <td>${contributor.approvals || 0}</td>
                <td>${contributor.issues_opened || 0}</td>
                <td>${contributor.issues_closed || 0}</td>
                <td>${contributor.comments_total || 0}</td>
                <td>
                    <div class="sparkline" data-values="${JSON.stringify(contributor.activity_timeline || [])}"></div>
                </td>
            `;
            tbody.appendChild(row);
        });

        // Render sparklines
        renderSparklines();
    }

    renderTable();
}

/**
 * Render sparklines for activity timeline.
 */
function renderSparklines() {
    const sparklines = document.querySelectorAll('.sparkline');
    sparklines.forEach(sparkline => {
        const values = JSON.parse(sparkline.getAttribute('data-values') || '[]');
        if (!values || values.length === 0) return;

        const width = 60;
        const height = 20;
        const max = Math.max(...values, 1);

        const points = values.map((val, i) => {
            const x = values.length === 1 ? width / 2 : (i / (values.length - 1)) * width;
            const y = height - (val / max) * height;
            return `${x},${y}`;
        }).filter(p => {
            const coords = p.split(',');
            return !isNaN(coords[0]) && !isNaN(coords[1]);
        }).join(' ');

        if (!points) return;

        sparkline.innerHTML = `
            <svg width="${width}" height="${height}" style="display: block;">
                <polyline
                    fill="none"
                    stroke="currentColor"
                    stroke-width="1.5"
                    points="${points}"
                />
            </svg>
        `;
    });
}

/**
 * Show contributor details modal.
 */
function showContributorDetails(userId) {
    const modal = document.getElementById('contributor-modal');
    const modalContent = document.getElementById('modal-contributor-content');

    if (!modal || !modalContent) return;

    // Find contributor data
    const contributorCard = document.querySelector(`[data-user-id="${userId}"]`);
    if (!contributorCard) return;

    const login = contributorCard.querySelector('.contributor-info .contributor-name a')?.textContent.trim() || userId;
    const avatar = contributorCard.querySelector('.contributor-header .contributor-avatar img')?.src || '';

    // Build modal content
    modalContent.innerHTML = `
        <div class="contributor-detail-header">
            <img src="${avatar}" alt="${login}" class="contributor-detail-avatar">
            <div>
                <h2>${login}</h2>
                <a href="https://github.com/${login}" target="_blank" rel="noopener noreferrer">
                    View GitHub Profile
                </a>
            </div>
        </div>
        <div class="contributor-detail-stats">
            <p>Detailed statistics and activity breakdown would appear here.</p>
            <p>This is a placeholder for future implementation.</p>
        </div>
    `;

    modal.style.display = 'block';
}

/**
 * Close contributor details modal.
 */
function closeContributorModal() {
    const modal = document.getElementById('contributor-modal');
    if (modal) {
        modal.style.display = 'none';
    }
}

/**
 * Render contribution timeline chart.
 */
function renderContributionTimeline(selector, data) {
    if (!data || data.length === 0) return;

    const container = document.querySelector(selector);
    if (!container) return;

    // Use existing TimeSeriesChart from charts.js
    const theme = document.documentElement.getAttribute('data-theme') || 'light';
    const chart = new TimeSeriesChart(selector.replace('#', ''), {
        theme,
        height: 300,
        showArea: true,
        xAxisLabel: 'Date',
        yAxisLabel: 'Contributions'
    });

    // Transform data to expected format
    const chartData = data.map(d => ({
        date: new Date(d.date || d.week || d.month),
        value: d.count || d.value || 0
    }));

    chart.render(chartData);
}

/**
 * Render contribution types chart.
 */
function renderContributionTypes(selector, data) {
    if (!data || data.length === 0) return;

    const container = document.querySelector(selector);
    if (!container) return;

    const theme = document.documentElement.getAttribute('data-theme') || 'light';
    const chart = new DonutChart(selector.replace('#', ''), {
        theme,
        height: 300,
        showLegend: true
    });

    // Transform data to expected format
    const chartData = data.map(d => ({
        label: d.type || d.label,
        value: d.count || d.value || 0
    }));

    chart.render(chartData);
}

/**
 * Render contribution by repo chart.
 */
function renderContributionByRepo(selector, data) {
    if (!data || data.length === 0) return;

    const container = document.querySelector(selector);
    if (!container) return;

    const theme = document.documentElement.getAttribute('data-theme') || 'light';
    const chart = new BarChart(selector.replace('#', ''), {
        theme,
        xAxisLabel: 'Contributions'
    });

    // Transform data to expected format
    const chartData = data.slice(0, 10).map((d, i) => ({
        label: d.repo || d.label,
        value: d.count || d.value || 0,
        rank: i + 1
    }));

    chart.render(chartData);
}
