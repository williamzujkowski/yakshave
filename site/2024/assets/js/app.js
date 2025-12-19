/**
 * Main application for gh-year-end reports.
 *
 * This module initializes charts, handles view switching, theme toggling,
 * and responsive chart resizing.
 */

/**
 * Application state management.
 */
class AppState {
    constructor() {
        this.theme = 'light';
        this.currentView = 'overview';
        this.currentRepo = null;
        this.charts = new Map();
        this.data = {
            timeseries: null,
            leaderboards: null,
            hygieneScores: null
        };
    }

    /**
     * Set the current theme and update all charts.
     */
    setTheme(theme) {
        this.theme = theme;
        document.documentElement.setAttribute('data-theme', theme);

        // Update all active charts
        this.charts.forEach(chart => {
            if (chart && typeof chart.updateTheme === 'function') {
                chart.updateTheme(theme);
            }
        });

        // Store preference
        localStorage.setItem('gh-year-end-theme', theme);
    }

    /**
     * Get the stored theme preference or detect from system.
     */
    getInitialTheme() {
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
     * Register a chart instance.
     */
    registerChart(id, chart) {
        this.charts.set(id, chart);
    }

    /**
     * Destroy a chart instance.
     */
    destroyChart(id) {
        const chart = this.charts.get(id);
        if (chart && typeof chart.destroy === 'function') {
            chart.destroy();
        }
        this.charts.delete(id);
    }

    /**
     * Destroy all charts.
     */
    destroyAllCharts() {
        this.charts.forEach(chart => {
            if (chart && typeof chart.destroy === 'function') {
                chart.destroy();
            }
        });
        this.charts.clear();
    }

    /**
     * Set the current view.
     */
    setView(view) {
        this.currentView = view;
    }

    /**
     * Set the current repository filter.
     */
    setRepo(repoId) {
        this.currentRepo = repoId;
    }
}

/**
 * Global app state.
 */
const appState = new AppState();

/**
 * Initialize the application.
 */
async function initializeApp() {
    console.log('Initializing gh-year-end report application...');

    // Set initial theme
    const initialTheme = appState.getInitialTheme();
    appState.setTheme(initialTheme);
    updateThemeToggle(initialTheme);

    // Setup event listeners
    setupThemeToggle();
    setupViewSwitcher();
    setupRepoSelector();
    setupResponsiveCharts();

    // Load data
    try {
        await loadData();
        console.log('Data loaded successfully');

        // Render initial view
        renderCurrentView();
    } catch (error) {
        console.error('Failed to load data:', error);
        showError('Failed to load report data. Please check the console for details.');
    }
}

/**
 * Load all data files.
 */
async function loadData() {
    const basePath = './data';

    try {
        // Load time series data
        appState.data.timeseries = await loadJSON(`${basePath}/metrics_time_series.json`);
        console.log('Loaded time series data');
    } catch (error) {
        console.warn('Time series data not available:', error);
        appState.data.timeseries = null;
    }

    try {
        // Load leaderboard data
        appState.data.leaderboards = await loadJSON(`${basePath}/metrics_leaderboard.json`);
        console.log('Loaded leaderboard data');
    } catch (error) {
        console.warn('Leaderboard data not available:', error);
        appState.data.leaderboards = null;
    }

    try {
        // Load hygiene scores
        appState.data.hygieneScores = await loadJSON(`${basePath}/metrics_hygiene_scores.json`);
        console.log('Loaded hygiene scores data');
    } catch (error) {
        console.warn('Hygiene scores data not available:', error);
        appState.data.hygieneScores = null;
    }

    // Check if we have at least some data
    if (!appState.data.timeseries && !appState.data.leaderboards && !appState.data.hygieneScores) {
        throw new Error('No data files found');
    }
}

/**
 * Setup theme toggle functionality.
 */
function setupThemeToggle() {
    const themeToggle = document.getElementById('theme-toggle');
    if (!themeToggle) return;

    themeToggle.addEventListener('click', () => {
        const newTheme = appState.theme === 'light' ? 'dark' : 'light';
        appState.setTheme(newTheme);
        updateThemeToggle(newTheme);

        // Re-render current view with new theme
        renderCurrentView();
    });
}

/**
 * Update theme toggle button appearance.
 */
function updateThemeToggle(theme) {
    const themeToggle = document.getElementById('theme-toggle');
    if (!themeToggle) return;

    if (theme === 'dark') {
        themeToggle.innerHTML = '☀️';
        themeToggle.setAttribute('aria-label', 'Switch to light mode');
    } else {
        themeToggle.innerHTML = '🌙';
        themeToggle.setAttribute('aria-label', 'Switch to dark mode');
    }
}

/**
 * Setup view switcher functionality.
 */
function setupViewSwitcher() {
    const viewButtons = document.querySelectorAll('[data-view]');

    viewButtons.forEach(button => {
        button.addEventListener('click', () => {
            const view = button.getAttribute('data-view');

            // Update active state
            viewButtons.forEach(btn => btn.classList.remove('active'));
            button.classList.add('active');

            // Switch view
            appState.setView(view);
            renderCurrentView();
        });
    });
}

/**
 * Setup repository selector functionality.
 */
function setupRepoSelector() {
    const repoSelector = document.getElementById('repo-selector');
    if (!repoSelector) return;

    repoSelector.addEventListener('change', (event) => {
        const repoId = event.target.value || null;
        appState.setRepo(repoId);
        renderCurrentView();
    });
}

/**
 * Setup responsive chart resizing.
 */
function setupResponsiveCharts() {
    let resizeTimeout;

    window.addEventListener('resize', () => {
        clearTimeout(resizeTimeout);
        resizeTimeout = setTimeout(() => {
            renderCurrentView();
        }, 250);
    });
}

/**
 * Render the current view based on app state.
 */
function renderCurrentView() {
    const { currentView, currentRepo } = appState;

    // Clear existing charts
    appState.destroyAllCharts();

    // Route to appropriate render function
    switch (currentView) {
        case 'overview':
            renderOverviewView();
            break;
        case 'activity':
            renderActivityView(currentRepo);
            break;
        case 'contributors':
            renderContributorsView(currentRepo);
            break;
        case 'hygiene':
            renderHygieneView();
            break;
        default:
            console.warn(`Unknown view: ${currentView}`);
            renderOverviewView();
    }
}

/**
 * Render the overview view with summary statistics.
 */
function renderOverviewView() {
    console.log('Rendering overview view');

    // Transform data
    const timeseries = appState.data.timeseries
        ? transformTimeSeries(appState.data.timeseries, 'week')
        : null;

    if (!timeseries) {
        showError('No time series data available for overview');
        return;
    }

    const theme = appState.theme;

    // Render PR activity chart
    if (timeseries.prs_opened && timeseries.prs_opened.org.length > 0) {
        const prChart = new TimeSeriesChart('chart-pr-activity', {
            theme,
            height: 300,
            showArea: true,
            xAxisLabel: 'Week',
            yAxisLabel: 'PRs Opened'
        });
        prChart.render(timeseries.prs_opened.org);
        appState.registerChart('chart-pr-activity', prChart);
    }

    // Render issue activity chart
    if (timeseries.issues_opened && timeseries.issues_opened.org.length > 0) {
        const issueChart = new TimeSeriesChart('chart-issue-activity', {
            theme,
            height: 300,
            showArea: true,
            xAxisLabel: 'Week',
            yAxisLabel: 'Issues Opened'
        });
        issueChart.render(timeseries.issues_opened.org);
        appState.registerChart('chart-issue-activity', issueChart);
    }

    // Render top contributors
    if (appState.data.leaderboards) {
        const leaderboards = transformLeaderboard(appState.data.leaderboards, 5);

        if (leaderboards.prs_opened && leaderboards.prs_opened.org.length > 0) {
            const barData = leaderboards.prs_opened.org.map(entry => ({
                label: entry.user_id,
                value: entry.value,
                rank: entry.rank
            }));

            const contributorChart = new BarChart('chart-top-contributors', {
                theme,
                xAxisLabel: 'PRs Opened'
            });
            contributorChart.render(barData);
            appState.registerChart('chart-top-contributors', contributorChart);
        }
    }
}

/**
 * Render the activity view with detailed time series charts.
 */
function renderActivityView(repoId = null) {
    console.log('Rendering activity view', { repoId });

    const timeseries = appState.data.timeseries
        ? transformTimeSeries(appState.data.timeseries, 'week')
        : null;

    if (!timeseries) {
        showError('No time series data available for activity view');
        return;
    }

    const theme = appState.theme;
    const scope = repoId ? timeseries : timeseries;

    // Get appropriate data based on scope
    const getData = (metric) => {
        if (!metric) return [];
        return repoId && metric.repos[repoId]
            ? metric.repos[repoId]
            : metric.org;
    };

    // Render multiple time series charts
    const metrics = [
        { key: 'prs_opened', label: 'PRs Opened', containerId: 'chart-prs-opened' },
        { key: 'prs_merged', label: 'PRs Merged', containerId: 'chart-prs-merged' },
        { key: 'issues_opened', label: 'Issues Opened', containerId: 'chart-issues-opened' },
        { key: 'issues_closed', label: 'Issues Closed', containerId: 'chart-issues-closed' },
        { key: 'reviews_submitted', label: 'Reviews Submitted', containerId: 'chart-reviews' },
        { key: 'comments_total', label: 'Comments', containerId: 'chart-comments' }
    ];

    metrics.forEach(({ key, label, containerId }) => {
        const container = document.getElementById(containerId);
        if (!container) return;

        const data = getData(timeseries[key]);

        if (data && data.length > 0) {
            const chart = new TimeSeriesChart(containerId, {
                theme,
                height: 250,
                showArea: true,
                xAxisLabel: 'Week',
                yAxisLabel: label
            });
            chart.render(data);
            appState.registerChart(containerId, chart);
        } else {
            container.innerHTML = '<p style="text-align: center; color: #888;">No data available</p>';
        }
    });

    // Render heat map for commits
    if (timeseries.commits_count) {
        const commitData = getData(timeseries.commits_count);
        if (commitData && commitData.length > 0) {
            const heatMap = new HeatMap('chart-commit-heatmap', {
                theme,
                height: 200
            });
            heatMap.render(commitData);
            appState.registerChart('chart-commit-heatmap', heatMap);
        }
    }
}

/**
 * Render the contributors view with leaderboards.
 */
function renderContributorsView(repoId = null) {
    console.log('Rendering contributors view', { repoId });

    if (!appState.data.leaderboards) {
        showError('No leaderboard data available');
        return;
    }

    const leaderboards = transformLeaderboard(appState.data.leaderboards, 10);
    const theme = appState.theme;

    // Get appropriate data based on scope
    const getData = (metric) => {
        if (!metric) return [];
        return repoId && metric.repos[repoId]
            ? metric.repos[repoId]
            : metric.org;
    };

    // Render leaderboard charts
    const metrics = [
        { key: 'prs_opened', label: 'PRs Opened', containerId: 'chart-leaderboard-prs' },
        { key: 'prs_merged', label: 'PRs Merged', containerId: 'chart-leaderboard-merged' },
        { key: 'reviews_submitted', label: 'Reviews Submitted', containerId: 'chart-leaderboard-reviews' },
        { key: 'comments_total', label: 'Comments', containerId: 'chart-leaderboard-comments' }
    ];

    metrics.forEach(({ key, label, containerId }) => {
        const container = document.getElementById(containerId);
        if (!container) return;

        const entries = getData(leaderboards[key]);

        if (entries && entries.length > 0) {
            const barData = entries.map(entry => ({
                label: entry.user_id,
                value: entry.value,
                rank: entry.rank
            }));

            const chart = new BarChart(containerId, {
                theme,
                xAxisLabel: label
            });
            chart.render(barData);
            appState.registerChart(containerId, chart);
        } else {
            container.innerHTML = '<p style="text-align: center; color: #888;">No data available</p>';
        }
    });
}

/**
 * Render the hygiene view with repository health scores.
 */
function renderHygieneView() {
    console.log('Rendering hygiene view');

    if (!appState.data.hygieneScores) {
        showError('No hygiene score data available');
        return;
    }

    const hygieneData = transformHygieneScores(appState.data.hygieneScores);
    const theme = appState.theme;

    if (hygieneData.length === 0) {
        showError('No repositories with hygiene scores');
        return;
    }

    // Render score distribution donut chart
    const scoreRanges = [
        { label: 'Excellent (80-100)', min: 80, max: 100 },
        { label: 'Good (60-79)', min: 60, max: 79 },
        { label: 'Fair (40-59)', min: 40, max: 59 },
        { label: 'Poor (0-39)', min: 0, max: 39 }
    ];

    const distribution = scoreRanges.map(range => ({
        label: range.label,
        value: hygieneData.filter(d => d.score >= range.min && d.score <= range.max).length
    })).filter(d => d.value > 0);

    if (distribution.length > 0) {
        const donutChart = new DonutChart('chart-hygiene-distribution', {
            theme,
            height: 300
        });
        donutChart.render(distribution);
        appState.registerChart('chart-hygiene-distribution', donutChart);
    }

    // Render individual repository gauges (top 6 repos)
    const topRepos = hygieneData.slice(0, 6);
    topRepos.forEach((repo, index) => {
        const containerId = `chart-hygiene-repo-${index + 1}`;
        const container = document.getElementById(containerId);
        if (!container) return;

        const gauge = new GaugeChart(containerId, {
            theme,
            height: 200
        });
        gauge.render(repo.score, repo.repo_full_name);
        appState.registerChart(containerId, gauge);
    });

    // Render bar chart of all repo scores
    const barData = hygieneData.slice(0, 15).map((repo, index) => ({
        label: repo.repo_full_name,
        value: repo.score,
        rank: index + 1
    }));

    if (barData.length > 0) {
        const barChart = new BarChart('chart-hygiene-bars', {
            theme,
            xAxisLabel: 'Hygiene Score'
        });
        barChart.render(barData);
        appState.registerChart('chart-hygiene-bars', barChart);
    }
}

/**
 * Show an error message to the user.
 */
function showError(message) {
    console.error(message);

    // Find main content area
    const contentArea = document.querySelector('.content') || document.querySelector('main') || document.body;

    // Create error message
    const errorDiv = document.createElement('div');
    errorDiv.className = 'error-message';
    errorDiv.style.cssText = 'padding: 20px; margin: 20px; background-color: #fef2f2; color: #991b1b; border: 1px solid #fecaca; border-radius: 4px;';
    errorDiv.innerHTML = `<strong>Error:</strong> ${message}`;

    contentArea.insertBefore(errorDiv, contentArea.firstChild);
}

/**
 * Initialize the app when DOM is ready.
 */
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initializeApp);
} else {
    initializeApp();
}
