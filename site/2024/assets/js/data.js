/**
 * Data loading and transformation utilities for gh-year-end reports.
 *
 * This module provides functions to load JSON data files and transform them
 * into formats suitable for D3.js visualizations.
 */

/**
 * Extract the year from the current URL path.
 *
 * Expects URL format: /YYYY/index.html or /YYYY/engineer.html
 *
 * @returns {string|null} Year as a string (e.g., "2024"), or null if not found
 */
function getCurrentYearFromURL() {
    const pathname = window.location.pathname;
    // Match /YYYY/ pattern in the path
    const yearMatch = pathname.match(/\/(\d{4})\//);
    if (yearMatch) {
        return yearMatch[1];
    }
    console.warn(`Could not extract year from URL path: ${pathname}`);
    return null;
}

/**
 * Get the data directory path for the current year.
 *
 * @returns {string} Path to data directory (e.g., "/2024/data" or "../data" as fallback)
 */
function getDataPath() {
    const year = getCurrentYearFromURL();
    if (year) {
        // Construct path relative to site root
        return `/${year}/data`;
    }
    // Fallback to relative path if year cannot be determined
    console.warn("Using fallback data path: ../data");
    return "../data";
}

/**
 * Load JSON data from a URL.
 *
 * @param {string} url - URL or path to the JSON file
 * @returns {Promise<Object>} Parsed JSON data
 * @throws {Error} If the fetch fails or response is not OK
 */
async function loadJSON(url) {
    try {
        const response = await fetch(url);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        return await response.json();
    } catch (error) {
        console.error(`Failed to load JSON from ${url}:`, error);
        throw error;
    }
}

/**
 * Load a data file for the current year.
 *
 * @param {string} filename - Name of the JSON file (e.g., "metrics_time_series.json")
 * @returns {Promise<Object>} Parsed JSON data
 * @throws {Error} If the fetch fails or response is not OK
 */
async function loadYearData(filename) {
    const dataPath = getDataPath();
    const url = `${dataPath}/${filename}`;
    return loadJSON(url);
}

/**
 * Transform time series data from Parquet export format to chart-ready format.
 *
 * Input format (from metrics_time_series.parquet):
 * {
 *   year: [2025, 2025, ...],
 *   period_type: ["week", "week", ...],
 *   period_start: ["2025-01-01", "2025-01-08", ...],
 *   period_end: ["2025-01-07", "2025-01-14", ...],
 *   scope: ["org", "org", ...],
 *   repo_id: [null, null, ...],
 *   metric_key: ["prs_opened", "prs_opened", ...],
 *   value: [42, 38, ...]
 * }
 *
 * Output format:
 * {
 *   prs_opened: {
 *     org: [{date: Date, value: 42}, ...],
 *     repos: {
 *       "repo_id_1": [{date: Date, value: 10}, ...],
 *       "repo_id_2": [{date: Date, value: 15}, ...]
 *     }
 *   },
 *   prs_merged: { ... }
 * }
 *
 * @param {Object} data - Raw time series data in columnar format
 * @param {string} periodType - Filter by period type: "week" or "month"
 * @returns {Object} Transformed data grouped by metric and scope
 */
function transformTimeSeries(data, periodType = "week") {
    if (!data || !data.period_type) {
        console.warn("Invalid time series data format");
        return {};
    }

    const result = {};
    const rowCount = data.period_type.length;

    // Process each row
    for (let i = 0; i < rowCount; i++) {
        // Skip if not matching the requested period type
        if (data.period_type[i] !== periodType) {
            continue;
        }

        const metricKey = data.metric_key[i];
        const scope = data.scope[i];
        const repoId = data.repo_id[i];
        const value = data.value[i];

        // Parse period_start as the date
        const date = new Date(data.period_start[i]);

        // Initialize metric structure if needed
        if (!result[metricKey]) {
            result[metricKey] = {
                org: [],
                repos: {}
            };
        }

        // Add to appropriate scope
        if (scope === "org") {
            result[metricKey].org.push({ date, value });
        } else if (scope === "repo" && repoId) {
            if (!result[metricKey].repos[repoId]) {
                result[metricKey].repos[repoId] = [];
            }
            result[metricKey].repos[repoId].push({ date, value });
        }
    }

    // Sort all time series by date
    Object.keys(result).forEach(metricKey => {
        result[metricKey].org.sort((a, b) => a.date - b.date);
        Object.keys(result[metricKey].repos).forEach(repoId => {
            result[metricKey].repos[repoId].sort((a, b) => a.date - b.date);
        });
    });

    return result;
}

/**
 * Transform leaderboard data from Parquet export format to chart-ready format.
 *
 * Input format (from metrics_leaderboard.parquet):
 * {
 *   year: [2025, 2025, ...],
 *   metric_key: ["prs_opened", "prs_opened", ...],
 *   scope: ["org", "org", ...],
 *   repo_id: [null, null, ...],
 *   user_id: ["user_1", "user_2", ...],
 *   value: [150, 120, ...],
 *   rank: [1, 2, ...]
 * }
 *
 * Output format:
 * {
 *   prs_opened: {
 *     org: [{rank: 1, user_id: "user_1", value: 150}, ...],
 *     repos: {
 *       "repo_id_1": [{rank: 1, user_id: "user_3", value: 50}, ...],
 *       "repo_id_2": [{rank: 1, user_id: "user_5", value: 40}, ...]
 *     }
 *   },
 *   prs_merged: { ... }
 * }
 *
 * @param {Object} data - Raw leaderboard data in columnar format
 * @param {number} topN - Return only top N entries per leaderboard (default: 10)
 * @returns {Object} Transformed data grouped by metric and scope
 */
function transformLeaderboard(data, topN = 10) {
    if (!data || !data.metric_key) {
        console.warn("Invalid leaderboard data format");
        return {};
    }

    const result = {};
    const rowCount = data.metric_key.length;

    // Process each row
    for (let i = 0; i < rowCount; i++) {
        const metricKey = data.metric_key[i];
        const scope = data.scope[i];
        const repoId = data.repo_id[i];
        const userId = data.user_id[i];
        const value = data.value[i];
        const rank = data.rank[i];

        // Skip if rank is beyond topN
        if (rank > topN) {
            continue;
        }

        // Initialize metric structure if needed
        if (!result[metricKey]) {
            result[metricKey] = {
                org: [],
                repos: {}
            };
        }

        const entry = { rank, user_id: userId, value };

        // Add to appropriate scope
        if (scope === "org") {
            result[metricKey].org.push(entry);
        } else if (scope === "repo" && repoId) {
            if (!result[metricKey].repos[repoId]) {
                result[metricKey].repos[repoId] = [];
            }
            result[metricKey].repos[repoId].push(entry);
        }
    }

    // Sort all leaderboards by rank (should already be sorted, but ensure it)
    Object.keys(result).forEach(metricKey => {
        result[metricKey].org.sort((a, b) => a.rank - b.rank);
        Object.keys(result[metricKey].repos).forEach(repoId => {
            result[metricKey].repos[repoId].sort((a, b) => a.rank - b.rank);
        });
    });

    return result;
}

/**
 * Transform hygiene scores data from Parquet export format to chart-ready format.
 *
 * Input format (from metrics_hygiene_scores.parquet):
 * {
 *   repo_id: ["repo_1", "repo_2", ...],
 *   repo_full_name: ["org/repo1", "org/repo2", ...],
 *   year: [2025, 2025, ...],
 *   score: [85, 72, ...],
 *   has_readme: [true, true, ...],
 *   has_license: [true, false, ...],
 *   ...
 *   notes: ["missing LICENSE", "", ...]
 * }
 *
 * Output format:
 * [
 *   {
 *     repo_id: "repo_1",
 *     repo_full_name: "org/repo1",
 *     score: 85,
 *     breakdown: {
 *       has_readme: true,
 *       has_license: true,
 *       ...
 *     },
 *     notes: "missing LICENSE"
 *   },
 *   ...
 * ]
 *
 * @param {Object} data - Raw hygiene scores data in columnar format
 * @returns {Array} Array of repository hygiene score objects
 */
function transformHygieneScores(data) {
    if (!data || !data.repo_id) {
        console.warn("Invalid hygiene scores data format");
        return [];
    }

    const result = [];
    const rowCount = data.repo_id.length;

    // Breakdown fields to extract
    const breakdownFields = [
        'has_readme',
        'has_license',
        'has_contributing',
        'has_code_of_conduct',
        'has_security_md',
        'has_codeowners',
        'has_ci_workflows',
        'branch_protection_enabled',
        'requires_reviews',
        'dependabot_enabled',
        'secret_scanning_enabled'
    ];

    // Process each row
    for (let i = 0; i < rowCount; i++) {
        const breakdown = {};

        // Extract breakdown fields
        breakdownFields.forEach(field => {
            if (data[field] !== undefined) {
                breakdown[field] = data[field][i];
            }
        });

        result.push({
            repo_id: data.repo_id[i],
            repo_full_name: data.repo_full_name[i],
            score: data.score[i],
            breakdown: breakdown,
            notes: data.notes[i] || ""
        });
    }

    // Sort by score descending
    result.sort((a, b) => b.score - a.score);

    return result;
}

/**
 * Get unique metric keys from time series or leaderboard data.
 *
 * @param {Object} data - Transformed data object
 * @returns {Array<string>} Array of metric keys
 */
function getMetricKeys(data) {
    return Object.keys(data).sort();
}

/**
 * Get unique repository IDs from transformed data.
 *
 * @param {Object} data - Transformed data object (time series or leaderboard)
 * @param {string} metricKey - Metric key to extract repos from
 * @returns {Array<string>} Array of repository IDs
 */
function getRepositoryIds(data, metricKey) {
    if (!data[metricKey] || !data[metricKey].repos) {
        return [];
    }
    return Object.keys(data[metricKey].repos).sort();
}

/**
 * Get date range from time series data.
 *
 * @param {Array<Object>} timeSeries - Time series array [{date, value}, ...]
 * @returns {Object} Object with min and max dates: {min: Date, max: Date}
 */
function getDateRange(timeSeries) {
    if (!timeSeries || timeSeries.length === 0) {
        return { min: new Date(), max: new Date() };
    }

    const dates = timeSeries.map(d => d.date);
    return {
        min: new Date(Math.min(...dates)),
        max: new Date(Math.max(...dates))
    };
}

/**
 * Calculate summary statistics from time series data.
 *
 * @param {Array<Object>} timeSeries - Time series array [{date, value}, ...]
 * @returns {Object} Summary stats: {total, mean, median, min, max}
 */
function calculateStats(timeSeries) {
    if (!timeSeries || timeSeries.length === 0) {
        return { total: 0, mean: 0, median: 0, min: 0, max: 0 };
    }

    const values = timeSeries.map(d => d.value);
    const total = values.reduce((sum, v) => sum + v, 0);
    const mean = total / values.length;

    const sorted = [...values].sort((a, b) => a - b);
    const median = sorted.length % 2 === 0
        ? (sorted[sorted.length / 2 - 1] + sorted[sorted.length / 2]) / 2
        : sorted[Math.floor(sorted.length / 2)];

    return {
        total,
        mean: Math.round(mean * 100) / 100,
        median,
        min: Math.min(...values),
        max: Math.max(...values)
    };
}
