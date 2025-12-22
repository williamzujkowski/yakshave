/**
 * Leaderboards page functionality.
 *
 * Handles:
 * - Tab switching between leaderboard categories
 * - Contributor comparison tool
 */

/**
 * Initialize leaderboard tabs.
 */
function initLeaderboardTabs() {
    const tabButtons = document.querySelectorAll('.tab-button');
    const tabContents = document.querySelectorAll('.tab-content');

    tabButtons.forEach(button => {
        button.addEventListener('click', () => {
            const tabId = button.getAttribute('data-tab');

            // Remove active class from all buttons and contents
            tabButtons.forEach(btn => btn.classList.remove('active'));
            tabContents.forEach(content => content.classList.remove('active'));

            // Add active class to clicked button and corresponding content
            button.classList.add('active');
            const targetContent = document.getElementById(`tab-${tabId}`);
            if (targetContent) {
                targetContent.classList.add('active');
            }

            // Update URL parameter
            if (typeof updateUrlParam === 'function') {
                updateUrlParam('tab', tabId);
            }
        });
    });
}

/**
 * Initialize contributor comparison tool.
 */
function initComparisonTool(contributorsData) {
    const compareBtn = document.getElementById('compare-btn');
    const select1 = document.getElementById('compare-contributor-1');
    const select2 = document.getElementById('compare-contributor-2');
    const resultDiv = document.getElementById('comparison-result');

    if (!compareBtn || !select1 || !select2 || !resultDiv) return;

    compareBtn.addEventListener('click', () => {
        const userId1 = select1.value;
        const userId2 = select2.value;

        if (!userId1 || !userId2) {
            alert('Please select two contributors to compare');
            return;
        }

        if (userId1 === userId2) {
            alert('Please select different contributors');
            return;
        }

        const contributor1 = contributorsData.find(c => c.user_id === userId1);
        const contributor2 = contributorsData.find(c => c.user_id === userId2);

        if (!contributor1 || !contributor2) {
            alert('Could not find contributor data');
            return;
        }

        renderComparison(contributor1, contributor2, resultDiv);
    });
}

/**
 * Render comparison between two contributors.
 */
function renderComparison(contributor1, contributor2, container) {
    const metrics = [
        { key: 'prs_merged', label: 'PRs Merged' },
        { key: 'prs_opened', label: 'PRs Opened' },
        { key: 'reviews_submitted', label: 'Reviews Submitted' },
        { key: 'approvals', label: 'Approvals' },
        { key: 'issues_opened', label: 'Issues Opened' },
        { key: 'issues_closed', label: 'Issues Closed' },
        { key: 'comments_total', label: 'Comments Total' }
    ];

    let html = `
        <div class="comparison-header">
            <div class="comparison-contributor">
                <img src="${contributor1.avatar_url || 'https://github.com/identicons/' + contributor1.login + '.png'}"
                     alt="${contributor1.login}">
                <h3>${contributor1.login}</h3>
            </div>
            <div class="comparison-vs">vs</div>
            <div class="comparison-contributor">
                <img src="${contributor2.avatar_url || 'https://github.com/identicons/' + contributor2.login + '.png'}"
                     alt="${contributor2.login}">
                <h3>${contributor2.login}</h3>
            </div>
        </div>
        <div class="comparison-metrics">
    `;

    metrics.forEach(metric => {
        const val1 = contributor1[metric.key] || 0;
        const val2 = contributor2[metric.key] || 0;
        const max = Math.max(val1, val2, 1);
        const percent1 = (val1 / max) * 100;
        const percent2 = (val2 / max) * 100;

        html += `
            <div class="comparison-metric">
                <div class="comparison-metric-label">${metric.label}</div>
                <div class="comparison-bars">
                    <div class="comparison-bar comparison-bar-left">
                        <div class="comparison-bar-value">${val1}</div>
                        <div class="comparison-bar-fill" style="width: ${percent1}%"></div>
                    </div>
                    <div class="comparison-bar comparison-bar-right">
                        <div class="comparison-bar-fill" style="width: ${percent2}%"></div>
                        <div class="comparison-bar-value">${val2}</div>
                    </div>
                </div>
            </div>
        `;
    });

    html += '</div>';

    container.innerHTML = html;
    container.style.display = 'block';
}
