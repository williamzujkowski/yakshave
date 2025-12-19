# gh-year-end JavaScript Chart Library

This directory contains the D3.js-based chart library and application code for rendering GitHub Year-End Community Health Reports.

## Files

### `data.js` - Data Loading and Transformation Utilities

Provides functions to load JSON data files exported by the metrics engine and transform them into formats suitable for D3.js visualizations.

**Key Functions:**

- `loadJSON(url)` - Fetch and parse JSON data from a URL
- `transformTimeSeries(data, periodType)` - Transform time series data from columnar Parquet export format to chart-ready nested format
- `transformLeaderboard(data, topN)` - Transform leaderboard data with optional top-N filtering
- `transformHygieneScores(data)` - Transform hygiene scores into array format
- `getMetricKeys(data)` - Extract unique metric keys
- `getRepositoryIds(data, metricKey)` - Extract repository IDs for a metric
- `getDateRange(timeSeries)` - Get min/max dates from time series
- `calculateStats(timeSeries)` - Calculate summary statistics (total, mean, median, min, max)

**Expected Data Formats:**

Time series (from `metrics_time_series.parquet`):
```javascript
{
  year: [2025, 2025, ...],
  period_type: ["week", "week", ...],
  period_start: ["2025-01-01", "2025-01-08", ...],
  period_end: ["2025-01-07", "2025-01-14", ...],
  scope: ["org", "org", ...],
  repo_id: [null, null, ...],
  metric_key: ["prs_opened", "prs_opened", ...],
  value: [42, 38, ...]
}
```

Leaderboards (from `metrics_leaderboard.parquet`):
```javascript
{
  year: [2025, 2025, ...],
  metric_key: ["prs_opened", "prs_opened", ...],
  scope: ["org", "org", ...],
  repo_id: [null, null, ...],
  user_id: ["user_1", "user_2", ...],
  value: [150, 120, ...],
  rank: [1, 2, ...]
}
```

Hygiene scores (from `metrics_hygiene_scores.parquet`):
```javascript
{
  repo_id: ["repo_1", "repo_2", ...],
  repo_full_name: ["org/repo1", "org/repo2", ...],
  year: [2025, 2025, ...],
  score: [85, 72, ...],
  has_readme: [true, true, ...],
  has_license: [true, false, ...],
  // ... other breakdown fields
  notes: ["missing LICENSE", "", ...]
}
```

### `charts.js` - D3.js Chart Components

Provides reusable chart classes for visualizing GitHub metrics.

**Chart Classes:**

#### `TimeSeriesChart`
Line/area charts for activity over time.

```javascript
const chart = new TimeSeriesChart('container-id', {
  theme: 'light',
  height: 300,
  showArea: true,
  showPoints: false,
  xAxisLabel: 'Week',
  yAxisLabel: 'Count'
});
chart.render(data); // data: [{date: Date, value: number}, ...]
```

#### `BarChart`
Horizontal bar charts for leaderboards.

```javascript
const chart = new BarChart('container-id', {
  theme: 'light',
  xAxisLabel: 'Count'
});
chart.render(data); // data: [{label: string, value: number, rank: number}, ...]
```

#### `HeatMap`
Activity heat map (contribution-style calendar).

```javascript
const chart = new HeatMap('container-id', {
  theme: 'light',
  cellSize: 12,
  cellPadding: 2
});
chart.render(data); // data: [{date: Date, value: number}, ...]
```

#### `GaugeChart`
Semi-circular gauge for scores (0-100).

```javascript
const chart = new GaugeChart('container-id', {
  theme: 'light',
  height: 200,
  minValue: 0,
  maxValue: 100
});
chart.render(value, label); // value: number, label: string
```

#### `DonutChart`
Donut chart for distribution breakdowns.

```javascript
const chart = new DonutChart('container-id', {
  theme: 'light',
  height: 300,
  innerRadiusRatio: 0.6,
  showLegend: true,
  showLabels: true
});
chart.render(data); // data: [{label: string, value: number}, ...]
```

**Common Features:**

- Responsive sizing (charts adapt to container width)
- Dark/light mode color schemes
- Interactive tooltips on hover
- Smooth transitions and animations
- Legend support
- Theme updating without full re-render

**Base Chart Methods:**

All charts inherit from `BaseChart` and support:

- `render(data)` - Render or update the chart
- `updateTheme(theme)` - Switch between 'light' and 'dark' themes
- `destroy()` - Clean up chart and remove tooltips

### `app.js` - Main Application

Main application code that initializes charts, handles view switching, theme toggling, and responsive resizing.

**Key Components:**

#### `AppState` Class
Manages global application state:

- Current theme (light/dark)
- Current view (overview/activity/contributors/hygiene)
- Current repository filter
- Loaded data
- Active chart instances

**Main Functions:**

- `initializeApp()` - Initialize app on page load
- `loadData()` - Load all JSON data files
- `renderCurrentView()` - Render charts based on current state
- `renderOverviewView()` - Render overview with summary stats
- `renderActivityView(repoId)` - Render detailed activity time series
- `renderContributorsView(repoId)` - Render contributor leaderboards
- `renderHygieneView()` - Render repository health scores

**Event Handlers:**

- Theme toggle button (`#theme-toggle`)
- View switcher buttons (`[data-view]`)
- Repository selector dropdown (`#repo-selector`)
- Window resize for responsive charts

## Usage

### HTML Structure

Your HTML page should include:

```html
<!DOCTYPE html>
<html lang="en" data-theme="light">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>GitHub Year-End Report</title>
  <link rel="stylesheet" href="assets/css/styles.css">
</head>
<body>
  <!-- Theme toggle -->
  <button id="theme-toggle" aria-label="Toggle theme">🌙</button>

  <!-- View switcher -->
  <nav>
    <button data-view="overview" class="active">Overview</button>
    <button data-view="activity">Activity</button>
    <button data-view="contributors">Contributors</button>
    <button data-view="hygiene">Hygiene</button>
  </nav>

  <!-- Repository selector -->
  <select id="repo-selector">
    <option value="">All Repositories</option>
    <!-- Options populated dynamically -->
  </select>

  <!-- Chart containers -->
  <main class="content">
    <div id="chart-pr-activity"></div>
    <div id="chart-issue-activity"></div>
    <!-- More chart containers -->
  </main>

  <!-- Scripts -->
  <script src="https://d3js.org/d3.v7.min.js"></script>
  <script src="assets/js/data.js"></script>
  <script src="assets/js/charts.js"></script>
  <script src="assets/js/app.js"></script>
</body>
</html>
```

### Data Files

Place JSON data files in the `data/` directory relative to the HTML page:

- `data/metrics_time_series.json` - Time series metrics
- `data/metrics_leaderboard.json` - Leaderboard rankings
- `data/metrics_hygiene_scores.json` - Repository hygiene scores

These files are generated by the Python metrics engine and exported in columnar JSON format (matching Parquet structure).

### Standalone Usage

You can also use individual chart components without the full app:

```html
<div id="my-chart"></div>

<script src="https://d3js.org/d3.v7.min.js"></script>
<script src="assets/js/charts.js"></script>
<script>
  const data = [
    { date: new Date('2025-01-01'), value: 42 },
    { date: new Date('2025-01-08'), value: 38 },
    // ...
  ];

  const chart = new TimeSeriesChart('my-chart', {
    theme: 'light',
    height: 400
  });
  chart.render(data);
</script>
```

## Dependencies

- **D3.js v7** - Include from CDN or bundle locally:
  ```html
  <script src="https://d3js.org/d3.v7.min.js"></script>
  ```

No other external dependencies required. The library is designed to work offline if D3.js is bundled locally.

## Color Schemes

The library includes built-in color schemes for light and dark modes:

**Light Mode:**
- Primary: `#0969da` (GitHub blue)
- Secondary: `#1f883d` (GitHub green)
- Tertiary: `#bf3989` (GitHub pink)
- Quaternary: `#fb8500` (Orange)

**Dark Mode:**
- Primary: `#539bf5` (Light blue)
- Secondary: `#4ac26b` (Light green)
- Tertiary: `#e275ad` (Light pink)
- Quaternary: `#fb8500` (Orange)

Both schemes include heatmap gradients and multi-color palettes for bar charts and donut charts.

## Browser Support

- Modern browsers with ES6+ support
- Chrome 60+
- Firefox 60+
- Safari 12+
- Edge 79+

## Testing

Charts are tested through the smoke tests defined in issue #51. Run:

```bash
uv run gh-year-end all --config config/config.yaml
```

Then open the generated HTML report in a browser to verify all charts render correctly.

## Performance Notes

- Charts use D3's efficient data binding and transitions
- Time series data is sorted and indexed for fast lookups
- Tooltips are created once and reused across interactions
- Responsive resizing is debounced (250ms) to avoid excessive re-renders
- Chart instances are destroyed and recreated on view changes to prevent memory leaks

## Customization

### Adding New Chart Types

1. Extend `BaseChart` class in `charts.js`
2. Implement `render(data)` method
3. Use `this.colors` for theme-aware colors
4. Call `this.initSVG()` to set up chart area
5. Use `this.showTooltip()` and `this.hideTooltip()` for interactions

### Adding New Metrics

1. Add metric key to appropriate data transformation function in `data.js`
2. Add chart container to HTML template
3. Add rendering logic to appropriate view function in `app.js`

### Customizing Colors

Modify `ColorSchemes` object in `charts.js`:

```javascript
const ColorSchemes = {
  light: {
    primary: '#your-color',
    // ...
  },
  dark: {
    primary: '#your-color',
    // ...
  }
};
```

## License

Part of the gh-year-end project. See main repository LICENSE file.
