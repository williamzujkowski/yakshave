/**
 * D3.js chart library for gh-year-end reports.
 *
 * This module provides reusable chart components for visualizing GitHub metrics:
 * - TimeSeriesChart: Line/area charts for activity over time
 * - BarChart: Horizontal bar charts for leaderboards
 * - HeatMap: Activity heat map (contribution-style calendar)
 * - GaugeChart: Semi-circular gauge for hygiene scores
 * - DonutChart: Donut chart for distribution breakdowns
 *
 * All charts support:
 * - Responsive sizing
 * - Dark/light mode color schemes
 * - Tooltips on hover
 * - Smooth transitions
 * - Legend support
 */

/**
 * Color schemes for charts (light and dark mode).
 */
const ColorSchemes = {
    light: {
        primary: '#0969da',
        secondary: '#1f883d',
        tertiary: '#bf3989',
        quaternary: '#fb8500',
        background: '#ffffff',
        text: '#1f2328',
        gridLines: '#d1d9e0',
        tooltip: '#24292f',
        tooltipText: '#ffffff',
        bars: ['#0969da', '#1f883d', '#bf3989', '#fb8500', '#6639ba'],
        gradient: ['#0969da', '#54aeff'],
        heatmap: ['#ebedf0', '#9be9a8', '#40c463', '#30a14e', '#216e39']
    },
    dark: {
        primary: '#539bf5',
        secondary: '#4ac26b',
        tertiary: '#e275ad',
        quaternary: '#fb8500',
        background: '#0d1117',
        text: '#e6edf3',
        gridLines: '#30363d',
        tooltip: '#484f58',
        tooltipText: '#e6edf3',
        bars: ['#539bf5', '#4ac26b', '#e275ad', '#fb8500', '#986ee2'],
        gradient: ['#539bf5', '#79c0ff'],
        heatmap: ['#161b22', '#0e4429', '#006d32', '#26a641', '#39d353']
    }
};

/**
 * Get the current color scheme based on theme.
 *
 * @param {string} theme - Theme name: 'light' or 'dark'
 * @returns {Object} Color scheme object
 */
function getColorScheme(theme = 'light') {
    return ColorSchemes[theme] || ColorSchemes.light;
}

/**
 * Base chart class with common functionality.
 */
class BaseChart {
    constructor(containerId, options = {}) {
        this.containerId = containerId;
        this.container = d3.select(`#${containerId}`);
        this.options = {
            theme: 'light',
            margin: { top: 20, right: 20, bottom: 40, left: 60 },
            ...options
        };
        this.colors = getColorScheme(this.options.theme);
        this.svg = null;
        this.tooltip = null;
    }

    /**
     * Get the dimensions of the chart area.
     */
    getDimensions() {
        const containerNode = this.container.node();
        const width = containerNode ? containerNode.clientWidth : 800;
        const height = this.options.height || 400;

        return {
            width,
            height,
            innerWidth: width - this.options.margin.left - this.options.margin.right,
            innerHeight: height - this.options.margin.top - this.options.margin.bottom
        };
    }

    /**
     * Initialize the SVG element.
     */
    initSVG() {
        const { width, height } = this.getDimensions();

        this.container.selectAll('*').remove();

        this.svg = this.container
            .append('svg')
            .attr('width', width)
            .attr('height', height);

        this.chartArea = this.svg
            .append('g')
            .attr('transform', `translate(${this.options.margin.left},${this.options.margin.top})`);

        return this.svg;
    }

    /**
     * Create or update the tooltip.
     */
    getTooltip() {
        if (!this.tooltip) {
            this.tooltip = d3.select('body')
                .append('div')
                .attr('class', 'chart-tooltip')
                .style('position', 'absolute')
                .style('visibility', 'hidden')
                .style('background-color', this.colors.tooltip)
                .style('color', this.colors.tooltipText)
                .style('padding', '8px 12px')
                .style('border-radius', '4px')
                .style('font-size', '12px')
                .style('pointer-events', 'none')
                .style('z-index', '1000')
                .style('box-shadow', '0 2px 8px rgba(0, 0, 0, 0.15)');
        }
        return this.tooltip;
    }

    /**
     * Show tooltip at specified position.
     */
    showTooltip(content, x, y) {
        const tooltip = this.getTooltip();
        tooltip
            .html(content)
            .style('visibility', 'visible')
            .style('left', `${x + 10}px`)
            .style('top', `${y - 10}px`);
    }

    /**
     * Hide tooltip.
     */
    hideTooltip() {
        if (this.tooltip) {
            this.tooltip.style('visibility', 'hidden');
        }
    }

    /**
     * Update theme and redraw chart.
     */
    updateTheme(theme) {
        this.options.theme = theme;
        this.colors = getColorScheme(theme);
        if (this.tooltip) {
            this.tooltip
                .style('background-color', this.colors.tooltip)
                .style('color', this.colors.tooltipText);
        }
    }

    /**
     * Destroy the chart and clean up.
     */
    destroy() {
        if (this.tooltip) {
            this.tooltip.remove();
            this.tooltip = null;
        }
        if (this.container) {
            this.container.selectAll('*').remove();
        }
    }
}

/**
 * Time series line/area chart.
 */
class TimeSeriesChart extends BaseChart {
    constructor(containerId, options = {}) {
        super(containerId, {
            showArea: true,
            showPoints: false,
            showPeakAnnotations: true,
            peakThreshold: 3,
            xAxisLabel: 'Date',
            yAxisLabel: 'Count',
            ...options
        });
    }

    /**
     * Find peak values in the data.
     * Returns the top N peaks based on value.
     *
     * @param {Array} data - Array of {date: Date, value: number}
     * @param {number} topN - Number of peaks to find (default: 3)
     * @returns {Array} Array of peak data points
     */
    findPeaks(data, topN = 3) {
        if (!data || data.length === 0) return [];

        // Sort by value descending and take top N
        return [...data]
            .sort((a, b) => b.value - a.value)
            .slice(0, topN)
            .sort((a, b) => a.date - b.date); // Re-sort by date for display
    }

    /**
     * Render the time series chart.
     *
     * @param {Array} data - Array of {date: Date, value: number}
     */
    render(data) {
        if (!data || data.length === 0) {
            this.container.html('<p style="text-align: center; color: #888;">No data available</p>');
            return;
        }

        this.initSVG();
        const { innerWidth, innerHeight } = this.getDimensions();

        // Scales
        const xScale = d3.scaleTime()
            .domain(d3.extent(data, d => d.date))
            .range([0, innerWidth]);

        const yScale = d3.scaleLinear()
            .domain([0, d3.max(data, d => d.value)])
            .nice()
            .range([innerHeight, 0]);

        // Axes
        const xAxis = d3.axisBottom(xScale)
            .ticks(6)
            .tickFormat(d3.timeFormat('%b %d'));

        const yAxis = d3.axisLeft(yScale)
            .ticks(5);

        this.chartArea.append('g')
            .attr('class', 'x-axis')
            .attr('transform', `translate(0,${innerHeight})`)
            .call(xAxis)
            .selectAll('text')
            .style('fill', this.colors.text);

        this.chartArea.append('g')
            .attr('class', 'y-axis')
            .call(yAxis)
            .selectAll('text')
            .style('fill', this.colors.text);

        // Style axes
        this.chartArea.selectAll('.domain, .tick line')
            .style('stroke', this.colors.gridLines);

        // Grid lines
        this.chartArea.append('g')
            .attr('class', 'grid')
            .attr('opacity', 0.1)
            .call(d3.axisLeft(yScale)
                .ticks(5)
                .tickSize(-innerWidth)
                .tickFormat('')
            )
            .select('.domain')
            .remove();

        // Area
        if (this.options.showArea) {
            const area = d3.area()
                .x(d => xScale(d.date))
                .y0(innerHeight)
                .y1(d => yScale(d.value))
                .curve(d3.curveMonotoneX);

            this.chartArea.append('path')
                .datum(data)
                .attr('class', 'area')
                .attr('fill', this.colors.primary)
                .attr('fill-opacity', 0.2)
                .attr('d', area);
        }

        // Line
        const line = d3.line()
            .x(d => xScale(d.date))
            .y(d => yScale(d.value))
            .curve(d3.curveMonotoneX);

        this.chartArea.append('path')
            .datum(data)
            .attr('class', 'line')
            .attr('fill', 'none')
            .attr('stroke', this.colors.primary)
            .attr('stroke-width', 2)
            .attr('d', line);

        // Points (optional)
        if (this.options.showPoints) {
            this.chartArea.selectAll('.point')
                .data(data)
                .join('circle')
                .attr('class', 'point')
                .attr('cx', d => xScale(d.date))
                .attr('cy', d => yScale(d.value))
                .attr('r', 3)
                .attr('fill', this.colors.primary)
                .attr('stroke', this.colors.background)
                .attr('stroke-width', 1.5);
        }

        // Interaction overlay
        const bisect = d3.bisector(d => d.date).left;
        const overlay = this.chartArea.append('rect')
            .attr('class', 'overlay')
            .attr('width', innerWidth)
            .attr('height', innerHeight)
            .attr('fill', 'none')
            .attr('pointer-events', 'all');

        const focus = this.chartArea.append('g')
            .attr('class', 'focus')
            .style('display', 'none');

        focus.append('line')
            .attr('class', 'x-hover-line')
            .attr('stroke', this.colors.gridLines)
            .attr('stroke-width', 1)
            .attr('stroke-dasharray', '3,3')
            .attr('y1', 0)
            .attr('y2', innerHeight);

        focus.append('circle')
            .attr('r', 4)
            .attr('fill', this.colors.primary)
            .attr('stroke', this.colors.background)
            .attr('stroke-width', 2);

        overlay
            .on('mouseover', () => focus.style('display', null))
            .on('mouseout', () => {
                focus.style('display', 'none');
                this.hideTooltip();
            })
            .on('mousemove', (event) => {
                const [xPos] = d3.pointer(event);
                const x0 = xScale.invert(xPos);
                const i = bisect(data, x0, 1);
                const d0 = data[i - 1];
                const d1 = data[i];
                const d = d1 && (x0 - d0.date > d1.date - x0) ? d1 : d0;

                focus.attr('transform', `translate(${xScale(d.date)},${yScale(d.value)})`);
                focus.select('.x-hover-line').attr('y2', innerHeight - yScale(d.value));

                const dateStr = d3.timeFormat('%b %d, %Y')(d.date);
                const tooltipContent = `<strong>${dateStr}</strong><br/>Value: ${d.value}`;
                this.showTooltip(tooltipContent, event.pageX, event.pageY);
            });

        // Peak annotations
        if (this.options.showPeakAnnotations && data.length > 0) {
            const peaks = this.findPeaks(data, this.options.peakThreshold);

            // Add annotation group
            const annotations = this.chartArea.append('g')
                .attr('class', 'peak-annotations');

            // Add annotation for each peak
            peaks.forEach((peak, index) => {
                const peakGroup = annotations.append('g')
                    .attr('class', `peak-annotation peak-${index + 1}`)
                    .attr('transform', `translate(${xScale(peak.date)},${yScale(peak.value)})`);

                // Peak marker circle
                peakGroup.append('circle')
                    .attr('r', 5)
                    .attr('fill', this.colors.tertiary)
                    .attr('stroke', this.colors.background)
                    .attr('stroke-width', 2)
                    .attr('class', 'peak-marker');

                // Peak value label (above the point)
                peakGroup.append('text')
                    .attr('y', -15)
                    .attr('text-anchor', 'middle')
                    .attr('fill', this.colors.text)
                    .attr('class', 'peak-label')
                    .style('font-size', '11px')
                    .style('font-weight', 'bold')
                    .style('pointer-events', 'none')
                    .text(peak.value);

                // Add small annotation badge
                peakGroup.append('rect')
                    .attr('x', -15)
                    .attr('y', -28)
                    .attr('width', 30)
                    .attr('height', 12)
                    .attr('rx', 3)
                    .attr('fill', this.colors.tertiary)
                    .attr('opacity', 0.15)
                    .attr('class', 'peak-badge');
            });
        }

        // Axis labels
        if (this.options.xAxisLabel) {
            this.chartArea.append('text')
                .attr('x', innerWidth / 2)
                .attr('y', innerHeight + 35)
                .attr('text-anchor', 'middle')
                .attr('fill', this.colors.text)
                .style('font-size', '12px')
                .text(this.options.xAxisLabel);
        }

        if (this.options.yAxisLabel) {
            this.chartArea.append('text')
                .attr('transform', 'rotate(-90)')
                .attr('x', -innerHeight / 2)
                .attr('y', -45)
                .attr('text-anchor', 'middle')
                .attr('fill', this.colors.text)
                .style('font-size', '12px')
                .text(this.options.yAxisLabel);
        }
    }
}

/**
 * Horizontal bar chart for leaderboards.
 */
class BarChart extends BaseChart {
    constructor(containerId, options = {}) {
        super(containerId, {
            xAxisLabel: 'Count',
            yAxisLabel: '',
            ...options
        });
    }

    /**
     * Render the bar chart.
     *
     * @param {Array} data - Array of {label: string, value: number, rank: number}
     */
    render(data) {
        if (!data || data.length === 0) {
            this.container.html('<p style="text-align: center; color: #888;">No data available</p>');
            return;
        }

        // Adjust height based on number of bars
        this.options.height = Math.max(300, data.length * 40 + 60);

        this.initSVG();
        const { innerWidth, innerHeight } = this.getDimensions();

        // Scales
        const xScale = d3.scaleLinear()
            .domain([0, d3.max(data, d => d.value)])
            .nice()
            .range([0, innerWidth]);

        const yScale = d3.scaleBand()
            .domain(data.map(d => d.label))
            .range([0, innerHeight])
            .padding(0.2);

        // Axes
        const xAxis = d3.axisBottom(xScale).ticks(5);

        this.chartArea.append('g')
            .attr('class', 'x-axis')
            .attr('transform', `translate(0,${innerHeight})`)
            .call(xAxis)
            .selectAll('text')
            .style('fill', this.colors.text);

        const yAxis = d3.axisLeft(yScale);

        this.chartArea.append('g')
            .attr('class', 'y-axis')
            .call(yAxis)
            .selectAll('text')
            .style('fill', this.colors.text)
            .style('font-size', '11px');

        // Style axes
        this.chartArea.selectAll('.domain, .tick line')
            .style('stroke', this.colors.gridLines);

        // Bars
        this.chartArea.selectAll('.bar')
            .data(data)
            .join('rect')
            .attr('class', 'bar')
            .attr('x', 0)
            .attr('y', d => yScale(d.label))
            .attr('width', d => xScale(d.value))
            .attr('height', yScale.bandwidth())
            .attr('fill', (d, i) => this.colors.bars[i % this.colors.bars.length])
            .attr('rx', 3)
            .on('mouseover', (event, d) => {
                const tooltipContent = `<strong>${d.label}</strong><br/>Rank: #${d.rank}<br/>Count: ${d.value}`;
                this.showTooltip(tooltipContent, event.pageX, event.pageY);
            })
            .on('mouseout', () => this.hideTooltip());

        // Value labels
        this.chartArea.selectAll('.value-label')
            .data(data)
            .join('text')
            .attr('class', 'value-label')
            .attr('x', d => xScale(d.value) + 5)
            .attr('y', d => yScale(d.label) + yScale.bandwidth() / 2)
            .attr('dy', '0.35em')
            .attr('fill', this.colors.text)
            .style('font-size', '11px')
            .text(d => d.value);

        // Axis labels
        if (this.options.xAxisLabel) {
            this.chartArea.append('text')
                .attr('x', innerWidth / 2)
                .attr('y', innerHeight + 35)
                .attr('text-anchor', 'middle')
                .attr('fill', this.colors.text)
                .style('font-size', '12px')
                .text(this.options.xAxisLabel);
        }
    }
}

/**
 * Activity heat map (contribution-style calendar).
 */
class HeatMap extends BaseChart {
    constructor(containerId, options = {}) {
        super(containerId, {
            cellSize: 12,
            cellPadding: 2,
            startDay: 0, // 0 = Sunday, 1 = Monday
            ...options
        });
    }

    /**
     * Render the heat map.
     *
     * @param {Array} data - Array of {date: Date, value: number}
     */
    render(data) {
        if (!data || data.length === 0) {
            this.container.html('<p style="text-align: center; color: #888;">No data available</p>');
            return;
        }

        // Create a map for quick lookup
        const dataMap = new Map(data.map(d => [d.date.toISOString().split('T')[0], d.value]));

        // Get date range
        const dates = data.map(d => d.date);
        const minDate = new Date(Math.min(...dates));
        const maxDate = new Date(Math.max(...dates));

        // Calculate dimensions
        const { cellSize, cellPadding } = this.options;
        const weeks = Math.ceil((maxDate - minDate) / (7 * 24 * 60 * 60 * 1000)) + 1;
        const width = weeks * (cellSize + cellPadding) + 80;
        const height = 7 * (cellSize + cellPadding) + 40;

        this.options.height = height;
        this.options.margin = { top: 20, right: 20, bottom: 20, left: 40 };

        this.initSVG();

        // Color scale
        const maxValue = d3.max(data, d => d.value);
        const colorScale = d3.scaleQuantize()
            .domain([0, maxValue])
            .range(this.colors.heatmap);

        // Generate all dates in range
        const allDates = [];
        const currentDate = new Date(minDate);
        while (currentDate <= maxDate) {
            allDates.push(new Date(currentDate));
            currentDate.setDate(currentDate.getDate() + 1);
        }

        // Group by week
        const weekData = d3.group(allDates, d => {
            const weekStart = new Date(d);
            weekStart.setDate(d.getDate() - d.getDay() + this.options.startDay);
            return weekStart.toISOString().split('T')[0];
        });

        const weeks_array = Array.from(weekData.entries()).sort((a, b) => a[0].localeCompare(b[0]));

        // Day labels
        const dayLabels = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
        this.chartArea.selectAll('.day-label')
            .data(dayLabels)
            .join('text')
            .attr('class', 'day-label')
            .attr('x', 0)
            .attr('y', (d, i) => i * (cellSize + cellPadding) + cellSize / 2)
            .attr('dy', '0.35em')
            .attr('text-anchor', 'end')
            .attr('dx', '-5')
            .attr('fill', this.colors.text)
            .style('font-size', '10px')
            .text(d => d);

        // Cells
        weeks_array.forEach((weekEntry, weekIndex) => {
            const days = weekEntry[1];

            this.chartArea.selectAll(`.cell-week-${weekIndex}`)
                .data(days)
                .join('rect')
                .attr('class', `cell cell-week-${weekIndex}`)
                .attr('x', weekIndex * (cellSize + cellPadding))
                .attr('y', d => d.getDay() * (cellSize + cellPadding))
                .attr('width', cellSize)
                .attr('height', cellSize)
                .attr('rx', 2)
                .attr('fill', d => {
                    const dateKey = d.toISOString().split('T')[0];
                    const value = dataMap.get(dateKey) || 0;
                    return colorScale(value);
                })
                .attr('stroke', this.colors.gridLines)
                .attr('stroke-width', 1)
                .on('mouseover', (event, d) => {
                    const dateKey = d.toISOString().split('T')[0];
                    const value = dataMap.get(dateKey) || 0;
                    const dateStr = d3.timeFormat('%b %d, %Y')(d);
                    const tooltipContent = `<strong>${dateStr}</strong><br/>Activity: ${value}`;
                    this.showTooltip(tooltipContent, event.pageX, event.pageY);
                })
                .on('mouseout', () => this.hideTooltip());
        });

        // Legend
        const legendWidth = 200;
        const legendHeight = 20;
        const legend = this.svg.append('g')
            .attr('class', 'legend')
            .attr('transform', `translate(${this.options.margin.left},${height - 30})`);

        legend.append('text')
            .attr('x', 0)
            .attr('y', -5)
            .attr('fill', this.colors.text)
            .style('font-size', '10px')
            .text('Less');

        this.colors.heatmap.forEach((color, i) => {
            legend.append('rect')
                .attr('x', 30 + i * 15)
                .attr('y', -15)
                .attr('width', 12)
                .attr('height', 12)
                .attr('fill', color)
                .attr('stroke', this.colors.gridLines)
                .attr('rx', 2);
        });

        legend.append('text')
            .attr('x', 30 + this.colors.heatmap.length * 15 + 5)
            .attr('y', -5)
            .attr('fill', this.colors.text)
            .style('font-size', '10px')
            .text('More');
    }
}

/**
 * Semi-circular gauge chart for scores (0-100).
 */
class GaugeChart extends BaseChart {
    constructor(containerId, options = {}) {
        super(containerId, {
            minValue: 0,
            maxValue: 100,
            ...options
        });
    }

    /**
     * Render the gauge chart.
     *
     * @param {number} value - Current value (0-100)
     * @param {string} label - Label to display
     */
    render(value, label = '') {
        this.initSVG();
        const { innerWidth, innerHeight } = this.getDimensions();

        const centerX = innerWidth / 2;
        const centerY = innerHeight - 20;
        const radius = Math.min(innerWidth, innerHeight * 1.5) / 2 - 20;

        // Arc generator
        const arc = d3.arc()
            .innerRadius(radius * 0.7)
            .outerRadius(radius)
            .startAngle(-Math.PI / 2)
            .cornerRadius(5);

        // Background arc
        this.chartArea.append('path')
            .datum({ endAngle: Math.PI / 2 })
            .attr('class', 'gauge-background')
            .attr('d', arc)
            .attr('fill', this.colors.gridLines)
            .attr('transform', `translate(${centerX},${centerY})`);

        // Value arc
        const valueAngle = -Math.PI / 2 + (Math.PI * value / this.options.maxValue);

        this.chartArea.append('path')
            .datum({ endAngle: valueAngle })
            .attr('class', 'gauge-value')
            .attr('d', arc)
            .attr('fill', value >= 80 ? this.colors.secondary :
                         value >= 50 ? this.colors.quaternary :
                         this.colors.tertiary)
            .attr('transform', `translate(${centerX},${centerY})`);

        // Value text
        this.chartArea.append('text')
            .attr('class', 'gauge-value-text')
            .attr('x', centerX)
            .attr('y', centerY - 10)
            .attr('text-anchor', 'middle')
            .attr('fill', this.colors.text)
            .style('font-size', '36px')
            .style('font-weight', 'bold')
            .text(value);

        // Label text
        if (label) {
            this.chartArea.append('text')
                .attr('class', 'gauge-label')
                .attr('x', centerX)
                .attr('y', centerY + 20)
                .attr('text-anchor', 'middle')
                .attr('fill', this.colors.text)
                .style('font-size', '14px')
                .text(label);
        }

        // Min/Max labels
        this.chartArea.append('text')
            .attr('x', centerX - radius)
            .attr('y', centerY + 30)
            .attr('text-anchor', 'start')
            .attr('fill', this.colors.text)
            .style('font-size', '10px')
            .text(this.options.minValue);

        this.chartArea.append('text')
            .attr('x', centerX + radius)
            .attr('y', centerY + 30)
            .attr('text-anchor', 'end')
            .attr('fill', this.colors.text)
            .style('font-size', '10px')
            .text(this.options.maxValue);
    }
}

/**
 * Donut chart for distribution breakdowns.
 */
class DonutChart extends BaseChart {
    constructor(containerId, options = {}) {
        super(containerId, {
            innerRadiusRatio: 0.6,
            showLegend: true,
            showLabels: true,
            ...options
        });
    }

    /**
     * Render the donut chart.
     *
     * @param {Array} data - Array of {label: string, value: number}
     */
    render(data) {
        if (!data || data.length === 0) {
            this.container.html('<p style="text-align: center; color: #888;">No data available</p>');
            return;
        }

        this.initSVG();
        const { innerWidth, innerHeight } = this.getDimensions();

        const centerX = innerWidth / 2;
        const centerY = innerHeight / 2;
        const radius = Math.min(innerWidth, innerHeight) / 2 - 40;
        const innerRadius = radius * this.options.innerRadiusRatio;

        // Pie layout
        const pie = d3.pie()
            .value(d => d.value)
            .sort(null);

        // Arc generator
        const arc = d3.arc()
            .innerRadius(innerRadius)
            .outerRadius(radius);

        const labelArc = d3.arc()
            .innerRadius(radius * 0.8)
            .outerRadius(radius * 0.8);

        // Color scale
        const colorScale = d3.scaleOrdinal()
            .domain(data.map(d => d.label))
            .range(this.colors.bars);

        // Draw slices
        const slices = this.chartArea.selectAll('.slice')
            .data(pie(data))
            .join('g')
            .attr('class', 'slice')
            .attr('transform', `translate(${centerX},${centerY})`);

        slices.append('path')
            .attr('d', arc)
            .attr('fill', d => colorScale(d.data.label))
            .attr('stroke', this.colors.background)
            .attr('stroke-width', 2)
            .on('mouseover', (event, d) => {
                const percent = ((d.value / d3.sum(data, d => d.value)) * 100).toFixed(1);
                const tooltipContent = `<strong>${d.data.label}</strong><br/>Count: ${d.data.value}<br/>Percent: ${percent}%`;
                this.showTooltip(tooltipContent, event.pageX, event.pageY);
            })
            .on('mouseout', () => this.hideTooltip());

        // Labels
        if (this.options.showLabels) {
            slices.append('text')
                .attr('transform', d => `translate(${labelArc.centroid(d)})`)
                .attr('text-anchor', 'middle')
                .attr('fill', this.colors.text)
                .style('font-size', '11px')
                .style('font-weight', 'bold')
                .text(d => {
                    const percent = (d.value / d3.sum(data, d => d.value)) * 100;
                    return percent > 5 ? `${percent.toFixed(0)}%` : '';
                });
        }

        // Legend
        if (this.options.showLegend) {
            const legend = this.svg.append('g')
                .attr('class', 'legend')
                .attr('transform', `translate(${innerWidth - 100},20)`);

            const legendItems = legend.selectAll('.legend-item')
                .data(data)
                .join('g')
                .attr('class', 'legend-item')
                .attr('transform', (d, i) => `translate(0,${i * 20})`);

            legendItems.append('rect')
                .attr('width', 12)
                .attr('height', 12)
                .attr('fill', d => colorScale(d.label))
                .attr('rx', 2);

            legendItems.append('text')
                .attr('x', 18)
                .attr('y', 6)
                .attr('dy', '0.35em')
                .attr('fill', this.colors.text)
                .style('font-size', '11px')
                .text(d => d.label);
        }

        // Center text (total)
        const total = d3.sum(data, d => d.value);
        this.chartArea.append('text')
            .attr('x', centerX)
            .attr('y', centerY - 5)
            .attr('text-anchor', 'middle')
            .attr('fill', this.colors.text)
            .style('font-size', '24px')
            .style('font-weight', 'bold')
            .text(total);

        this.chartArea.append('text')
            .attr('x', centerX)
            .attr('y', centerY + 15)
            .attr('text-anchor', 'middle')
            .attr('fill', this.colors.text)
            .style('font-size', '12px')
            .text('Total');
    }
}

/**
 * Render functions for Executive Summary charts.
 * These are wrapper functions that create chart instances and render data.
 */

/**
 * Render collaboration chart (time series).
 *
 * @param {string} selector - CSS selector for chart container
 * @param {Array} data - Array of {date, reviews, comments, cross_team}
 */
function renderCollaborationChart(selector, data) {
    if (!data || data.length === 0) {
        const container = d3.select(selector);
        container.html('<p style="text-align: center; color: #888; padding: 40px;">No data available</p>');
        return;
    }

    // Parse ISO date strings to Date objects
    const parsedData = data.map(d => ({
        date: new Date(d.date),
        value: (d.reviews || 0) + (d.comments || 0)
    }));

    const containerId = selector.replace('#', '');
    const chart = new TimeSeriesChart(containerId, {
        height: 300,
        yAxisLabel: 'Activity Count',
        xAxisLabel: 'Date'
    });

    chart.render(parsedData);
}

/**
 * Render velocity chart (time series).
 *
 * @param {string} selector - CSS selector for chart container
 * @param {Array} data - Array of {date, prs_opened, prs_merged, time_to_merge}
 */
function renderVelocityChart(selector, data) {
    if (!data || data.length === 0) {
        const container = d3.select(selector);
        container.html('<p style="text-align: center; color: #888; padding: 40px;">No data available</p>');
        return;
    }

    // Parse ISO date strings to Date objects
    const parsedData = data.map(d => ({
        date: new Date(d.date),
        value: d.prs_merged || 0
    }));

    const containerId = selector.replace('#', '');
    const chart = new TimeSeriesChart(containerId, {
        height: 300,
        yAxisLabel: 'PRs Merged',
        xAxisLabel: 'Date'
    });

    chart.render(parsedData);
}

/**
 * Render quality chart (bar chart of hygiene adoption rates).
 *
 * @param {string} selector - CSS selector for chart container
 * @param {Array} data - Array of {category, value} where value is percentage
 */
function renderQualityChart(selector, data) {
    if (!data || data.length === 0) {
        const container = d3.select(selector);
        container.html('<p style="text-align: center; color: #888; padding: 40px;">No data available</p>');
        return;
    }

    // Transform data to bar chart format
    const barData = data.map((d, i) => ({
        label: d.category,
        value: d.value,
        rank: i + 1
    }));

    const containerId = selector.replace('#', '');
    const chart = new BarChart(containerId, {
        height: 300,
        xAxisLabel: 'Adoption Rate (%)'
    });

    chart.render(barData);
}

/**
 * Render community chart (time series).
 *
 * @param {string} selector - CSS selector for chart container
 * @param {Array} data - Array of {date, active_contributors, new_contributors}
 */
function renderCommunityChart(selector, data) {
    if (!data || data.length === 0) {
        const container = d3.select(selector);
        container.html('<p style="text-align: center; color: #888; padding: 40px;">No data available</p>');
        return;
    }

    // Parse ISO date strings to Date objects
    const parsedData = data.map(d => ({
        date: new Date(d.date),
        value: d.active_contributors || 0
    }));

    const containerId = selector.replace('#', '');
    const chart = new TimeSeriesChart(containerId, {
        height: 300,
        yAxisLabel: 'Active Contributors',
        xAxisLabel: 'Date'
    });

    chart.render(parsedData);
}
