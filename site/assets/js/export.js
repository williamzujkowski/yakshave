/**
 * export.js - CSV export functionality for tables
 * Provides CSV download capability for data tables
 */

/**
 * Export a table to CSV format
 * @param {string|HTMLElement} tableSelector - CSS selector or table element
 * @param {string} filename - Output filename (default: 'export.csv')
 */
function exportTableToCSV(tableSelector, filename) {
  const table = typeof tableSelector === 'string'
    ? document.querySelector(tableSelector)
    : tableSelector;

  if (!table) {
    console.error('Table not found:', tableSelector);
    return;
  }

  const rows = table.querySelectorAll('tr');
  const csv = [];

  rows.forEach(row => {
    const cols = row.querySelectorAll('td, th');
    const rowData = [];

    cols.forEach(col => {
      // Clean text and escape quotes
      let text = col.textContent.trim().replace(/"/g, '""');
      // Remove extra whitespace and newlines
      text = text.replace(/\s+/g, ' ');
      rowData.push('"' + text + '"');
    });

    if (rowData.length > 0) {
      csv.push(rowData.join(','));
    }
  });

  const csvContent = csv.join('\n');
  const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
  const link = document.createElement('a');
  link.href = URL.createObjectURL(blob);
  link.download = filename || 'export.csv';
  link.style.display = 'none';
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(link.href);
}

/**
 * Initialize export buttons for all exportable tables
 */
function initializeExportButtons() {
  document.querySelectorAll('table[data-exportable]').forEach(table => {
    // Find or create wrapper
    let wrapper = table.closest('.table-wrapper');
    if (!wrapper) {
      wrapper = table.closest('.table-responsive');
    }
    if (!wrapper) {
      // Create wrapper if it doesn't exist
      wrapper = document.createElement('div');
      wrapper.className = 'table-wrapper';
      table.parentNode.insertBefore(wrapper, table);
      wrapper.appendChild(table);
    }

    // Check if export button already exists
    const existingBtn = wrapper.querySelector('.export-btn');
    if (existingBtn) {
      return;
    }

    // Create export button
    const btn = document.createElement('button');
    btn.className = 'btn btn-sm export-btn';
    btn.setAttribute('aria-label', 'Export table to CSV');
    btn.innerHTML = '<svg width="14" height="14" viewBox="0 0 16 16" fill="currentColor" aria-hidden="true" style="margin-right: 4px;"><path d="M8.5 1.5A1.5 1.5 0 0 0 7 0h-5a1.5 1.5 0 0 0-1.5 1.5v13A1.5 1.5 0 0 0 2 16h10a1.5 1.5 0 0 0 1.5-1.5v-10A1.5 1.5 0 0 0 12 3.5h-3.5v-2zM2 1h5a.5.5 0 0 1 .5.5V4H2V1zm10 4.5V15H2V5h9.5a.5.5 0 0 1 .5.5z"/></svg>Export CSV';

    const exportName = table.dataset.exportName || 'data.csv';
    btn.onclick = () => exportTableToCSV(table, exportName);

    // Insert button before table
    wrapper.insertBefore(btn, table);
  });
}

// Auto-initialize when DOM is ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initializeExportButtons);
} else {
  initializeExportButtons();
}
