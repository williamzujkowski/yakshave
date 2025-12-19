// Year Selector Dropdown Handler

(function() {
  'use strict';

  // Get elements
  const yearToggle = document.getElementById('yearToggle');
  const yearDropdown = document.getElementById('yearDropdown');

  if (!yearToggle || !yearDropdown) {
    return;
  }

  // Toggle dropdown
  function toggleDropdown() {
    const isExpanded = yearToggle.getAttribute('aria-expanded') === 'true';

    if (isExpanded) {
      closeDropdown();
    } else {
      openDropdown();
    }
  }

  // Open dropdown
  function openDropdown() {
    yearToggle.setAttribute('aria-expanded', 'true');
    yearDropdown.setAttribute('aria-hidden', 'false');

    // Add click listener to document to close on outside click
    setTimeout(() => {
      document.addEventListener('click', handleOutsideClick);
    }, 0);
  }

  // Close dropdown
  function closeDropdown() {
    yearToggle.setAttribute('aria-expanded', 'false');
    yearDropdown.setAttribute('aria-hidden', 'true');

    // Remove click listener
    document.removeEventListener('click', handleOutsideClick);
  }

  // Handle clicks outside dropdown
  function handleOutsideClick(event) {
    const isClickInside = yearToggle.contains(event.target) || yearDropdown.contains(event.target);

    if (!isClickInside) {
      closeDropdown();
    }
  }

  // Handle keyboard navigation
  function handleKeydown(event) {
    if (event.key === 'Escape') {
      closeDropdown();
      yearToggle.focus();
    }
  }

  // Event listeners
  yearToggle.addEventListener('click', toggleDropdown);
  yearDropdown.addEventListener('keydown', handleKeydown);

  // Close dropdown on page navigation
  yearDropdown.querySelectorAll('.year-option').forEach(option => {
    option.addEventListener('click', () => {
      closeDropdown();
    });
  });
})();
