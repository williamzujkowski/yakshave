const CACHE_NAME = 'year-review-v1';
const ASSETS = [
  './',
  './2025/',
  './2025/index.html',
  './2025/summary.html',
  './2025/engineers.html',
  './2025/repos.html',
  './2025/leaderboards.html',
  './2025/awards.html',
  './2025/assets/css/style.css',
  './2025/assets/js/charts.js',
  './2025/assets/js/contributors.js'
];

// Install - cache assets
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME).then(cache => cache.addAll(ASSETS))
  );
});

// Fetch - cache-first strategy
self.addEventListener('fetch', event => {
  event.respondWith(
    caches.match(event.request).then(response => {
      return response || fetch(event.request).then(fetchResponse => {
        return caches.open(CACHE_NAME).then(cache => {
          cache.put(event.request, fetchResponse.clone());
          return fetchResponse;
        });
      });
    }).catch(() => caches.match('./2025/'))
  );
});

// Activate - clean old caches
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys => Promise.all(
      keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k))
    ))
  );
});
