# Parallel Processing Implementation Guide

**Status**: Partially Implemented (Pulls collector complete, others pending)
**Actual Implementation**: Issue #100 - Parallel repo processing
**Estimated Impact**: 30-40% reduction in collection time
**Risk**: Medium - requires careful concurrency management

## Overview

This document provides implementation guidance for adding parallel processing to the collectors. As of the latest update, parallel repo-level processing has been implemented for the pulls collector. Further optimizations can add parallel processing within individual repos for reviews and comments.

## Implemented: Repo-Level Parallel Processing (Issue #100)

**Implementation Date**: 2025-12-18
**Files Modified**:
- `src/gh_year_end/collect/orchestrator.py` - Added `_collect_repos_parallel()` helper function
- `src/gh_year_end/collect/pulls.py` - Added `collect_single_repo_pulls()` function

### How It Works

Instead of processing repos sequentially:
```python
# OLD: Sequential
for repo in repos:
    await collect_pulls(repo, ...)
```

The new implementation processes multiple repos in parallel:
```python
# NEW: Parallel with semaphore control
await _collect_repos_parallel(
    repos=repos,
    collect_fn=collect_single_repo_pulls,
    endpoint_name="pulls",
    checkpoint=checkpoint,
    max_concurrency=config.rate_limit.max_concurrency,
    rest_client=rest_client,
    paths=paths,
    config=config,
)
```

### Key Features

1. **Semaphore Control**: Respects `max_concurrency` setting from config to prevent overwhelming the API
2. **Checkpoint Integration**: Each repo's completion is tracked independently for resume support
3. **Error Isolation**: One repo failure doesn't stop others (using `asyncio.gather` with `return_exceptions=True`)
4. **Stats Aggregation**: Automatically aggregates metrics (e.g., `pulls_collected`) from parallel tasks
5. **Reusable Pattern**: The `_collect_repos_parallel()` helper can be applied to other collectors

### Configuration

Uses existing `rate_limit.max_concurrency` setting:
```yaml
rate_limit:
  max_concurrency: 3  # Process up to 3 repos concurrently
```

### Next Steps

The same pattern can be applied to:
- Issues collector (`collect/issues.py`)
- Reviews collector (`collect/reviews.py`)
- Commits collector (`collect/commits.py`)
- Comments collectors (`collect/comments.py`)

## Future Enhancement: Per-Repo Parallel Processing

The sections below describe further optimizations that can be applied within individual repos for reviews and comments collection.

## Current Sequential Pattern

### Reviews Collector (`collect/reviews.py`)

```python
# Current: Sequential
for pr_number in pr_numbers:
    async for reviews, metadata in rest_client.list_reviews(owner, repo, pr_number):
        for review in reviews:
            await writer.write(...)
```

**Problem**: For a repo with 1,000 PRs, this makes 1,000 sequential API calls.

### Comments Collectors (`collect/comments.py`)

```python
# Current: Sequential
for issue_number in issue_numbers:
    async for comments_page, metadata in rest_client.list_issue_comments(...):
        for comment in comments_page:
            await writer.write(...)
```

**Problem**: Similar sequential bottleneck.

## Proposed Parallel Pattern

### Pattern 1: Semaphore-Controlled Parallelism

```python
async def _collect_reviews_parallel(
    repo_full_name: str,
    pr_numbers: list[int],
    rest_client: RestClient,
    writer: AsyncJSONLWriter,
    max_concurrent: int = 5,
) -> dict[str, int]:
    """Collect reviews for multiple PRs in parallel."""

    semaphore = asyncio.Semaphore(max_concurrent)
    stats = {"prs_processed": 0, "reviews_collected": 0, "errors": 0}
    stats_lock = asyncio.Lock()

    async def process_single_pr(pr_number: int) -> None:
        """Process a single PR with semaphore control."""
        async with semaphore:
            try:
                review_count = 0
                owner, repo = repo_full_name.split("/")

                async for reviews, metadata in rest_client.list_reviews(
                    owner, repo, pr_number
                ):
                    for review in reviews:
                        await writer.write(
                            source="github_rest",
                            endpoint=f"/repos/{owner}/{repo}/pulls/{pr_number}/reviews",
                            data=review,
                            page=metadata["page"],
                        )
                        review_count += 1

                # Thread-safe stats update
                async with stats_lock:
                    stats["prs_processed"] += 1
                    stats["reviews_collected"] += review_count

            except Exception as e:
                logger.error("Error processing PR %s#%d: %s", repo_full_name, pr_number, e)
                async with stats_lock:
                    stats["errors"] += 1

    # Launch all PR processing tasks in parallel
    await asyncio.gather(
        *[process_single_pr(pr_num) for pr_num in pr_numbers],
        return_exceptions=True
    )

    return stats
```

### Pattern 2: Batched Parallelism (More Conservative)

```python
async def _collect_reviews_batched(
    repo_full_name: str,
    pr_numbers: list[int],
    rest_client: RestClient,
    writer: AsyncJSONLWriter,
    batch_size: int = 10,
    max_concurrent: int = 5,
) -> dict[str, int]:
    """Collect reviews in batches for better control."""

    stats = {"prs_processed": 0, "reviews_collected": 0, "errors": 0}

    # Split into batches
    batches = [
        pr_numbers[i:i + batch_size]
        for i in range(0, len(pr_numbers), batch_size)
    ]

    for batch_idx, batch in enumerate(batches, 1):
        logger.info(
            "Processing batch %d/%d (%d PRs)",
            batch_idx, len(batches), len(batch)
        )

        # Process batch in parallel
        batch_stats = await _collect_reviews_parallel(
            repo_full_name, batch, rest_client, writer, max_concurrent
        )

        # Aggregate stats
        stats["prs_processed"] += batch_stats["prs_processed"]
        stats["reviews_collected"] += batch_stats["reviews_collected"]
        stats["errors"] += batch_stats["errors"]

    return stats
```

## Implementation Steps

### Step 1: Add Parallel Helper to Reviews Collector

**File**: `src/gh_year_end/collect/reviews.py`

1. Add the `_collect_reviews_parallel()` function after `_collect_reviews_for_repo()`
2. Add configuration parameter for `max_concurrent_prs` (default: 5)
3. Update `_collect_reviews_for_repo()` to use parallel processing

**Modified function**:
```python
async def _collect_reviews_for_repo(
    repo_full_name: str,
    pr_numbers: list[int],
    rest_client: RestClient,
    paths: PathManager,
    max_concurrent_prs: int = 5,  # New parameter
) -> dict[str, int]:
    """Collect reviews for a single repository."""
    output_path = paths.reviews_raw_path(repo_full_name)

    async with AsyncJSONLWriter(output_path) as writer:
        # Use parallel processing instead of sequential loop
        return await _collect_reviews_parallel(
            repo_full_name,
            pr_numbers,
            rest_client,
            writer,
            max_concurrent=max_concurrent_prs,
        )
```

### Step 2: Add Configuration Option

**File**: `src/gh_year_end/config.py`

Add to `CollectionConfig`:
```python
class CollectionConfig(BaseModel):
    """Collection configuration section."""

    enable: CollectionEnableConfig = Field(default_factory=CollectionEnableConfig)
    commits: CommitsConfig = Field(default_factory=CommitsConfig)
    hygiene: HygieneConfig = Field(default_factory=HygieneConfig)
    max_concurrent_items: int = Field(
        default=5,
        ge=1,
        le=20,
        description="Max concurrent PRs/issues to process within a repo"
    )
```

**File**: `config/config.yaml`

Add:
```yaml
collection:
  enable:
    pulls: true
    issues: true
    reviews: true
    comments: true
    commits: true
    hygiene: true
  max_concurrent_items: 5  # New setting
```

### Step 3: Apply Same Pattern to Comments Collectors

**File**: `src/gh_year_end/collect/comments.py`

Apply the same parallel pattern to:
1. `collect_issue_comments()` - parallel issue processing
2. `collect_review_comments()` - parallel PR processing

### Step 4: Writer Thread Safety Verification

**File**: `src/gh_year_end/storage/writer.py`

Verify that `AsyncJSONLWriter` is thread-safe for concurrent writes.
If not, add an internal lock:

```python
class AsyncJSONLWriter:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._file = None
        self._lock = asyncio.Lock()  # Add lock

    async def write(self, **kwargs) -> None:
        async with self._lock:  # Protect writes
            # existing write logic
```

## Testing Strategy

### Phase 1: Unit Tests

Create `tests/test_parallel_processing.py`:

```python
import asyncio
import pytest
from gh_year_end.collect.reviews import _collect_reviews_parallel

@pytest.mark.asyncio
async def test_parallel_review_collection():
    """Test parallel review collection."""
    # Mock setup
    pr_numbers = list(range(1, 51))  # 50 PRs

    # Test with different concurrency levels
    for concurrency in [1, 5, 10]:
        start = asyncio.get_event_loop().time()
        stats = await _collect_reviews_parallel(
            "test/repo",
            pr_numbers,
            mock_rest_client,
            mock_writer,
            max_concurrent=concurrency,
        )
        elapsed = asyncio.get_event_loop().time() - start

        assert stats["prs_processed"] == 50
        print(f"Concurrency {concurrency}: {elapsed:.2f}s")
```

### Phase 2: Integration Tests

Test on small real org (10-20 repos):

```bash
# Test with original sequential processing
uv run gh-year-end collect --config config/test.yaml

# Test with parallel processing (max_concurrent_items=5)
uv run gh-year-end collect --config config/test_parallel.yaml

# Compare durations and validate results match
```

### Phase 3: Monitor Rate Limits

```python
# Add monitoring to parallel execution
async def _collect_reviews_parallel(...):
    start_time = time.time()

    # ... parallel execution ...

    elapsed = time.time() - start_time
    req_per_sec = stats["prs_processed"] / elapsed

    logger.info(
        "Parallel collection stats: %d PRs in %.2fs (%.2f req/sec)",
        stats["prs_processed"], elapsed, req_per_sec
    )
```

## Safety Considerations

### 1. Respect Global Rate Limiter

The `RestClient` already uses `AdaptiveRateLimiter`, which enforces:
- Max concurrency via semaphore
- Primary rate limit protection
- Secondary rate limit protection

**Key**: Parallel processing within a repo happens UNDER the global concurrency limit.

### 2. Gradual Rollout

1. Start with `max_concurrent_items: 3`
2. Monitor for 429 responses
3. Gradually increase to 5, then 10 if stable
4. Never exceed global `max_concurrency` setting

### 3. Error Handling

Use `return_exceptions=True` in `asyncio.gather()` to prevent one failure from canceling all tasks:

```python
results = await asyncio.gather(
    *[process_single_pr(pr) for pr in pr_numbers],
    return_exceptions=True
)

# Log exceptions without crashing
for i, result in enumerate(results):
    if isinstance(result, Exception):
        logger.error("PR %d failed: %s", pr_numbers[i], result)
```

## Expected Performance

### Baseline (Sequential)
- 1,000 PRs at 1 req/sec = 1,000 seconds (16.7 min)

### With Parallel (max_concurrent_items=5)
- 1,000 PRs at 5 req/sec = 200 seconds (3.3 min)
- **80% reduction** for this phase

### With Parallel + Optimized Rate Limiting
- Combined with Phase 1 & 2 optimizations
- **Overall 60-70% reduction** in total collection time

## Rollback Plan

If parallel processing causes issues:

1. Set `max_concurrent_items: 1` in config (reverts to sequential)
2. Or use feature flag:

```python
if config.collection.parallel_enabled:
    stats = await _collect_reviews_parallel(...)
else:
    stats = await _collect_reviews_sequential(...)  # Original code
```

## Future Enhancements

### 1. Dynamic Concurrency Adjustment

Adjust concurrency based on rate limit remaining:

```python
def calculate_optimal_concurrency(rate_limiter):
    remaining_pct = rate_limiter.get_state().remaining_percent
    if remaining_pct > 75:
        return 10
    elif remaining_pct > 50:
        return 5
    elif remaining_pct > 25:
        return 3
    else:
        return 1
```

### 2. Cross-Repo Parallelization

Currently, repos are processed sequentially. Could parallelize:

```python
# Process multiple repos in parallel
await asyncio.gather(
    *[collect_reviews_for_repo(repo, ...) for repo in repos],
    return_exceptions=True
)
```

But this requires careful coordination with global rate limiter.

## Summary

**Effort**: 4-6 hours implementation + 2-3 hours testing
**Impact**: 30-40% reduction in collection time
**Risk**: Medium (mitigated by gradual rollout and safety measures)
**Recommended**: Implement after Phase 1 & 2 are validated in production
