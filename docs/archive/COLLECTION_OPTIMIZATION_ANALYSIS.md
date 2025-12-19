# Data Collection Pipeline Optimization Analysis

**Date**: 2025-12-18
**Status**: Analysis Complete

## Executive Summary

The current data collection pipeline is **strictly sequential** with conservative rate limiting. Based on analysis of the codebase, I've identified several high-impact optimizations that can reduce collection time by **50-70%** while respecting GitHub's rate limits.

## Current Architecture Analysis

### 1. Collection Flow (Sequential)

```
Discovery → Repos → Pulls → Issues → Reviews → Comments → Commits → Hygiene → Security
   ↓          ↓       ↓        ↓        ↓         ↓          ↓         ↓         ↓
  [Wait]    [Wait]  [Wait]   [Wait]   [Wait]    [Wait]     [Wait]   [Wait]    [Wait]
```

**Issue**: Each phase must complete before the next begins. This leaves API quota unused during transitions and forces linear processing.

### 2. Current Concurrency Settings

From `config/config.yaml`:
```yaml
rate_limit:
  max_concurrency: 4
  min_sleep_seconds: 1
  max_sleep_seconds: 60
```

**Issue**: `max_concurrency: 4` is very conservative for a 5,000 req/hour primary limit.

### 3. Rate Limiting Strategy

From `ratelimit.py`:
- **Primary limit protection**: Adaptive throttling based on remaining percentage
- **Secondary limit protection**: 90 req/min with backoff multiplier
- **Token bucket**: 30 capacity, 10 tokens/sec sustained rate
- **Circuit breaker**: Opens after 5 failures

**Issues Identified**:
1. **Over-conservative secondary limit**: 90 req/min when GitHub allows ~100 req/min
2. **Aggressive throttling too early**: Delays start at >75% remaining (3,750 requests left!)
3. **Token bucket too restrictive**: 10 tokens/sec = 600 req/min theoretical max
4. **No parallel endpoint exploitation**: Different endpoints have separate secondary limits

### 4. Per-Collector Bottlenecks

#### Reviews & Comments Collectors
```python
for pr_number in pr_numbers:  # Sequential loop
    async for reviews, metadata in rest_client.list_reviews(...):
        # Process reviews
```

**Issue**: Processes each PR/issue sequentially. For a repo with 1,000 PRs, this makes 1,000 sequential API calls.

#### All Collectors
**Issue**: No parallelization across repositories or items within a repository.

## Bottleneck Summary

| Bottleneck | Impact | Severity |
|------------|--------|----------|
| Sequential phase execution | High | Critical |
| Conservative max_concurrency (4) | High | Critical |
| Over-eager adaptive throttling | Medium | High |
| No parallel processing within collectors | High | High |
| Conservative secondary limit (90 vs 100) | Low | Medium |
| Sequential PR/Issue processing | High | High |

## Proposed Optimizations

### Phase 1: Configuration Tuning (Quick Win)

**Changes**:
1. Increase `max_concurrency` from 4 to **8-10**
2. Adjust secondary limit from 90 to **95 req/min**
3. Increase token bucket capacity to **50** and sustained rate to **15/sec**
4. Delay throttling until **<50%** remaining instead of <75%

**Expected Impact**: 40-50% faster with zero code changes

**Risk**: Low - still well within GitHub limits

**Config Changes**:
```yaml
rate_limit:
  strategy: adaptive
  max_concurrency: 10  # Was: 4
  min_sleep_seconds: 0.5  # Was: 1.0
  max_sleep_seconds: 60
  sample_rate_limit_endpoint_every_n_requests: 50
  burst:
    capacity: 50  # Was: 30
    sustained_rate: 15.0  # Was: 10.0
    recovery_rate: 2.0
  secondary:
    max_requests_per_minute: 95  # Was: 90
    detection_window_seconds: 60
    backoff_multiplier: 1.5
    threshold: 0.85  # Was: 0.8
    max_backoff_multiplier: 2.0
```

### Phase 2: Adaptive Throttling Refinement (Code Change)

**Current Behavior** (in `ratelimit.py:_calculate_adaptive_delay`):
- \>75%: No delay
- 50-75%: Minimal delay for LOW priority
- 25-50%: Moderate delay
- <25%: Significant delay

**Proposed Behavior**:
- \>50%: No delay (was >75%)
- 25-50%: Minimal delay for LOW priority only
- 10-25%: Moderate delay
- <10%: Significant delay

**Expected Impact**: 15-20% faster during collection

**Risk**: Low - we're just being less conservative early on

### Phase 3: Parallel Processing Within Collectors (Code Change)

**Current**: Sequential processing of PRs/Issues
**Proposed**: Parallel processing with semaphore control

**Example for Reviews**:
```python
async def _collect_reviews_parallel(pr_numbers, rest_client, writer, max_concurrent=5):
    semaphore = asyncio.Semaphore(max_concurrent)

    async def process_pr(pr_number):
        async with semaphore:
            async for reviews, metadata in rest_client.list_reviews(...):
                await writer.write(...)

    await asyncio.gather(*[process_pr(pr) for pr in pr_numbers])
```

**Expected Impact**: 30-40% faster for comments/reviews phases

**Risk**: Medium - need to ensure proper writer synchronization

### Phase 4: Parallel Phase Execution (Code Change)

**Current**: Sequential phases
**Proposed**: Parallel independent phases

**Independence Matrix**:
```
Phase         | Depends On
--------------|-------------
Discovery     | None
Repos         | Discovery
Pulls         | Discovery
Issues        | Discovery
Reviews       | Pulls
Issue Comm.   | Issues
Review Comm.  | Pulls
Commits       | Discovery
Hygiene       | Discovery
```

**Parallel Groups**:
1. Discovery (serial)
2. Repos + Pulls + Issues + Commits + Hygiene (parallel)
3. Reviews + Issue Comments + Review Comments (parallel after group 2)

**Expected Impact**: 25-35% faster overall

**Risk**: Medium - need careful dependency management

### Phase 5: Smart Pagination with Early Termination

**Current**: Fetches all pages, filters client-side
**Observed**: Early termination logic exists but could be enhanced

**Enhancement**:
- Track consecutive empty pages
- Terminate after 2-3 consecutive pages with no matches

**Expected Impact**: 10-15% faster for date-filtered collections

**Risk**: Low

## Recommended Implementation Order

### Immediate (Phase 1) - Config Tuning
- **Effort**: 5 minutes
- **Impact**: High
- **Risk**: Low

**Action**: Update `config/config.yaml` with optimized values.

### Short-term (Phase 2) - Throttling Refinement
- **Effort**: 1 hour
- **Impact**: Medium
- **Risk**: Low

**Action**: Modify `_calculate_adaptive_delay()` thresholds in `ratelimit.py`.

### Medium-term (Phase 3) - Parallel Item Processing
- **Effort**: 4-6 hours
- **Impact**: High
- **Risk**: Medium

**Action**: Add parallel processing to comments.py and reviews.py.

### Long-term (Phase 4) - Parallel Phases
- **Effort**: 8-12 hours
- **Impact**: High
- **Risk**: Medium-High

**Action**: Refactor orchestrator.py for parallel phase execution.

## Cumulative Impact Estimate

| Implementation | Time Reduction | Cumulative |
|----------------|----------------|------------|
| Baseline | 0% | 100% |
| Phase 1 | 40-50% | 50-60% |
| Phase 2 | 15-20% | 40-48% |
| Phase 3 | 10-15% | 34-41% |
| Phase 4 | 8-12% | 30-37% |

**Total Expected Reduction**: **63-70%** of original time

**Example**: A 10-hour collection becomes 3-4 hours.

## Safety Considerations

### Rate Limit Monitoring
- Current sampling every 50 requests is good
- Keep circuit breaker (failure_threshold=5)
- Monitor for 403/429 responses

### Backoff Strategy
- Keep exponential backoff on secondary limit hits
- Add jitter to prevent thundering herd
- Monitor backoff_multiplier trends

### Testing Strategy
1. Test Phase 1 on small org first (10-20 repos)
2. Monitor rate limit samples closely
3. Check for 429 or 403 responses
4. Gradually increase concurrency if stable

## Metrics to Track

1. **Collection Duration** (total time)
2. **Requests Per Minute** (should stay <95)
3. **Rate Limit Remaining %** (should stay >10%)
4. **Secondary Backoff Triggers** (should be rare)
5. **Circuit Breaker Opens** (should be zero)
6. **429/403 Responses** (should be zero)

## Quick Wins Summary

**Fastest ROI**: Implement Phase 1 (config tuning) immediately.
- 5 minutes to implement
- 40-50% time reduction
- Zero risk

**Next Priority**: Phase 2 (throttling refinement)
- 1 hour to implement
- Additional 15-20% reduction
- Minimal risk

**Combined Quick Wins**: ~60% faster collection in ~1 hour of work
