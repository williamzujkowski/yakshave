# Datetime Timezone Comparison Fix - Summary

**Date**: 2025-12-18
**Issue**: https://github.com/williamzujkowski/yakshave/issues/109
**Branch**: feat/phase-6-report

## Problem

Critical bug in PR collection caused 0 PRs to be collected from 20 repositories.

**Error Message**:
```
TypeError: can't compare offset-naive and offset-aware datetimes
```

## Root Cause Analysis

### 1. Config Datetimes are Timezone-Aware

YAML configuration contains ISO 8601 timestamps with timezone:
```yaml
windows:
  year: 2025
  since: "2025-01-01T00:00:00Z"
  until: "2026-01-01T00:00:00Z"
```

Pydantic parses these as timezone-aware `datetime` objects (UTC).

### 2. PR Collector Was Stripping Timezone

In `src/gh_year_end/collect/pulls.py`, lines 265-271:

```python
# BROKEN CODE
updated_at = datetime.fromisoformat(updated_at_str.replace("Z", "+00:00"))
updated_at_naive = updated_at.replace(tzinfo=None)  # Strip timezone
if since <= updated_at_naive < until:  # FAILS: naive vs aware
```

### 3. Comparison Failed

Python cannot compare offset-naive and offset-aware datetimes, causing TypeError.

## Solution

Removed timezone stripping to keep all datetimes timezone-aware (UTC).

### Code Changes

**File**: `/home/william/git/yakshave/src/gh_year_end/collect/pulls.py`

**Function**: `_filter_prs_by_date()` (lines 237-281)
```python
# FIXED CODE
updated_at = datetime.fromisoformat(updated_at_str.replace("Z", "+00:00"))
# No longer stripping timezone
if since <= updated_at < until:  # Works: aware vs aware
```

**Function**: `_all_prs_before_date()` (lines 284-314)
```python
# FIXED CODE
updated_at = datetime.fromisoformat(updated_at_str.replace("Z", "+00:00"))
# No longer stripping timezone
if updated_at >= since:  # Works: aware vs aware
```

## Testing

Created comprehensive test suite in `/home/william/git/yakshave/tests/test_pulls_datetime_fix.py`:

### Test Coverage

1. **test_filter_prs_by_date_timezone_aware**
   - Verifies timezone-aware datetime comparison works
   - Tests PR filtering with GitHub API timestamps
   - Validates correct PRs are included/excluded based on date range

2. **test_all_prs_before_date_timezone_aware**
   - Tests early termination check with timezone-aware datetimes
   - Validates logic for stopping pagination when all PRs are before date range

3. **test_filter_prs_handles_missing_updated_at**
   - Ensures PRs without `updated_at` field are skipped gracefully
   - Tests robustness against malformed data

### Test Results

```bash
$ uv run pytest tests/test_pulls_datetime_fix.py -v
============================= test session starts ==============================
collected 3 items

tests/test_pulls_datetime_fix.py::test_filter_prs_by_date_timezone_aware PASSED
tests/test_pulls_datetime_fix.py::test_all_prs_before_date_timezone_aware PASSED
tests/test_pulls_datetime_fix.py::test_filter_prs_handles_missing_updated_at PASSED

============================== 3 passed in 0.06s ===============================
```

## Impact

### Before Fix
- 0 PRs collected from 20 repositories
- Silent failure (no obvious error message in logs)
- Data pipeline incomplete

### After Fix
- PR collection works correctly
- Proper date filtering applied
- Data pipeline can proceed to normalization

## Verification of Other Collectors

Checked all other collectors for similar issues:

| Collector | File | Date Filtering | Status |
|-----------|------|----------------|--------|
| Issues | `src/gh_year_end/collect/issues.py` | String comparison (line 216) | No bug |
| Commits | `src/gh_year_end/collect/commits.py` | Passes ISO strings to API | No bug |
| Reviews | `src/gh_year_end/collect/reviews.py` | No date filtering | No bug |
| Comments | `src/gh_year_end/collect/comments.py` | No date filtering | No bug |

## Standards Compliance

This fix follows project standards from `/home/william/git/yakshave/CLAUDE.md`:

1. **Config-first**: Respects config datetime format
2. **Deterministic**: Consistent timezone handling (UTC)
3. **Type safety**: Proper timezone-aware datetime usage
4. **Testing**: Comprehensive test coverage added
5. **Documentation**: Clear comments explaining timezone awareness

## Files Changed

1. `/home/william/git/yakshave/src/gh_year_end/collect/pulls.py`
   - Fixed `_filter_prs_by_date()` function
   - Fixed `_all_prs_before_date()` function
   - Updated docstrings to clarify timezone-aware parameters

2. `/home/william/git/yakshave/tests/test_pulls_datetime_fix.py` (NEW)
   - Added comprehensive test coverage
   - All tests passing

## Next Steps

1. Verify PR collection works with real data
2. Monitor logs for any timezone-related issues
3. Consider adding timezone validation to config schema
4. Document timezone handling in developer guide

## Related Issues

- GitHub Issue: https://github.com/williamzujkowski/yakshave/issues/109
- Project: gh-year-end
- Phase: 6 (Report Generation)
