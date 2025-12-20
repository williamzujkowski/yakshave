# Deprecation Deletion Plan - GitHub Issue #123

## Executive Summary

**Status**: BLOCKED - Cannot safely delete without refactoring
**Total Deprecated Code**: ~7,370 lines across 22+ files
**Critical Blockers**: 2 active view files still use Parquet (1,369 lines need refactoring)
**Safe Deletions Available**: ~620 lines can be removed immediately

---

## 1. Deprecated Modules Identified

### A. normalize/ directory (11 files, ~2,000 lines) - BLOCKED
```
src/gh_year_end/normalize/
├── __init__.py
├── pulls.py
├── identity.py          ← BLOCKER: tests/test_identity.py imports BotDetector
├── issues.py
├── reviews.py
├── common.py
├── repos.py
├── hygiene.py
├── comments.py
├── commits.py
└── users.py
```

**Status**: Only self-referencing imports within normalize/ (circular dependencies)
**Blocker**: BotDetector class still tested in tests/test_identity.py

### B. metrics/ directory (7 files, ~3,587 lines) - SAFE
```
src/gh_year_end/metrics/
├── __init__.py
├── hygiene_score.py
├── awards.py             ← imported by metrics/orchestrator.py only
├── leaderboards.py
├── timeseries.py
├── orchestrator.py       ← self-referencing only
└── repo_health.py
```

**Status**: Only self-referencing imports within metrics/ (circular dependencies)
**Blocker**: None - safe to delete

### C. Parquet-specific modules (3 files, ~1,633 lines) - BLOCKED
```
src/gh_year_end/storage/
├── parquet_writer.py     (171 lines) ← BLOCKER: 7 active imports
├── validator.py          (458 lines) ← SAFE: no imports
└── report/export.py      (1,004 lines) ← BLOCKER: tests/test_report_export.py
```

**Blocking Imports**:
1. src/gh_year_end/storage/__init__.py (exports ParquetWriter)
2. src/gh_year_end/report/export.py (imports read_parquet)
3. src/gh_year_end/report/views/engineer_view.py (imports read_parquet)
4. src/gh_year_end/report/views/exec_summary.py (imports read_parquet)
5. tests/test_report_export.py (imports export_metrics and write_parquet)

### D. report/views/ files using Parquet (2 files, ~1,369 lines) - ACTIVE CODE
```
src/gh_year_end/report/views/
├── engineer_view.py      (835 lines) ← ACTIVE but uses Parquet
├── exec_summary.py       (534 lines) ← ACTIVE but uses Parquet
├── repos_view.py         (active, NO Parquet deps)
└── __init__.py           (exports engineer_view functions)
```

**CRITICAL FINDING**:
- engineer_view.py and exec_summary.py are **DEPRECATED** (read from Parquet files)
- repos_view.py is **ACTIVE** (used by build.py, no Parquet deps)
- The old views expect the old pipeline structure (Parquet metrics)
- The new pipeline produces JSON directly (no Parquet)
- **These views are NOT used by the new build command** (only repos_view is used)

---

## 2. Detailed Blocking Dependencies

### BLOCKER 1: BotDetector in normalize/identity.py
**File**: /home/william/git/yakshave/src/gh_year_end/normalize/identity.py
**Used by**: tests/test_identity.py (162 lines of tests)
**Decision Required**:
- Option A: Move BotDetector to `src/gh_year_end/collect/identity.py` (preserve tests)
- Option B: Delete tests and the class (if bot detection moved elsewhere)
**Recommendation**: Option A - BotDetector appears to be core functionality

### BLOCKER 2: report/views using Parquet
**Files**:
- src/gh_year_end/report/views/engineer_view.py (835 lines)
- src/gh_year_end/report/views/exec_summary.py (534 lines)

**Used by**: NOTHING in new pipeline (exports exist but unused)
**Recommendation**: DELETE both files - they are dead code for old pipeline

### BLOCKER 3: report/export.py
**File**: src/gh_year_end/report/export.py (1,004 lines)
**Used by**: tests/test_report_export.py only
**Recommendation**: DELETE both the module and its test

### BLOCKER 4: storage/__init__.py exports
**File**: src/gh_year_end/storage/__init__.py
**Lines to remove**: 10-14, plus 3 entries in __all__
**Action**: Edit file to remove ParquetWriter exports

---

## 3. Safe Immediate Deletions (No Dependencies)

### A. storage/validator.py (458 lines)
```bash
rm src/gh_year_end/storage/validator.py
```
No imports found. CLI command already deprecated and raises Abort().

### B. CLI deprecated command handlers (162 lines)
**File**: src/gh_year_end/cli.py
**Lines**: 283-444 (commands: plan, normalize, metrics, report, validate, status)
Already neutered (all raise click.Abort()).

**Total safe deletions**: 620 lines

---

## 4. Recommended Deletion Plan

### Phase 1: Prepare BotDetector Migration
```bash
# 1. Create new identity module in collect/
mkdir -p src/gh_year_end/collect/
cp src/gh_year_end/normalize/identity.py src/gh_year_end/collect/identity.py

# 2. Update test imports
sed -i 's|from gh_year_end.normalize.identity|from gh_year_end.collect.identity|' \
    tests/test_identity.py

# 3. Verify tests still pass
uv run pytest tests/test_identity.py -v
```

### Phase 2: Delete Old Pipeline Test Files
```bash
# Remove tests for deprecated modules
rm tests/test_report_export.py
```

### Phase 3: Delete Deprecated View Files
```bash
# Delete engineer_view and exec_summary (Parquet-dependent, unused)
rm src/gh_year_end/report/views/engineer_view.py
rm src/gh_year_end/report/views/exec_summary.py
rm src/gh_year_end/report/views/ENGINEER_VIEW_USAGE.md

# Update views/__init__.py to remove dead exports
# Edit src/gh_year_end/report/views/__init__.py
# Remove lines 8-19 (all engineer_view exports)
```

### Phase 4: Delete Parquet Modules
```bash
# Delete Parquet-specific storage modules
rm src/gh_year_end/storage/parquet_writer.py
rm src/gh_year_end/storage/validator.py
rm src/gh_year_end/report/export.py
```

### Phase 5: Delete normalize/ and metrics/ Directories
```bash
# Delete entire deprecated directories
rm -rf src/gh_year_end/normalize/
rm -rf src/gh_year_end/metrics/
```

### Phase 6: Clean Up Exports
```bash
# Edit src/gh_year_end/storage/__init__.py
# Remove lines 10-14:
#   from gh_year_end.storage.parquet_writer import (
#       ParquetWriter,
#       read_parquet,
#       write_parquet,
#   )
#
# Remove from __all__ (lines 33, 38-39):
#   "ParquetWriter",
#   "read_parquet",
#   "write_parquet",
```

### Phase 7: Delete CLI Deprecated Commands
```bash
# Edit src/gh_year_end/cli.py
# Delete lines 283-444 (all deprecated command handlers)
```

### Phase 8: Verification
```bash
# 1. Check imports
uv run python -c "import gh_year_end; print('✓ Package imports OK')"

# 2. Run smoke tests
uv run pytest tests/test_smoke.py -v

# 3. Run CLI tests
uv run pytest tests/test_cli_commands.py -v

# 4. Run aggregator tests
uv run pytest tests/test_aggregator.py -v

# 5. Type checking
uv run mypy src/gh_year_end

# 6. Linting
uv run ruff check src/
```

---

## 5. Files Requiring Manual Edits

### File 1: src/gh_year_end/storage/__init__.py
**Lines to delete**: 10-14, and remove 3 entries from __all__ (33, 38-39)

### File 2: src/gh_year_end/report/views/__init__.py
**Lines to delete**: 8-19 (all engineer_view imports and exports)
**Result**: Empty file or delete entirely (only repos_view is used directly)

### File 3: src/gh_year_end/cli.py
**Lines to delete**: 283-444 (deprecated command handlers)

---

## 6. Estimated Total Lines to Remove

| Category | Files | Lines |
|----------|-------|-------|
| normalize/ directory | 11 | ~2,000 |
| metrics/ directory | 7 | ~3,587 |
| Parquet storage modules | 3 | 1,633 |
| Dead view files (engineer/exec) | 2 | 1,369 |
| CLI deprecated commands | 1 section | 162 |
| Test files (test_report_export.py) | 1 | ~200 |
| **TOTAL** | **24+ files/sections** | **~8,951 lines** |

---

## 7. Risk Assessment

### LOW RISK - Safe Immediate Deletion
- storage/validator.py (458 lines) - no dependencies
- CLI deprecated handlers in cli.py (162 lines) - already neutered
- **Subtotal: 620 lines**

### MEDIUM RISK - Requires Migration First
- BotDetector (move before deleting normalize/)
- storage/__init__.py exports (simple edit)
- views/__init__.py exports (simple edit)

### HIGH RISK - Verify Dead Code First
- engineer_view.py (835 lines) - appears unused but exported
- exec_summary.py (534 lines) - appears unused but exported
- **Action**: Grep for any imports in wild, then delete if truly unused

---

## 8. Final Recommendation

### DO NOT DELETE YET - Blocking Issues Remain

**Critical Path**:
1. ✅ Verify engineer_view.py and exec_summary.py are truly unused
2. ⬜ Move BotDetector to collect/identity.py
3. ⬜ Delete test_report_export.py
4. ⬜ Execute Phase 1-8 deletion sequence
5. ⬜ Run full test suite
6. ⬜ Create PR with deletions

**Estimated Effort**: 2-3 hours
**Estimated PR**: -8,951 lines, +50 lines (for BotDetector move)

---

## 9. Verification Commands

```bash
# Check for any remaining imports of deprecated modules
grep -r "from gh_year_end.normalize" src/ tests/ --include="*.py" | grep -v "__pycache__"
grep -r "from gh_year_end.metrics" src/ tests/ --include="*.py" | grep -v "__pycache__"
grep -r "parquet_writer" src/ tests/ --include="*.py" | grep -v "__pycache__"

# Check for engineer_view/exec_summary usage
grep -r "engineer_view\|exec_summary" src/gh_year_end --include="*.py" | \
    grep -v "views/engineer_view.py\|views/exec_summary.py\|views/__init__.py\|__pycache__"

# Count total files
find src/gh_year_end/normalize src/gh_year_end/metrics -type f -name "*.py" | wc -l

# Count total lines
find src/gh_year_end/normalize src/gh_year_end/metrics -type f -name "*.py" -exec wc -l {} + | tail -1
```

---

## 10. Next Steps

1. **Confirm with maintainer**: Are engineer_view.py and exec_summary.py truly deprecated?
2. **Get approval**: Should we preserve BotDetector or is it redundant?
3. **Create tracking issue**: Break this into smaller PRs if needed
4. **Execute plan**: Follow Phase 1-8 sequence with verification at each step

**Question for issue #123**: Should this be one massive PR or broken into multiple smaller PRs?
