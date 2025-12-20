# Project Plan: GitHub Year-End Community Health Report (2025)

## Implementation Status

**Current Phase**: Phase 6 (Static Report Site) - In Progress

**Completed Phases**:
- Phase 0: Foundation (packaging, CLI, config validation) - Complete
- Phase 1: GitHub client and repository discovery - Complete
- Phase 2: Data collectors and collection orchestrator - Complete
- Phase 3: Hygiene snapshot collection - Complete
- Phase 4: Normalization to Parquet - Complete
- Phase 5: Metrics engine - Complete
- Phase 6: Static report site - In Progress

**Implementation Details**: See [docs/archive/](docs/archive/) for detailed implementation summaries from each phase.

---

## 0) Goals, non-negotiables, and success criteria

### Goals

* Pull all relevant GitHub activity for a target **org or user** for **calendar year 2025**.
* Save an **immutable snapshot dataset** locally so you can:

  * iterate on metrics/charts without re-fetching GitHub
  * reproduce the same report later (auditability)
  * re-run year-over-year (2026, 2027…) with the same pipeline
* Produce a **static, shareable site** (D3/HTML/JS) with:

  * Exec-friendly summaries (“health signals”, “risks”, “highlights”)
  * Engineer-friendly drilldowns and leaderboards (“bragging rights”)

### Non-negotiables

* **Config-first**: all tunables in one config file + schema validation.
* **Deterministic**: stable ordering, stable IDs, repeatable outputs.
* **Pull once**: never re-fetch if raw snapshot exists unless explicitly requested.
* **Rate-limit safe**: adaptive throttling + backoff using GitHub guidance. ([GitHub Docs][2])
* **Security hygiene**: never log tokens; redact secrets; least privilege.
* **Maintainability**: modules ≤ 300–400 lines; functions ≤ 50 lines; DRY/KISS.
* **TDD**: unit tests for transforms/metrics; integration tests behind a marker.

### Success criteria

* One command produces:

  * `data/raw/year=2025/...` (immutable snapshot)
  * `data/curated/year=2025/*.parquet` (normalized tables)
  * `data/metrics/year=2025/*.parquet` (derived metric tables)
  * `site/year=2025/` (static report)
* Report includes:

  * PR/issue/review/comment leaderboards (humans only option)
  * repo popularity/activity charts (contributors in last year, etc.)
  * repo hygiene signals (CODEOWNERS, SECURITY.md, CI workflows, etc.)
  * at least 10 “unhinged but insightful” awards + 5 “risk signals”

---

## 1) High-level architecture

### Components

1. **Collector** (Python CLI)

* Fetch data from GitHub.com (REST + GraphQL as needed)
* Store as raw JSONL with request/response metadata and rate-limit headers

2. **Normalizer** (Python)

* Convert raw JSON into curated normalized tables (Parquet)
* Resolve identities; apply bot/human filtering rules (config-driven)

3. **Metrics Engine** (Python)

* Compute metrics tables (leaderboards, time series, repo health, hygiene)

4. **Report Generator** (Python + D3 site)

* Export JSON/CSV bundles for frontend
* Build static HTML/D3 site with “Exec mode” + “Engineer mode”

### Storage choice (recommended)

* **Parquet** (durable, compact, columnar)
* **DuckDB** (query engine; no server; cross-platform)
* Optional: SQLite only for small metadata/state (not for analytics)

> This keeps it evergreen and portable without operating OpenSearch.

---

## 2) Repository layout (agent should implement)

```
.
├── README.md
├── pyproject.toml
├── config/
│   ├── config.example.yaml
│   ├── schema.json
│   └── awards.example.yaml
├── src/
│   └── gh_year_end/
│       ├── cli.py
│       ├── config.py
│       ├── logging.py
│       ├── time.py
│       ├── storage/
│       │   ├── paths.py
│       │   ├── raw_writer.py
│       │   ├── parquet_writer.py
│       │   └── manifest.py
│       ├── github/
│       │   ├── auth.py
│       │   ├── http.py
│       │   ├── ratelimit.py
│       │   ├── rest.py
│       │   ├── graphql.py
│       │   └── discovery.py
│       ├── collect/
│       │   ├── repos.py
│       │   ├── pulls.py
│       │   ├── issues.py
│       │   ├── reviews.py
│       │   ├── comments.py
│       │   ├── commits.py
│       │   └── hygiene.py
│       ├── normalize/
│       │   ├── users.py
│       │   ├── repos.py
│       │   ├── pulls.py
│       │   ├── issues.py
│       │   ├── reviews.py
│       │   ├── comments.py
│       │   ├── commits.py
│       │   └── hygiene.py
│       ├── metrics/
│       │   ├── leaderboards.py
│       │   ├── timeseries.py
│       │   ├── repo_health.py
│       │   ├── hygiene_score.py
│       │   └── awards.py
│       └── report/
│           ├── export.py
│           ├── build.py
│           └── templates/
├── site/
│   ├── assets/
│   ├── templates/
│   └── year=YYYY/   # generated
└── tests/
    ├── test_config.py
    ├── test_ratelimit.py
    ├── test_normalize_*.py
    ├── test_metrics_*.py
    └── fixtures/
```

---

## 3) Config model (single source of truth)

### `config.yaml` (shape)

```yaml
github:
  target:
    mode: org          # org | user
    name: example-org
  auth:
    token_env: GITHUB_TOKEN
  discovery:
    include_forks: false
    include_archived: false
    visibility: all    # all|public|private
  windows:
    year: 2025
    since: "2025-01-01T00:00:00Z"
    until: "2026-01-01T00:00:00Z"

rate_limit:
  strategy: adaptive
  max_concurrency: 4
  min_sleep_seconds: 1
  max_sleep_seconds: 60
  sample_rate_limit_endpoint_every_n_requests: 50

identity:
  bots:
    exclude_patterns:
      - ".*\\[bot\\]$"
      - "^dependabot$"
      - "^renovate\\[bot\\]$"
    include_overrides: []
  humans_only: true

collection:
  enable:
    pulls: true
    issues: true
    reviews: true
    comments: true
    commits: true
    hygiene: true
  commits:
    include_files: true
    classify_files: true
  hygiene:
    paths:
      - "SECURITY.md"
      - "README.md"
      - "LICENSE"
      - "CONTRIBUTING.md"
      - "CODE_OF_CONDUCT.md"
      - "CODEOWNERS"
      - ".github/CODEOWNERS"
    workflow_prefixes:
      - ".github/workflows/"
    branch_protection:
      mode: sample          # skip | best_effort | sample
      sample_top_repos_by: prs_merged
      sample_count: 25
    security_features:
      best_effort: true

storage:
  root: "./data"
  raw_format: jsonl
  curated_format: parquet
  dataset_version: "v1"

report:
  title: "Year in Review 2025"
  output_dir: "./site/year=2025"
  theme: "engineer_exec_toggle"
  awards_config: "./config/awards.yaml"
```

### Requirements

* Validate config against `schema.json` on startup.
* Refuse to run if `since/until` not aligned to the `year` boundary unless override.

---

## 4) GitHub API approach and rate-limit requirements

### Rate-limit behavior (must implement)

The collector must:

* If `retry-after` exists, sleep that many seconds. ([GitHub Docs][2])
* If `x-ratelimit-remaining == 0`, sleep until `x-ratelimit-reset` (epoch seconds). ([GitHub Docs][2])
* Avoid secondary limits by controlling concurrency and request pacing; GitHub documents secondary limits such as concurrency caps and points/minute. ([GitHub Docs][3])
* Record periodic snapshots of the `/rate_limit` endpoint response into raw data for auditing.

### GraphQL usage

Use GraphQL where it reduces calls (PRs with nested reviews, authors, labels). GitHub’s GraphQL API uses point-based limits. ([GitHub Docs][4])
Keep queries small; paginate deterministically.

---

## 5) Raw snapshot (“evidence locker”) spec

### Raw storage structure (must implement)

```
data/raw/year=2025/source=github/target=<org_or_user>/
  manifest.json
  rate_limit_samples.jsonl
  repos.jsonl
  pulls/<repo_full_name>.jsonl
  issues/<repo_full_name>.jsonl
  reviews/<repo_full_name>.jsonl
  issue_comments/<repo_full_name>.jsonl
  review_comments/<repo_full_name>.jsonl
  commits/<repo_full_name>.jsonl
  repo_tree/<repo_full_name>.jsonl
  branch_protection/<repo_full_name>.jsonl   # if collected
  security_features/<repo_full_name>.jsonl   # if collected
```

### JSONL record envelope (must use everywhere)

Each record must wrap:

* `fetched_at` (UTC ISO8601)
* `request`: method, url, params/body, pagination cursor/page, repo, window
* `response`: JSON payload (verbatim)
* `response_headers`: include rate limit headers

### Manifest

`manifest.json` must include:

* tool version + git commit
* config digest (hash of loaded config)
* run id
* start/end timestamps
* counts per endpoint + failures + retries
* list of repos processed (stable order)

---

## 6) Curated normalized schema (Parquet tables)

> All tables must include `year`, `repo_id` where applicable, and timestamps normalized to UTC.

### Dimensions

**dim_repo**

* ids, owner/name/full_name, flags (archived/fork/private), default_branch
* stars/forks/watchers, topics[], language, created_at, pushed_at

**dim_user**

* user_id (node id), login, type, profile_url
* `is_bot` (derived) + `bot_reason`
* (optional) `display_name` if available

**dim_time** (optional)

* date, week, month, quarter, year

### Facts (activity)

**fact_pull_request**

* pr_id, repo_id, number, author_user_id
* created_at, updated_at, closed_at, merged_at
* state (open/closed/merged)
* is_draft, labels[], milestone
* additions/deletions/changed_files (if available)
* title_len, body_len

**fact_issue** (non-PR issues)

* issue_id, repo_id, number, author_user_id
* created_at, updated_at, closed_at, state
* labels[], title_len, body_len

**fact_review**

* review_id, repo_id, pr_id, reviewer_user_id
* submitted_at, state, body_len

**fact_issue_comment**

* comment_id, repo_id
* parent_type (issue|pr), parent_id
* author_user_id, created_at, body_len

**fact_review_comment** (inline code comments)

* comment_id, repo_id, pr_id
* author_user_id, created_at
* path, line (nullable), body_len

### Commit-level (optional but recommended for “docs champions”)

**fact_commit**

* commit_sha, repo_id
* author_user_id (nullable), committer_user_id (nullable)
* authored_at, committed_at
* message_len

**fact_commit_file**

* commit_sha, repo_id
* path, file_ext
* additions, deletions, changes
* is_docs, is_iac, is_test, is_ci (derived)

### Repo hygiene (required per your “yes”)

**fact_repo_files_presence**

* repo_id, captured_at
* path, exists, sha (nullable), size_bytes (nullable)

**fact_repo_hygiene**

* repo_id, captured_at, default_branch
* branch protection fields (nullable/best-effort)
* requires reviews / status checks (nullable)
* allows_force_pushes, allows_deletions, etc. (nullable)

**fact_repo_security_features**

* repo_id, captured_at
* dependabot_alerts_enabled (nullable)
* secret_scanning_enabled (nullable)
* push_protection_enabled (nullable)

### Identity rules traceability

**dim_identity_rule**

* rule_id, type (regex|allowlist|denylist)
* pattern/value
* description

---

## 7) Metrics layer (derived tables)

These are computed from curated tables and drive the report.

### Core alignment (CHAOSS)

* PRs are **Change Requests**. ([CHAOSS][1])
* Reviews are **Change Request Reviews**. ([CHAOSS][5])
* Contributors include many contribution types. ([CHAOSS][6])

### Tables

**metrics_leaderboard**

* year, metric_key, scope (org|repo), repo_id (nullable)
* user_id, value, rank

Example `metric_key`:

* `prs_opened`, `prs_closed`, `prs_merged`
* `issues_opened`, `issues_closed`
* `reviews_submitted`, `approvals`, `changes_requested`
* `comments_total`, `review_comments_total`
* `docs_commits`, `docs_lines_changed`

**metrics_repo_health**

* repo_id
* active_contributors_30d/90d/365d
* prs_opened/merged, issues_opened/closed
* review_coverage (pct PRs with ≥1 review)
* median_time_to_first_review
* median_time_to_merge
* stale_pr_count (open > N days)
* stale_issue_count (open > N days)

**metrics_time_series**

* period (week or month)
* counts for prs/issues/reviews/comments/commits

**metrics_repo_hygiene_score**

* repo_id, score (0–100)
* missing_security_md, missing_codeowners, missing_ci
* branch_protection_unknown, no_branch_protection (nullable-aware)
* notes[]

**metrics_awards**

* award_key, title, description
* winner_user_id or winner_repo_id
* supporting_stats JSON (for rendering)

---

## 8) Awards (“slightly unhinged”) catalog

Create `config/awards.yaml` to define awards declaratively:

* filters (humans only)
* metric key + ranking
* tie-breakers
* display copy

Examples:

* “Merge Machine” (most PRs merged)
* “Review Paladin” (most approvals + comments)
* “Docs Druid” (most docs changes)
* “Bug Janitor” (most issues closed)
* “Bus Factor Alarm” (repo: high activity + low contributors)
* “Stale Queue Dragon” (repo with most long-lived PRs)

---

## 9) CLI commands (agent must implement)

Single binary entrypoint: `gh-year-end`

### Commands

* `gh-year-end plan --config config.yaml`

  * Prints what will be collected and where it will be stored (no writes)

* `gh-year-end collect --config config.yaml [--force]`

  * Writes raw snapshot only
  * `--force` allows re-fetching (otherwise skip if raw exists)

* `gh-year-end normalize --config config.yaml`

  * Produces curated Parquet tables (deterministic)

* `gh-year-end metrics --config config.yaml`

  * Produces metrics tables

* `gh-year-end report --config config.yaml`

  * Produces static site assets

* `gh-year-end all --config config.yaml`

  * Runs collect → normalize → metrics → report (safe defaults)

All commands must:

* write/update `manifest.json`
* exit non-zero on hard failures
* never print secrets

---

## 10) Phased implementation roadmap (LLM agent execution order)

### Phase 0 — Foundation (1–2 days)

**Deliverables**

* Packaging, CLI skeleton, config schema validation
* Logging with redaction
* Path management + manifest writer
* Unit tests: config + paths + manifest

**Acceptance**

* `gh-year-end plan` works and validates config.

### Phase 1 — Repo discovery + rate-limit-safe HTTP (2–4 days)

**Deliverables**

* Repo list for org/user in stable order
* REST client + GraphQL client with shared adaptive throttling
* Record rate-limit samples and headers
* Unit tests: throttle logic (simulate headers), stable ordering

**Acceptance**

* Can enumerate all repos without hitting secondary limits.

### Phase 2 — Core collection (PRs/issues/reviews/comments) (4–10 days)

**Deliverables**

* Collectors produce raw JSONL per repo, windowed to 2025
* Pagination correctness (REST + GraphQL)
* Restartable (skip already collected repo files unless `--force`)
* Integration test (small org/user) behind `-m integration`

**Acceptance**

* Raw dataset is complete for 2025 for a sample org.

### Phase 3 — Hygiene snapshot collection (1–3 days)

**Deliverables**

* Repo tree fetch per repo default branch
* Presence checks for SECURITY.md/CODEOWNERS/workflows etc.
* Branch protection sampling mode implemented
* Best-effort security feature flags (nullable)

**Acceptance**

* Hygiene tables can be generated without org-admin perms (with nulls).

### Phase 4 — Normalization to Parquet (3–7 days)

**Deliverables**

* Normalizers for each fact/dim table
* Identity + bot filtering implemented and traceable
* Golden-fixture tests for stable outputs

**Acceptance**

* Running normalize twice produces identical Parquet checksums.

### Phase 5 — Metrics engine (3–7 days)

**Deliverables**

* Leaderboards, time series, repo health, hygiene score
* Awards generator from `awards.yaml`
* Tests validating metrics on fixture dataset

**Acceptance**

* Metrics tables populate and align with definitions (PRs=Change Requests, reviews=Change Request Reviews). ([CHAOSS][1])

### Phase 6 — Static report site (5–10 days)

**Deliverables**

* JSON export bundles for D3
* Static pages: Exec / Engineer toggles
* Build script to output `site/year=2025/`
* Smoke test: generated site loads offline

**Acceptance**

* One command builds a complete shareable report.

### Phase 7 — Year-over-year support (optional but recommended)

**Deliverables**

* Partition dataset by `year=YYYY`
* “Compare 2024 vs 2025” page scaffolding
* Schema versioning and migration notes

---

## 11) Testing strategy (TDD expectations)

### Unit tests (must)

* Config parsing + schema validation
* Rate-limit/backoff logic (header-driven) ([GitHub Docs][2])
* Normalization correctness (fixtures)
* Metric correctness (fixtures)

### Integration tests (optional, gated)

* Run `collect` against a tiny target (1–2 repos) with a real token
* Store fixtures from integration runs as sanitized samples (no secrets)

---

## 12) GitHub Issues/PR workflow (agent-ready)

### Issue templates

Create:

* `Bug report`
* `Feature request`
* `Metric request`
* `Award idea`

### Labels

* `collector`, `normalize`, `metrics`, `report`, `hygiene`, `identity`, `docs`, `tech-debt`, `mvp`

### PR cadence

* Small PRs by phase; each PR must:

  * add/adjust tests
  * update docs (README or module docs)
  * keep modules ≤ 300–400 lines

---

## 13) Optional future: OpenSearch integration (only if needed)

Add only if you want:

* full-text search across comments
* ad-hoc slicing beyond the curated report

This would ingest curated/metrics tables into OpenSearch and optionally generate Dashboards assets—but it’s not required for the year-end wrap.

---

## 14) Implementation notes the agent must follow (hard rules)

* Do not re-fetch data if `data/raw/year=2025/...` exists unless `--force`.
* Always write `manifest.json` with counts/errors.
* Always store headers relevant to rate limiting.
* Always keep identity/bot filtering explainable (`dim_identity_rule`, `bot_reason`).
* Always ensure deterministic ordering and stable IDs in normalized outputs.
* Never log tokens or request auth headers.

---

[1]: https://chaoss.community/kb/metric-change-requests/?utm_source=chatgpt.com "Metric: Change Requests - CHAOSS project"
[2]: https://docs.github.com/rest/guides/best-practices-for-using-the-rest-api?utm_source=chatgpt.com "Best practices for using the REST API"
[3]: https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api?utm_source=chatgpt.com "Rate limits for the REST API"
[4]: https://docs.github.com/en/graphql/overview/rate-limits-and-query-limits-for-the-graphql-api?utm_source=chatgpt.com "Rate limits and query limits for the GraphQL API"
[5]: https://chaoss.community/kb/metric-change-request-reviews/?utm_source=chatgpt.com "Metric: Change Request Reviews - CHAOSS"
[6]: https://chaoss.community/kb/metric-contributors/?utm_source=chatgpt.com "Metric: Contributors - CHAOSS"
