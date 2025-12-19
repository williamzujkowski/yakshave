# Engineer View Usage

The engineer view provides comprehensive technical metrics and detailed analytics for engineering teams.

## Basic Usage

```python
from pathlib import Path
from gh_year_end.report.views import generate_engineer_view

# Generate full engineer view
metrics_path = Path("data/metrics/year=2025")
view_data = generate_engineer_view(metrics_path, year=2025)

# Access view components
leaderboards = view_data["leaderboards"]
repo_breakdown = view_data["repo_breakdown"]
time_series = view_data["time_series"]
awards = view_data["awards"]
contributor_profiles = view_data["contributor_profiles"]
filters = view_data["filters"]
```

## View Structure

### Leaderboards

Full rankings for all 10 metrics at both organization and per-repository scopes:

```python
# Organization-wide leaderboard
org_leaderboards = view_data["leaderboards"]["org_wide"]
prs_opened = org_leaderboards["prs_opened"]

# Access rankings
for ranking in prs_opened["rankings"]:
    print(f"Rank {ranking['rank']}: {ranking['login']} - {ranking['value']} PRs")
    print(f"Percentile: {ranking['percentile']}%")

# Per-repository leaderboards
per_repo = view_data["leaderboards"]["per_repo"]
for repo_id, repo_data in per_repo.items():
    print(f"Repository: {repo_data['repo_name']}")
    for metric, leaderboard in repo_data["leaderboards"].items():
        print(f"  {metric}: {len(leaderboard['rankings'])} entries")
```

### Repository Breakdown

Detailed metrics for each repository:

```python
for repo_id, repo_data in view_data["repo_breakdown"].items():
    print(f"Repository: {repo_data['repo_name']}")

    # Health metrics
    health = repo_data["health_metrics"]
    print(f"  Active contributors (30d): {health['active_contributors_30d']}")
    print(f"  PRs merged: {health['prs_merged']}")
    print(f"  Review coverage: {health['review_coverage']}%")

    # Hygiene metrics
    hygiene = repo_data["hygiene_metrics"]
    print(f"  Hygiene score: {hygiene['hygiene_score']}")
    print(f"  Has SECURITY.md: {hygiene['has_security_md']}")

    # Contributors
    for contributor in repo_data["contributors"]:
        print(f"  {contributor['login']}: {len(contributor['metrics'])} metrics")
```

### Time Series

Weekly and monthly activity trends:

```python
# Weekly trends
for week in view_data["time_series"]["weekly"]:
    print(f"{week['period']}: {week['prs']} PRs, {week['issues']} issues")

# Monthly trends
for month in view_data["time_series"]["monthly"]:
    print(f"{month['period']}: {month['prs']} PRs, {month['issues']} issues")
```

### Awards

Detailed award information with criteria and honorable mentions:

```python
for award in view_data["awards"]:
    print(f"Award: {award['title']}")
    print(f"Category: {award['category']}")
    print(f"Winner: {award['winner']['name']}")

    # Supporting statistics
    if award["supporting_stats"]:
        print(f"Stats: {award['supporting_stats']}")

    # Honorable mentions (top 5)
    for mention in award["honorable_mentions"]:
        print(f"  Rank {mention['rank']}: {mention['login']}")
```

### Contributor Profiles

Individual contributor statistics:

```python
for user_id, profile in view_data["contributor_profiles"].items():
    print(f"User: {profile['login']}")

    # All metrics for this user
    for metric, stats in profile["metrics"].items():
        print(f"  {metric}: {stats['value']} (rank #{stats['rank']})")

    # Repositories contributed to
    print(f"  Contributed to {len(profile['repos_contributed'])} repos:")
    for repo in profile["repos_contributed"]:
        print(f"    - {repo['repo_name']}")

    # Review activity
    if profile["review_activity"]:
        reviews = profile["review_activity"]
        print(f"  Reviews submitted: {reviews['value']} (rank #{reviews['rank']})")
```

## Filtering

### Filter by Repository

Show metrics for a single repository:

```python
from gh_year_end.report.views import filter_by_repo

filtered_view = filter_by_repo(view_data, repo_id="R_kgDOA...")
```

### Filter by User

Show metrics for a single contributor:

```python
from gh_year_end.report.views import filter_by_user

filtered_view = filter_by_user(view_data, user_id="U_kgDOA...")
```

### Filter by Metric

Show only specific metrics:

```python
from gh_year_end.report.views import filter_by_metric

# Show only PR-related metrics
pr_view = filter_by_metric(view_data, metric_key="prs_merged")
```

## Available Metrics

The engineer view includes all 10 core metrics:

1. **prs_opened** - Pull requests opened
2. **prs_closed** - Pull requests closed
3. **prs_merged** - Pull requests merged
4. **issues_opened** - Issues opened
5. **issues_closed** - Issues closed
6. **reviews_submitted** - Reviews submitted
7. **approvals** - Approvals given
8. **changes_requested** - Changes requested
9. **comments_total** - Total comments (issue + review)
10. **review_comments_total** - Review comments only

## Use Cases

### Personal Bragging Rights

Extract individual contributor stats for sharing:

```python
user_id = "U_kgDOA..."
profile = view_data["contributor_profiles"][user_id]

print(f"My 2025 Stats:")
print(f"- {profile['metrics']['prs_merged']['value']} PRs merged")
print(f"- Rank #{profile['metrics']['prs_merged']['rank']} in org")
print(f"- Contributed to {len(profile['repos_contributed'])} repositories")
```

### Team Analytics

Analyze team performance by repository:

```python
repo_id = "R_kgDOA..."
repo = view_data["repo_breakdown"][repo_id]

print(f"Team Metrics for {repo['repo_name']}:")
print(f"- {repo['health_metrics']['active_contributors_90d']} active contributors")
print(f"- {repo['health_metrics']['prs_merged']} PRs merged")
print(f"- {repo['health_metrics']['review_coverage']}% review coverage")
print(f"- {len(repo['contributors'])} total contributors")
```

### Trend Analysis

Identify activity patterns:

```python
import pandas as pd

# Convert to DataFrame for analysis
df = pd.DataFrame(view_data["time_series"]["monthly"])

# Find peak activity month
peak_month = df.loc[df["prs"].idxmax()]
print(f"Peak month: {peak_month['period']} with {peak_month['prs']} PRs")

# Calculate average
avg_prs = df["prs"].mean()
print(f"Average PRs per month: {avg_prs:.1f}")
```

## Data Export

Export view data for custom analysis:

```python
import json

# Export to JSON
with open("engineer_view_2025.json", "w") as f:
    json.dump(view_data, f, indent=2, default=str)

# Export specific sections
with open("leaderboards_2025.json", "w") as f:
    json.dump(view_data["leaderboards"], f, indent=2)
```
