"""Tests for Jinja2 template rendering.

Tests verify that templates render correctly with various data inputs.
"""

from pathlib import Path

import pytest
from jinja2 import Environment, FileSystemLoader, TemplateNotFound, select_autoescape


@pytest.fixture
def template_env():
    """Create Jinja2 environment with templates directory."""
    template_dir = Path(__file__).parent.parent / "site" / "templates"
    if not template_dir.exists():
        pytest.skip(f"Templates directory not found: {template_dir}")

    env = Environment(
        loader=FileSystemLoader(str(template_dir)),
        autoescape=select_autoescape(["html", "xml"]),
    )

    # Add custom filters for templates
    env.filters["format_date"] = lambda d: d
    env.filters["format_number"] = lambda n: f"{n:,}"
    env.filters["truncate"] = lambda s, length: s[:length] + "..." if len(s) > length else s
    env.filters["round"] = lambda n, digits=0: round(n, digits)

    return env


@pytest.fixture
def base_context():
    """Provide base context data needed by all templates."""
    return {
        "config": {
            "report": {
                "title": "Test Year in Review 2025",
            },
            "github": {
                "target": {
                    "name": "test-org",
                    "mode": "org",
                },
                "windows": {
                    "year": 2025,
                    "since": "2025-01-01T00:00:00Z",
                    "until": "2026-01-01T00:00:00Z",
                },
            },
        },
        "generation_date": "2025-12-18",
        "version": "1.0.0",
        "stats": {
            "total_prs": 1500,
            "total_issues": 500,
            "total_reviews": 2000,
            "total_repos": 50,
        },
    }


def test_base_template_exists(template_env):
    """Test that base.html template exists and can be loaded."""
    try:
        template = template_env.get_template("base.html")
        assert template is not None
    except TemplateNotFound:
        pytest.fail("base.html template not found")


def test_base_template_has_blocks(template_env):
    """Test that base template defines expected blocks."""
    template_source = template_env.loader.get_source(template_env, "base.html")[0]

    # Check for required blocks
    assert "{% block title %}" in template_source
    assert "{% block content %}" in template_source
    assert "{% block extra_css %}" in template_source
    assert "{% block extra_js %}" in template_source


def test_index_template_renders(template_env, base_context):
    """Test that index.html renders with valid context."""
    template = template_env.get_template("index.html")

    context = {
        **base_context,
        "summary": {
            "total_contributors": 75,
            "total_prs_merged": 1200,
            "total_reviews": 2000,
            "total_repos": 50,
        },
        "highlights": {
            "most_active_month": "March",
            "most_active_month_prs": 150,
            "avg_review_time": "2.5 hours",
            "review_coverage": 85,
            "new_contributors": 12,
        },
        "activity_timeline": [],
    }

    html = template.render(**context)

    # Verify key elements are present
    assert "Year in Review: 2025" in html
    assert "test-org" in html
    assert "75" in html  # total contributors
    assert "1200" in html or "1,200" in html  # total PRs merged


def test_summary_template_renders(template_env, base_context):
    """Test that summary.html renders with valid context."""
    template = template_env.get_template("summary.html")

    context = {
        **base_context,
        "health": {
            "review_coverage": 85,
            "review_coverage_change": 5,
            "avg_merge_time": "3.2 hours",
            "stale_pr_count": 12,
            "active_contributors": 65,
        },
        "risks": [
            {
                "title": "High Stale PR Count",
                "description": "Many PRs are open for too long",
                "category": "velocity",
                "severity": "medium",
                "affected_count": 12,
            }
        ],
        "insights": {
            "avg_reviewers_per_pr": 2.3,
            "review_participation_rate": 65,
            "cross_team_reviews": 25,
            "prs_per_week": 28,
            "median_pr_size": 125,
            "merge_rate": 88,
            "repos_with_ci": 92,
            "repos_with_codeowners": 78,
            "repos_with_security_policy": 65,
            "new_contributors": 12,
            "contributor_retention": 85,
            "bus_factor": 8,
        },
        "recommendations": [],
        "top_repos": [],
        "collaboration_data": {},
        "velocity_data": {},
        "quality_data": {},
        "community_data": {},
    }

    html = template.render(**context)

    assert "Executive Summary" in html
    assert "Health Signals" in html
    assert "85%" in html  # review coverage


def test_engineers_template_renders(template_env, base_context):
    """Test that engineers.html renders with valid context."""
    template = template_env.get_template("engineers.html")

    context = {
        **base_context,
        "top_contributors": [
            {
                "user_id": "user1",
                "login": "alice",
                "rank": 1,
                "prs_merged": 150,
                "reviews_submitted": 200,
                "issues_closed": 50,
                "comments_total": 300,
            }
        ],
        "all_contributors": [
            {
                "user_id": "user1",
                "login": "alice",
                "rank": 1,
                "prs_merged": 150,
                "prs_opened": 160,
                "reviews_submitted": 200,
                "approvals": 180,
                "issues_opened": 25,
                "issues_closed": 50,
                "comments_total": 300,
                "activity_timeline": [10, 15, 20, 25],
            }
        ],
        "contribution_timeline": {},
        "contribution_types": {},
        "contribution_by_repo": {},
    }

    html = template.render(**context)

    assert "Contributors" in html
    assert "alice" in html
    assert "150" in html  # PRs merged


def test_leaderboards_template_renders(template_env, base_context):
    """Test that leaderboards.html renders with valid context."""
    template = template_env.get_template("leaderboards.html")

    context = {
        **base_context,
        "leaderboards": {
            "prs_merged": [
                {"login": "alice", "value": 150, "rank": 1},
                {"login": "bob", "value": 120, "rank": 2},
                {"login": "charlie", "value": 100, "rank": 3},
            ],
            "prs_opened": [],
            "reviews_submitted": [],
            "approvals": [],
            "changes_requested": [],
            "issues_opened": [],
            "issues_closed": [],
            "comments_total": [],
            "review_comments_total": [],
            "overall": [],
        },
        "all_contributors": [],
    }

    html = template.render(**context)

    assert "Leaderboards" in html
    assert "alice" in html
    assert "Most PRs Merged" in html


def test_repos_template_renders(template_env, base_context):
    """Test that repos.html renders with valid context."""
    template = template_env.get_template("repos.html")

    context = {
        **base_context,
        "repo_summary": {
            "healthy_count": 40,
            "warning_count": 8,
            "avg_hygiene_score": 82.5,
            "avg_contributors": 12.3,
        },
        "repos": [
            {
                "repo_id": "repo1",
                "name": "awesome-project",
                "full_name": "test-org/awesome-project",
                "language": "Python",
                "is_private": False,
                "hygiene_score": 95,
                "hygiene_score_category": "high",
                "prs_merged": 250,
                "active_contributors_365d": 15,
                "review_coverage": 90,
                "median_time_to_merge": "2.5 hours",
                "health_status": "healthy",
            }
        ],
        "hygiene": {
            "security_md_count": 42,
            "security_features_count": 38,
            "codeowners_count": 35,
            "branch_protection_count": 40,
            "ci_workflows_count": 45,
            "avg_workflows": 3.2,
            "readme_count": 50,
            "contributing_count": 28,
        },
        "repo_activity": {},
    }

    html = template.render(**context)

    assert "Repository Health Dashboard" in html
    assert "awesome-project" in html
    assert "95" in html  # hygiene score


def test_awards_template_renders(template_env, base_context):
    """Test that awards.html renders with valid context."""
    template = template_env.get_template("awards.html")

    context = {
        **base_context,
        "awards": {
            "individual": [
                {
                    "award_key": "merge_machine",
                    "title": "Merge Machine",
                    "description": "Most PRs merged",
                    "winner_name": "alice",
                    "supporting_stats": "150 PRs merged",
                }
            ],
            "repository": [
                {
                    "award_key": "most_active",
                    "title": "Most Active Repository",
                    "description": "Highest activity",
                    "winner_name": "test-org/awesome-project",
                    "supporting_stats": "500 contributions",
                }
            ],
            "risk": [
                {
                    "title": "Missing Security Policy",
                    "description": "Repos without SECURITY.md",
                    "category": "security",
                    "winner_name": "8 repositories",
                    "supporting_stats": "8 repos affected",
                }
            ],
        },
        "special_mentions": {
            "first_contributions": [],
            "consistent_contributors": [],
            "largest_prs": [],
            "fastest_merges": [],
        },
        "fun_facts": {
            "total_lines_changed": 150000,
            "busiest_day": "March 15",
            "most_active_hour": "2 PM",
            "total_comments": 5000,
            "avg_pr_size": 125,
            "most_used_emoji": ":rocket:",
        },
    }

    html = template.render(**context)

    assert "Awards & Recognition" in html
    assert "Merge Machine" in html
    assert "alice" in html


def test_all_templates_have_navigation_blocks(template_env):
    """Test that all page templates set navigation active states."""
    templates_to_check = [
        ("index.html", "nav_index"),
        ("summary.html", "nav_summary"),
        ("engineers.html", "nav_engineers"),
        ("leaderboards.html", "nav_leaderboards"),
        ("repos.html", "nav_repos"),
        ("awards.html", "nav_awards"),
    ]

    for template_name, block_name in templates_to_check:
        template_source = template_env.loader.get_source(template_env, template_name)[0]
        assert f"{{% block {block_name} %}}" in template_source, (
            f"{template_name} missing {block_name} block"
        )


def test_templates_extend_base(template_env):
    """Test that all page templates extend base.html."""
    templates = [
        "index.html",
        "summary.html",
        "engineers.html",
        "leaderboards.html",
        "repos.html",
        "awards.html",
    ]

    for template_name in templates:
        template_source = template_env.loader.get_source(template_env, template_name)[0]
        assert '{% extends "base.html" %}' in template_source, (
            f"{template_name} does not extend base.html"
        )


def test_templates_have_page_headers(template_env, base_context):
    """Test that all page templates have proper page headers."""
    templates = [
        "index.html",
        "summary.html",
        "engineers.html",
        "leaderboards.html",
        "repos.html",
        "awards.html",
    ]

    for template_name in templates:
        template = template_env.get_template(template_name)

        # Provide minimal context for rendering
        minimal_context = {
            **base_context,
            "summary": {},
            "highlights": {},
            "activity_timeline": [],
            "health": {},
            "risks": [],
            "insights": {},
            "recommendations": [],
            "top_repos": [],
            "top_contributors": [],
            "all_contributors": [],
            "leaderboards": {
                "prs_merged": [],
                "prs_opened": [],
                "reviews_submitted": [],
                "approvals": [],
                "changes_requested": [],
                "issues_opened": [],
                "issues_closed": [],
                "comments_total": [],
                "review_comments_total": [],
                "overall": [],
            },
            "repo_summary": {},
            "repos": [],
            "hygiene": {},
            "repo_activity": {},
            "awards": {"individual": [], "repository": [], "risk": []},
            "special_mentions": {
                "first_contributions": [],
                "consistent_contributors": [],
                "largest_prs": [],
                "fastest_merges": [],
            },
            "fun_facts": {},
            "collaboration_data": {},
            "velocity_data": {},
            "quality_data": {},
            "community_data": {},
            "contribution_timeline": {},
            "contribution_types": {},
            "contribution_by_repo": {},
        }

        html = template.render(**minimal_context)
        assert '<div class="page-header">' in html, f"{template_name} missing page-header div"


def test_responsive_meta_tag_in_base(template_env, base_context):
    """Test that base template includes responsive viewport meta tag."""
    template = template_env.get_template("base.html")
    html = template.render(**base_context, content="")

    assert 'name="viewport"' in html
    assert "width=device-width" in html


def test_theme_toggle_in_base(template_env, base_context):
    """Test that base template includes theme toggle button."""
    template = template_env.get_template("base.html")
    html = template.render(**base_context, content="")

    assert 'class="theme-toggle"' in html
    assert "sun-icon" in html
    assert "moon-icon" in html


def test_navigation_links_in_base(template_env, base_context):
    """Test that base template includes all navigation links."""
    template = template_env.get_template("base.html")
    html = template.render(**base_context, content="")

    expected_links = [
        "index.html",
        "summary.html",
        "engineers.html",
        "leaderboards.html",
        "repos.html",
        "awards.html",
    ]

    for link in expected_links:
        assert link in html, f"Missing navigation link to {link}"


def test_css_link_in_base(template_env, base_context):
    """Test that base template links to style.css."""
    template = template_env.get_template("base.html")
    html = template.render(**base_context, content="")

    assert 'href="assets/css/style.css"' in html


def test_d3_script_in_chart_pages(template_env, base_context):
    """Test that pages with charts include D3.js script tag."""
    chart_pages = [
        "index.html",
        "summary.html",
        "engineers.html",
        "leaderboards.html",
        "repos.html",
    ]

    minimal_context = {
        **base_context,
        "summary": {},
        "highlights": {},
        "activity_timeline": [],
        "health": {},
        "risks": [],
        "insights": {},
        "recommendations": [],
        "top_repos": [],
        "top_contributors": [],
        "all_contributors": [],
        "leaderboards": {
            "prs_merged": [],
            "prs_opened": [],
            "reviews_submitted": [],
            "approvals": [],
            "changes_requested": [],
            "issues_opened": [],
            "issues_closed": [],
            "comments_total": [],
            "review_comments_total": [],
            "overall": [],
        },
        "repo_summary": {},
        "repos": [],
        "hygiene": {},
        "repo_activity": {},
        "collaboration_data": {},
        "velocity_data": {},
        "quality_data": {},
        "community_data": {},
        "contribution_timeline": {},
        "contribution_types": {},
        "contribution_by_repo": {},
    }

    for page in chart_pages:
        template = template_env.get_template(page)
        html = template.render(**minimal_context)

        assert "d3.v7.min.js" in html, f"{page} missing D3.js script"


def test_templates_escape_user_content(template_env, base_context):
    """Test that templates properly escape user-generated content."""
    template = template_env.get_template("engineers.html")

    # Inject potential XSS in user login
    context = {
        **base_context,
        "top_contributors": [
            {
                "user_id": "user1",
                "login": "<script>alert('xss')</script>",
                "rank": 1,
                "prs_merged": 150,
                "reviews_submitted": 200,
                "issues_closed": 50,
                "comments_total": 300,
            }
        ],
        "all_contributors": [],
        "contribution_timeline": {},
        "contribution_types": {},
        "contribution_by_repo": {},
    }

    html = template.render(**context)

    # Script tags should be escaped
    assert "<script>alert('xss')</script>" not in html
    assert "&lt;script&gt;" in html or html.count("<script>") == html.count("</script>")


def test_footer_has_generation_info(template_env, base_context):
    """Test that footer includes generation information."""
    template = template_env.get_template("base.html")
    html = template.render(**base_context, content="")

    assert "2025-12-18" in html  # generation date
    assert "1.0.0" in html  # version


def test_templates_handle_missing_optional_data(template_env, base_context):
    """Test that templates gracefully handle missing optional data."""
    template = template_env.get_template("index.html")

    # Minimal context with missing optional fields
    context = {
        **base_context,
        "summary": {},
        "highlights": {},
        "activity_timeline": [],
    }

    # Should render without errors
    html = template.render(**context)
    assert html is not None
    assert len(html) > 0


def test_chart_containers_have_ids(template_env, base_context):
    """Test that chart containers have proper IDs for D3 selection."""
    template = template_env.get_template("index.html")

    context = {
        **base_context,
        "summary": {},
        "highlights": {},
        "activity_timeline": [],
    }

    html = template.render(**context)

    # Check for chart container IDs
    assert 'id="chart-activity-timeline"' in html
