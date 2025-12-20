#!/usr/bin/env python3
"""Demonstration of MetricsAggregator usage.

This script shows how to use the MetricsAggregator class to compute
metrics in-memory during collection.
"""

from gh_year_end.collect.aggregator import MetricsAggregator


def main():
    """Demonstrate aggregator usage."""
    # Create aggregator for 2024
    agg = MetricsAggregator(year=2024, target_name="myorg", target_mode="org")

    # Add a repository
    repo = {
        "full_name": "myorg/myrepo",
        "name": "myrepo",
        "description": "A sample repository",
        "html_url": "https://github.com/myorg/myrepo",
    }
    agg.add_repo(repo)

    # Add some PRs
    pr1 = {
        "user": {"login": "alice", "avatar_url": "https://example.com/alice.png", "type": "User"},
        "created_at": "2024-01-15T10:00:00Z",
        "merged_at": "2024-01-16T10:00:00Z",
    }
    pr2 = {
        "user": {"login": "bob", "avatar_url": "https://example.com/bob.png", "type": "User"},
        "created_at": "2024-02-10T10:00:00Z",
    }
    # Bot PR (should be filtered)
    pr_bot = {
        "user": {"login": "dependabot[bot]", "avatar_url": "", "type": "Bot"},
        "created_at": "2024-01-20T10:00:00Z",
    }

    agg.add_pr("myorg/myrepo", pr1)
    agg.add_pr("myorg/myrepo", pr2)
    agg.add_pr("myorg/myrepo", pr_bot)

    # Add an issue
    issue = {
        "user": {
            "login": "charlie",
            "avatar_url": "https://example.com/charlie.png",
            "type": "User",
        },
        "created_at": "2024-01-25T10:00:00Z",
        "state": "closed",
        "closed_at": "2024-01-30T10:00:00Z",
    }
    agg.add_issue("myorg/myrepo", issue)

    # Add a review
    review = {
        "user": {"login": "alice", "avatar_url": "https://example.com/alice.png", "type": "User"},
        "submitted_at": "2024-01-15T12:00:00Z",
        "state": "APPROVED",
    }
    agg.add_review("myorg/myrepo", 1, review)

    # Add comments
    comment1 = {
        "user": {"login": "bob", "avatar_url": "https://example.com/bob.png", "type": "User"},
        "created_at": "2024-01-15T14:00:00Z",
    }
    comment2 = {
        "user": {"login": "alice", "avatar_url": "https://example.com/alice.png", "type": "User"},
        "created_at": "2024-01-16T09:00:00Z",
    }
    agg.add_comment("myorg/myrepo", comment1, comment_type="issue")
    agg.add_comment("myorg/myrepo", comment2, comment_type="review")

    # Export metrics
    metrics = agg.export()

    # Print summary
    print("Summary:")
    print(f"  Total PRs: {metrics['summary']['total_prs']}")
    print(f"  Total Issues: {metrics['summary']['total_issues']}")
    print(f"  Total Reviews: {metrics['summary']['total_reviews']}")
    print(f"  Total Contributors: {metrics['summary']['total_contributors']}")
    print()

    # Print leaderboards
    print("PR Leaderboard:")
    for entry in metrics["leaderboards"]["prs_opened"]:
        print(f"  {entry['user']}: {entry['count']} PRs")
    print()

    print("Review Leaderboard:")
    for entry in metrics["leaderboards"]["reviews_submitted"]:
        print(f"  {entry['user']}: {entry['count']} reviews")
    print()

    # Print repo health
    print("Repository Health:")
    for repo_health in metrics["repo_health"]:
        print(f"  {repo_health['repo']}:")
        print(f"    Contributors: {repo_health['contributor_count']}")
        print(f"    PRs: {repo_health['pr_count']}")
        print(f"    Issues: {repo_health['issue_count']}")
    print()

    # Print awards
    print("Awards:")
    if "top_pr_author" in metrics["awards"]:
        print(
            f"  Top PR Author: {metrics['awards']['top_pr_author']['user']} ({metrics['awards']['top_pr_author']['count']} PRs)"
        )
    if "top_reviewer" in metrics["awards"]:
        print(
            f"  Top Reviewer: {metrics['awards']['top_reviewer']['user']} ({metrics['awards']['top_reviewer']['count']} reviews)"
        )
    print()

    print("Bot filtering: dependabot[bot] was filtered from all metrics")


if __name__ == "__main__":
    main()
