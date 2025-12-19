"""Repository metadata collector with detailed GraphQL queries.

Collects comprehensive repository information including languages, topics,
branch protection, license info, and file presence indicators.
"""

import logging
from typing import Any

from gh_year_end.config import Config
from gh_year_end.github.graphql import GraphQLClient, GraphQLError
from gh_year_end.github.ratelimit import AdaptiveRateLimiter
from gh_year_end.storage.writer import AsyncJSONLWriter

logger = logging.getLogger(__name__)


class RepoMetadataError(Exception):
    """Raised when repository metadata collection fails."""


# GraphQL query for detailed repository metadata
REPO_METADATA_QUERY = """
query($owner: String!, $name: String!) {
  repository(owner: $owner, name: $name) {
    id
    databaseId
    name
    nameWithOwner
    description
    createdAt
    updatedAt
    pushedAt
    url
    homepageUrl
    isPrivate
    isFork
    isArchived
    isDisabled
    isLocked
    isMirror
    isTemplate
    hasIssuesEnabled
    hasProjectsEnabled
    hasWikiEnabled
    hasDiscussionsEnabled
    stargazerCount
    forkCount
    watchers {
      totalCount
    }
    diskUsageKb: diskUsage
    defaultBranchRef {
      name
      id
    }
    primaryLanguage {
      name
      color
    }
    languages(first: 100, orderBy: {field: SIZE, direction: DESC}) {
      totalSize
      totalCount
      edges {
        size
        node {
          name
          color
        }
      }
    }
    repositoryTopics(first: 20) {
      totalCount
      nodes {
        topic {
          name
        }
      }
    }
    licenseInfo {
      name
      spdxId
      url
      key
    }
    codeOfConduct {
      name
      key
      url
    }
    fundingLinks {
      platform
      url
    }
    securityPolicyUrl
    owner {
      __typename
      login
      ... on User {
        name
        email
      }
      ... on Organization {
        name
        email
      }
    }
    collaborators {
      totalCount
    }
    issues {
      totalCount
    }
    closedIssues: issues(states: CLOSED) {
      totalCount
    }
    openIssues: issues(states: OPEN) {
      totalCount
    }
    pullRequests {
      totalCount
    }
    mergedPullRequests: pullRequests(states: MERGED) {
      totalCount
    }
    openPullRequests: pullRequests(states: OPEN) {
      totalCount
    }
    closedPullRequests: pullRequests(states: CLOSED) {
      totalCount
    }
    releases {
      totalCount
    }
    deployments {
      totalCount
    }
    vulnerabilityAlerts {
      totalCount
    }
    hasVulnerabilityAlertsEnabled
    dependencyGraphManifests {
      totalCount
    }
  }
}
"""

# Separate query for branch protection (may fail due to permissions)
BRANCH_PROTECTION_QUERY = """
query($owner: String!, $name: String!) {
  repository(owner: $owner, name: $name) {
    branchProtectionRule: branchProtectionRules(first: 1) {
      nodes {
        id
        pattern
        allowsDeletions
        allowsForcePushes
        dismissesStaleReviews
        isAdminEnforced
        lockAllowsFetchAndMerge
        lockBranch
        requireLastPushApproval
        requiredApprovingReviewCount
        requiredDeploymentEnvironments
        requiresApprovingReviews
        requiresCodeOwnerReviews
        requiresCommitSignatures
        requiresConversationResolution
        requiresDeployments
        requiresLinearHistory
        requiresStatusChecks
        requiresStrictStatusChecks
        restrictsPushes
        restrictsReviewDismissals
      }
    }
  }
}
"""

# Query for checking specific file presence
FILE_PRESENCE_QUERY = """
query($owner: String!, $name: String!, $expression: String!) {
  repository(owner: $owner, name: $name) {
    object(expression: $expression) {
      ... on Blob {
        id
        byteSize
        text
      }
    }
  }
}
"""

# Query for workflows presence
WORKFLOWS_QUERY = """
query($owner: String!, $name: String!) {
  repository(owner: $owner, name: $name) {
    defaultBranchRef {
      target {
        ... on Commit {
          tree {
            entries {
              name
              type
              path
            }
          }
        }
      }
    }
  }
}
"""


async def collect_repo_metadata(
    repos: list[dict[str, Any]],
    graphql_client: GraphQLClient,
    writer: AsyncJSONLWriter,
    rate_limiter: AdaptiveRateLimiter,
    config: Config,
) -> dict[str, Any]:
    """Collect detailed repository metadata using GraphQL.

    Args:
        repos: List of basic repo metadata from discovery.
        graphql_client: GraphQL client for queries.
        writer: JSONL writer for raw data output.
        rate_limiter: Rate limiter for throttling.
        config: Application configuration.

    Returns:
        Stats dictionary with counts of repos processed, errors, etc.

    Raises:
        RepoMetadataError: If collection fails critically.
    """
    logger.info("Starting repository metadata collection for %d repos", len(repos))

    stats: dict[str, Any] = {
        "repos_total": len(repos),
        "repos_processed": 0,
        "repos_failed": 0,
        "branch_protection_accessible": 0,
        "branch_protection_failed": 0,
        "errors": [],
    }

    for idx, repo in enumerate(repos, 1):
        repo_name = repo.get("full_name", "unknown")
        logger.info(
            "Collecting metadata for repo %d/%d: %s",
            idx,
            len(repos),
            repo_name,
        )

        try:
            # Parse owner and repo name
            owner, name = _parse_repo_name(repo_name)

            # Fetch detailed metadata
            metadata = await _fetch_repo_metadata(
                owner=owner,
                name=name,
                graphql_client=graphql_client,
                rate_limiter=rate_limiter,
            )

            if not metadata:
                logger.warning("No metadata returned for %s", repo_name)
                stats["repos_failed"] += 1
                stats["errors"].append(
                    {
                        "repo": repo_name,
                        "error": "Empty metadata response",
                    }
                )
                continue

            # Add basic metadata from discovery
            metadata["discovery_metadata"] = repo

            # Try to fetch branch protection (may fail due to permissions)
            default_branch = metadata.get("defaultBranchRef", {}).get("name", "main")
            branch_protection = await _fetch_branch_protection(
                owner=owner,
                name=name,
                branch=default_branch,
                graphql_client=graphql_client,
                rate_limiter=rate_limiter,
                config=config,
            )

            if branch_protection:
                metadata["branchProtection"] = branch_protection
                stats["branch_protection_accessible"] += 1
            else:
                stats["branch_protection_failed"] += 1

            # Check for specific files if hygiene collection is enabled
            if config.collection.enable.hygiene:
                file_presence = await _check_file_presence(
                    owner=owner,
                    name=name,
                    default_branch=default_branch,
                    graphql_client=graphql_client,
                    rate_limiter=rate_limiter,
                    config=config,
                )
                metadata["filePresence"] = file_presence

            # Write to JSONL
            await writer.write(
                source="github_graphql",
                endpoint=f"repository:{owner}/{name}",
                data=metadata,
            )

            stats["repos_processed"] += 1
            logger.debug(
                "Successfully collected metadata for %s (%d/%d)",
                repo_name,
                stats["repos_processed"],
                len(repos),
            )

        except Exception as e:
            logger.error("Failed to collect metadata for %s: %s", repo_name, e)
            stats["repos_failed"] += 1
            stats["errors"].append(
                {
                    "repo": repo_name,
                    "error": str(e),
                }
            )
            continue

    logger.info(
        "Repository metadata collection complete: processed=%d, failed=%d",
        stats["repos_processed"],
        stats["repos_failed"],
    )

    return stats


async def _fetch_repo_metadata(
    owner: str,
    name: str,
    graphql_client: GraphQLClient,
    rate_limiter: AdaptiveRateLimiter,
) -> dict[str, Any]:
    """Fetch detailed repository metadata.

    Args:
        owner: Repository owner.
        name: Repository name.
        graphql_client: GraphQL client.
        rate_limiter: Rate limiter.

    Returns:
        Repository metadata dictionary.
    """
    try:
        data = await graphql_client.execute(
            REPO_METADATA_QUERY,
            {"owner": owner, "name": name},
        )
        repo_data: dict[str, Any] = data.get("repository", {})
        return repo_data

    except GraphQLError as e:
        logger.warning(
            "GraphQL error fetching metadata for %s/%s: %s",
            owner,
            name,
            e,
        )
        return {}

    except Exception as e:
        logger.error(
            "Unexpected error fetching metadata for %s/%s: %s",
            owner,
            name,
            e,
        )
        return {}


async def _fetch_branch_protection(
    owner: str,
    name: str,
    branch: str,
    graphql_client: GraphQLClient,
    rate_limiter: AdaptiveRateLimiter,
    config: Config,
) -> dict[str, Any] | None:
    """Fetch branch protection settings.

    Args:
        owner: Repository owner.
        name: Repository name.
        branch: Branch name to check.
        graphql_client: GraphQL client.
        rate_limiter: Rate limiter.
        config: Application configuration.

    Returns:
        Branch protection settings or None if not accessible.
    """
    # Check if branch protection collection is enabled
    bp_mode = config.collection.hygiene.branch_protection.mode
    if bp_mode == "skip":
        return None

    try:
        data = await graphql_client.execute(
            BRANCH_PROTECTION_QUERY,
            {"owner": owner, "name": name},
        )

        repo_data = data.get("repository", {})
        rules = repo_data.get("branchProtectionRule", {}).get("nodes", [])

        if rules:
            # Return first matching rule
            first_rule: dict[str, Any] = rules[0]
            return first_rule

        return None

    except GraphQLError as e:
        logger.debug(
            "Branch protection not accessible for %s/%s: %s",
            owner,
            name,
            e,
        )
        return None

    except Exception as e:
        logger.debug(
            "Error fetching branch protection for %s/%s: %s",
            owner,
            name,
            e,
        )
        return None


async def _check_file_presence(
    owner: str,
    name: str,
    default_branch: str,
    graphql_client: GraphQLClient,
    rate_limiter: AdaptiveRateLimiter,
    config: Config,
) -> dict[str, bool]:
    """Check presence of specific files in repository.

    Args:
        owner: Repository owner.
        name: Repository name.
        default_branch: Default branch name.
        graphql_client: GraphQL client.
        rate_limiter: Rate limiter.
        config: Application configuration.

    Returns:
        Dictionary mapping file paths to presence boolean.
    """
    file_presence: dict[str, bool] = {}
    paths_to_check = config.collection.hygiene.paths

    for path in paths_to_check:
        try:
            expression = f"{default_branch}:{path}"
            data = await graphql_client.execute(
                FILE_PRESENCE_QUERY,
                {"owner": owner, "name": name, "expression": expression},
            )

            repo_data = data.get("repository", {})
            file_obj = repo_data.get("object")

            # If object exists and has an id, file is present
            file_presence[path] = file_obj is not None and file_obj.get("id") is not None

        except GraphQLError:
            # File doesn't exist or not accessible
            file_presence[path] = False

        except Exception as e:
            logger.debug(
                "Error checking file %s for %s/%s: %s",
                path,
                owner,
                name,
                e,
            )
            file_presence[path] = False

    # Check for workflows
    has_workflows = await _check_workflows_presence(
        owner=owner,
        name=name,
        graphql_client=graphql_client,
        rate_limiter=rate_limiter,
        config=config,
    )
    file_presence["_has_workflows"] = has_workflows

    return file_presence


async def _check_workflows_presence(
    owner: str,
    name: str,
    graphql_client: GraphQLClient,
    rate_limiter: AdaptiveRateLimiter,
    config: Config,
) -> bool:
    """Check if repository has any workflow files.

    Args:
        owner: Repository owner.
        name: Repository name.
        graphql_client: GraphQL client.
        rate_limiter: Rate limiter.
        config: Application configuration.

    Returns:
        True if workflows directory exists and has files.
    """
    try:
        data = await graphql_client.execute(
            WORKFLOWS_QUERY,
            {"owner": owner, "name": name},
        )

        repo_data = data.get("repository", {})
        default_branch = repo_data.get("defaultBranchRef", {})
        target = default_branch.get("target", {})
        tree = target.get("tree", {})
        entries = tree.get("entries", [])

        # Check if any entry is in .github/workflows/
        workflow_prefixes = config.collection.hygiene.workflow_prefixes
        for entry in entries:
            entry_path = entry.get("path", "")
            for prefix in workflow_prefixes:
                if entry_path.startswith(prefix) and entry.get("type") == "blob":
                    return True

        return False

    except GraphQLError:
        return False

    except Exception as e:
        logger.debug(
            "Error checking workflows for %s/%s: %s",
            owner,
            name,
            e,
        )
        return False


def _parse_repo_name(full_name: str) -> tuple[str, str]:
    """Parse full repository name into owner and repo name.

    Args:
        full_name: Full repository name in format "owner/repo".

    Returns:
        Tuple of (owner, repo_name).

    Raises:
        ValueError: If name format is invalid.
    """
    parts = full_name.split("/")
    if len(parts) != 2:
        msg = f"Invalid repository name format: {full_name}"
        raise ValueError(msg)

    return parts[0], parts[1]
