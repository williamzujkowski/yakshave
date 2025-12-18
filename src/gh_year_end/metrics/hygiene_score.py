"""Hygiene score calculator for repository health metrics.

Calculates a 0-100 hygiene score for each repository based on:
- Essential documentation files (README, LICENSE, etc.)
- Security features (Dependabot, secret scanning)
- Development process hygiene (CI workflows, branch protection, code reviews)
"""

import logging
from pathlib import Path

import pandas as pd

from gh_year_end.config import Config

logger = logging.getLogger(__name__)


# Scoring weights (total: 100 points)
WEIGHTS = {
    "readme": 15,
    "license": 10,
    "contributing": 5,
    "code_of_conduct": 5,
    "security_md": 10,
    "codeowners": 10,
    "ci_workflows": 15,
    "branch_protection": 15,
    "requires_reviews": 5,
    "dependabot": 5,
    "secret_scanning": 5,
}


def calculate_hygiene_scores(
    curated_path: Path,
    config: Config,
) -> pd.DataFrame:
    """Calculate hygiene scores for all repositories.

    Reads curated Parquet tables and computes a weighted hygiene score based on
    the presence of documentation, security features, and development practices.

    Args:
        curated_path: Path to curated data directory (year=YYYY/).
        config: Application configuration.

    Returns:
        DataFrame with schema:
            - repo_id: Repository node ID (str)
            - repo_full_name: Full repository name (str)
            - year: Year (int)
            - score: Overall hygiene score 0-100 (int)
            - has_readme: README file exists (bool)
            - has_license: LICENSE file exists (bool)
            - has_contributing: CONTRIBUTING file exists (bool)
            - has_code_of_conduct: CODE_OF_CONDUCT file exists (bool)
            - has_security_md: SECURITY.md file exists (bool)
            - has_codeowners: CODEOWNERS file exists (bool)
            - has_ci_workflows: CI workflows exist (bool)
            - branch_protection_enabled: Branch protection enabled (bool, nullable)
            - requires_reviews: Requires PR reviews (bool, nullable)
            - dependabot_enabled: Dependabot alerts enabled (bool, nullable)
            - secret_scanning_enabled: Secret scanning enabled (bool, nullable)
            - notes: Comma-separated list of issues/warnings (str)

    Raises:
        FileNotFoundError: If curated tables are missing.
    """
    logger.info("Starting hygiene score calculation from %s", curated_path)

    # Read curated tables
    file_presence_df = _read_parquet_safe(curated_path / "fact_repo_files_presence.parquet")
    hygiene_df = _read_parquet_safe(curated_path / "fact_repo_hygiene.parquet")
    security_df = _read_parquet_safe(curated_path / "fact_repo_security_features.parquet")

    # Get unique repos from file presence (primary source)
    if file_presence_df.empty and hygiene_df.empty and security_df.empty:
        logger.warning("No hygiene data found in curated tables")
        return _empty_hygiene_score_dataframe(config.github.windows.year)

    # Collect all repo IDs and names
    repos = _collect_repos(file_presence_df, hygiene_df, security_df)

    if repos.empty:
        logger.warning("No repositories found in hygiene data")
        return _empty_hygiene_score_dataframe(config.github.windows.year)

    # Calculate scores for each repo
    scores = []
    for _, repo in repos.iterrows():
        repo_id = repo["repo_id"]
        repo_full_name = repo["repo_full_name"]

        score_data = _calculate_repo_score(
            repo_id,
            repo_full_name,
            file_presence_df,
            hygiene_df,
            security_df,
            config,
        )
        scores.append(score_data)

    result_df = pd.DataFrame(scores)

    # Sort for deterministic output
    result_df = result_df.sort_values(
        by=["repo_full_name"],
    ).reset_index(drop=True)

    logger.info("Calculated hygiene scores for %d repositories", len(result_df))
    return result_df


def _read_parquet_safe(path: Path) -> pd.DataFrame:
    """Read Parquet file safely, returning empty DataFrame if not found.

    Args:
        path: Path to Parquet file.

    Returns:
        DataFrame or empty DataFrame if file doesn't exist.
    """
    if not path.exists():
        logger.debug("Parquet file not found: %s", path)
        return pd.DataFrame()

    try:
        return pd.read_parquet(path)
    except Exception as e:
        logger.error("Failed to read %s: %s", path, e)
        return pd.DataFrame()


def _collect_repos(
    file_presence_df: pd.DataFrame,
    hygiene_df: pd.DataFrame,
    security_df: pd.DataFrame,
) -> pd.DataFrame:
    """Collect unique repositories from all hygiene tables.

    Args:
        file_presence_df: File presence DataFrame.
        hygiene_df: Branch protection DataFrame.
        security_df: Security features DataFrame.

    Returns:
        DataFrame with repo_id and repo_full_name columns.
    """
    all_repos = []

    if not file_presence_df.empty and "repo_id" in file_presence_df.columns:
        all_repos.append(file_presence_df[["repo_id", "repo_full_name"]].copy())

    if not hygiene_df.empty and "repo_id" in hygiene_df.columns:
        all_repos.append(hygiene_df[["repo_id", "repo_full_name"]].copy())

    if not security_df.empty and "repo_id" in security_df.columns:
        all_repos.append(security_df[["repo_id", "repo_full_name"]].copy())

    if not all_repos:
        return pd.DataFrame(columns=["repo_id", "repo_full_name"])

    combined = pd.concat(all_repos, ignore_index=True)
    return combined.drop_duplicates(subset=["repo_id"]).reset_index(drop=True)


def _calculate_repo_score(
    repo_id: str,
    repo_full_name: str,
    file_presence_df: pd.DataFrame,
    hygiene_df: pd.DataFrame,
    security_df: pd.DataFrame,
    config: Config,
) -> dict[str, object]:
    """Calculate hygiene score for a single repository.

    Args:
        repo_id: Repository node ID.
        repo_full_name: Full repository name.
        file_presence_df: File presence DataFrame.
        hygiene_df: Branch protection DataFrame.
        security_df: Security features DataFrame.
        config: Application configuration.

    Returns:
        Dictionary with score data for the repository.
    """
    # Extract file presence flags
    files = _extract_file_presence(repo_id, file_presence_df)

    # Extract branch protection flags
    branch_protection = _extract_branch_protection(repo_id, hygiene_df)

    # Extract security features
    security = _extract_security_features(repo_id, security_df)

    # Calculate score
    score = 0
    notes = []

    # Documentation files
    if files["has_readme"]:
        score += WEIGHTS["readme"]
    else:
        notes.append("missing README")

    if files["has_license"]:
        score += WEIGHTS["license"]
    else:
        notes.append("missing LICENSE")

    if files["has_contributing"]:
        score += WEIGHTS["contributing"]
    else:
        notes.append("missing CONTRIBUTING")

    if files["has_code_of_conduct"]:
        score += WEIGHTS["code_of_conduct"]
    else:
        notes.append("missing CODE_OF_CONDUCT")

    if files["has_security_md"]:
        score += WEIGHTS["security_md"]
    else:
        notes.append("missing SECURITY.md")

    if files["has_codeowners"]:
        score += WEIGHTS["codeowners"]
    else:
        notes.append("missing CODEOWNERS")

    # CI workflows
    if files["has_ci_workflows"]:
        score += WEIGHTS["ci_workflows"]
    else:
        notes.append("no CI workflows")

    # Branch protection (nullable)
    if branch_protection["enabled"] is True:
        score += WEIGHTS["branch_protection"]

        if branch_protection["requires_reviews"] is True:
            score += WEIGHTS["requires_reviews"]
        elif branch_protection["requires_reviews"] is False:
            notes.append("no required reviews")
    elif branch_protection["enabled"] is False:
        notes.append("no branch protection")
    else:
        notes.append("branch protection status unknown")

    # Security features (nullable)
    if security["dependabot"] is True:
        score += WEIGHTS["dependabot"]
    elif security["dependabot"] is False:
        notes.append("Dependabot not enabled")
    else:
        notes.append("Dependabot status unknown")

    if security["secret_scanning"] is True:
        score += WEIGHTS["secret_scanning"]
    elif security["secret_scanning"] is False:
        notes.append("secret scanning not enabled")
    else:
        notes.append("secret scanning status unknown")

    return {
        "repo_id": repo_id,
        "repo_full_name": repo_full_name,
        "year": config.github.windows.year,
        "score": score,
        "has_readme": files["has_readme"],
        "has_license": files["has_license"],
        "has_contributing": files["has_contributing"],
        "has_code_of_conduct": files["has_code_of_conduct"],
        "has_security_md": files["has_security_md"],
        "has_codeowners": files["has_codeowners"],
        "has_ci_workflows": files["has_ci_workflows"],
        "branch_protection_enabled": branch_protection["enabled"],
        "requires_reviews": branch_protection["requires_reviews"],
        "dependabot_enabled": security["dependabot"],
        "secret_scanning_enabled": security["secret_scanning"],
        "notes": ", ".join(notes) if notes else "",
    }


def _extract_file_presence(
    repo_id: str,
    file_presence_df: pd.DataFrame,
) -> dict[str, bool]:
    """Extract file presence flags for a repository.

    Args:
        repo_id: Repository node ID.
        file_presence_df: File presence DataFrame.

    Returns:
        Dictionary with file presence flags.
    """
    if file_presence_df.empty:
        return {
            "has_readme": False,
            "has_license": False,
            "has_contributing": False,
            "has_code_of_conduct": False,
            "has_security_md": False,
            "has_codeowners": False,
            "has_ci_workflows": False,
        }

    repo_files = file_presence_df[file_presence_df["repo_id"] == repo_id]

    if repo_files.empty:
        return {
            "has_readme": False,
            "has_license": False,
            "has_contributing": False,
            "has_code_of_conduct": False,
            "has_security_md": False,
            "has_codeowners": False,
            "has_ci_workflows": False,
        }

    # Helper to check if any file exists
    def has_file(patterns: list[str]) -> bool:
        for pattern in patterns:
            matching = repo_files[
                repo_files["path"].str.upper().str.contains(pattern.upper(), na=False)
            ]
            if not matching.empty and matching["exists"].any():
                return True
        return False

    return {
        "has_readme": has_file(["README"]),
        "has_license": has_file(["LICENSE", "LICENCE"]),
        "has_contributing": has_file(["CONTRIBUTING"]),
        "has_code_of_conduct": has_file(["CODE_OF_CONDUCT", "CODE-OF-CONDUCT"]),
        "has_security_md": has_file(["SECURITY.MD", "SECURITY.RST"]),
        "has_codeowners": has_file(["CODEOWNERS"]),
        "has_ci_workflows": has_file([".GITHUB/WORKFLOWS/", ".GITLAB-CI"]),
    }


def _extract_branch_protection(
    repo_id: str,
    hygiene_df: pd.DataFrame,
) -> dict[str, bool | None]:
    """Extract branch protection flags for a repository.

    Args:
        repo_id: Repository node ID.
        hygiene_df: Branch protection DataFrame.

    Returns:
        Dictionary with branch protection flags (nullable).
    """
    if hygiene_df.empty:
        return {
            "enabled": None,
            "requires_reviews": None,
        }

    repo_hygiene = hygiene_df[hygiene_df["repo_id"] == repo_id]

    if repo_hygiene.empty:
        return {
            "enabled": None,
            "requires_reviews": None,
        }

    # Use the most recent record
    latest = repo_hygiene.sort_values("captured_at", ascending=False).iloc[0]

    # If there's an error, protection status is unknown
    if pd.notna(latest.get("error")):
        return {
            "enabled": None,
            "requires_reviews": None,
        }

    # Check if requires_reviews is not null
    requires_reviews = latest.get("requires_reviews")
    if pd.isna(requires_reviews):
        # No protection data available
        return {
            "enabled": None,
            "requires_reviews": None,
        }

    # If we get here, we have valid protection data
    # Protection is enabled if requires_reviews field has a value (True or False)
    return {
        "enabled": True,
        "requires_reviews": bool(requires_reviews),
    }


def _extract_security_features(
    repo_id: str,
    security_df: pd.DataFrame,
) -> dict[str, bool | None]:
    """Extract security feature flags for a repository.

    Args:
        repo_id: Repository node ID.
        security_df: Security features DataFrame.

    Returns:
        Dictionary with security feature flags (nullable).
    """
    if security_df.empty:
        return {
            "dependabot": None,
            "secret_scanning": None,
        }

    repo_security = security_df[security_df["repo_id"] == repo_id]

    if repo_security.empty:
        return {
            "dependabot": None,
            "secret_scanning": None,
        }

    # Use the most recent record
    latest = repo_security.sort_values("captured_at", ascending=False).iloc[0]

    # If there's an error, status is unknown
    if pd.notna(latest.get("error")):
        return {
            "dependabot": None,
            "secret_scanning": None,
        }

    dependabot = latest.get("dependabot_alerts_enabled")
    secret_scanning = latest.get("secret_scanning_enabled")

    return {
        "dependabot": None if pd.isna(dependabot) else bool(dependabot),
        "secret_scanning": None if pd.isna(secret_scanning) else bool(secret_scanning),
    }


def _empty_hygiene_score_dataframe(year: int) -> pd.DataFrame:
    """Create empty DataFrame with hygiene score schema.

    Args:
        year: Year for the dataset.

    Returns:
        Empty DataFrame with correct columns and types.
    """
    return pd.DataFrame(
        columns=[
            "repo_id",
            "repo_full_name",
            "year",
            "score",
            "has_readme",
            "has_license",
            "has_contributing",
            "has_code_of_conduct",
            "has_security_md",
            "has_codeowners",
            "has_ci_workflows",
            "branch_protection_enabled",
            "requires_reviews",
            "dependabot_enabled",
            "secret_scanning_enabled",
            "notes",
        ]
    ).astype(
        {
            "repo_id": "string",
            "repo_full_name": "string",
            "year": "int64",
            "score": "int64",
            "has_readme": "bool",
            "has_license": "bool",
            "has_contributing": "bool",
            "has_code_of_conduct": "bool",
            "has_security_md": "bool",
            "has_codeowners": "bool",
            "has_ci_workflows": "bool",
            "branch_protection_enabled": "boolean",
            "requires_reviews": "boolean",
            "dependabot_enabled": "boolean",
            "secret_scanning_enabled": "boolean",
            "notes": "string",
        }
    )
