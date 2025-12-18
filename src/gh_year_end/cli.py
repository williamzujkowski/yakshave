"""CLI entry point for gh-year-end."""

from pathlib import Path

import click
from rich.console import Console

from gh_year_end import __version__
from gh_year_end.config import load_config
from gh_year_end.logging import setup_logging

console = Console()


@click.group()
@click.version_option(version=__version__, prog_name="gh-year-end")
@click.option("--verbose", "-v", is_flag=True, default=False, help="Enable verbose output")
@click.pass_context
def main(ctx: click.Context, verbose: bool) -> None:
    """GitHub Year-End Community Health Report Generator.

    Generate comprehensive year-end reports for GitHub organizations or users,
    including activity metrics, leaderboards, and repository health scores.
    """
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    setup_logging(verbose=verbose)


@main.command()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="Path to config.yaml file",
)
@click.pass_context
def plan(ctx: click.Context, config: Path) -> None:
    """Show what will be collected without making any changes.

    Validates the config and prints the collection plan including:
    - Target org/user
    - Time window
    - What data will be collected
    - Where data will be stored
    """
    cfg = load_config(config)
    console.print("[bold]Collection Plan[/bold]")
    console.print(f"  Target: {cfg.github.target.mode} / {cfg.github.target.name}")
    console.print(f"  Year: {cfg.github.windows.year}")
    console.print(f"  Since: {cfg.github.windows.since}")
    console.print(f"  Until: {cfg.github.windows.until}")
    console.print(f"  Storage root: {cfg.storage.root}")
    console.print("\n[bold]Enabled collectors:[/bold]")
    collection = cfg.collection.enable
    console.print(f"  - PRs: {collection.pulls}")
    console.print(f"  - Issues: {collection.issues}")
    console.print(f"  - Reviews: {collection.reviews}")
    console.print(f"  - Comments: {collection.comments}")
    console.print(f"  - Commits: {collection.commits}")
    console.print(f"  - Hygiene: {collection.hygiene}")


@main.command()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="Path to config.yaml file",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    default=False,
    help="Delete checkpoint and start fresh",
)
@click.option(
    "--resume",
    "-r",
    is_flag=True,
    default=False,
    help="Require existing checkpoint (fail if none)",
)
@click.option(
    "--from-repo",
    type=str,
    default=None,
    help="Resume starting from specific repo (e.g., 'owner/repo')",
)
@click.option(
    "--retry-failed",
    is_flag=True,
    default=False,
    help="Only retry repos marked as failed",
)
@click.pass_context
def collect(
    ctx: click.Context,
    config: Path,
    force: bool,
    resume: bool,
    from_repo: str | None,
    retry_failed: bool,
) -> None:
    """Collect raw data from GitHub API.

    Fetches all configured data types (PRs, issues, reviews, etc.) from
    the target org/user and stores as raw JSONL files.

    Supports checkpoint/resume functionality:
    - Use --resume to continue from last checkpoint
    - Use --force to delete checkpoint and start fresh
    - Use --from-repo to resume from specific repository
    - Use --retry-failed to only retry failed repositories
    """
    import asyncio

    from gh_year_end.collect.orchestrator import run_collection
    from gh_year_end.storage.checkpoint import CheckpointManager
    from gh_year_end.storage.paths import PathManager

    cfg = load_config(config)
    paths = PathManager(cfg)

    # Initialize checkpoint manager
    checkpoint_path = paths.checkpoint_path
    checkpoint_mgr = CheckpointManager(checkpoint_path)

    # Handle force mode
    if force:
        if checkpoint_mgr.exists():
            console.print("[yellow]Force mode: deleting existing checkpoint[/yellow]")
            checkpoint_mgr.delete_if_exists()
        console.print("[yellow]Force mode: will re-fetch all data[/yellow]")

    # Handle resume mode
    if resume:
        if not checkpoint_mgr.exists():
            console.print("[bold red]Error:[/bold red] --resume requires existing checkpoint")
            console.print("Run without --resume to start new collection")
            raise click.Abort()

        checkpoint_mgr.load()
        if not checkpoint_mgr.validate_config(cfg):
            console.print(
                "[bold red]Error:[/bold red] Checkpoint config mismatch. Use --force to restart."
            )
            raise click.Abort()

        console.print("[cyan]Resuming from checkpoint[/cyan]")
        stats = checkpoint_mgr.get_stats()
        console.print(f"  Repos complete: {stats['repos_complete']}/{stats['total_repos']}")
        console.print(f"  Repos in progress: {stats['repos_in_progress']}")
        console.print(f"  Repos failed: {stats['repos_failed']}")

    # Handle from_repo option
    if from_repo:
        console.print(f"[cyan]Starting from repo: {from_repo}[/cyan]")

    # Handle retry_failed option
    if retry_failed:
        console.print("[cyan]Retrying failed repositories only[/cyan]")

    console.print()
    console.print(f"[bold]Collecting data for {cfg.github.target.name}[/bold]")
    console.print(f"  Year: {cfg.github.windows.year}")
    console.print(f"  Target: {cfg.github.target.mode} / {cfg.github.target.name}")
    console.print(f"  Storage: {cfg.storage.root}")
    console.print()
    console.print("[bold cyan]Starting collection...[/bold cyan]")

    try:
        # Run async collection with checkpoint support
        stats = asyncio.run(
            run_collection(
                cfg,
                force=force,
                resume=resume,
                from_repo=from_repo,
                retry_failed=retry_failed,
            )
        )

        # Display summary
        console.print()
        console.print("[bold green]Collection complete![/bold green]")
        console.print()
        console.print("[bold]Summary:[/bold]")
        console.print(f"  Duration: {stats.get('duration_seconds', 0):.2f} seconds")
        console.print(
            f"  Repos discovered: {stats.get('discovery', {}).get('repos_discovered', 0)}"
        )
        console.print(f"  Repos processed: {stats.get('repos', {}).get('repos_processed', 0)}")
        console.print(f"  PRs collected: {stats.get('pulls', {}).get('pulls_collected', 0)}")
        console.print(f"  Issues collected: {stats.get('issues', {}).get('issues_collected', 0)}")
        console.print(
            f"  Reviews collected: {stats.get('reviews', {}).get('reviews_collected', 0)}"
        )
        console.print(f"  Comments collected: {stats.get('comments', {}).get('total_comments', 0)}")
        console.print(
            f"  Commits collected: {stats.get('commits', {}).get('commits_collected', 0)}"
        )
        console.print(f"  Rate limit samples: {len(stats.get('rate_limit_samples', []))}")

    except KeyboardInterrupt:
        console.print("\n[yellow]Collection interrupted by user[/yellow]")
        console.print("Checkpoint saved. Use --resume to continue.")
        raise click.Abort() from None
    except Exception as e:
        console.print(f"\n[bold red]Error:[/bold red] {e}")
        if ctx.obj.get("verbose"):
            import traceback

            console.print("\n[dim]Traceback:[/dim]")
            console.print(traceback.format_exc())
        raise click.Abort() from e


@main.command()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="Path to config.yaml file",
)
@click.pass_context
def status(ctx: click.Context, config: Path) -> None:
    """Show current collection status from checkpoint.

    Displays progress information including:
    - Run status and current phase
    - Completed phases
    - Repository progress (complete/in-progress/failed/pending)
    - Error details for failed repositories
    - Estimated time to completion (if in progress)
    """
    from datetime import UTC, datetime

    from gh_year_end.storage.checkpoint import CheckpointManager
    from gh_year_end.storage.paths import PathManager

    cfg = load_config(config)
    paths = PathManager(cfg)

    checkpoint_path = paths.checkpoint_path
    checkpoint_mgr = CheckpointManager(checkpoint_path)

    if not checkpoint_mgr.exists():
        console.print("[yellow]No checkpoint found[/yellow]")
        console.print("Run 'collect' command to start collection")
        return

    try:
        checkpoint_mgr.load()
    except Exception as e:
        console.print(f"[bold red]Error loading checkpoint:[/bold red] {e}")
        raise click.Abort() from e

    # Get checkpoint data
    data = checkpoint_mgr._data

    console.print("[bold]Collection Status[/bold]")
    console.print()

    # Basic info
    console.print("[bold cyan]Run Information:[/bold cyan]")
    console.print(f"  Target: {data.get('target', {}).get('name', 'unknown')}")
    console.print(f"  Year: {data.get('year', 'unknown')}")
    console.print(f"  Created: {data.get('created_at', 'unknown')}")
    console.print(f"  Updated: {data.get('updated_at', 'unknown')}")
    console.print()

    # Phase status
    phases = data.get("phases", {})
    current_phase = data.get("current_phase")

    console.print("[bold cyan]Phase Progress:[/bold cyan]")
    if current_phase:
        console.print(f"  Current phase: [yellow]{current_phase}[/yellow]")

    if phases:
        completed_phases = [p for p, info in phases.items() if info.get("status") == "complete"]
        console.print(
            f"  Completed phases: {', '.join(completed_phases) if completed_phases else 'none'}"
        )
    else:
        console.print("  No phases started")
    console.print()

    # Repo stats
    stats = checkpoint_mgr.get_stats()
    console.print("[bold cyan]Repository Progress:[/bold cyan]")
    console.print(f"  Total repositories: {stats['total_repos']}")
    console.print(f"  Complete: [green]{stats['repos_complete']}[/green]")
    console.print(f"  In progress: [yellow]{stats['repos_in_progress']}[/yellow]")
    console.print(f"  Failed: [red]{stats['repos_failed']}[/red]")
    console.print(f"  Pending: {stats['repos_pending']}")

    if stats["total_repos"] > 0:
        progress_pct = (stats["repos_complete"] / stats["total_repos"]) * 100
        console.print(f"  Progress: {progress_pct:.1f}%")
    console.print()

    # Show failed repos with errors
    failed_repos = []
    repos = data.get("repos", {})
    for repo_name, repo_data in repos.items():
        if repo_data.get("status") == "failed":
            error = repo_data.get("error", {})
            failed_repos.append((repo_name, error))

    if failed_repos:
        console.print("[bold red]Failed Repositories:[/bold red]")
        for repo_name, error in failed_repos[:5]:  # Show first 5
            console.print(f"  {repo_name}")
            if error:
                endpoint = error.get("endpoint", "unknown")
                message = error.get("message", "no message")
                retryable = error.get("retryable", False)
                console.print(f"    Endpoint: {endpoint}")
                console.print(f"    Error: {message}")
                console.print(f"    Retryable: {'yes' if retryable else 'no'}")

        if len(failed_repos) > 5:
            console.print(f"  ... and {len(failed_repos) - 5} more")
        console.print()

    # Calculate ETA if in progress
    if stats["repos_in_progress"] > 0 or stats["repos_pending"] > 0:
        created_at_str = data.get("created_at")
        if created_at_str:
            try:
                created_at = datetime.fromisoformat(created_at_str)
                now = datetime.now(UTC)
                elapsed = (now - created_at).total_seconds()

                repos_done = stats["repos_complete"]
                repos_remaining = stats["repos_pending"] + stats["repos_in_progress"]

                if repos_done > 0 and elapsed > 0:
                    avg_time_per_repo = elapsed / repos_done
                    eta_seconds = avg_time_per_repo * repos_remaining
                    eta_minutes = eta_seconds / 60

                    console.print("[bold cyan]Estimated Time:[/bold cyan]")
                    console.print(f"  Elapsed: {elapsed / 60:.1f} minutes")
                    console.print(f"  Average per repo: {avg_time_per_repo:.1f} seconds")
                    console.print(f"  ETA for completion: {eta_minutes:.1f} minutes")
                    console.print()
            except (ValueError, ZeroDivisionError):
                pass

    # Show hint for resuming
    if stats["repos_pending"] > 0 or stats["repos_in_progress"] > 0 or stats["repos_failed"] > 0:
        console.print("[bold]Next Steps:[/bold]")
        console.print("  Resume collection: gh-year-end collect --config <config> --resume")
        if stats["repos_failed"] > 0:
            console.print(
                "  Retry failed only: gh-year-end collect --config <config> --retry-failed"
            )


@main.command()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="Path to config.yaml file",
)
@click.pass_context
def normalize(ctx: click.Context, config: Path) -> None:
    """Normalize raw data to curated Parquet tables.

    Converts raw JSONL files to normalized Parquet tables with
    consistent schemas, bot detection, and identity resolution.
    """
    from datetime import UTC, datetime

    from gh_year_end.storage.paths import PathManager

    cfg = load_config(config)
    paths = PathManager(cfg)

    console.print(f"[bold]Normalizing data for year {cfg.github.windows.year}[/bold]")
    console.print(f"  Source: {paths.raw_root}")
    console.print(f"  Target: {paths.curated_root}")
    console.print()

    # Check if raw data exists
    if not paths.manifest_path.exists():
        console.print("[bold red]Error:[/bold red] No raw data found. Run 'collect' command first.")
        raise click.Abort()

    console.print("[bold cyan]Checking raw data...[/bold cyan]")

    # Check for repos file
    if not paths.repos_raw_path.exists():
        console.print(
            "[bold red]Error:[/bold red] No repos.jsonl found. Collection may be incomplete."
        )
        raise click.Abort()

    console.print(f"  Found repos data: {paths.repos_raw_path}")

    # Ensure curated directory exists
    paths.curated_root.mkdir(parents=True, exist_ok=True)

    console.print()
    console.print("[bold cyan]Starting normalization...[/bold cyan]")

    start_time = datetime.now(UTC)
    tables_written = 0
    total_rows = 0
    errors: list[str] = []

    try:
        # Import normalizers
        from gh_year_end.normalize.comments import (
            normalize_issue_comments,
            normalize_review_comments,
        )
        from gh_year_end.normalize.commits import normalize_commit_files, normalize_commits
        from gh_year_end.normalize.hygiene import (
            normalize_branch_protection,
            normalize_file_presence,
            normalize_security_features,
        )
        from gh_year_end.normalize.issues import normalize_issues
        from gh_year_end.normalize.pulls import normalize_pulls
        from gh_year_end.normalize.repos import normalize_repos
        from gh_year_end.normalize.reviews import normalize_reviews
        from gh_year_end.normalize.users import normalize_identity_rules, normalize_users

        # Normalize dimension tables first
        console.print("  [cyan]Normalizing dim_user...[/cyan]")
        dim_user = normalize_users(cfg)
        dim_user.write_parquet(paths.curated_path("dim_user"))
        console.print(f"    ✓ dim_user: {len(dim_user)} rows")
        tables_written += 1
        total_rows += len(dim_user)

        console.print("  [cyan]Normalizing dim_identity_rule...[/cyan]")
        dim_identity_rule = normalize_identity_rules(cfg)
        dim_identity_rule.write_parquet(paths.curated_path("dim_identity_rule"))
        console.print(f"    ✓ dim_identity_rule: {len(dim_identity_rule)} rows")
        tables_written += 1
        total_rows += len(dim_identity_rule)

        console.print("  [cyan]Normalizing dim_repo...[/cyan]")
        dim_repo = normalize_repos(paths.raw_root, cfg)
        dim_repo.to_parquet(paths.curated_path("dim_repo"))
        console.print(f"    ✓ dim_repo: {len(dim_repo)} rows")
        tables_written += 1
        total_rows += len(dim_repo)

        # Normalize fact tables (with error handling for missing data)
        try:
            console.print("  [cyan]Normalizing fact_pull_request...[/cyan]")
            fact_pr = normalize_pulls(paths.raw_root, cfg)
            fact_pr.to_parquet(paths.curated_path("fact_pull_request"))
            console.print(f"    ✓ fact_pull_request: {len(fact_pr)} rows")
            tables_written += 1
            total_rows += len(fact_pr)
        except FileNotFoundError as e:
            console.print(f"    [yellow]⊘ fact_pull_request: skipped ({e})[/yellow]")
            errors.append(f"fact_pull_request: {e}")

        try:
            console.print("  [cyan]Normalizing fact_issue...[/cyan]")
            fact_issue = normalize_issues(paths.raw_root, cfg)
            fact_issue.to_parquet(paths.curated_path("fact_issue"))
            console.print(f"    ✓ fact_issue: {len(fact_issue)} rows")
            tables_written += 1
            total_rows += len(fact_issue)
        except FileNotFoundError as e:
            console.print(f"    [yellow]⊘ fact_issue: skipped ({e})[/yellow]")
            errors.append(f"fact_issue: {e}")

        try:
            console.print("  [cyan]Normalizing fact_review...[/cyan]")
            fact_review = normalize_reviews(paths.raw_root, cfg)
            fact_review.to_parquet(paths.curated_path("fact_review"))
            console.print(f"    ✓ fact_review: {len(fact_review)} rows")
            tables_written += 1
            total_rows += len(fact_review)
        except FileNotFoundError as e:
            console.print(f"    [yellow]⊘ fact_review: skipped ({e})[/yellow]")
            errors.append(f"fact_review: {e}")

        try:
            console.print("  [cyan]Normalizing fact_issue_comment...[/cyan]")
            normalize_issue_comments(paths, cfg)
            # Check if file was created and get row count
            if paths.fact_issue_comment_path.exists():
                import polars as pl

                df = pl.read_parquet(paths.fact_issue_comment_path)
                console.print(f"    ✓ fact_issue_comment: {len(df)} rows")
                tables_written += 1
                total_rows += len(df)
            else:
                console.print("    [yellow]⊘ fact_issue_comment: skipped (no data)[/yellow]")
        except FileNotFoundError as e:
            console.print(f"    [yellow]⊘ fact_issue_comment: skipped ({e})[/yellow]")
            errors.append(f"fact_issue_comment: {e}")

        try:
            console.print("  [cyan]Normalizing fact_review_comment...[/cyan]")
            normalize_review_comments(paths, cfg)
            # Check if file was created and get row count
            if paths.fact_review_comment_path.exists():
                import polars as pl

                df = pl.read_parquet(paths.fact_review_comment_path)
                console.print(f"    ✓ fact_review_comment: {len(df)} rows")
                tables_written += 1
                total_rows += len(df)
            else:
                console.print("    [yellow]⊘ fact_review_comment: skipped (no data)[/yellow]")
        except FileNotFoundError as e:
            console.print(f"    [yellow]⊘ fact_review_comment: skipped ({e})[/yellow]")
            errors.append(f"fact_review_comment: {e}")

        try:
            console.print("  [cyan]Normalizing fact_commit...[/cyan]")
            normalize_commits(paths, cfg)
            # Check if file was created and get row count
            if paths.fact_commit_path.exists():
                import polars as pl

                df = pl.read_parquet(paths.fact_commit_path)
                console.print(f"    ✓ fact_commit: {len(df)} rows")
                tables_written += 1
                total_rows += len(df)
            else:
                console.print("    [yellow]⊘ fact_commit: skipped (no data)[/yellow]")
        except FileNotFoundError as e:
            console.print(f"    [yellow]⊘ fact_commit: skipped ({e})[/yellow]")
            errors.append(f"fact_commit: {e}")

        try:
            console.print("  [cyan]Normalizing fact_commit_file...[/cyan]")
            normalize_commit_files(paths, cfg)
            # Check if file was created and get row count
            if paths.fact_commit_file_path.exists():
                import polars as pl

                df = pl.read_parquet(paths.fact_commit_file_path)
                console.print(f"    ✓ fact_commit_file: {len(df)} rows")
                tables_written += 1
                total_rows += len(df)
            else:
                console.print("    [yellow]⊘ fact_commit_file: skipped (no data)[/yellow]")
        except FileNotFoundError as e:
            console.print(f"    [yellow]⊘ fact_commit_file: skipped ({e})[/yellow]")
            errors.append(f"fact_commit_file: {e}")

        try:
            console.print("  [cyan]Normalizing fact_repo_files_presence...[/cyan]")
            fact_repo_files = normalize_file_presence(paths.raw_root, cfg)
            fact_repo_files.to_parquet(paths.curated_path("fact_repo_files_presence"))
            console.print(f"    ✓ fact_repo_files_presence: {len(fact_repo_files)} rows")
            tables_written += 1
            total_rows += len(fact_repo_files)
        except FileNotFoundError as e:
            console.print(f"    [yellow]⊘ fact_repo_files_presence: skipped ({e})[/yellow]")
            errors.append(f"fact_repo_files_presence: {e}")

        try:
            console.print("  [cyan]Normalizing fact_repo_hygiene...[/cyan]")
            fact_repo_hygiene = normalize_branch_protection(paths.raw_root, cfg)
            fact_repo_hygiene.to_parquet(paths.curated_path("fact_repo_hygiene"))
            console.print(f"    ✓ fact_repo_hygiene: {len(fact_repo_hygiene)} rows")
            tables_written += 1
            total_rows += len(fact_repo_hygiene)
        except FileNotFoundError as e:
            console.print(f"    [yellow]⊘ fact_repo_hygiene: skipped ({e})[/yellow]")
            errors.append(f"fact_repo_hygiene: {e}")

        try:
            console.print("  [cyan]Normalizing fact_repo_security_features...[/cyan]")
            fact_repo_security = normalize_security_features(paths.raw_root, cfg)
            fact_repo_security.to_parquet(paths.curated_path("fact_repo_security_features"))
            console.print(f"    ✓ fact_repo_security_features: {len(fact_repo_security)} rows")
            tables_written += 1
            total_rows += len(fact_repo_security)
        except FileNotFoundError as e:
            console.print(f"    [yellow]⊘ fact_repo_security_features: skipped ({e})[/yellow]")
            errors.append(f"fact_repo_security_features: {e}")

    except Exception as e:
        console.print(f"\n[bold red]Normalization failed:[/bold red] {e}")
        if ctx.obj.get("verbose"):
            import traceback

            console.print("\n[dim]Traceback:[/dim]")
            console.print(traceback.format_exc())
        errors.append(str(e))
        raise click.Abort() from e

    end_time = datetime.now(UTC)
    duration = (end_time - start_time).total_seconds()

    console.print()
    console.print("[bold green]Normalization complete![/bold green]")
    console.print()
    console.print("[bold]Summary:[/bold]")
    console.print(f"  Duration: {duration:.2f} seconds")
    console.print(f"  Tables written: {tables_written}")
    console.print(f"  Total rows: {total_rows}")
    if errors:
        console.print(f"  Errors: {len(errors)}")
        for error in errors:
            console.print(f"    - {error}")


@main.command()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="Path to config.yaml file",
)
@click.pass_context
def metrics(ctx: click.Context, config: Path) -> None:
    """Compute metrics from curated data.

    Calculates leaderboards, time series, repository health scores,
    hygiene scores, and awards from the normalized Parquet tables.
    """
    from gh_year_end.metrics.orchestrator import run_metrics
    from gh_year_end.storage.paths import PathManager

    cfg = load_config(config)
    paths = PathManager(cfg)

    console.print(f"[bold]Computing metrics for year {cfg.github.windows.year}[/bold]")
    console.print(f"  Source: {paths.curated_root}")
    console.print(f"  Target: {paths.metrics_root}")
    console.print()

    # Check if curated data exists
    if not paths.curated_root.exists():
        console.print(
            "[bold red]Error:[/bold red] No curated data found. Run 'normalize' command first."
        )
        raise click.Abort()

    console.print("[bold cyan]Starting metrics calculation...[/bold cyan]")
    console.print()

    try:
        # Run metrics calculation
        stats = run_metrics(cfg)

        # Display summary
        console.print()
        console.print("[bold green]Metrics calculation complete![/bold green]")
        console.print()
        console.print("[bold]Summary:[/bold]")
        console.print(f"  Duration: {stats.get('duration_seconds', 0):.2f} seconds")
        console.print(f"  Metrics calculated: {len(stats.get('metrics_written', []))}")
        console.print(f"  Total rows: {stats.get('total_rows', 0)}")

        if stats.get("metrics_written"):
            console.print()
            console.print("[bold]Metrics tables:[/bold]")
            for table in stats["metrics_written"]:
                console.print(f"  - {table}")

        errors = stats.get("errors", [])
        if errors:
            console.print()
            console.print(f"[yellow]Warnings:[/yellow] {len(errors)} metric(s) skipped")
            for error in errors:
                console.print(f"  - {error}")

    except ValueError as e:
        console.print(f"\n[bold red]Error:[/bold red] {e}")
        raise click.Abort() from e
    except Exception as e:
        console.print(f"\n[bold red]Metrics calculation failed:[/bold red] {e}")
        if ctx.obj.get("verbose"):
            import traceback

            console.print("\n[dim]Traceback:[/dim]")
            console.print(traceback.format_exc())
        raise click.Abort() from e


@main.command()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="Path to config.yaml file",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    default=False,
    help="Rebuild site even if it already exists",
)
@click.pass_context
def report(ctx: click.Context, config: Path, force: bool) -> None:
    """Generate static HTML report.

    Exports metrics to JSON and builds a static D3-powered site with
    exec summary and engineer drilldown views.
    """
    from datetime import UTC, datetime

    from gh_year_end.report.build import build_site
    from gh_year_end.report.export import export_metrics
    from gh_year_end.storage.paths import PathManager

    cfg = load_config(config)
    paths = PathManager(cfg)

    console.print(f"[bold]Generating report for year {cfg.github.windows.year}[/bold]")
    console.print(f"  Source: {paths.metrics_root}")
    console.print(f"  Output: {paths.site_root}")
    console.print()

    # Check if metrics data exists
    if not paths.metrics_root.exists():
        console.print(
            "[bold red]Error:[/bold red] No metrics data found. Run 'metrics' command first."
        )
        raise click.Abort()

    # Check for metrics tables
    metrics_files = list(paths.metrics_root.glob("*.parquet"))
    if not metrics_files:
        console.print(
            "[bold red]Error:[/bold red] No metrics tables found. Run 'metrics' command first."
        )
        raise click.Abort()

    console.print(f"[bold cyan]Found {len(metrics_files)} metrics tables[/bold cyan]")
    console.print()

    start_time = datetime.now(UTC)
    export_stats: dict[str, int | list[str]] = {
        "tables_exported": [],
        "total_rows": 0,
        "errors": [],
    }
    build_stats: dict[str, int | list[str]] = {
        "templates_rendered": [],
        "data_files_written": 0,
        "assets_copied": 0,
        "errors": [],
    }

    try:
        # Export metrics to JSON
        console.print("[bold cyan]Exporting metrics to JSON...[/bold cyan]")
        export_stats = export_metrics(cfg, paths)

        tables_exported = export_stats.get("tables_exported", [])
        if tables_exported and isinstance(tables_exported, list):
            console.print(f"  Exported {len(tables_exported)} tables")
            total_rows = export_stats.get("total_rows", 0)
            console.print(f"  Total rows: {total_rows}")

        export_errors = export_stats.get("errors", [])
        if export_errors and isinstance(export_errors, list) and len(export_errors) > 0:
            console.print(f"\n[yellow]Export warnings:[/yellow] {len(export_errors)}")
            for error in export_errors[:5]:  # Show first 5 errors
                console.print(f"  - {error}")

        # Build static site
        console.print()
        console.print("[bold cyan]Building static site...[/bold cyan]")
        build_stats = build_site(cfg, paths)

        templates_rendered = build_stats.get("templates_rendered", [])
        if isinstance(templates_rendered, list):
            console.print(f"  Rendered {len(templates_rendered)} templates")

        data_files = build_stats.get("data_files_written", 0)
        console.print(f"  Data files: {data_files}")

        assets_copied = build_stats.get("assets_copied", 0)
        console.print(f"  Assets copied: {assets_copied}")

        build_errors = build_stats.get("errors", [])
        if build_errors and isinstance(build_errors, list) and len(build_errors) > 0:
            console.print(f"\n[yellow]Build warnings:[/yellow] {len(build_errors)}")
            for error in build_errors[:5]:  # Show first 5 errors
                console.print(f"  - {error}")

    except ValueError as e:
        console.print(f"\n[bold red]Error:[/bold red] {e}")
        raise click.Abort() from e
    except Exception as e:
        console.print(f"\n[bold red]Report generation failed:[/bold red] {e}")
        if ctx.obj.get("verbose"):
            import traceback

            console.print("\n[dim]Traceback:[/dim]")
            console.print(traceback.format_exc())
        raise click.Abort() from e

    end_time = datetime.now(UTC)
    duration = (end_time - start_time).total_seconds()

    console.print()
    console.print("[bold green]Report generation complete![/bold green]")
    console.print()
    console.print("[bold]Summary:[/bold]")
    console.print(f"  Duration: {duration:.2f} seconds")

    tables_exported = export_stats.get("tables_exported", [])
    if isinstance(tables_exported, list):
        console.print(f"  Tables exported: {len(tables_exported)}")

    templates_rendered = build_stats.get("templates_rendered", [])
    if isinstance(templates_rendered, list):
        console.print(f"  Templates rendered: {len(templates_rendered)}")

    console.print(f"  Output: {paths.site_root}")

    # Show serve command hint
    console.print()
    console.print("[bold]To view the report:[/bold]")
    console.print(f"  python -m http.server -d {paths.site_root}")

    # Show errors summary if any
    export_errors = export_stats.get("errors", [])
    build_errors = build_stats.get("errors", [])
    total_errors = (len(export_errors) if isinstance(export_errors, list) else 0) + (
        len(build_errors) if isinstance(build_errors, list) else 0
    )

    if total_errors > 0:
        console.print()
        console.print(f"[yellow]Completed with {total_errors} warning(s)[/yellow]")


@main.command(name="all")
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="Path to config.yaml file",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    default=False,
    help="Re-fetch data even if raw files exist",
)
@click.pass_context
def run_all(ctx: click.Context, config: Path, force: bool) -> None:
    """Run the complete pipeline: collect -> normalize -> metrics -> report.

    This is the main command to generate a complete year-end report.
    """
    console.print("[bold]Running complete pipeline[/bold]\n")

    # Invoke each command in sequence
    ctx.invoke(collect, config=config, force=force)
    console.print()
    ctx.invoke(normalize, config=config)
    console.print()
    ctx.invoke(metrics, config=config)
    console.print()
    ctx.invoke(report, config=config, force=force)

    console.print("\n[bold green]Pipeline complete![/bold green]")


if __name__ == "__main__":
    main()
