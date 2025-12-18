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
    help="Re-fetch data even if raw files exist",
)
@click.pass_context
def collect(ctx: click.Context, config: Path, force: bool) -> None:
    """Collect raw data from GitHub API.

    Fetches all configured data types (PRs, issues, reviews, etc.) from
    the target org/user and stores as raw JSONL files.
    """
    import asyncio

    from gh_year_end.collect.orchestrator import run_collection

    cfg = load_config(config)
    console.print(f"[bold]Collecting data for {cfg.github.target.name}[/bold]")
    console.print(f"  Year: {cfg.github.windows.year}")
    console.print(f"  Target: {cfg.github.target.mode} / {cfg.github.target.name}")
    console.print(f"  Storage: {cfg.storage.root}")

    if force:
        console.print("[yellow]Force mode: will re-fetch existing data[/yellow]")

    console.print()
    console.print("[bold cyan]Starting collection...[/bold cyan]")

    try:
        # Run async collection
        stats = asyncio.run(run_collection(cfg, force=force))

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
    stats: dict[str, int | list[str]] = {
        "tables_written": 0,
        "total_rows": 0,
        "errors": [],
    }

    try:
        # TODO: Call normalizers for each table when implemented
        # Import normalize functions and call them sequentially

        console.print("[yellow]Note:[/yellow] Normalizers not yet implemented")
        console.print("  Expected normalizers:")
        console.print("    - dim_user (from users.py)")
        console.print("    - dim_repo (from repos.py)")
        console.print("    - dim_identity_rule (from users.py)")
        console.print("    - fact_pull_request (from pulls.py)")
        console.print("    - fact_issue (from issues.py)")
        console.print("    - fact_review (from reviews.py)")
        console.print("    - fact_issue_comment (from comments.py)")
        console.print("    - fact_review_comment (from comments.py)")
        console.print("    - fact_commit (from commits.py)")
        console.print("    - fact_commit_file (from commits.py)")
        console.print("    - fact_repo_files_presence (from hygiene.py)")
        console.print("    - fact_repo_hygiene (from hygiene.py)")
        console.print("    - fact_repo_security_features (from hygiene.py)")

    except Exception as e:
        console.print(f"\n[bold red]Normalization failed:[/bold red] {e}")
        if ctx.obj.get("verbose"):
            import traceback

            console.print("\n[dim]Traceback:[/dim]")
            console.print(traceback.format_exc())
        errors = stats["errors"]
        if isinstance(errors, list):
            errors.append(str(e))
        raise click.Abort() from e

    end_time = datetime.now(UTC)
    duration = (end_time - start_time).total_seconds()

    console.print()
    console.print("[bold green]Normalization complete![/bold green]")
    console.print()
    console.print("[bold]Summary:[/bold]")
    console.print(f"  Duration: {duration:.2f} seconds")
    console.print(f"  Tables written: {stats['tables_written']}")
    console.print(f"  Total rows: {stats['total_rows']}")
    errors = stats["errors"]
    if errors and isinstance(errors, list):
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
    cfg = load_config(config)
    console.print(f"[bold]Computing metrics for year {cfg.github.windows.year}[/bold]")
    # TODO: Implement metrics logic
    console.print("[dim]Metrics computation not yet implemented[/dim]")


@main.command()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="Path to config.yaml file",
)
@click.pass_context
def report(ctx: click.Context, config: Path) -> None:
    """Generate static HTML report.

    Exports metrics to JSON and builds a static D3-powered site with
    exec summary and engineer drilldown views.
    """
    cfg = load_config(config)
    console.print(f"[bold]Generating report for year {cfg.github.windows.year}[/bold]")
    console.print(f"  Output: {cfg.report.output_dir}")
    # TODO: Implement report generation
    console.print("[dim]Report generation not yet implemented[/dim]")


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
    ctx.invoke(report, config=config)

    console.print("\n[bold green]Pipeline complete![/bold green]")


if __name__ == "__main__":
    main()
