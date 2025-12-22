"""CLI entry point for gh-year-end.

Simplified CLI with 2 main commands:
- collect: Collect GitHub data and generate metrics JSON
- build: Build static HTML site from metrics JSON
"""

import asyncio
import json
from datetime import UTC, datetime
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

    \b
    Quick Start:
        1. Collect data and generate metrics: gh-year-end collect --config config.yaml
        2. Build static site: gh-year-end build --config config.yaml
        3. Or run both: gh-year-end all --config config.yaml
    """
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    setup_logging(verbose=verbose)


# ============================================================================
# MAIN COMMANDS
# ============================================================================


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
    help="Force re-collection even if data exists",
)
@click.option(
    "--year",
    type=int,
    default=None,
    help="Override year from config (recalculates since/until)",
)
@click.pass_context
def collect(ctx: click.Context, config: Path, force: bool, year: int | None) -> None:
    """Collect GitHub data and generate metrics JSON.

    This performs single-pass collection with in-memory metric aggregation,
    then writes the results to JSON files for the website.

    The collection process:
    1. Discovers all repositories in the target org/user
    2. Collects PRs, issues, reviews, comments, commits
    3. Aggregates metrics in-memory (no intermediate files)
    4. Writes final JSON files to site/{year}/data/

    Output files:
    - summary.json: Overall statistics
    - leaderboards.json: Ranked contributors by metric
    - timeseries.json: Weekly/monthly activity trends
    - repo_health.json: Per-repository health metrics
    - hygiene_scores.json: Repository hygiene/quality scores
    - awards.json: Top contributor awards
    """
    try:
        from gh_year_end.collect.orchestrator import collect_and_aggregate
    except ImportError as e:
        console.print(f"[bold red]Error:[/bold red] collect_and_aggregate not yet implemented: {e}")
        raise click.Abort() from e

    cfg = load_config(config)

    # Override year if provided
    if year is not None:
        cfg.github.windows.year = year
        cfg.github.windows.since = datetime(year, 1, 1, 0, 0, 0, tzinfo=UTC)
        cfg.github.windows.until = datetime(year + 1, 1, 1, 0, 0, 0, tzinfo=UTC)

    console.print(
        f"[bold]Collecting data for {cfg.github.target.name} ({cfg.github.windows.year})[/bold]"
    )
    console.print()
    console.print("[cyan]Running single-pass collection with in-memory aggregation...[/cyan]")

    try:
        # Run collection and aggregation
        verbose = ctx.obj.get("verbose", False)
        metrics = asyncio.run(collect_and_aggregate(cfg, force=force, verbose=verbose))

        # Write JSON files
        data_dir = Path(f"site/{cfg.github.windows.year}/data")
        data_dir.mkdir(parents=True, exist_ok=True)

        console.print()
        console.print("[bold cyan]Writing metrics to JSON...[/bold cyan]")

        files_written = 0
        for name, data in metrics.items():
            filepath = data_dir / f"{name}.json"
            with filepath.open("w") as f:
                json.dump(data, f, indent=2, default=str)
            console.print(f"  ✓ {filepath}")
            files_written += 1

        console.print()
        console.print("[bold green]Collection complete![/bold green]")
        console.print(f"  Files written: {files_written}")
        console.print(f"  Output directory: {data_dir}")

    except KeyboardInterrupt:
        console.print("\n[yellow]Collection interrupted by user[/yellow]")
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
@click.option(
    "--year",
    type=int,
    default=None,
    help="Override year from config (recalculates since/until)",
)
@click.pass_context
def build(ctx: click.Context, config: Path, year: int | None) -> None:
    """Build static HTML site from metrics JSON.

    Reads the JSON files generated by 'collect' and renders HTML templates
    using Jinja2. Creates a complete static site with:

    - Executive summary dashboard
    - Contributor leaderboards
    - Repository health metrics
    - Interactive D3.js visualizations
    - Time series activity charts

    The site is generated in site/{year}/ and can be served with any
    static web server or deployed to GitHub Pages.
    """

    from gh_year_end.report.build import build_site
    from gh_year_end.storage.paths import PathManager

    cfg = load_config(config)

    # Override year if provided
    if year is not None:
        cfg.github.windows.year = year
        cfg.github.windows.since = datetime(year, 1, 1, 0, 0, 0, tzinfo=UTC)
        cfg.github.windows.until = datetime(year + 1, 1, 1, 0, 0, 0, tzinfo=UTC)

    paths = PathManager(cfg)

    console.print(f"[bold]Building site for {cfg.github.windows.year}[/bold]")
    console.print()

    # Check if metrics data exists
    data_dir = Path(f"site/{cfg.github.windows.year}/data")
    if not data_dir.exists():
        console.print(f"[bold red]Error:[/bold red] No metrics data found at {data_dir}")
        console.print("[yellow]Run 'collect' command first to generate metrics JSON[/yellow]")
        raise click.Abort()

    # Check for required JSON files
    required_files = ["summary.json", "leaderboards.json"]
    missing_files = []
    for filename in required_files:
        if not (data_dir / filename).exists():
            missing_files.append(filename)

    if missing_files:
        console.print("[bold red]Error:[/bold red] Missing required JSON files:")
        for filename in missing_files:
            console.print(f"  - {filename}")
        console.print("[yellow]Run 'collect' command to generate missing files[/yellow]")
        raise click.Abort()

    console.print("[bold cyan]Building static site...[/bold cyan]")

    try:
        # Build the site
        build_stats = build_site(cfg, paths)

        # Display results
        console.print()
        console.print("[bold green]Site built successfully![/bold green]")
        console.print()

        templates_rendered = build_stats.get("templates_rendered", [])
        if isinstance(templates_rendered, list):
            console.print(f"  Templates rendered: {len(templates_rendered)}")

        data_files = build_stats.get("data_files_written", 0)
        console.print(f"  Data files: {data_files}")

        assets_copied = build_stats.get("assets_copied", 0)
        console.print(f"  Assets copied: {assets_copied}")

        console.print(f"  Output: {paths.site_root}")

        # Show errors/warnings if any
        build_errors = build_stats.get("errors", [])
        if build_errors and isinstance(build_errors, list) and len(build_errors) > 0:
            console.print(f"\n[yellow]Warnings:[/yellow] {len(build_errors)}")
            for error in build_errors[:5]:
                console.print(f"  - {error}")

        # Show serve command hint
        console.print()
        console.print("[bold]To view the site:[/bold]")
        console.print(f"  python -m http.server -d {paths.site_root}")

    except Exception as e:
        console.print(f"\n[bold red]Build failed:[/bold red] {e}")
        if ctx.obj.get("verbose"):
            import traceback

            console.print("\n[dim]Traceback:[/dim]")
            console.print(traceback.format_exc())
        raise click.Abort() from e


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
    help="Force re-collection even if data exists",
)
@click.option(
    "--year",
    type=int,
    default=None,
    help="Override year from config (recalculates since/until)",
)
@click.pass_context
def run_all(ctx: click.Context, config: Path, force: bool, year: int | None) -> None:
    """Run the complete pipeline: collect data and build site.

    This is a convenience command that runs both 'collect' and 'build'
    in sequence. Equivalent to:

        gh-year-end collect --config CONFIG
        gh-year-end build --config CONFIG
    """
    console.print("[bold]Running complete pipeline[/bold]\n")

    # Invoke collect
    ctx.invoke(collect, config=config, force=force, year=year)

    console.print()

    # Invoke build
    ctx.invoke(build, config=config, year=year)

    console.print("\n[bold green]Pipeline complete![/bold green]")


@main.command(name="batch-years")
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="Path to config.yaml file",
)
@click.option(
    "--years",
    type=str,
    default=None,
    help="Comma-separated years (e.g., 2023,2024,2025)",
)
@click.option(
    "--from-year",
    type=int,
    default=None,
    help="Start year for range",
)
@click.option(
    "--to-year",
    type=int,
    default=None,
    help="End year for range (inclusive)",
)
@click.option(
    "--skip-collect",
    is_flag=True,
    default=False,
    help="Skip data collection, only build",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    default=False,
    help="Force re-collection even if data exists",
)
@click.pass_context
def batch_years(
    ctx: click.Context,
    config: Path,
    years: str | None,
    from_year: int | None,
    to_year: int | None,
    skip_collect: bool,
    force: bool,
) -> None:
    """Process multiple years in sequence.

    This command allows you to generate reports for multiple years using
    a single config file. The year value in the config will be overridden
    for each year processed.

    Specify years either as:
    - Comma-separated list: --years 2023,2024,2025
    - Range: --from-year 2023 --to-year 2025

    Each year is processed independently. If one year fails, the command
    continues with the remaining years.

    Examples:

        # Process specific years
        gh-year-end batch-years --config config.yaml --years 2023,2024,2025

        # Process a range
        gh-year-end batch-years --config config.yaml --from-year 2023 --to-year 2025

        # Only build sites (skip collection)
        gh-year-end batch-years --config config.yaml --years 2023,2024 --skip-collect

        # Force re-collection for all years
        gh-year-end batch-years --config config.yaml --years 2023,2024 --force
    """
    # Parse years
    year_list: list[int] = []
    if years:
        try:
            year_list = [int(y.strip()) for y in years.split(",")]
        except ValueError as e:
            console.print(f"[bold red]Error:[/bold red] Invalid year format: {e}")
            raise click.Abort() from e
    elif from_year is not None and to_year is not None:
        if from_year > to_year:
            console.print("[bold red]Error:[/bold red] from-year must be <= to-year")
            raise click.Abort()
        year_list = list(range(from_year, to_year + 1))
    else:
        console.print(
            "[bold red]Error:[/bold red] Specify --years or both --from-year and --to-year"
        )
        raise click.Abort()

    if not year_list:
        console.print("[bold red]Error:[/bold red] No years specified")
        raise click.Abort()

    console.print(
        f"[bold]Processing {len(year_list)} years:[/bold] {', '.join(map(str, year_list))}\n"
    )

    results: dict[int, str] = {}
    for year in year_list:
        console.print(f"\n{'=' * 60}")
        console.print(f"[bold cyan]Processing year {year}[/bold cyan]")
        console.print("=" * 60 + "\n")

        try:
            # Run collection if not skipped
            if not skip_collect:
                console.print("[cyan]Running collection...[/cyan]")
                ctx.invoke(collect, config=config, force=force, year=year)
                console.print()

            # Run build
            console.print("[cyan]Building site...[/cyan]")
            ctx.invoke(build, config=config, year=year)

            results[year] = "success"
            console.print(f"\n[bold green]Year {year} completed successfully![/bold green]")

        except click.Abort:
            results[year] = "failed: aborted"
            console.print(f"\n[bold yellow]Year {year} aborted[/bold yellow]")
        except KeyboardInterrupt:
            results[year] = "failed: interrupted by user"
            console.print(f"\n[bold yellow]Year {year} interrupted by user[/bold yellow]")
            console.print("\n[yellow]Stopping batch processing[/yellow]")
            break
        except Exception as e:
            results[year] = f"failed: {e}"
            console.print(f"\n[bold red]Year {year} failed:[/bold red] {e}")
            if ctx.obj.get("verbose"):
                import traceback

                console.print("\n[dim]Traceback:[/dim]")
                console.print(traceback.format_exc())

    # Print summary
    console.print(f"\n\n{'=' * 60}")
    console.print("[bold]BATCH PROCESSING SUMMARY[/bold]")
    console.print("=" * 60 + "\n")

    success_count = sum(1 for status in results.values() if status == "success")
    failed_count = len(results) - success_count

    for year in sorted(results.keys()):
        status = results[year]
        if status == "success":
            console.print(f"  [green]✓[/green] {year}: {status}")
        else:
            console.print(f"  [red]✗[/red] {year}: {status}")

    console.print()
    console.print(f"  [bold]Total:[/bold] {len(results)} years")
    console.print(f"  [bold green]Success:[/bold green] {success_count}")
    console.print(f"  [bold red]Failed:[/bold red] {failed_count}")

    if failed_count > 0:
        console.print("\n[yellow]Some years failed. Check logs above for details.[/yellow]")
        raise click.Abort()


if __name__ == "__main__":
    main()
