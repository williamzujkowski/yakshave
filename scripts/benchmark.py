#!/usr/bin/env python3
"""Performance benchmarking script for gh-year-end.

This script runs performance benchmarks for different collection scenarios
and generates a report with timing, memory, and API usage statistics.
"""

import argparse
import json
import resource
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass
class BenchmarkResult:
    """Result from a single benchmark run."""

    name: str
    config: str
    start_time: datetime
    end_time: datetime
    duration_seconds: float
    peak_memory_mb: float
    exit_code: int
    repos_collected: int = 0
    prs_collected: int = 0
    issues_collected: int = 0
    api_requests: int = 0
    rate_limit_waits: int = 0
    errors: int = 0
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "config": self.config,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "duration_seconds": self.duration_seconds,
            "duration_formatted": format_duration(self.duration_seconds),
            "peak_memory_mb": self.peak_memory_mb,
            "exit_code": self.exit_code,
            "repos_collected": self.repos_collected,
            "prs_collected": self.prs_collected,
            "issues_collected": self.issues_collected,
            "api_requests": self.api_requests,
            "rate_limit_waits": self.rate_limit_waits,
            "errors": self.errors,
            "extra": self.extra,
        }


def format_duration(seconds: float) -> str:
    """Format duration in human-readable format."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        mins = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{mins}m {secs}s"
    else:
        hours = int(seconds // 3600)
        mins = int((seconds % 3600) // 60)
        return f"{hours}h {mins}m"


def get_peak_memory_mb() -> float:
    """Get peak memory usage in MB."""
    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    return usage.ru_maxrss / 1024  # Convert KB to MB on Linux


def run_benchmark(
    name: str,
    config_path: str,
    command: list[str],
    data_dir: str = "./data",
    force: bool = False,
) -> BenchmarkResult:
    """Run a single benchmark.

    Args:
        name: Benchmark name
        config_path: Path to config file
        command: Command to run
        data_dir: Data directory for reading manifest
        force: Whether to force re-collection

    Returns:
        BenchmarkResult with timing and stats
    """
    print(f"\n{'='*60}")
    print(f"Running benchmark: {name}")
    print(f"Command: {' '.join(command)}")
    print(f"{'='*60}")

    start_time = datetime.now()

    # Run the command
    result = subprocess.run(
        command,
        capture_output=False,  # Show output in real-time
        text=True,
    )

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    peak_memory = get_peak_memory_mb()

    # Try to read manifest for stats
    repos = prs = issues = errors = 0
    manifest_path = Path(data_dir) / "manifest.json"
    if manifest_path.exists():
        try:
            with manifest_path.open() as f:
                manifest = json.load(f)
            stats = manifest.get("stats", {})
            repos = stats.get("repos_collected", 0)
            prs = stats.get("prs_collected", 0)
            issues = stats.get("issues_collected", 0)
            errors = stats.get("errors", 0)
        except Exception as e:
            print(f"Warning: Could not read manifest: {e}")

    return BenchmarkResult(
        name=name,
        config=config_path,
        start_time=start_time,
        end_time=end_time,
        duration_seconds=duration,
        peak_memory_mb=peak_memory,
        exit_code=result.returncode,
        repos_collected=repos,
        prs_collected=prs,
        issues_collected=issues,
        errors=errors,
    )


def run_collection_benchmark(
    name: str,
    config_path: str,
    force: bool = False,
) -> BenchmarkResult:
    """Run a collection benchmark."""
    cmd = ["uv", "run", "gh-year-end", "collect", "--config", config_path]
    if force:
        cmd.append("--force")
    return run_benchmark(name, config_path, cmd)


def run_build_benchmark(
    name: str,
    config_path: str,
) -> BenchmarkResult:
    """Run a build benchmark."""
    cmd = ["uv", "run", "gh-year-end", "build", "--config", config_path]
    return run_benchmark(name, config_path, cmd)


def run_full_pipeline_benchmark(
    name: str,
    config_path: str,
    force: bool = False,
) -> BenchmarkResult:
    """Run a full pipeline benchmark."""
    cmd = ["uv", "run", "gh-year-end", "all", "--config", config_path]
    if force:
        cmd.append("--force")
    return run_benchmark(name, config_path, cmd)


def generate_report(results: list[BenchmarkResult], output_path: str) -> None:
    """Generate a benchmark report.

    Args:
        results: List of benchmark results
        output_path: Path to write the report
    """
    report = {
        "generated_at": datetime.now().isoformat(),
        "system_info": {
            "python_version": sys.version,
            "platform": sys.platform,
        },
        "results": [r.to_dict() for r in results],
        "summary": generate_summary(results),
    }

    output_file = Path(output_path)
    with output_file.open("w") as f:
        json.dump(report, f, indent=2)

    print(f"\nReport written to: {output_path}")


def generate_summary(results: list[BenchmarkResult]) -> dict[str, Any]:
    """Generate summary statistics."""
    if not results:
        return {}

    successful = [r for r in results if r.exit_code == 0]
    failed = [r for r in results if r.exit_code != 0]

    summary = {
        "total_benchmarks": len(results),
        "successful": len(successful),
        "failed": len(failed),
    }

    if successful:
        durations = [r.duration_seconds for r in successful]
        memories = [r.peak_memory_mb for r in successful]
        summary["avg_duration_seconds"] = sum(durations) / len(durations)
        summary["max_duration_seconds"] = max(durations)
        summary["min_duration_seconds"] = min(durations)
        summary["avg_peak_memory_mb"] = sum(memories) / len(memories)
        summary["max_peak_memory_mb"] = max(memories)

    return summary


def print_results_table(results: list[BenchmarkResult]) -> None:
    """Print results as a table."""
    print("\n" + "=" * 80)
    print("BENCHMARK RESULTS")
    print("=" * 80)

    headers = ["Name", "Duration", "Memory (MB)", "Repos", "PRs", "Exit"]
    widths = [30, 12, 12, 8, 8, 6]

    # Print header
    header_line = " | ".join(h.ljust(w) for h, w in zip(headers, widths, strict=True))
    print(header_line)
    print("-" * len(header_line))

    # Print rows
    for r in results:
        row = [
            r.name[:30].ljust(30),
            format_duration(r.duration_seconds).ljust(12),
            f"{r.peak_memory_mb:.1f}".ljust(12),
            str(r.repos_collected).ljust(8),
            str(r.prs_collected).ljust(8),
            str(r.exit_code).ljust(6),
        ]
        print(" | ".join(row))

    print("=" * 80)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Run performance benchmarks")
    parser.add_argument(
        "--config",
        type=str,
        required=True,
        help="Path to config file",
    )
    parser.add_argument(
        "--scenario",
        type=str,
        choices=["collect", "build", "all", "resume"],
        default="all",
        help="Benchmark scenario to run",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-collection",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="benchmark_results.json",
        help="Output file for results",
    )

    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Error: Config file not found: {args.config}")
        sys.exit(1)

    results = []

    if args.scenario == "collect":
        result = run_collection_benchmark(
            name="Collection",
            config_path=args.config,
            force=args.force,
        )
        results.append(result)

    elif args.scenario == "build":
        result = run_build_benchmark(
            name="Site Build",
            config_path=args.config,
        )
        results.append(result)

    elif args.scenario == "all":
        result = run_full_pipeline_benchmark(
            name="Full Pipeline",
            config_path=args.config,
            force=args.force,
        )
        results.append(result)

    elif args.scenario == "resume":
        # First run collect, then simulate resume
        result1 = run_collection_benchmark(
            name="Initial Collection",
            config_path=args.config,
            force=True,
        )
        results.append(result1)

        # Run resume (should be fast since data exists)
        result2 = run_collection_benchmark(
            name="Resume (cached)",
            config_path=args.config,
            force=False,
        )
        results.append(result2)

    print_results_table(results)
    generate_report(results, args.output)


if __name__ == "__main__":
    main()
