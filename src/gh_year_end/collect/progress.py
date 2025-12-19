"""Progress tracking and display for data collection.

Provides real-time progress reporting with ETA estimation using the rich library.
Tracks current phase, repo progress, rate limit status, and calculates ETA based
on historical request rates.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, ClassVar

from rich.console import Console
from rich.live import Live
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

if TYPE_CHECKING:
    from types import TracebackType

    from gh_year_end.github.ratelimit import AdaptiveRateLimiter


@dataclass
class ProgressStats:
    """Statistics for progress tracking."""

    total_repos: int = 0
    completed_repos: int = 0
    skipped_repos: int = 0
    failed_repos: int = 0
    current_repo: str = ""
    current_phase: str = ""
    phase_start_time: float = 0.0
    collection_start_time: float = 0.0

    # Request tracking for ETA
    request_times: deque[float] = field(default_factory=lambda: deque(maxlen=100))
    total_requests: int = 0

    # Rate limit tracking
    rate_limit_remaining: int = 5000
    rate_limit_reset: datetime | None = None
    rate_limit_wait_seconds: float = 0.0

    # Item counts per phase
    items_collected: dict[str, int] = field(default_factory=dict)


class ProgressTracker:
    """Tracks and displays collection progress with ETA estimation.

    Uses rich library for terminal progress display with:
    - Overall progress bar
    - Current phase and repo indicators
    - Rate limit status
    - ETA based on rolling average request time
    """

    PHASES: ClassVar[list[str]] = [
        "discovery",
        "repo_metadata",
        "pulls",
        "issues",
        "reviews",
        "comments",
        "commits",
        "branch_protection",
        "security_features",
    ]

    def __init__(
        self,
        total_repos: int = 0,
        verbose: bool = False,
        quiet: bool = False,
        rate_limiter: AdaptiveRateLimiter | None = None,
    ) -> None:
        """Initialize progress tracker.

        Args:
            total_repos: Total number of repos to process.
            verbose: Enable detailed logging.
            quiet: Minimal output mode.
            rate_limiter: Rate limiter for status updates.
        """
        self.stats = ProgressStats(
            total_repos=total_repos,
            collection_start_time=time.time(),
        )
        self.verbose = verbose
        self.quiet = quiet
        self.rate_limiter = rate_limiter
        self.console = Console()
        self._live: Live | None = None
        self._progress: Progress | None = None
        self._overall_task: TaskID | None = None
        self._phase_task: TaskID | None = None
        self._completed_phases: set[str] = set()

    def start(self) -> None:
        """Start progress display."""
        if self.quiet:
            return

        self._progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TextColumn("[cyan]{task.fields[status]}"),
            TimeElapsedColumn(),
            console=self.console,
            refresh_per_second=2,
        )

        self._overall_task = self._progress.add_task(
            "Overall",
            total=len(self.PHASES),
            status="Starting...",
        )

        self._live = Live(
            self._create_display(),
            console=self.console,
            refresh_per_second=2,
        )
        self._live.start()

    def stop(self) -> None:
        """Stop progress display."""
        if self._live:
            self._live.stop()
            self._live = None
        if self._progress:
            self._progress = None

    def _create_display(self) -> Table:
        """Create the progress display table."""
        table = Table(show_header=False, box=None, padding=(0, 1))

        # Phase progress
        if self._progress and self._overall_task is not None:
            table.add_row(self._progress)

        # Status line
        status_parts = []

        # Current phase and repo
        if self.stats.current_phase:
            phase_display = self.stats.current_phase.replace("_", " ").title()
            status_parts.append(f"[bold cyan]Phase:[/] {phase_display}")

        if self.stats.current_repo:
            status_parts.append(f"[bold cyan]Repo:[/] {self.stats.current_repo}")

        # Repo progress
        if self.stats.total_repos > 0:
            repo_progress = (
                f"{self.stats.completed_repos}/{self.stats.total_repos} repos "
                f"({self.stats.skipped_repos} skipped, {self.stats.failed_repos} failed)"
            )
            status_parts.append(f"[bold cyan]Progress:[/] {repo_progress}")

        if status_parts:
            table.add_row(" | ".join(status_parts))

        # Rate limit status
        rate_status = self._get_rate_limit_status()
        if rate_status:
            table.add_row(rate_status)

        # ETA
        eta = self._calculate_eta()
        if eta:
            table.add_row(f"[bold cyan]ETA:[/] {eta}")

        # Items collected
        if self.stats.items_collected:
            items = ", ".join(
                f"{k}: {v}" for k, v in sorted(self.stats.items_collected.items())
            )
            table.add_row(f"[bold cyan]Collected:[/] {items}")

        return table

    def _get_rate_limit_status(self) -> str:
        """Get formatted rate limit status."""
        if self.rate_limiter:
            state = self.rate_limiter.get_state()
            remaining = state.remaining
            limit = state.limit
            reset_at = state.reset_at

            status_color = "green"
            if remaining < 100:
                status_color = "red"
            elif remaining < 500:
                status_color = "yellow"

            # Format reset time
            if reset_at > 0:
                reset_dt = datetime.fromtimestamp(reset_at)
                reset_str = reset_dt.strftime("%H:%M:%S")
            else:
                reset_str = "unknown"

            return (
                f"[bold cyan]Rate Limit:[/] "
                f"[{status_color}]{remaining}[/]/{limit} "
                f"(resets {reset_str})"
            )
        return ""

    def _calculate_eta(self) -> str:
        """Calculate estimated time to completion."""
        if not self.stats.request_times:
            return ""

        # Calculate average request time
        avg_request_time = sum(self.stats.request_times) / len(self.stats.request_times)

        # Estimate remaining requests
        completed = self.stats.completed_repos + self.stats.skipped_repos
        repos_remaining = self.stats.total_repos - completed
        if repos_remaining <= 0:
            return "Almost done!"

        # Estimate requests per repo based on current phase
        requests_per_repo = self._estimate_requests_per_repo()
        estimated_remaining_requests = repos_remaining * requests_per_repo

        # Calculate ETA including rate limit waits
        estimated_seconds = estimated_remaining_requests * avg_request_time
        estimated_seconds += self.stats.rate_limit_wait_seconds

        if estimated_seconds < 60:
            return f"~{int(estimated_seconds)}s"
        elif estimated_seconds < 3600:
            return f"~{int(estimated_seconds / 60)}m"
        else:
            hours = int(estimated_seconds / 3600)
            minutes = int((estimated_seconds % 3600) / 60)
            return f"~{hours}h {minutes}m"

    def _estimate_requests_per_repo(self) -> float:
        """Estimate average requests per repo based on current data."""
        if self.stats.completed_repos == 0:
            return 10.0  # Default estimate

        return self.stats.total_requests / max(self.stats.completed_repos, 1)

    def update_display(self) -> None:
        """Refresh the display."""
        if self._live:
            self._live.update(self._create_display())

    def set_phase(self, phase: str) -> None:
        """Set the current collection phase.

        Args:
            phase: Phase name (e.g., 'pulls', 'issues').
        """
        self.stats.current_phase = phase
        self.stats.phase_start_time = time.time()

        if self._progress and self._overall_task is not None:
            completed = len(self._completed_phases)
            self._progress.update(
                self._overall_task,
                completed=completed,
                status=f"Phase: {phase.replace('_', ' ').title()}",
            )

        self.update_display()

    def mark_phase_complete(self, phase: str) -> None:
        """Mark a phase as complete.

        Args:
            phase: Phase name.
        """
        self._completed_phases.add(phase)

        if self._progress and self._overall_task is not None:
            self._progress.update(
                self._overall_task,
                completed=len(self._completed_phases),
                status=f"Completed: {phase.replace('_', ' ').title()}",
            )

        self.update_display()

    def set_repo(self, repo_full_name: str) -> None:
        """Set the current repository being processed.

        Args:
            repo_full_name: Full repository name (owner/repo).
        """
        self.stats.current_repo = repo_full_name
        self.update_display()

    def mark_repo_complete(self) -> None:
        """Mark the current repo as complete."""
        self.stats.completed_repos += 1
        self.update_display()

    def mark_repo_skipped(self) -> None:
        """Mark the current repo as skipped (from checkpoint)."""
        self.stats.skipped_repos += 1
        self.update_display()

    def mark_repo_failed(self) -> None:
        """Mark the current repo as failed."""
        self.stats.failed_repos += 1
        self.update_display()

    def record_request(self, duration: float) -> None:
        """Record a request duration for ETA calculation.

        Args:
            duration: Request duration in seconds.
        """
        self.stats.request_times.append(duration)
        self.stats.total_requests += 1
        self.update_display()

    def record_rate_limit_wait(self, wait_seconds: float) -> None:
        """Record rate limit wait time.

        Args:
            wait_seconds: Time spent waiting for rate limit reset.
        """
        self.stats.rate_limit_wait_seconds += wait_seconds
        self.update_display()

    def update_items_collected(self, item_type: str, count: int) -> None:
        """Update the count of items collected.

        Args:
            item_type: Type of item (e.g., 'pulls', 'issues').
            count: Number of items collected (added to existing count).
        """
        current = self.stats.items_collected.get(item_type, 0)
        self.stats.items_collected[item_type] = current + count
        self.update_display()

    def set_total_repos(self, total: int) -> None:
        """Set the total number of repos.

        Args:
            total: Total repo count.
        """
        self.stats.total_repos = total
        self.update_display()

    def get_summary(self) -> dict[str, Any]:
        """Get progress summary as a dictionary.

        Returns:
            Summary statistics.
        """
        elapsed = time.time() - self.stats.collection_start_time
        return {
            "elapsed_seconds": round(elapsed, 2),
            "total_repos": self.stats.total_repos,
            "completed_repos": self.stats.completed_repos,
            "skipped_repos": self.stats.skipped_repos,
            "failed_repos": self.stats.failed_repos,
            "total_requests": self.stats.total_requests,
            "rate_limit_wait_seconds": round(self.stats.rate_limit_wait_seconds, 2),
            "items_collected": dict(self.stats.items_collected),
            "avg_request_time": (
                round(sum(self.stats.request_times) / len(self.stats.request_times), 3)
                if self.stats.request_times
                else 0.0
            ),
        }

    def __enter__(self) -> ProgressTracker:
        """Context manager entry."""
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Context manager exit."""
        self.stop()
