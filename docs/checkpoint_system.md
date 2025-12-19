# Checkpoint and Resume System

## Overview

The checkpoint system provides granular progress tracking for data collection, enabling:

- **Resumable collection**: Resume from exact point of interruption
- **Page-level tracking**: Track progress at repo + endpoint + page level
- **Graceful shutdown**: Handle Ctrl+C (SIGINT) and SIGTERM signals
- **Atomic writes**: Prevent corruption with temp file + atomic rename
- **Config validation**: Detect configuration changes between runs
- **Concurrent safety**: File locking prevents multiple processes from corrupting state

## Architecture

### Data Structures

```
CheckpointStatus (enum)
├── PENDING      - Not started
├── IN_PROGRESS  - Currently processing
├── COMPLETE     - Successfully finished
└── FAILED       - Encountered error

EndpointProgress (dataclass)
├── status: CheckpointStatus
├── pages_collected: int
├── records_collected: int
└── last_page_written: int

RepoProgress (dataclass)
├── status: CheckpointStatus
├── started_at: datetime | None
├── completed_at: datetime | None
├── endpoints: dict[str, EndpointProgress]
└── error: dict | None

CheckpointManager
├── Phase tracking (discovery, pulls, issues, etc.)
├── Repo tracking (per-repo status)
├── Endpoint tracking (per-endpoint progress)
└── Signal handling (graceful shutdown)
```

### File Layout

```
data/raw/year=2025/source=github/target=sample-org/
├── checkpoint.json       # Progress state
├── checkpoint.json.lock  # File lock
├── manifest.json         # Final collection manifest
└── ...
```

## Usage

### Basic Usage

```python
from pathlib import Path
from gh_year_end.config import Config, load_config
from gh_year_end.storage import CheckpointManager
from gh_year_end.storage.paths import PathManager

# Load config
config = load_config(Path("config/config.yaml"))
paths = PathManager(config)

# Create checkpoint manager
checkpoint = CheckpointManager(
    checkpoint_path=paths.checkpoint_path,
    lock_path=paths.checkpoint_path.with_suffix(".json.lock"),
)

# Install signal handlers for graceful shutdown
checkpoint.install_signal_handlers()

# Create new checkpoint or load existing
if checkpoint.exists():
    checkpoint.load()

    # Validate config hasn't changed
    if not checkpoint.validate_config(config):
        print("Config changed! Checkpoint invalid.")
        checkpoint.delete_if_exists()
        checkpoint.create_new(config)
else:
    checkpoint.create_new(config)

# Update with discovered repos
repos = [{"full_name": "org/repo1"}, {"full_name": "org/repo2"}]
checkpoint.update_repos(repos)

# Get repos that need processing
repos_to_process = checkpoint.get_repos_to_process(
    retry_failed=False,     # Skip failed repos
    from_repo="org/repo2",  # Optional: start from specific repo
)

# Process each repo
for repo in repos_to_process:
    repo_name = repo["full_name"]

    # Process endpoint
    endpoint = "pulls"

    # Check if already complete
    if checkpoint.is_repo_endpoint_complete(repo_name, endpoint):
        print(f"Skipping {repo_name}/{endpoint} - already complete")
        continue

    # Mark as in progress
    checkpoint.mark_repo_endpoint_in_progress(repo_name, endpoint)

    # Get resume page (1 if starting fresh)
    resume_page = checkpoint.get_resume_page(repo_name, endpoint)

    try:
        # Paginate through API
        page = resume_page
        while True:
            # Fetch page
            records = fetch_page(repo_name, endpoint, page)

            if not records:
                break

            # Write records to storage
            write_records(records)

            # Update checkpoint
            checkpoint.update_progress(
                repo=repo_name,
                endpoint=endpoint,
                page=page,
                records=len(records),
            )

            page += 1

        # Mark complete
        checkpoint.mark_repo_endpoint_complete(repo_name, endpoint)

    except RateLimitError as e:
        # Retryable error - checkpoint saved, can resume later
        checkpoint.mark_repo_endpoint_failed(
            repo=repo_name,
            endpoint=endpoint,
            error=str(e),
            retryable=True,
        )

    except ForbiddenError as e:
        # Non-retryable error - mark repo as failed
        checkpoint.mark_repo_endpoint_failed(
            repo=repo_name,
            endpoint=endpoint,
            error=str(e),
            retryable=False,
        )
```

### Using Context Manager

```python
# Context manager handles file locking
with CheckpointManager(checkpoint_path, lock_path) as checkpoint:
    if not checkpoint.exists():
        checkpoint.create_new(config)
    else:
        checkpoint.load()

    # Process repos...
    checkpoint.update_repos(repos)

    # Lock automatically released on exit
```

### Phase Tracking

```python
# Track high-level phases
checkpoint.set_current_phase("discovery")
# ... do discovery work ...
checkpoint.mark_phase_complete("discovery")

checkpoint.set_current_phase("pulls")
# ... collect pulls ...
checkpoint.mark_phase_complete("pulls")

# Check if phase is complete
if checkpoint.is_phase_complete("discovery"):
    print("Discovery already done")
```

### Getting Statistics

```python
stats = checkpoint.get_stats()
print(f"Total repos: {stats['total_repos']}")
print(f"Complete: {stats['repos_complete']}")
print(f"In progress: {stats['repos_in_progress']}")
print(f"Pending: {stats['repos_pending']}")
print(f"Failed: {stats['repos_failed']}")
```

## Integration with Collectors

### Example: Pull Request Collector

```python
async def collect_pulls_with_checkpoint(
    repos: list[dict],
    rest_client: RestClient,
    paths: PathManager,
    checkpoint: CheckpointManager,
) -> dict:
    """Collect pull requests with checkpoint support."""

    repos_to_process = checkpoint.get_repos_to_process()

    for repo in repos_to_process:
        repo_name = repo["full_name"]
        endpoint = "pulls"

        if checkpoint.is_repo_endpoint_complete(repo_name, endpoint):
            continue

        checkpoint.mark_repo_endpoint_in_progress(repo_name, endpoint)

        # Get resume page
        start_page = checkpoint.get_resume_page(repo_name, endpoint)

        async with AsyncJSONLWriter(paths.pulls_raw_path(repo_name)) as writer:
            page = start_page

            while True:
                # Fetch page
                response = await rest_client.get_pulls(
                    repo=repo_name,
                    page=page,
                    per_page=100,
                )

                if not response.data:
                    break

                # Write records
                for pull in response.data:
                    await writer.write(
                        source="github_rest",
                        endpoint="pulls",
                        data=pull,
                    )

                # Update checkpoint
                checkpoint.update_progress(
                    repo=repo_name,
                    endpoint=endpoint,
                    page=page,
                    records=len(response.data),
                )

                page += 1

        checkpoint.mark_repo_endpoint_complete(repo_name, endpoint)

    return {"pulls_collected": sum(...)}
```

## Signal Handling

The checkpoint manager installs handlers for:

- **SIGINT** (Ctrl+C): User interrupt
- **SIGTERM**: Graceful shutdown request

When a signal is received:

1. Current checkpoint state is saved
2. Log message indicates graceful shutdown
3. `KeyboardInterrupt` is raised to allow cleanup
4. Process exits

This ensures no progress is lost when interrupting collection.

## Best Practices

### DO

- Install signal handlers early: `checkpoint.install_signal_handlers()`
- Update progress frequently (every page or every N records)
- Save checkpoint periodically (auto-saved every 10 pages or 100 records)
- Use retryable errors for rate limits and transient failures
- Use non-retryable errors for permissions issues (403, 404)

### DON'T

- Don't modify checkpoint data structure directly (use methods)
- Don't skip checkpoint validation on resume
- Don't process repos without checking `get_repos_to_process()`
- Don't forget to mark endpoints complete
- Don't rely on checkpoint alone (use manifest for final state)

## Configuration Changes

If configuration changes between runs:

```python
if checkpoint.exists():
    checkpoint.load()

    if not checkpoint.validate_config(config):
        print("Configuration changed - starting fresh collection")
        checkpoint.delete_if_exists()
        checkpoint.create_new(config)
else:
    checkpoint.create_new(config)
```

Config digest is computed from the entire config structure (excluding report paths).

## Performance Considerations

### Checkpoint Frequency

The system auto-saves when:
- Every 10 pages collected
- Every 100 records collected

Manual saves via `checkpoint.save()` can be added for critical points.

### File Locking

File locking (fcntl) ensures only one process can modify checkpoint at a time. Use context manager for automatic lock management:

```python
with checkpoint:
    # Lock held here
    checkpoint.update_progress(...)
# Lock released here
```

### Atomic Writes

All saves use temp file + atomic rename to prevent corruption:

1. Write to `.checkpoint_XXXXX.tmp`
2. Flush to disk
3. Atomic rename to `checkpoint.json`

This ensures checkpoint is never in partially-written state.

## Troubleshooting

### Checkpoint exists but collection isn't resuming

Check that `get_repos_to_process()` is being used to get repo list. Don't process all repos unconditionally.

### "Config changed" on every run

Ensure config file hasn't been modified. Even whitespace changes will invalidate checkpoint. Use `--force` to ignore and start fresh.

### Lock file not released

If process crashes without cleanup, lock file may remain. Safe to delete manually:

```bash
rm data/raw/year=2025/.../checkpoint.json.lock
```

### Progress seems stuck

Check endpoint status:

```python
checkpoint.load()
stats = checkpoint.get_stats()
print(stats)
```

Look for repos in `in_progress` state - these may need manual recovery.

## Future Enhancements

Potential improvements:

1. **Parallel safety**: Distributed locking for multi-node collection
2. **Progress UI**: Real-time progress visualization
3. **Retry policies**: Configurable retry strategies for failed repos
4. **Checkpoint history**: Keep history of checkpoint states
5. **Metrics**: Track collection velocity and ETA
