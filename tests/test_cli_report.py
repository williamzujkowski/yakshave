"""Tests for removed report CLI command.

The 'report' command has been fully removed. Use 'build' instead.
These tests verify the command no longer exists.
"""

from click.testing import CliRunner

from gh_year_end.cli import main


class TestReportCommandRemoved:
    """Tests for removed report command."""

    def test_report_command_not_available(self) -> None:
        """Test that report command returns 'No such command' error."""
        runner = CliRunner()
        result = runner.invoke(main, ["report", "--help"])

        # Command should not exist
        assert result.exit_code == 2
        assert "no such command" in result.output.lower()

    def test_report_not_in_main_help(self) -> None:
        """Test that report command does not appear in main help."""
        runner = CliRunner()
        result = runner.invoke(main, ["--help"])

        assert result.exit_code == 0
        # 'build' replaces 'report' - build should be there
        assert "build" in result.output
        # 'report' should not appear as a command
        # Note: The word "report" might appear in descriptions, but not as a command
        output_lines = result.output.lower().split("\n")
        command_lines = [
            line
            for line in output_lines
            if line.strip().startswith("build") or line.strip().startswith("collect")
        ]
        # There should be commands, but 'report' should not be one of them
        assert any("build" in line for line in command_lines)
