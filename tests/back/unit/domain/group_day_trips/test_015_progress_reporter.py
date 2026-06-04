import logging

from mobility.trips.group_day_trips.core.progress import GroupDayTripsProgressReporter


def test_progress_feedback_suppresses_info_logs_and_restores_level(monkeypatch):
    """Rich progress owns the console, so normal INFO logs stay quiet during a run."""
    root_logger = logging.getLogger()
    previous_level = root_logger.level
    root_logger.setLevel(logging.INFO)
    monkeypatch.setenv("MOBILITY_FEEDBACK", "progress")

    try:
        with GroupDayTripsProgressReporter(label="Test run", total_iterations=2) as progress:
            assert root_logger.level == logging.WARNING
            progress.iteration_step(1, "activity sequences")
            progress.finish_iteration(1)

        assert root_logger.level == logging.INFO
    finally:
        root_logger.setLevel(previous_level)


def test_logs_feedback_reports_iteration_duration(monkeypatch, caplog):
    """Log feedback uses plain INFO messages, including one completion line per iteration."""
    monkeypatch.setenv("MOBILITY_FEEDBACK", "logs")

    with caplog.at_level(logging.INFO):
        with GroupDayTripsProgressReporter(label="Test run", total_iterations=1) as progress:
            progress.iteration_step(1, "destination sequences")
            progress.finish_iteration(1)

    assert "Test run" in caplog.text
    assert "Iteration 1: destination sequences" in caplog.text
    assert "Iteration 1: done in " in caplog.text


def test_ci_default_feedback_uses_logs(monkeypatch):
    """CI output should avoid Rich live displays that can conflict with other progress bars."""
    monkeypatch.delenv("MOBILITY_FEEDBACK", raising=False)
    monkeypatch.delenv("MOBILITY_PROGRESS", raising=False)
    monkeypatch.setenv("CI", "true")

    with GroupDayTripsProgressReporter(label="Test run", total_iterations=1) as progress:
        assert progress.use_logs is True
        assert progress.use_rich is False


def test_progress_feedback_keeps_overall_label_when_phase_changes(monkeypatch):
    """Phase messages should not replace the run context shown on the overall task."""
    monkeypatch.setenv("MOBILITY_FEEDBACK", "progress")

    with GroupDayTripsProgressReporter(label="Run scenario=base replication=2", total_iterations=1) as progress:
        progress.step("Building initial population state")
        assert progress._progress.tasks[progress._run_task_id].description == "Run scenario=base replication=2"


def test_progress_feedback_counts_cached_previous_iterations(monkeypatch):
    """Starting iteration 4 means iterations 1 to 3 are already complete or cached."""
    monkeypatch.setenv("MOBILITY_FEEDBACK", "progress")

    with GroupDayTripsProgressReporter(label="Run", total_iterations=5) as progress:
        progress.iteration_step(4, "preparing transport costs")
        assert progress._progress.tasks[progress._run_task_id].completed == 3

        progress.finish_iteration(4)
        assert progress._progress.tasks[progress._run_task_id].completed == 4
