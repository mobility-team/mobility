import logging
import os
from contextvars import ContextVar
from time import perf_counter

from rich.progress import Progress, ProgressColumn, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.text import Text


class TaskCountColumn(ProgressColumn):
    """Show completed/total only for tasks that have a known total."""

    def render(self, task) -> Text:
        if task.total is None:
            return Text("")
        return Text(f"{int(task.completed)}/{int(task.total)}")


def _legacy_progress_to_feedback(progress: str) -> str:
    """Map the old MOBILITY_PROGRESS environment variable to feedback modes."""
    value = str(progress).lower()
    if value in {"auto", "rich"}:
        return "progress"
    return "logs"


class GroupDayTripsProgressReporter:
    """Small user-facing progress reporter for group-day-trips runs.

    The model still uses normal logs for diagnostics. This class is only for
    coarse business milestones that help users understand where a long run is.
    """

    def __init__(
        self,
        *,
        label: str,
        total_iterations: int | None = None,
        enabled: bool | None = None,
    ) -> None:
        self.label = label
        self.total_iterations = total_iterations
        self._token = None
        self._progress = None
        self._run_task_id = None
        self._phase_task_id = None
        self._iteration_task_ids = {}
        self._iteration_start_times = {}
        self._completed_iterations = 0
        self._previous_root_logging_level = None
        feedback = os.environ.get(
            "MOBILITY_FEEDBACK",
            _legacy_progress_to_feedback(os.environ.get("MOBILITY_PROGRESS", "rich")),
        ).lower()
        self.enabled = (
            enabled
            if enabled is not None
            else feedback in {"progress", "logs", "debug"}
        )
        self.use_rich = self.enabled and feedback == "progress"
        self.use_logs = self.enabled and feedback in {"logs", "debug"}

    def __enter__(self):
        self._token = _CURRENT_GROUP_DAY_TRIPS_PROGRESS.set(self)
        if self.use_rich:
            # Rich owns the run display. While it is active, keep warnings and
            # errors visible but prevent INFO/DEBUG logs from breaking the bars.
            root_logger = logging.getLogger()
            self._previous_root_logging_level = root_logger.level
            if root_logger.level < logging.WARNING:
                root_logger.setLevel(logging.WARNING)
            self._progress = Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                TaskCountColumn(),
                TimeElapsedColumn(),
                transient=False,
            )
            self._progress.start()
            self._run_task_id = self._progress.add_task(
                self.label,
                total=self.total_iterations,
            )
        else:
            self.step(self.label)
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        final_label = f"{self.label}: done" if exc_type is None else f"{self.label}: failed"
        if self.use_rich and self._progress is not None and self._run_task_id is not None:
            update_kwargs = {"description": final_label}
            if self.total_iterations is not None:
                update_kwargs["completed"] = self.total_iterations
            self._progress.update(self._run_task_id, **update_kwargs)
        else:
            self.step(final_label)

        if self._progress is not None:
            self._progress.stop()

        if self._previous_root_logging_level is not None:
            logging.getLogger().setLevel(self._previous_root_logging_level)

        if self._token is not None:
            _CURRENT_GROUP_DAY_TRIPS_PROGRESS.reset(self._token)

    def step(self, message: str) -> None:
        """Report one coarse progress milestone."""
        if not self.enabled:
            return

        if self._progress is not None:
            # Keep the overall run label stable. Setup/finalization messages
            # live on their own line so the scenario and replication stay visible.
            if self._phase_task_id is None:
                self._phase_task_id = self._progress.add_task(message, total=None)
            else:
                self._progress.update(self._phase_task_id, description=message)
            return

        if self.use_logs:
            logging.info(message)

    def iteration_step(self, iteration: int, message: str) -> None:
        """Report the current work inside one model iteration."""
        if not self.enabled:
            return

        self._iteration_start_times.setdefault(iteration, perf_counter())
        description = f"Iteration {iteration}: {message}"

        if self._progress is not None:
            self._completed_iterations = max(self._completed_iterations, iteration - 1)
            if self._run_task_id is not None:
                self._progress.update(
                    self._run_task_id,
                    completed=self._completed_iterations,
                )
            task_id = self._iteration_task_ids.get(iteration)
            if task_id is None:
                task_id = self._progress.add_task(description, total=None)
                self._iteration_task_ids[iteration] = task_id
            else:
                self._progress.update(task_id, description=description)
            return

        if self.use_logs:
            logging.info(description)

    def finish_iteration(self, iteration: int) -> None:
        """Mark one model iteration as complete and report its elapsed time."""
        if not self.enabled:
            return

        started_at = self._iteration_start_times.get(iteration)
        elapsed = "" if started_at is None else f" in {perf_counter() - started_at:.1f}s"
        description = f"Iteration {iteration}: done{elapsed}"

        if self._progress is not None:
            task_id = self._iteration_task_ids.get(iteration)
            if task_id is None:
                task_id = self._progress.add_task(description, total=1, completed=1)
            else:
                self._progress.update(
                    task_id,
                    description=description,
                    total=1,
                    completed=1,
                )
            if self._run_task_id is not None:
                self._completed_iterations = max(self._completed_iterations, iteration)
                self._progress.update(
                    self._run_task_id,
                    completed=self._completed_iterations,
                )
            return

        if self.use_logs:
            logging.info(description)


_NO_GROUP_DAY_TRIPS_PROGRESS = GroupDayTripsProgressReporter(label="", enabled=False)

_CURRENT_GROUP_DAY_TRIPS_PROGRESS = ContextVar(
    "current_group_day_trips_progress",
    default=_NO_GROUP_DAY_TRIPS_PROGRESS,
)


def get_group_day_trips_progress() -> GroupDayTripsProgressReporter:
    """Return the active progress reporter, or a no-op reporter outside a run."""
    return _CURRENT_GROUP_DAY_TRIPS_PROGRESS.get()


def is_group_day_trips_progress_active() -> bool:
    """Return True when a run already owns the progress reporter."""
    return _CURRENT_GROUP_DAY_TRIPS_PROGRESS.get() is not _NO_GROUP_DAY_TRIPS_PROGRESS
