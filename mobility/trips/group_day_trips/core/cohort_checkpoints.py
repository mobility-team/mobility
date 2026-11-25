"""Persist and validate completion markers for a coordinated cohort."""

import json
import logging
import os
import pathlib
import tempfile

from .performance_trace import trace_simulation_step


class CohortCheckpoints:
    """Checkpoint operations using the coordinator runs, parameters and identity."""

    def _prepare_checkpoint_resume(self, *, needs_mnl_flows: bool = True) -> int:
        """Find the last consecutive completed step shared by the cohort."""
        if not self.parameters.outputs.checkpoint_each_step:
            return 0
        self._clear_resume_checkpoint_loaders()
        resume_iteration = 0
        for iteration in range(1, self.parameters.run.n_steps + 1):
            try:
                complete = self._checkpoint_manifest_matches(iteration, needs_mnl_flows=needs_mnl_flows) and all(
                    not asset.is_update_needed()
                    for run in self.runs.values()
                    for asset in run.iterations.iteration(iteration).checkpoint_assets().values()
                )
            except (OSError, ValueError, UnicodeError, KeyError, TypeError, AttributeError):
                complete = False
            if not complete:
                break
            resume_iteration = iteration

        for run in self.runs.values():
            for state_asset in run.iteration_state_assets:
                if state_asset.iteration <= resume_iteration:
                    state_asset.resume_checkpoint_loader = run.iterations.iteration(state_asset.iteration).load_state

        if resume_iteration:
            logging.info("Resuming the coordinated scenario cohort from step %s.", resume_iteration)
        return resume_iteration

    def _clear_resume_checkpoint_loaders(self) -> None:
        """Prevent one scenario from resuming without its cohort partners."""
        for run in self.runs.values():
            for state_asset in run.iteration_state_assets:
                state_asset.resume_checkpoint_loader = None

    def _checkpoint_manifest_path(self, iteration: int) -> pathlib.Path:
        """Return the cohort marker written after all scenario states are saved."""
        checkpoint_folder = (
            next(iter(self.runs.values())).iterations.base_folder
            / "coordinated-checkpoints"
        )
        return checkpoint_folder / (
            f"{self._checkpoint_cohort_hash}_iteration_{iteration}.json"
        )

    def _checkpoint_manifest_matches(self, iteration: int, *, needs_mnl_flows: bool = True) -> bool:
        """Verify the cohort identity and required files before resuming."""
        manifest = json.loads(self._checkpoint_manifest_path(iteration).read_text(encoding="utf-8"))
        return (
            isinstance(manifest, dict)
            and manifest.get("version") == 4
            and manifest.get("cohort_hash") == self._checkpoint_cohort_hash
            and type(manifest.get("iteration")) is int
            and manifest.get("iteration") == iteration
            and isinstance(manifest.get("diagnostics"), list)
            and (not self.parameters.outputs.cache_step_events or all(
                not run.iteration_state_assets[iteration - 1].transition_events_asset.is_update_needed()
                for run in self.runs.values()
            ))
            and (not needs_mnl_flows or iteration != self.parameters.run.n_steps or
                 set(manifest.get("mnl_flows", {})) == {str(key) for key in self.runs})
            and (not needs_mnl_flows or iteration != self.parameters.run.n_steps or all(
                pathlib.Path(item["path"]).is_file()
                for item in manifest.get("mnl_flows", {}).values()
            ))
        )

    @trace_simulation_step("checkpoint manifest")
    def _write_checkpoint_manifest(self, iteration: int, diagnostics=None, mnl_flows=None) -> None:
        """Mark an outer iteration resumable only after every scenario completes."""
        if not self.parameters.outputs.checkpoint_each_step:
            return
        manifest_path = self._checkpoint_manifest_path(iteration)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        flow_files = {}
        for run_key, flows in (mnl_flows or {}).items():
            path = manifest_path.with_name(f"{manifest_path.stem}_mnl_{run_key}.parquet")
            temporary = path.with_suffix(".tmp")
            try:
                flows.write_parquet(temporary)
                os.replace(temporary, path)
            finally:
                temporary.unlink(missing_ok=True)
            flow_files[str(run_key)] = {"path": str(path)}
        payload = json.dumps(
            {
                "version": 4,
                "cohort_hash": self._checkpoint_cohort_hash,
                "iteration": iteration,
                "diagnostics": diagnostics or [],
                "mnl_flows": flow_files,
            },
        )
        fd, temporary_path = tempfile.mkstemp(
            prefix=f".{manifest_path.name}.", suffix=".tmp", dir=manifest_path.parent
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as file:
                file.write(payload)
            os.replace(temporary_path, manifest_path)
        finally:
            pathlib.Path(temporary_path).unlink(missing_ok=True)

    def _invalidate_checkpoint_manifests_from(self, iteration: int) -> None:
        """Invalidate this and later cohort states before recomputing a step."""
        folder = self._checkpoint_manifest_path(1).parent
        prefix = f"{self._checkpoint_cohort_hash}_iteration_"
        for path in folder.glob(f"{prefix}*.json"):
            step = path.stem.removeprefix(prefix)
            if step.isdigit() and int(step) >= iteration:
                path.unlink(missing_ok=True)

    def remove_checkpoint_manifests(self) -> None:
        """Remove this scenario cohort's completion markers and saved flows."""
        manifest_folder = self._checkpoint_manifest_path(1).parent
        if manifest_folder.exists():
            for manifest_path in manifest_folder.glob(
                f"{self._checkpoint_cohort_hash}_iteration_*.json"
            ):
                manifest_path.unlink()
            for flow_path in manifest_folder.glob(
                f"{self._checkpoint_cohort_hash}_iteration_*_mnl_*.parquet"
            ):
                flow_path.unlink()
