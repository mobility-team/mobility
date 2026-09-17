from __future__ import annotations

from mobility.runtime.assets.in_memory_asset import InMemoryAsset
from mobility.runtime.parameter_values import (
    SensitivityCase,
    resolve_parameter_values,
)


class GeneralizedCost(InMemoryAsset):
    """Base class for generalized costs that can vary during a model run."""

    def for_iteration(
        self,
        iteration: int,
        *,
        travel_costs,
        scenario: str | None = None,
        sensitivity_case: SensitivityCase | None = None,
    ) -> "GeneralizedCost":
        """Return a generalized-cost variant with resolved parameter values."""
        resolved_inputs = resolve_parameter_values(
            self.inputs,
            scenario=scenario,
            iteration=iteration,
            sensitivity_case=sensitivity_case,
        )
        resolved_inputs["travel_costs"] = travel_costs

        if (
            resolved_inputs == self.inputs
            and travel_costs is self.inputs["travel_costs"]
        ):
            return self

        return self._from_resolved_inputs(resolved_inputs)

    def _from_resolved_inputs(self, inputs: dict) -> "GeneralizedCost":
        """Create the concrete generalized cost from resolved inputs."""
        raise NotImplementedError(
            f"{self.__class__.__name__} should implement "
            "_from_resolved_inputs()."
        )
