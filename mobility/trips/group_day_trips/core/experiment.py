"""Definitions of the run contexts that share one experiment."""

from dataclasses import dataclass

from mobility.runtime.parameter_values import SensitivityCase


@dataclass(frozen=True)
class ExperimentMember:
    """One scenario, sensitivity case and seeded replication in an experiment."""

    run_id: int
    scenario: str
    sensitivity_case: SensitivityCase | None
    replication: int
    seed: int

    @property
    def sensitivity_case_id(self) -> str:
        return "base" if self.sensitivity_case is None else self.sensitivity_case.case_id


@dataclass(frozen=True)
class ExperimentDefinition:
    """A fixed cohort of contexts sharing candidate plans for one day type."""

    day_type: str
    members: tuple[ExperimentMember, ...]

    def member(
        self,
        *,
        scenario: str,
        sensitivity_case: SensitivityCase | None,
        replication: int,
    ) -> ExperimentMember:
        """Find a declared context without changing the experiment cohort."""
        for member in self.members:
            if (
                member.scenario == scenario
                and member.sensitivity_case == sensitivity_case
                and member.replication == replication
            ):
                return member
        raise ValueError(
            "The requested scenario, sensitivity case and replication are not "
            "part of this experiment. Sensitivity cases must be declared in "
            "the model setup before the experiment is created."
        )
