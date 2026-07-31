from __future__ import annotations

from copy import deepcopy
from typing import Any, Annotated

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .parameter_values import PopulationSegmentValue


SEGMENT_SELECTOR_FIELDS = (
    "country",
    "csp",
    "home_zone_id",
    "city_category",
    "n_cars",
)


class PopulationSegment(BaseModel):
    """A named population subset selected from demand-group attributes."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: Annotated[str, Field(min_length=1)]
    share: Annotated[float, Field(default=1.0, gt=0.0, le=1.0)]
    country: str | list[str] | None = None
    csp: str | list[str] | None = None
    home_zone_id: int | list[int] | None = None
    city_category: str | list[str] | None = None
    n_cars: str | int | list[str | int] | None = None

    @model_validator(mode="after")
    def validate_selector(self) -> "PopulationSegment":
        if not self.selector:
            raise ValueError(
                f"Population segment '{self.name}' needs at least one selector."
            )
        return self

    @property
    def selector(self) -> dict[str, tuple[Any, ...]]:
        """Return normalized selector values by demand-group column."""
        selector = {}
        for field in SEGMENT_SELECTOR_FIELDS:
            value = getattr(self, field)
            if value is None:
                continue
            values = value if isinstance(value, list) else [value]
            if not values:
                raise ValueError(
                    f"Population segment '{self.name}' selector '{field}' is empty."
                )
            selector[field] = tuple(values)
        return selector

def validate_population_segments(
    segments: list[PopulationSegment],
) -> list[PopulationSegment]:
    """Validate segment names and return the definitions unchanged."""
    names = [segment.name for segment in segments]
    duplicates = sorted({name for name in names if names.count(name) > 1})
    if duplicates:
        raise ValueError(
            "Population segment names should be unique. Duplicates: "
            + ", ".join(duplicates)
        )
    return segments


def resolve_population_segment_values(
    value: Any,
    *,
    memberships: set[str],
    segments: list[PopulationSegment],
) -> Any:
    """Resolve segment-aware values recursively for one demand subgroup."""
    definitions = {segment.name: segment for segment in segments}

    if isinstance(value, PopulationSegmentValue):
        unknown = sorted(set(value.segment_values) - set(definitions))
        if unknown:
            raise ValueError(
                "Parameter values refer to undefined population segments: "
                + ", ".join(unknown)
            )
        matching = [
            definitions[name]
            for name in value.segment_values
            if name in memberships
        ]
        selected = _most_specific_segment(matching)
        selected_value = (
            value.default
            if selected is None
            else value.segment_values[selected.name]
        )
        return resolve_population_segment_values(
            selected_value,
            memberships=memberships,
            segments=segments,
        )

    if isinstance(value, BaseModel):
        data = {
            field: resolve_population_segment_values(
                getattr(value, field),
                memberships=memberships,
                segments=segments,
            )
            for field in value.__class__.model_fields
        }
        return value.__class__.model_validate(data)
    if isinstance(value, dict):
        return {
            key: resolve_population_segment_values(
                item,
                memberships=memberships,
                segments=segments,
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [
            resolve_population_segment_values(
                item,
                memberships=memberships,
                segments=segments,
            )
            for item in value
        ]
    if isinstance(value, tuple):
        return tuple(
            resolve_population_segment_values(
                item,
                memberships=memberships,
                segments=segments,
            )
            for item in value
        )
    return deepcopy(value)


def population_segment_defaults(value: Any) -> Any:
    """Replace all segment-aware values with their global defaults."""
    if isinstance(value, PopulationSegmentValue):
        return population_segment_defaults(value.default)
    if isinstance(value, BaseModel):
        return value.__class__.model_validate(
            {
                field: population_segment_defaults(getattr(value, field))
                for field in value.__class__.model_fields
            }
        )
    if isinstance(value, dict):
        return {key: population_segment_defaults(item) for key, item in value.items()}
    if isinstance(value, list):
        return [population_segment_defaults(item) for item in value]
    if isinstance(value, tuple):
        return tuple(population_segment_defaults(item) for item in value)
    return deepcopy(value)


def _most_specific_segment(
    matching: list[PopulationSegment],
) -> PopulationSegment | None:
    if not matching:
        return None

    maximal = [
        candidate
        for candidate in matching
        if not any(
            other.name != candidate.name and _is_more_specific(other, candidate)
            for other in matching
        )
    ]
    if len(maximal) != 1:
        names = ", ".join(sorted(segment.name for segment in maximal))
        raise ValueError(
            "Population segment values are ambiguous for this demand subgroup: "
            f"{names}. Add a more specific segment or remove one value."
        )
    return maximal[0]


def _is_more_specific(
    candidate: PopulationSegment,
    other: PopulationSegment,
) -> bool:
    candidate_selector = candidate.selector
    other_selector = other.selector
    if not set(candidate_selector).issuperset(other_selector):
        return False
    selector_is_strict = set(candidate_selector) != set(other_selector)
    for field, other_values in other_selector.items():
        candidate_values = set(candidate_selector[field])
        if not candidate_values.issubset(other_values):
            return False
        selector_is_strict = (
            selector_is_strict or candidate_values != set(other_values)
        )

    if selector_is_strict:
        return True

    # With identical selectors, a partial-share membership is a subset of the
    # matching full segment. Two partial shares remain ambiguous.
    return candidate.share < 1.0 and other.share == 1.0
