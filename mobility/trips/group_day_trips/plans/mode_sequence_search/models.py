from dataclasses import dataclass
from typing import Any

import polars as pl


@dataclass(frozen=True)
class ModeSearchInputs:
    """Normalized inputs shared by the Python and Rust mode-sequence search backends."""

    modes_by_name: dict[str, Any]
    mode_enum_values: list[str]
    mode_id_by_name: dict[str, int]
    mode_name_by_id: dict[int, str]
    is_return_mode_by_id: dict[int, bool]
    return_mode_id_by_id: dict[int, int | None]
    needs_vehicle_by_id: dict[int, bool]
    leg_mode_costs: pl.DataFrame
