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
    leg_mode_costs: pl.DataFrame
