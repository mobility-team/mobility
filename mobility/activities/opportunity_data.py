from __future__ import annotations


def normalize_local_admin_unit_ids(
    local_admin_unit_ids: list[str] | tuple[str, ...] | None,
) -> list[str]:
    """Return a stable list of local admin unit IDs."""
    if local_admin_unit_ids is None:
        return []
    return sorted(set(str(local_admin_unit_id) for local_admin_unit_id in local_admin_unit_ids))


def missing_countries(
    countries: list[str],
    data_by_country: dict[str, type],
) -> list[str]:
    """Return countries with no built-in opportunity data."""
    return [country for country in countries if country not in data_by_country]


def complete_country_priority_order(
    countries: list[str],
    country_priority_order: list[str],
) -> list[str]:
    """Return a priority order that also contains countries not listed by the modeller."""
    priority_order = []
    for country in country_priority_order + countries:
        if country in countries and country not in priority_order:
            priority_order.append(country)
    return priority_order


def keep_first_flow_source(
    flows,
    flow_columns: list[str],
    country_priority_order: list[str],
):
    """Keep one row when several country files contain the same flow."""
    priority = {country: position for position, country in enumerate(country_priority_order)}
    flows = flows.copy()
    flows["_country_priority"] = flows["_flow_country"].map(priority).fillna(len(priority))
    flows = flows.sort_values("_country_priority")
    flows = flows.drop_duplicates(subset=flow_columns, keep="first")
    return flows.drop(columns=["_flow_country", "_country_priority"])
