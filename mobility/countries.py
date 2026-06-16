from __future__ import annotations

from collections.abc import Iterable


def normalize_country_codes(countries: str | Iterable[str] | None) -> list[str]:
    """Return a stable list of lower-case country codes."""
    if countries is None:
        return []
    if isinstance(countries, str):
        countries = [countries]
    normalized = set()
    for country in countries:
        if country is None:
            continue
        text = str(country).strip().lower()
        if text:
            normalized.add(text)
    return sorted(normalized)
