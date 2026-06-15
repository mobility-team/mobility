from mobility.transport.modes.public_transport.gtfs.gtfs_source_providers import (
    FrenchGTFS,
    SwissGTFS,
)


def available_gtfs_sources():
    """Return built-in GTFS source data by country."""
    return {
        "fr": FrenchGTFS,
        "ch": SwissGTFS,
    }
