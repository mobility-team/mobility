"""Generate and cache a timetable from its service inputs."""

import os
from copy import deepcopy
from pathlib import Path

from mobility.runtime.assets.file_asset import FileAsset
from .gtfs_feed_spec import GTFSFeedSpec, build_gtfs_zip


class CustomGTFS(FileAsset):
    """Cache a generated GTFS ZIP from its service specification."""

    def __init__(self, feed: GTFSFeedSpec) -> None:
        # Builder edits must not change the feed after its hash is computed.
        feed = deepcopy(feed)
        feed.validate()
        folder = Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"])
        super().__init__({"version": "2", "feed": feed}, folder / "custom_gtfs.zip")

    def get_cached_asset(self) -> Path:
        """Return the saved ZIP path."""
        return self.cache_path

    def create_and_get_asset(self) -> Path:
        """Write the service before replacing the final cached ZIP."""
        temporary = self.cache_path.with_suffix(".zip.part")
        try:
            build_gtfs_zip(self.inputs["feed"], temporary)
            temporary.replace(self.cache_path)
        finally:
            temporary.unlink(missing_ok=True)
        return self.cache_path


