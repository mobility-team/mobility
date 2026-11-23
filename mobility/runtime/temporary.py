"""Temporary folders for Mobility-owned files."""

import os
from pathlib import Path


def temporary_folder() -> Path:
    """Keep temporary files alongside the configured project data."""
    project_folder = os.environ.get("MOBILITY_PROJECT_DATA_FOLDER")
    if project_folder is None:
        package_folder = Path(os.environ.get("MOBILITY_PACKAGE_DATA_FOLDER", Path.home() / ".mobility" / "data"))
        project_folder = package_folder / "projects"
    folder = Path(project_folder) / "temporary"
    folder.mkdir(parents=True, exist_ok=True)
    return folder
