"""Create and restore the small, versioned CI input bundle."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import tempfile
from zipfile import ZIP_DEFLATED, ZipFile

import geopandas as gpd

from mobility.spatial.osm.geofabrik_regions import GeofabrikRegions

EXTRACT_DATE = "260101"


def digest(data):
    return hashlib.sha256(data).hexdigest()


def asset_in(directory):
    os.environ["MOBILITY_PACKAGE_DATA_FOLDER"] = str(directory)
    return GeofabrikRegions(extract_date=EXTRACT_DATE)


def validate_regions(path):
    regions = gpd.read_file(path)
    if len(regions) != 23 or regions.geometry.isna().any() or regions.geometry.is_empty.any():
        raise ValueError("Expected 23 nonempty Geofabrik region boundaries")
    if not regions["url"].str.endswith(f"-{EXTRACT_DATE}.osm.pbf").all():
        raise ValueError("Unexpected Geofabrik extract URLs")


def create(output, source_cache=None):
    # Generate in a fresh directory: do not accidentally package stale local data.
    with tempfile.TemporaryDirectory() as temp:
        asset = asset_in(temp)
        if source_cache is None:
            asset.get()
            provenance = "Fresh download from Geofabrik"
        else:
            source = Path(source_cache)
            source_file = source / asset.cache_path.name
            source_marker = source / asset.hash_path.name
            if source_marker.read_text().strip() != asset.inputs_hash:
                raise ValueError("Source cache marker does not match")
            asset.cache_path.write_bytes(source_file.read_bytes())
            provenance = "Existing Mobility cache; source download date unknown"
        validate_regions(asset.cache_path)
        data = asset.cache_path.read_bytes()
        marker = asset.hash_path.read_bytes()
        manifest = {
            "format": 1,
            "asset": "GeofabrikRegions",
            "extract_date": EXTRACT_DATE,
            "inputs_hash": asset.inputs_hash,
            "files": {asset.cache_path.name: digest(data), asset.hash_path.name: digest(marker)},
            "source": "https://download.geofabrik.de/europe/",
            "provenance": provenance,
        }
        with ZipFile(output, "w", ZIP_DEFLATED) as bundle:
            bundle.writestr("manifest.json", json.dumps(manifest, indent=2) + "\n")
            bundle.writestr(asset.cache_path.name, data)
            bundle.writestr(asset.hash_path.name, marker)
    print(f"Created {output}")


def restore(bundle_path, destination):
    destination = Path(destination)
    with tempfile.TemporaryDirectory() as temp, ZipFile(bundle_path) as bundle:
        asset = asset_in(temp)
        manifest = json.loads(bundle.read("manifest.json"))
        expected = {asset.cache_path.name, asset.hash_path.name}
        if (manifest.get("format") != 1 or manifest.get("asset") != "GeofabrikRegions"
                or manifest.get("extract_date") != EXTRACT_DATE
                or manifest.get("inputs_hash") != asset.inputs_hash
                or set(manifest.get("files", {})) != expected
                or set(bundle.namelist()) != expected | {"manifest.json"}):
            raise ValueError("Input bundle is incompatible with this Mobility version")
        contents = {name: bundle.read(name) for name in expected}
        if any(digest(data) != manifest["files"][name] for name, data in contents.items()):
            raise ValueError("Input bundle checksum mismatch")
        if contents[asset.hash_path.name].decode().strip() != asset.inputs_hash:
            raise ValueError("Input bundle cache marker does not match")
        candidate = Path(temp) / asset.cache_path.name
        candidate.write_bytes(contents[asset.cache_path.name])
        validate_regions(candidate)
        destination.mkdir(parents=True, exist_ok=True)
        for name, data in contents.items():
            (destination / name).write_bytes(data)
    print(f"Restored Geofabrik regions to {destination}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    create_parser = commands.add_parser("create")
    create_parser.add_argument("bundle", type=Path)
    create_parser.add_argument("--source-cache", type=Path, help="Use a validated existing Mobility cache")
    restore_parser = commands.add_parser("restore")
    restore_parser.add_argument("bundle", type=Path)
    restore_parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    if args.command == "create":
        create(args.bundle, args.source_cache)
    else:
        restore(args.bundle, args.destination)


if __name__ == "__main__":
    main()
