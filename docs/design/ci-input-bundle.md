# CI input bundle

The bundle contains prepared Geofabrik region boundaries and the URLs of the
2026-01-01 OSM extracts. It does not contain the OSM extracts or other external
data, so integration tests can still need the network. The current snapshot was
freshly downloaded on 2026-11-23; its source is Geofabrik and OpenStreetMap
contributors. It is published as the `ci-inputs-geofabrik-v3` prerelease.

Each CI job downloads the pinned release asset, verifies its SHA-256 checksum,
and restores it to the Mobility package-data folder. The restore command checks
the asset identity, file hashes and contents before writing the files. Test
jobs start in fresh containers; they do not clear inputs between the unit and
integration tests. The runtime-image quickstart restores the same bundle in its
separate Docker container.

## When to replace it

A maintainer should create a new bundle when the Geofabrik region asset's inputs,
cache identity, output format, or default extract date change; when the included
source data needs updating; or when a new CI input is deliberately added. A
restore compatibility error means the current bundle cannot be used with the
checked-out code. Do not refresh just to fix a temporary network outage: the
purpose of the snapshot is to keep these inputs stable between runs. We do not
currently schedule automatic refreshes.

The boundary polygons and extract URLs are separate: the polygons are fetched
when the bundle is built, while the extract date in the URLs is defined by the
code. Record both the generation date and the extract date in release notes.
Only bundle data that can be redistributed, and retain source attribution.

## Generate, validate and publish

A maintainer with repository release permissions runs these steps from the
Mobility revision being prepared, on a machine with the Pixi environment and
network access to Geofabrik. Use a new version number each time; the examples
below use v4. The create command builds the asset in a fresh temporary folder,
checks the 23 regions and writes a zip with a manifest and per-file hashes.

```sh
pixi run python .github/scripts/ci_input_bundle.py create mobility-ci-geofabrik-v4.zip
pixi run python .github/scripts/ci_input_bundle.py restore mobility-ci-geofabrik-v4.zip /tmp/mobility-ci-v4-check
pixi run python -c "import hashlib; from pathlib import Path; print(hashlib.sha256(Path('mobility-ci-geofabrik-v4.zip').read_bytes()).hexdigest())"
```

Review the restored data, manifest and source attribution. Verify that the
release notes state the Mobility revision, generation date, extract date and
source. Then publish the zip as a new prerelease asset, for example:

```sh
gh release create ci-inputs-geofabrik-v4 mobility-ci-geofabrik-v4.zip --target main --prerelease --title "CI inputs: Geofabrik regions v4" --notes "Generated YYYY-MM-DD from Geofabrik boundaries using Mobility revision COMMIT; OSM extract date 2026-01-01. Source: Geofabrik / OpenStreetMap contributors. Contains boundaries only."
```

Replace the placeholders with the actual provenance and use the actual tested
commit as the target. In the same change, update the release URL, filename and
SHA-256 in both `.github/workflows/test-reusable.yml` and
`.github/workflows/build-runtime-image.yml`, and update the version and date
in this guide. Run the bundle unit tests and inspect the CI test and runtime
image jobs. Do not replace an existing release asset: old CI runs need their
original inputs.

If the source is unavailable, a trusted existing package-data folder can be
used with `--source-cache /path/to/mobility/data` on the create command. The
script checks the asset identity and contents but cannot establish when those
boundaries were downloaded. Disclose that limitation in the release notes.
The release is the durable copy; an Actions cache can be an optional speedup,
but should not be the only copy.
