# CI input bundle

The CI input bundle helps tests run when Geofabrik is unavailable. It contains
region boundaries and the Limousin OpenStreetMap extract for 2026-01-01.
Other external data is not included, so some tests still need the network.

CI downloads the bundle from a GitHub release and checks it before putting the
files in Mobility's data folder. The current bundle is `ci-inputs-geofabrik-v4`,
generated on 2026-11-23 from Geofabrik data. The Limousin extract was
checked against Geofabrik's published MD5 checksum. Source: Geofabrik and
OpenStreetMap contributors.

## When to make a new bundle

Make a new bundle when Mobility changes the region data it needs, the default
extract date, or the way it identifies cached data. Also make one when the
source data needs updating or you want to add another CI input. A compatibility
error during restore means the bundle needs checking. A temporary network
outage alone does not require a new bundle.

There is no automatic refresh. The boundaries are downloaded when you create
the bundle; the extract date comes from Mobility's code.

## How to make and publish a bundle

A maintainer with release permissions can follow these steps from the Mobility
revision being tested. You need Pixi and a connection to Geofabrik. The example
uses `v5`; choose a new version each time.

1. Create the bundle, restore it to a test folder, and calculate its checksum:

   ```sh
   pixi run python .github/scripts/ci_input_bundle.py create mobility-ci-geofabrik-v5.zip --osm-extract /path/to/limousin-260101.osm.pbf
   pixi run python .github/scripts/ci_input_bundle.py restore mobility-ci-geofabrik-v5.zip /tmp/mobility-ci-v5-check
   pixi run python -c "import hashlib; from pathlib import Path; print(hashlib.sha256(Path('mobility-ci-geofabrik-v5.zip').read_bytes()).hexdigest())"
   ```

   Download the dated Limousin extract and compare its checksum with Geofabrik's
   published checksum before building: add `.md5` to the extract URL to find
   it. The script downloads the boundaries, includes the extract, checks the
   23 regions and records file checksums in the zip. Review the restored files and
   the manifest.

2. Publish the zip as a new GitHub prerelease. Record the tested Mobility
   commit, generation date, extract date and data source in the release notes.
   Only include data that can be redistributed. For example, replacing the
   placeholders with the tested commit and date:

   ```sh
   gh release create ci-inputs-geofabrik-v5 mobility-ci-geofabrik-v5.zip --target COMMIT --prerelease --title "CI inputs: Geofabrik regions v5" --notes "Generated YYYY-MM-DD with Mobility COMMIT; extract date 2026-01-01. Source: Geofabrik / OpenStreetMap contributors. Includes the Limousin extract."
   ```

3. Update the bundle URL, filename and checksum in both
   `.github/workflows/test-reusable.yml` and
   `.github/workflows/build-runtime-image.yml`. Update the version and date in
   this guide, run the bundle unit tests, and check both CI jobs.

Keep old releases: previous CI runs refer to them. If Geofabrik is unavailable,
you can create a bundle from a trusted existing Mobility data folder by adding
`--source-cache /path/to/mobility/data` to the create command. State in the
release notes that its original download date is unknown.
