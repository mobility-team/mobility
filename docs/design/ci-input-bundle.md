# CI input bundle

The CI input bundle contains the prepared Geofabrik region boundaries for the
2026-01-01 OSM extract. It does not contain the OSM extracts or other external
data. Integration tests can still download those inputs.

Each CI job downloads the pinned release asset, verifies its SHA-256 checksum,
and restores it to the Mobility package-data folder. The restore command checks
the cache identity, file hashes and contents before writing the files. Test
jobs start in fresh containers; they do not clear the input folder between the
unit and integration tests. The runtime-image quickstart restores the same bundle
inside its separate Docker container.

The pinned v3 bundle was freshly downloaded on 2026-11-23. Its 23 boundaries
and extract URLs were validated before publication. The source is Geofabrik and OpenStreetMap contributors. Keep this
provenance in the release notes when replacing the bundle.

To build a new snapshot in the Pixi environment on a machine that can reach
Geofabrik:

```sh
pixi run python .github/scripts/ci_input_bundle.py create mobility-ci-geofabrik-v4.zip
pixi run python .github/scripts/ci_input_bundle.py restore mobility-ci-geofabrik-v4.zip /tmp/mobility-ci-input-check
```

To recover a snapshot from a trusted existing package-data folder, add
`--source-cache /path/to/mobility/data` to the create command. The script checks
the asset identity and contents, and records that the original download date
is unknown. Check the result and its provenance before publishing a new,
versioned release asset. Record its SHA-256 in both CI workflows and update the
release URL. Do not overwrite a published bundle: old CI runs need their
original inputs.

A cache identity change or a new extract date requires a new bundle; the restore
step fails rather than downloading or silently using an incompatible snapshot.
The GitHub release is the durable copy. GitHub Actions caches may be added as a
performance optimisation but should not be the only copy.
