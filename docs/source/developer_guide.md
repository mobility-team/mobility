# Developer Guide

This page is for people who want to contribute to Mobility.

You do not need to be a professional software developer to make useful contributions. Many Mobility contributors are transport modellers, interns, students, or analysts who know some Python and want the tool to become clearer, faster, or more useful for real studies.

The development workflow is meant to stay simple and explicit.

## Install For Development

Install the development version when you want to change Mobility code, run tests, or open a pull request.

First clone the repository:

```shell
git clone https://github.com/mobility-team/mobility.git
cd mobility
```

Then create the Pixi environment:

```shell
pixi install
```

The repository `pixi.toml` installs Mobility in editable mode.

Editable mode means that Python imports the code from your local repository. When you change a `.py` file, the next Python run uses that change.

Check that the local package is imported:

```shell
pixi run python -c "import mobility; print(mobility.__file__)"
```

The printed path should point to the repository you cloned.

### Mamba Fallback

If you already use mamba, this path is still supported for now:

```shell
mamba env create -n mobility -f environment.yml
mamba activate mobility
python -m pip install -e ".[dev,truststore]"
python -c "import mobility; print(mobility.__file__)"
```

## Run Tests

Use the project test command:

```shell
pixi run python -m pytest --local --use-truststore
```

`--use-truststore` is for tests that download data on company networks. It is not needed for normal user scripts unless you hit a certificate error. For user scripts, see the certificate notes in the installation page.

If you use mamba, run:

```shell
mamba run -n mobility python -m pytest --local --use-truststore
```

## Writing Style

Write package code, tests, comments, and documentation in plain language.

A transport modeller who knows some Python should get the main idea without needing developer vocabulary.

Use comments around logical modelling blocks when they make the code easier to read. Good comments explain the modelling step, the assumption, or the reason why a block exists.

## Public API

Keep user-facing examples on the public objects imported from `mobility`.

Internal assets are useful for the package. In user-facing examples, focus first on the objects that a project modeller is expected to call directly.

## Quickstart Maintenance

The user quickstart is `examples/quickstart-fr.py`.

The CI quickstart is `examples/quickstart-fr-ci.py`.

When changing the quickstart workflow, update both files and the quickstart documentation.

## GTFS Preparation Code

The modelling steps and assumptions are described in [GTFS Data Preparation](gtfs-data-preparation). Keep that page up to date when changing how supply is selected or transfers are calculated.

Three classes divide the preparation work:

| Class | Responsibility |
| --- | --- |
| `GTFSFeed` | Read one ZIP file, retain local trips, check times and create frequency-based departures. |
| `GTFSTimetable` | Combine feeds, choose the service date and prepare transfers. |
| `GTFSRouter` | Select source files and save or reuse the prepared timetable through `FileAsset`. Despite its existing name, this class does not calculate paths. |

Keep preparation methods with the class responsible for the modelling step. GTFS column names such as `trip_id` and `service_id` stay unchanged so contributors can compare the code with input files.

Polars reads the CSV tables and performs large numeric conversions. pandas handles the combined timetable, and GeoPandas handles spatial operations. Each ZIP file is extracted and checked for identical contents in one pass; temporary extracted files are removed after reading it.

`GTFSRouter.get()` returns the path of the JSON summary linking the six Parquet tables. The tables are written before the summary so an interrupted write cannot appear complete. The saved result depends on the source inputs, the version and the contents of manually added files. Change the `version` input if a change makes previously saved timetables unsuitable for reuse.

The transfer table's `specificity` column records rule priority: -1 for an added walking connection, 0 for a declared rule without route restrictions, 1 when one route is named, and 2 when both routes are named. The R graph calculation applies that priority before calculating transfer costs. Unsupported or unusable transfer rules remove the affected directed stop pairs, including added walking connections, with a warning. They do not stop preparation.

The graph finds the first departure at or after the arrival plus minimum connection time. Its transfer cost includes the whole time from arrival to that departure, then averages across arrivals. `PublicTransportGraph` has its own preparation version so cost changes rebuild graphs without rebuilding unchanged GTFS tables.

The small-feed tests in `tests/back/unit/domain/transport_modes/test_004_gtfs_preparation.py` cover calendar selection, frequency departures, time checks, saved outputs and transfer restrictions, including the R graph reader.
