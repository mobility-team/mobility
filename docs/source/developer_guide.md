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
