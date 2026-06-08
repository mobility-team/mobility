# Developer Guide

This page is for people who want to contribute to Mobility.

You do not need to be a professional software developer to make useful contributions. Many Mobility contributors are transport modellers, interns, students, or analysts who know some Python and want the tool to become clearer, faster, or more useful for real studies.

The development workflow is meant to stay simple and explicit.

## Install For Development

Use the same mamba environment as users:

```shell
mamba env create -n mobility -f environment.yml
mamba activate mobility
pip install -e .
```

## Run Tests

Use the project test command:

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
