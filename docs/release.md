# Release checklist

Use this checklist when publishing a new Mobility version.

## Before tagging

1. Merge the release pull request into `main`.
2. Wait for the main CI workflows to pass.
3. Check that `pyproject.toml` has the version you want to release.
4. Check that PyPI Trusted Publishing is configured for:
   - project: `mobility-tools`,
   - repository: `mobility-team/mobility`,
   - workflow: `publish-github-release.yml`,
   - environment: `pypi`.
5. Check that the GitHub `pypi` environment requires reviewer approval.
6. Check that packages declared from external repositories, including
   `cppRoutingCCH`, are available at the versions required by `DESCRIPTION`.

## Tag the release

Create the tag from `main`:

```shell
git tag v0.2.1
git push origin v0.2.1
```

Use the same version as `pyproject.toml`. For example, `version = "0.2.1"` uses tag `v0.2.1`.

## Release workflow

Pushing the tag starts `publish-github-release.yml`. The workflow builds the
wheel and source archive once from the tagged commit and checks that the tag
matches `pyproject.toml`.

The same wheel artifact is then:

- attached to a draft GitHub release,
- published to PyPI after approval in the `pypi` environment,
- installed in the runtime image alongside the tagged `DESCRIPTION`.

Runtime-image publication waits for PyPI publication. The image must pass its
import, Mobility setup, and quickstart checks before the workflow pushes the
full-version, minor-version, commit-SHA, and `latest` tags to GHCR.

## After the workflow runs

1. Review the draft GitHub release notes.
2. Check that the GitHub release has:
   - the wheel,
   - the source archive,
   - `pixi.toml`,
   - `environment.yml`.
3. Check that PyPI has the new `mobility-tools` version.
4. Check that the runtime-image checks passed and that the GitHub container
   registry has the new `mobility-runtime` version.
5. Publish the draft GitHub release.
6. Test the user Pixi install command from a temporary project folder:

```shell
version="v0.2.1"
base="https://github.com/mobility-team/mobility/releases/download/$version"
curl -L -o pixi.toml "$base/pixi.toml"
pixi install
pixi run python -c "import mobility; print(mobility.__file__)"
```

7. Test the direct PyPI install command:

```shell
pip install mobility-tools==0.2.1
python -c "import mobility; print(mobility.__file__)"
```

8. Test the Docker image:

```shell
docker run --rm ghcr.io/mobility-team/mobility-runtime:0.2.1 python -c "import mobility; print(mobility.__file__)"
```

If runtime-image publication fails after the PyPI job succeeds, rerun only the
failed jobs from the same tagged workflow. Do not rebuild the wheel or reuse the
version with different contents: PyPI and the runtime image must continue to
refer to the wheel produced by the original tagged run.
