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

## Tag the release

Create the tag from `main`:

```shell
git tag v0.2.1
git push origin v0.2.1
```

Use the same version as `pyproject.toml`. For example, `version = "0.2.1"` uses tag `v0.2.1`.

## After the workflow runs

1. Review the draft GitHub release notes.
2. Check that the GitHub release has:
   - the wheel,
   - the source archive,
   - `pixi.toml`,
   - `environment.yml`.
3. Check that PyPI has the new `mobility-tools` version.
4. Publish the draft GitHub release.
5. Test the user Pixi install command from a temporary project folder:

```shell
version="v0.2.1"
base="https://github.com/mobility-team/mobility/releases/download/$version"
curl -L -o pixi.toml "$base/pixi.toml"
pixi install
pixi run python -c "import mobility; print(mobility.__file__)"
```

6. Test the direct PyPI install command:

```shell
pip install mobility-tools==0.2.1
python -c "import mobility; print(mobility.__file__)"
```
