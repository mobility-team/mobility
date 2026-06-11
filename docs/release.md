# Release checklist

Use this checklist when publishing a new Mobility version.

## Before tagging

1. Merge the release pull request into `main`.
2. Wait for the CI image workflow to publish a new image.
3. Update the pinned CI image digest in `.github/workflows/publish-github-release.yml`.
4. Check that `pyproject.toml` has the version you want to release.
5. Check that PyPI Trusted Publishing is configured for:
   - project: `mobility-tools`,
   - repository: `mobility-team/mobility`,
   - workflow: `publish-github-release.yml`,
   - environment: `pypi`.
6. Check that the GitHub `pypi` environment requires reviewer approval.

## Tag the release

Create the tag from `main`:

```shell
git tag v0.2.0
git push origin v0.2.0
```

Use the same version as `pyproject.toml`. For example, `version = "0.2.0"` uses tag `v0.2.0`.

## After the workflow runs

1. Review the draft GitHub release notes.
2. Check that the GitHub release has:
   - the wheel,
   - the source archive,
   - `environment.yml`.
3. Check that PyPI has the new `mobility-tools` version.
4. Publish the draft GitHub release.
5. Test the user install command:

```shell
pip install mobility-tools==0.2.0
python -c "import mobility; print(mobility.__file__)"
```
