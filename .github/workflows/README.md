# CI and release architecture

The workflows keep source, dependencies, and published artifacts from the same
revision together. A pull request must never combine its `DESCRIPTION` with a
previously published Mobility wheel.

## Pull-request checks

The normal test workflows install the checked-out package over the published CI
dependency image and synchronize the R dependencies declared by the pull
request. Together, they run the unit, integration, wheel-resource, and coverage
checks on that source revision.

When runtime-image inputs change, `build-runtime-image.yml` builds a wheel from
the pull-request checkout. The runtime Dockerfile installs that wheel alongside
the same checkout's `DESCRIPTION`, then runs import, setup, and quickstart
checks. Pull-request runtime validation does not install `mobility-tools` from
PyPI and never publishes an image.

The CI dependency image contains toolchains and dependencies only. Pull
requests validate changes to that image without publishing it; the image is
published after the dependency change reaches `main`.

## Release artifacts

`publish-github-release.yml` builds the wheel and source archive once from the
tagged commit. The tag must match the version in `pyproject.toml`. That same
wheel is checked, uploaded to the draft GitHub release, published to PyPI, and
installed in the runtime image.

Runtime-image publication starts only after PyPI publication succeeds. The
image is tested before it is pushed to GHCR with the full version, minor
version, commit SHA, and `latest` tags. If a post-publication image step fails,
rerun the failed jobs from the tagged release workflow; do not rebuild or
replace the already-published wheel.

## Workflow pin maintenance

GitHub Actions are pinned to full commit SHAs to reduce supply-chain risk.
The comment after each `uses:` line keeps the human version tag that was pinned.

To update an action pin, resolve the tag to a commit SHA from the action repository:

```shell
git ls-remote https://github.com/actions/checkout.git refs/tags/v4
git ls-remote https://github.com/actions/upload-artifact.git refs/tags/v4
git ls-remote https://github.com/actions/download-artifact.git refs/tags/v4
git ls-remote https://github.com/softprops/action-gh-release.git refs/tags/v2
git ls-remote https://github.com/pypa/gh-action-pypi-publish.git refs/tags/v1.14.0
git ls-remote https://github.com/codecov/codecov-action.git refs/tags/v5
git ls-remote https://github.com/docker/setup-buildx-action.git refs/tags/v3
git ls-remote https://github.com/docker/login-action.git refs/tags/v3
git ls-remote https://github.com/docker/build-push-action.git refs/tags/v6
git ls-remote https://github.com/actions/setup-python.git refs/tags/v5
git ls-remote https://github.com/mamba-org/setup-micromamba.git refs/tags/v1
git ls-remote https://github.com/r-lib/actions.git refs/tags/v2
```

Sometimes Git prints two lines for one tag:

```text
6733eb7d741f0b11ec6a39b58540dab7590f9b7d refs/tags/v1.14.0
cef221092ed1bacb1cc03d23a2d87d1d172e277b refs/tags/v1.14.0^{}
```

When that happens, use the second line, the one ending with `^{}`.
That second line is the exact code commit that GitHub Actions will run.

Example: update `actions/checkout` to `v4`.

First run:

```shell
git ls-remote https://github.com/actions/checkout.git refs/tags/v4
```

If Git prints:

```text
34e114876b0b11c390a56381ad16ebd13914f8d5 refs/tags/v4
```

then write this in the workflow:

```yaml
- uses: actions/checkout@34e114876b0b11c390a56381ad16ebd13914f8d5 # v4
```

The long value after `@` is the exact code version that GitHub Actions will run.
The short `# v4` comment is only there to help humans know which release tag it came from.

Only update these pins in a reviewed pull request.
