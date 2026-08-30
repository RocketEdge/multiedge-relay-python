# Contributing

Thanks for helping improve the MultiEdge Signal Relay Python SDK.

## Setup

```bash
uv sync            # installs the package + dev dependencies (Python 3.11+)
```

## Development workflow

1. Write a failing test first (TDD is the house rule), watch it fail, then implement.
2. Keep runtime dependencies to `httpx` + `pydantic` only; anything else goes behind
   an optional extra.
3. All gates must be green before a PR:

```bash
uv run pytest -m "not integration"
uv run ruff check .
uv run black --check .
uv run mypy --strict src
```

## Conventions

- Conventional Commits (`feat(subscriber): ...`, `fix(dlq): ...`).
- Google-style docstrings on every public class and function.
- Never weaken the never-silent-loss contract: no bare `except`, no swallowed
  errors, no silent cursor resets, no dropped signals.
- Public repo: no credentials, no internal endpoints — examples use
  `https://relay-api.multiedge.ai` and placeholder keys.
- Update `CHANGELOG.md` in the same PR as the behavior change.

## Releasing

Publishing is **CI-only, via PyPI trusted publishing** (OIDC). There is no API token
anywhere — not in GitHub secrets, not on a laptop. Never run `twine upload` or
`uv publish` by hand: a manual upload produces a release with no provenance and
silently diverges from what the tag built.

1. Bump `version` in `pyproject.toml`.
2. In `CHANGELOG.md`, rename `## [Unreleased]` to `## [X.Y.Z] - YYYY-MM-DD` and open a
   fresh empty `## [Unreleased]` above it.
3. Merge to `main` with all gates green.
4. Tag and push — the tag must equal the `pyproject.toml` version, or the release
   workflow fails its first step by design:

```bash
git tag -a vX.Y.Z -m "vX.Y.Z"
git push origin vX.Y.Z
```

`.github/workflows/release.yml` then builds the sdist+wheel and publishes them from the
`pypi` environment, which only `v*` tags may deploy to. Watch the run with
`gh run watch --workflow=release.yml`; the published version appears on
[PyPI](https://pypi.org/project/multiedge-relay/) with GitHub trusted-publisher
provenance.
