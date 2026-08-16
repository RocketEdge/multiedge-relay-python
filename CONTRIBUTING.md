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
  `https://api.multiedge.ai` and placeholder keys.
- Update `CHANGELOG.md` in the same PR as the behavior change.
