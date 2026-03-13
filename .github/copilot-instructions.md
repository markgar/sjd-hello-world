# Copilot Instructions

## Project

Local PySpark development environment matching Microsoft Fabric Runtime 1.3
(Spark 3.5, Java 11, Python 3.11, Delta Lake 3.2).

## Stack

- **Python 3.11** — use modern syntax (type hints, `match`, `|` unions)
- **PySpark 3.5 + Delta Lake 3.2** — primary data processing framework
- **ruff** — sole linter and formatter (no black, flake8, pylint, isort)
- **pytest + pytest-cov** — testing framework
- **pre-commit** — git hook runner

## Code conventions

- Line length: 120 characters
- Source code lives in `src/hello_pyspark_local_dev/` (created by sjd-builder from CONSTITUTION.md)
- Tests live in `tests/`
- Use `from __future__ import annotations` in all modules
- Imports sorted by ruff (isort-compatible)
- Follow existing `pyproject.toml` ruff rules: F, E, W, B, I, UP, S, PL

## Testing

- Mark Spark-dependent tests so they can be filtered: `pytest -m spark`
- Tests with `_int_` in the name are auto-marked as integration tests
- Target `pytest -m "not integration"` for fast local runs
- Place fixtures in `tests/conftest.py`

## PySpark patterns

- Prefer DataFrame API over SQL strings
- Use `spark.createDataFrame()` with explicit schemas in tests
- Stop SparkSessions in test fixtures (session-scoped)

## Fabric SJD constraints

- Never use `mssparkutils` or `notebookutils` in SJD code — they don't exist in the SJD runtime
- Use `DefaultAzureCredential` for all Azure auth — works identically local and Fabric
- Use environment variables for values that differ between environments

## Fabric API access

- **Before any Fabric REST API or OneLake DFS API call, check `devops_helpers/` for existing helpers.**
- If a helper exists, use it. Do not duplicate it with raw curl/urllib/requests.
- If no helper exists, raw API calls are fine.
