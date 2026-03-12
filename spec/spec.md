# Project Spec: People CSV Generator

## Overview

A PySpark job that generates imaginary person data and writes it to CSV.
Built on the existing `spark_project` package structure.

## Package

- **Package name:** `hello_pyspark_local_dev`
- **Source:** `src/hello_pyspark_local_dev/`
- **Tests:** `tests/`

> **IMPORTANT:** The existing repo is a template. The package **must** be renamed to match the package name specified above — including the `src/` directory, `pyproject.toml` `[project] name`, all imports, and any other references. Do not keep the template's original package name.

## Stack

- PySpark 3.5 + Delta Lake 3.2
- Python 3.11
- Microsoft Fabric Runtime 1.3 compatible

## Modules

- [People to CSV](people_to_csv.md)
- [CSV to Delta](csv_to_delta.md)
- [Test SQL Connectivity](test_sql_connectivity.md)

## Entry Point

- **File:** `main.py` (project root)
- Fabric Spark Job Definition main file
- Creates (or gets) the SparkSession
- Calls the three modules in order:
  1. People to CSV — generate person data and write CSV
  2. CSV to Delta — read the CSV and write to a `people` Delta table
  3. Test SQL Connectivity — verify JDBC connection to Azure SQL

## Environment Configuration

- The `LOCAL_DEV` environment variable is set to `"1"` via `.vscode/settings.json` using `terminal.integrated.env.linux`.
- This tells the code it's running locally (see [Local Dev / Fabric Strategy](../local-dev-fabric-strategy.md) for details).
- In Fabric, `LOCAL_DEV` is not set — its absence signals the production runtime.

## Development & Deployment Workflow

1. **Develop locally** — all modules are built and tested on the local PySpark environment first. No Fabric access is needed until the final step.
2. **Deploy to Fabric** — once everything works locally, deploy to Microsoft Fabric as a Spark Job Definition.
   - **Workspace:** `daily_etl`
   - **Spark Job Definition:** Use the package name (e.g., `hello_pyspark_local_dev`). If an SJD by this name does not exist, create it.
   - **Environment:** Use the package name (e.g., `hello_pyspark_local_dev`). If an Environment by this name does not exist, create it.
   - **Default Lakehouse:** `pyspark_devops`. If a Lakehouse by this name does not exist, create it **without schemas** (schema-disabled).
   - Do **not** reuse existing resources with different names. Always match the package name for the SJD and Environment.
   - For SJD deployment steps, follow the guidance in [Fabric Deployment Lessons Learned](../fabric-deployment-lessons-learned.md) and [Local Dev / Fabric Strategy](../local-dev-fabric-strategy.md).
3. **Verify on Fabric:**
   - Check the **Livy endpoint** for job logs and execution status.
   - Check the **Lakehouse** (`pyspark_devops`) to confirm the output files (CSV and Delta table) were written correctly.
