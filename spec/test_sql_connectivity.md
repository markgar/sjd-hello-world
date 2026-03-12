# Module: Test SQL Connectivity

## Description

Verify SQL connectivity to an Azure SQL Database by reading the `Application.Cities` table and returning the results as a DataFrame.

## Connection Details

- **Server:** `adventureworksltmg.database.windows.net`
- **Database:** `wideworldimporters`
- **Driver (local only):** JDBC (`com.microsoft.sqlserver.jdbc.SQLServerDriver`)
- **Table:** `Application.Cities`

## Authentication

| Environment       | Connector                                               | Auth mechanism                                                         |
|-------------------|---------------------------------------------------------|------------------------------------------------------------------------|
| Local development | JDBC (`spark.read.jdbc(...)`)                           | `DefaultAzureCredential` → access token (`az login` required)         |
| Microsoft Fabric  | Built-in Spark SQL connector (`spark.read...mssql()`)   | Workspace identity (automatic Entra auth, no token needed)             |

The module detects the runtime environment via `is_local_dev()` and selects the appropriate connector and auth method automatically.

### Local path details
- Uses `azure.identity.DefaultAzureCredential` to get an access token scoped to `https://database.windows.net/.default`.
- Passes the token as a JDBC `accessToken` connection property.
- `azure.identity` is imported **inside** the local-only helper so Fabric never loads it.

### Fabric path details
- Uses `.mssql(TABLE)` on `DataFrameReader` — a connector pre-registered in Fabric's Spark runtime.
- Auth is handled automatically by the workspace's Entra identity. No tokens or credential objects.
- `.mssql()` does not exist locally — it must only be called in the Fabric code path.

> **Local dev note:** The developer must be logged in to Azure (`az login`) before running locally. If `DefaultAzureCredential` fails, stop and ask the user to log in first.

## Module

- **File:** `src/hello_pyspark_local_dev/test_sql_connectivity.py`
- **Entry point:** `run(spark)` function
- Helper functions (private):
  - `get_jdbc_url()` — builds the JDBC connection URL (local only)
  - `_get_local_access_token()` — gets an Azure SQL access token via `DefaultAzureCredential`
  - `get_jdbc_properties()` — returns JDBC properties dict with driver and access token
  - `_read_cities_local(spark)` — reads cities via JDBC (local dev path)
  - `_read_cities_fabric(spark)` — reads cities via `.mssql()` (Fabric path)
- Public API:
  - `read_cities(spark)` — branches on `is_local_dev()` and delegates to the correct implementation
