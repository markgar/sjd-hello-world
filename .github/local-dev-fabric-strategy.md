# Local-First PySpark Development for Microsoft Fabric

A strategy for writing Python/PySpark code that runs identically in a local dev container and in Microsoft Fabric Spark Job Definitions — with zero code changes at deploy time.

---

## 1. Environment Detection

A single environment variable, `LOCAL_DEV`, tells the code where it's running.

| Variable | Local Dev | Fabric |
|---|---|---|
| `LOCAL_DEV` | `"1"` | Not set |

```python
import os

def is_local_dev() -> bool:
    """True when running in the local dev container, False in Fabric."""
    return os.environ.get("LOCAL_DEV") == "1"
```

Set `LOCAL_DEV=1` in `.vscode/settings.json` via `terminal.integrated.env.linux`:

```json
{
    "terminal.integrated.env.linux": {
        "LOCAL_DEV": "1"
    }
}
```

Never set it in Fabric — its absence is the signal.

All environment-specific branching flows through this single check. Avoid scattering `os.environ.get("LOCAL_DEV")` throughout your codebase; centralize it in a helper and import it everywhere.

---

## 2. Lakehouse File/Path Strategy

Fabric Spark jobs see the attached lakehouse as the working directory, so paths like `Files/data.csv` and `Tables/my_table` resolve directly. Locally, those same files live under a `lakehouse/` folder in your repo.

A `LAKEHOUSE_ROOT` variable bridges the gap:

```python
def _default_lakehouse_root():
    return "lakehouse" if os.environ.get("LOCAL_DEV") == "1" else ""

LAKEHOUSE_ROOT = os.environ.get("LAKEHOUSE_ROOT", _default_lakehouse_root())
```

| Environment | `LAKEHOUSE_ROOT` | Resolved path example |
|---|---|---|
| Local Dev | `"lakehouse"` | `lakehouse/Files/people.csv` |
| Fabric | `""` (empty string) | `Files/people.csv` |

All file and table access uses `os.path.join(LAKEHOUSE_ROOT, ...)`:

```python
# CSV files
csv_path = os.path.join(LAKEHOUSE_ROOT, "Files", "people.csv")

# Delta tables
table_path = os.path.join(LAKEHOUSE_ROOT, "Tables", "my_table")
df.write.format("delta").mode("overwrite").save(table_path)
```

**Local folder structure mirrors Fabric's lakehouse layout:**

```
lakehouse/
├── Files/        ← unstructured data (CSV, JSON, Parquet, etc.)
└── Tables/       ← Delta tables
    └── my_table/
        └── _delta_log/
```

The `lakehouse/` folder **must be added to `.gitignore`** so that local data files are never checked in. Add this entry to `.gitignore`:

```gitignore
lakehouse/
```

This gives you a real filesystem to develop and test against, without polluting the repo with generated data. The same code paths work on Fabric without modification.

The `LAKEHOUSE_ROOT` env var can also be overridden explicitly for non-standard setups (e.g., pointing to a shared network mount or a different local directory).

---

## 3. SparkSession Creation

Fabric's Spark runtime provides Delta Lake and cluster management out of the box. Locally, you must configure both yourself.

```python
from pyspark.sql import SparkSession

builder = SparkSession.builder.appName("my-job")

if is_local_dev():
    from delta import configure_spark_with_delta_pip

    builder = (
        builder
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
else:
    spark = builder.getOrCreate()
```

| Aspect | Local | Fabric |
|---|---|---|
| Spark master | `local[*]` (multi-threaded, single machine) | Managed by Fabric (multi-node cluster) |
| Delta Lake | Must be configured explicitly via `configure_spark_with_delta_pip()` | Pre-installed in the Fabric runtime |
| `delta-spark` import | Required | Not needed (never imported on Fabric) |

The conditional import of `delta` inside the `if is_local_dev()` block means Fabric never tries to import it from your code — it uses its own built-in Delta support.

---

## 4. Authentication & SQL Connectivity Strategy

Authentication for Azure SQL requires **different connectors** in each environment. This is a key exception to the "same code everywhere" rule — a public `read_*` function must branch on `is_local_dev()` and delegate to the correct implementation.

### Why two paths?

| Concern | Local Dev | Fabric |
|---|---|---|
| SQL connector | JDBC (`spark.read.jdbc(...)`) | Built-in Spark SQL connector (`.mssql()`) |
| Auth mechanism | `DefaultAzureCredential` → access token | Workspace identity (automatic Entra auth) |
| JDBC driver JAR | Must be on the classpath (dev container provides it) | Not needed — `.mssql()` handles transport |
| `azure-identity` | Required (`pip install azure-identity`) | **Not used** for SQL — the connector handles auth |

### Local Dev — JDBC + `DefaultAzureCredential`

The developer must be logged in via `az login`. `DefaultAzureCredential` obtains an access token scoped to Azure SQL, which is passed as a JDBC connection property.

```python
from azure.identity import DefaultAzureCredential

def _get_local_access_token() -> str:
    return DefaultAzureCredential().get_token("https://database.windows.net/.default").token

def _read_table_local(spark: SparkSession) -> DataFrame:
    url = f"jdbc:sqlserver://{SERVER}:1433;database={DATABASE};encrypt=true;trustServerCertificate=false;"
    props = {"driver": DRIVER, "accessToken": _get_local_access_token()}
    return spark.read.jdbc(url=url, table=TABLE, properties=props)
```

### Fabric — Built-in `.mssql()` Connector

Fabric's Spark runtime pre-registers `.mssql()` on `DataFrameReader`. It auto-authenticates using the workspace's Entra identity — no tokens, no imports, no credential objects.

```python
def _read_table_fabric(spark: SparkSession) -> DataFrame:
    url = f"jdbc:sqlserver://{SERVER}:1433;database={DATABASE};"
    return spark.read.option("url", url).mssql(TABLE)
```

> **Important:** `.mssql()` does not exist locally — never import or call it outside a Fabric code path. The conditional `import` of `azure.identity` inside the local path means Fabric never loads that package either.

### Public API — Branch on Environment

Expose a single function that branches internally. Callers never need to know which connector is used.

```python
def read_table(spark: SparkSession) -> DataFrame:
    if is_local_dev():
        return _read_table_local(spark)
    return _read_table_fabric(spark)
```

### Pattern Summary

1. Define constants for server, database, table, and driver at module level.
2. Write a `_read_*_local()` helper using JDBC + `DefaultAzureCredential` access token.
3. Write a `_read_*_fabric()` helper using `.mssql()` with just a URL option.
4. Write a public `read_*()` that calls `is_local_dev()` and delegates.
5. Keep `azure.identity` imports **inside** the local-only function so Fabric never loads them.

### General Auth (non-SQL Azure services)

For services other than SQL (e.g., Storage, Key Vault), `DefaultAzureCredential` works identically in both environments:

| Environment | Credential source |
|---|---|
| Local Dev | Azure CLI (`az login`) |
| Fabric | Workspace Managed Identity (automatic) |

```python
from azure.identity import DefaultAzureCredential

token = DefaultAzureCredential().get_token("https://storage.azure.com/.default").token
```

For configuration values that differ between environments (server names, database names, storage account URLs), use environment variables so the same code adapts without modification.

---

## 5. Custom Library Deployment (.whl via Fabric Environments)

Package your shared code as a Python wheel and deploy it through Fabric Environments — the only reliable path for custom libraries on Spark Job Definitions.

### Build

```bash
pip wheel . -w dist/ --no-deps
```

Use `--no-deps` to keep the wheel small and avoid conflicts with Fabric's pre-installed runtime packages.

### Deploy to a Fabric Environment

1. **Upload the `.whl`** (requires `curl` — the `fab` CLI only accepts JSON):

   ```bash
   TOKEN=$(az account get-access-token \
     --resource "https://analysis.windows.net/powerbi/api" \
     --query accessToken -o tsv)

   curl -X POST \
     "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/environments/$ENV_ID/staging/libraries/my_package-0.1.0-py3-none-any.whl?beta=false" \
     -H "Authorization: Bearer $TOKEN" \
     -H "Content-Type: application/octet-stream" \
     --data-binary @dist/my_package-0.1.0-py3-none-any.whl
   ```

2. **Publish the environment** (makes staged libraries effective):

   ```bash
   fab api -X post "workspaces/$WS_ID/environments/$ENV_ID/staging/publish?beta=false" --show_headers
   ```

   This is a long-running operation (2+ minutes). Poll `operations/{opId}` until `Succeeded`.

3. **Attach the environment to the SJD** by setting `environmentArtifactId` in the SJD config:

   ```json
   {
       "executableFile": "main.py",
       "defaultLakehouseArtifactId": "<lakehouse_guid>",
       "environmentArtifactId": "<environment_guid>",
       "additionalLibraryUris": [],
       "language": "Python"
   }
   ```

### What doesn't work

- Putting `.whl` paths in `additionalLibraryUris` — API rejects them for Python SJDs.
- Inlining `.whl` in the V2 deploy payload `Libs/` part — doesn't work reliably.
- `additionalLibraryUris` must always be `[]` for Python SJDs.

### Shared across items

Multiple SJDs and notebooks can reference the same Environment GUID. Updating the library means re-uploading and re-publishing the environment once — not redeploying every consumer.

---

## 6. Spark Job Definition Deployment

### Two-step approach (the only reliable method)

Single-step creation (POST with `displayName` + `type` + `definition` at once) silently drops the definition. Always use two steps:

```bash
# Step 1 — Create the empty SJD item
fab mkdir "<workspace>.Workspace/<name>.SparkJobDefinition"

# Step 2 — Deploy the code + config via updateDefinition
fab api -X post "workspaces/$WS_ID/items/$SJD_ID/updateDefinition" \
  -i @payload.json --show_headers
```

### V2 payload structure

The payload to `updateDefinition` has exactly two parts — no `Libs/` section:

```json
{
    "definition": {
        "format": "SparkJobDefinitionV2",
        "parts": [
            {
                "path": "SparkJobDefinitionV1.json",
                "payload": "<base64-encoded config JSON>",
                "payloadType": "InlineBase64"
            },
            {
                "path": "Main/main.py",
                "payload": "<base64-encoded Python file>",
                "payloadType": "InlineBase64"
            }
        ]
    }
}
```

- `executableFile` in the config is a bare filename (`"main.py"`), not a path.
- The V2 part path uses `Main/main.py`, but the config field is just `main.py`.
- HTTP 202 = async. Capture `x-ms-operation-id` from headers and poll until completion.

---

## 7. What NOT to Use

- **`mssparkutils` / `notebookutils`** — Not available in Spark Job Definitions. Only use standard PySpark and Python stdlib.
- **Fabric-only APIs in business logic** — Keep the core code portable. If you need Fabric-specific calls, isolate them behind `is_local_dev()` checks.
- **Hardcoded paths or credentials** — Use environment variables for anything that differs between local and Fabric.

---

## Summary

| Concern | Local Dev | Fabric | Bridge |
|---|---|---|---|
| Detection | `LOCAL_DEV=1` | `LOCAL_DEV` not set | `is_local_dev()` helper |
| File paths | `lakehouse/Files/...`, `lakehouse/Tables/...` | `Files/...`, `Tables/...` | `LAKEHOUSE_ROOT` env var |
| Spark + Delta | `local[*]` + `configure_spark_with_delta_pip()` | Managed cluster, Delta built-in | Conditional setup in session init |
| Auth | `az login` → `DefaultAzureCredential` | Managed Identity → `DefaultAzureCredential` | Same code, zero changes |
| SQL connectivity | JDBC + `DefaultAzureCredential` token | `.mssql()` connector + workspace Entra identity | `is_local_dev()` branch in public `read_*()` |
| Config values | Env vars (`.env`, devcontainer config) | Env vars (Fabric item settings) | `os.environ.get(...)` |
| Custom libraries | `pip install -e .` | `.whl` via Fabric Environments | Build `--no-deps`, upload, publish |
| Forbidden APIs | N/A | `mssparkutils`, `notebookutils` | Don't use them in SJD code |
