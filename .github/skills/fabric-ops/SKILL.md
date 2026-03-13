---
description: "Deploy SJD, Spark Job Definition, updateDefinition, whl upload, Fabric Environment, publish, LRO polling, Livy logs, fab CLI, curl, fabric_ops.py, run monitor logs status"
---

# Fabric Ops — Deployment, Monitoring & Debugging

Everything needed to deploy a Python package as a Fabric Spark Job Definition, run it, and retrieve logs. Organized by phase: **Build → Upload → Publish → Deploy SJD → Run → Check Logs**.

> **Rule:** Before running any Fabric API call, check `devops_helpers/fabric_ops.py` first. If a helper already exists for the operation, use it — don't duplicate it with raw curl. If no helper exists (phases 2–4 below), the commands shown here are ready to use.

---

## The `devops_helpers/fabric_ops.py` CLI

Covers **run, monitor, and log retrieval** (phases 5–6). Always use these instead of hitting the job-instance APIs directly.

| Command | What it does |
|---|---|
| `python devops_helpers/fabric_ops.py run --wait` | Submit SJD run, poll until complete, auto-show failure details |
| `python devops_helpers/fabric_ops.py logs` | Fetch driver stdout/stderr for latest run |
| `python devops_helpers/fabric_ops.py status` | Check latest run status |
| `python devops_helpers/fabric_ops.py livy` | Show Livy session details |
| `python devops_helpers/fabric_ops.py runs` | List recent runs |

Requires env vars: `WS_ID` (workspace ID), `SJD_ID` (Spark Job Definition item ID).

---

## Phase 1: Build the `.whl`

```bash
pip wheel . -w dist/ --no-deps
```

- `--no-deps` keeps the wheel small and avoids conflicts with Fabric's pre-installed runtime packages.
- Built from `pyproject.toml`.

---

## Phase 2: Upload to Fabric Environment

Uploads go to **staging** — they have no effect on running jobs until published.

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

### Gotchas

- **`fab api` can't upload binaries** — it only accepts JSON input. Use `curl` with a bearer token.
- **Deleting a library also requires `curl`** — the DELETE endpoint doesn't work through `fab api` either. Publish again after deletion.
- **Max 100 MB per `.whl` file.**
- **Use `?beta=false`** on all Environment staging/library/publish endpoints. Without it, the deprecated preview contract is used.

---

## Phase 3: Publish the Environment

Publishing makes staged libraries effective. Fabric pre-installs the library during publish (equivalent to `pip install`).

```bash
fab api -X post "workspaces/$WS_ID/environments/$ENV_ID/staging/publish?beta=false" --show_headers
```

### Gotchas

- **Publish takes 2+ minutes.** The API suggests `Retry-After: 120`. Poll at 30-second intervals.
- **Only one publish at a time.** Check environment status first — a concurrent publish will fail.
- **This is an LRO** — capture the operation ID and poll (see LRO Polling below).

### Shared across items

Multiple SJDs and notebooks can reference the same Environment GUID. Update the library once — not per-SJD.

---

## Phase 4: Deploy the Spark Job Definition

### Two-step approach (the only reliable method)

Single-step creation (POST with `displayName` + `type` + `definition` at once) returns HTTP 202 but the definition is **never applied**. Always use two steps:

```bash
# Step 1 — Create the empty SJD item
fab mkdir "<workspace>.Workspace/<name>.SparkJobDefinition"

# Step 2 — Deploy the code + config
fab api -X post "workspaces/$WS_ID/items/$SJD_ID/updateDefinition" \
  -i @payload.json --show_headers
```

### V2 payload structure

Exactly two parts — no `Libs/` section:

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

### SJD config (`SparkJobDefinitionV1.json`)

```json
{
    "executableFile": "main.py",
    "defaultLakehouseArtifactId": "<lakehouse_guid>",
    "environmentArtifactId": "<environment_guid>",
    "additionalLibraryUris": [],
    "language": "Python"
}
```

### Gotchas

- **`executableFile` is a bare filename** — `"main.py"`, not a path. The V2 part path uses `Main/main.py`, but the config field is just `main.py`.
- **`additionalLibraryUris` must be `[]`** — for Python SJDs, any `.whl` entry will be rejected by the API.
- **HTTP 202 = async** — capture `x-ms-operation-id` and poll until completion.

### What doesn't work for custom libraries

- `.whl` paths in `additionalLibraryUris` — API rejects them for Python SJDs.
- `.whl` inlined as a `Libs/` part in the V2 payload — doesn't work reliably.
- `.whl` uploaded to OneLake and referenced — not supported.
- The **only** working path: `.whl` → Fabric Environment → attach via `environmentArtifactId`.

---

## Phase 5: Run on Fabric

```bash
python devops_helpers/fabric_ops.py run --wait
```

Submits the job, polls until terminal state, and auto-shows failure details (Livy session + driver logs) if it fails.

---

## Phase 6: Check Logs & Debug

```bash
python devops_helpers/fabric_ops.py logs           # driver stdout + stderr
python devops_helpers/fabric_ops.py status          # run status + failure reason
python devops_helpers/fabric_ops.py livy            # Livy session details
python devops_helpers/fabric_ops.py runs            # list recent runs
```

The helper auto-discovers the latest run's Livy session and Spark application ID. On failure, `run --wait` automatically fetches and displays Livy + driver log details.

---

## LRO Polling

Every `fab api` 202 response is a long-running operation. You **must** capture `x-ms-operation-id` and poll.

```bash
RESPONSE=$(fab api -X post "<endpoint>" -i @payload.json --show_headers 2>&1)
OP_ID=$(echo "$RESPONSE" | grep -i "x-ms-operation-id" | awk '{print $2}' | tr -d '\r')

while true; do
  STATUS=$(fab api "operations/$OP_ID" -q status 2>&1 | tr -d '"')
  echo "Status: $STATUS"
  [[ "$STATUS" == "Running" || "$STATUS" == "NotStarted" || "$STATUS" == "Waiting" ]] || break
  sleep 5
done
```

- `fab import` handles LRO polling internally; `fab api` does **not**.
- Skipping LRO polling means you won't know if the deploy actually worked.

---

## fab CLI Gotchas

- **Always use `-f`** to skip prompts. The CLI runs non-interactively — there is no way to send keystrokes.
