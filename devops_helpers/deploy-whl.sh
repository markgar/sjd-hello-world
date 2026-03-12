#!/usr/bin/env bash
# deploy-whl.sh — Build the wheel and upload to lakehouse Files/libs/ via OneLake DFS API.
# Usage: ./deploy-whl.sh
set -euo pipefail

WS_ID="${WS_ID:?Set WS_ID (workspace)}"
LH_ID="${LH_ID:?Set LH_ID (lakehouse)}"

WHL_NAME="hello_pyspark_local_dev-0.1.0-py3-none-any.whl"
WHL_FILE="dist/${WHL_NAME}"

# --- Build ---
echo ">>> Building wheel..."
python -m build --wheel --quiet 2>&1
echo "    Built ${WHL_FILE} ($(wc -c < "$WHL_FILE") bytes)"

# --- Upload via OneLake DFS ---
ONELAKE_URL="https://onelake.dfs.fabric.microsoft.com/${WS_ID}/${LH_ID}/Files/libs/${WHL_NAME}"
TOKEN=$(az account get-access-token --resource "https://storage.azure.com/" --query accessToken -o tsv)
FILE_SIZE=$(wc -c < "$WHL_FILE")

echo ">>> Uploading to OneLake: Files/libs/${WHL_NAME}"

# Step 1: Create (or overwrite) file resource
HTTP_CREATE=$(curl -sS -o /dev/null -w "%{http_code}" -X PUT \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Length: 0" \
  "${ONELAKE_URL}?resource=file")
echo "    CREATE: HTTP ${HTTP_CREATE}"

# Step 2: Append file content
HTTP_APPEND=$(curl -sS -o /dev/null -w "%{http_code}" -X PATCH \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/octet-stream" \
  --data-binary @"$WHL_FILE" \
  "${ONELAKE_URL}?action=append&position=0")
echo "    APPEND: HTTP ${HTTP_APPEND}"

# Step 3: Flush (commit)
HTTP_FLUSH=$(curl -sS -o /dev/null -w "%{http_code}" -X PATCH \
  -H "Authorization: Bearer $TOKEN" \
  "${ONELAKE_URL}?action=flush&position=${FILE_SIZE}")
echo "    FLUSH:  HTTP ${HTTP_FLUSH}"

if [[ "$HTTP_FLUSH" == "200" ]]; then
  echo ">>> Done — wheel uploaded to lakehouse Files/libs/"
else
  echo ">>> FAILED — check HTTP responses above"
  exit 1
fi
