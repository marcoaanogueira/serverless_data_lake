#!/bin/bash
# destroy.sh — Clean up all AWS resources before cdk destroy
#
# Usage:
#   ./scripts/destroy.sh
#
# What it does:
#   1. Deletes all Firehose delivery streams
#   2. Deletes all Glue databases and tables
#   3. Empties S3 buckets (bronze, silver, gold, artifacts)
#   4. Runs cdk destroy

set -e

TENANT=$(python3 -c "import json; print(json.load(open('cdk.json'))['context']['tenant'])" 2>/dev/null || echo "decolares")

echo "==> Deleting Firehose streams..."
aws firehose list-delivery-streams --output json | python3 -c "
import sys, json, subprocess
streams = json.load(sys.stdin)['DeliveryStreamNames']
if not streams:
    print('    None found')
for s in streams:
    print(f'    {s}')
    subprocess.run(['aws','firehose','delete-delivery-stream','--delivery-stream-name', s],
                   capture_output=True, check=True)
"

echo ""
echo "==> Deleting Glue databases and tables..."
aws glue get-databases --output json 2>/dev/null | python3 -c "
import sys, json, subprocess
dbs = json.load(sys.stdin).get('DatabaseList', [])
if not dbs:
    print('    None found')
for db in dbs:
    name = db['Name']
    tables = json.loads(subprocess.check_output(
        ['aws','glue','get-tables','--database-name', name,'--query','TableList[*].Name','--output','json']
    ))
    for t in tables:
        print(f'    table: {name}.{t}')
        subprocess.run(['aws','glue','delete-table','--database-name', name,'--name', t], capture_output=True)
    print(f'    database: {name}')
    subprocess.run(['aws','glue','delete-database','--name', name], capture_output=True)
" 2>/dev/null

echo ""
echo "==> Deleting S3 buckets..."
for bucket in bronze silver gold artifacts; do
    BUCKET_NAME="${TENANT}-${bucket}"
    if aws s3api head-bucket --bucket "$BUCKET_NAME" 2>/dev/null; then
        echo "    s3://$BUCKET_NAME"
        aws s3 rb "s3://$BUCKET_NAME" --force --quiet
    fi
done

echo ""
echo "==> Running cdk destroy..."
cdk destroy --force

echo ""
echo "✅ Done!"
