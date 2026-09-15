#!/bin/bash
set -euo pipefail
mkdir -p /opt/hive/data
if ! /opt/hive/bin/schematool -dbType derby -info >/dev/null 2>&1; then
  /opt/hive/bin/schematool -dbType derby -initSchema
fi
exec /opt/hive/bin/hive --service metastore
