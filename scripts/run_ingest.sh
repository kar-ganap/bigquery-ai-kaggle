#!/usr/bin/env bash
set -euo pipefail

: "${SC_API_KEY:?Set SC_API_KEY}"
: "${BQ_PROJECT:?Set BQ_PROJECT}"
: "${BQ_DATASET:=ads_demo}"

if [ $# -lt 1 ]; then
  echo "Usage: $0 <page_id1> [page_id2 ...]"
  exit 1
fi

for pid in "$@"; do
  echo "Ingesting page_id=$pid ..."
  python scripts/ingest_fb_ads.py --page_id "$pid"
done
