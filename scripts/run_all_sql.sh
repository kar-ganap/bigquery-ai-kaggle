#!/usr/bin/env bash
set -euo pipefail

: "${BQ_PROJECT:?Set BQ_PROJECT}"
: "${BQ_DATASET:=ads_demo}"

replace() {
  # naive token replacement for yourproj and ads_demo
  sed -e "s/yourproj/$BQ_PROJECT/g" -e "s/ads_demo/$BQ_DATASET/g" "$1"
}

echo "Creating schema/table ..."
replace sql/00_schema.sql | bq query --project_id="$BQ_PROJECT" --use_legacy_sql=false

echo "Labeling ads ..."
replace sql/02_label_ads.sql | bq query --project_id="$BQ_PROJECT" --use_legacy_sql=false

echo "Building rollups ..."
replace sql/05_rollups.sql | bq query --project_id="$BQ_PROJECT" --use_legacy_sql=false

echo "All SQL done."
