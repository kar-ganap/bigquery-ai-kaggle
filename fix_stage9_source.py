#!/usr/bin/env python3
"""
Fix Stage 9 MultiDimensional Intelligence to use ads_with_dates as source of truth
"""

import json

# Load the multidimensional intelligence file
file_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/src/pipeline/stages/multidimensional_intelligence.py"

with open(file_path, 'r') as f:
    content = f.read()

print("🔧 FIXING STAGE 9 SOURCE OF TRUTH")
print("=" * 50)

# Count current occurrences
ads_raw_count = content.count('ads_raw_{run_id}')
ads_with_dates_count = content.count('ads_with_dates')

print(f"📊 Current state:")
print(f"   ads_raw_{{run_id}} references: {ads_raw_count}")
print(f"   ads_with_dates references: {ads_with_dates_count}")

# Replace all ads_raw_{run_id} references with ads_with_dates
fixed_content = content.replace(
    'ads_raw_{run_id}',
    'ads_with_dates'
)

# Also update the comment
fixed_content = fixed_content.replace(
    '# Use pipeline\'s run_id to access ads_raw_{run_id} table from Stage 4',
    '# Use ads_with_dates as source of truth (consistent with Stage 8)'
)

# Count after fix
new_ads_raw_count = fixed_content.count('ads_raw_{run_id}')
new_ads_with_dates_count = fixed_content.count('ads_with_dates')

print(f"\\n✅ After fix:")
print(f"   ads_raw_{{run_id}} references: {new_ads_raw_count}")
print(f"   ads_with_dates references: {new_ads_with_dates_count}")

# Save the fixed file
with open(file_path, 'w') as f:
    f.write(fixed_content)

print("\\n🎯 STAGE 9 NOW USES ads_with_dates AS SOURCE OF TRUTH!")
print("📊 This ensures consistency with Stage 8 Analysis")
print("✅ All intelligence modules will now use the same data source")