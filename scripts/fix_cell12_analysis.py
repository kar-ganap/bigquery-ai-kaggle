#!/usr/bin/env python3
"""Fix Cell 12 to analyze in-memory data instead of non-existent BigQuery table"""

import json

notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

# Load the notebook
with open(notebook_path, 'r') as f:
    nb = json.load(f)

# Find Cell 12 and replace its content
if len(nb['cells']) > 12:
    cell = nb['cells'][12]

    # New content that analyzes the in-memory data
    new_content = '''# Examine Stage 1 Discovery Results (In-Memory Analysis)
print("📊 STAGE 1 DISCOVERY ANALYSIS")
print("=" * 40)

if 'competitors_discovered' in locals() and competitors_discovered:
    print(f"✅ Discovery Stage Completed Successfully")
    print(f"📊 Analysis Results:")

    # Calculate statistics
    total_candidates = len(competitors_discovered)
    unique_companies = len(set(c.company_name for c in competitors_discovered))
    unique_sources = len(set(c.source_url for c in competitors_discovered))
    unique_queries = len(set(c.query_used for c in competitors_discovered))

    scores = [c.raw_score for c in competitors_discovered]
    avg_score = sum(scores) / len(scores)
    min_score = min(scores)
    max_score = max(scores)

    print(f"   Total Candidates: {total_candidates:,}")
    print(f"   Unique Companies: {unique_companies:,}")
    print(f"   Unique Sources: {unique_sources:,}")
    print(f"   Unique Queries: {unique_queries:,}")
    print(f"   Score Range: {min_score:.3f} - {max_score:.3f}")
    print(f"   Average Score: {avg_score:.3f}")

    # Source distribution analysis
    print(f"\\n📋 Source Distribution:")
    source_counts = {}
    for candidate in competitors_discovered:
        domain = candidate.source_url.split('/')[2] if '//' in candidate.source_url else 'unknown'
        source_counts[domain] = source_counts.get(domain, 0) + 1

    # Show top 5 sources
    top_sources = sorted(source_counts.items(), key=lambda x: x[1], reverse=True)[:5]
    for domain, count in top_sources:
        print(f"   • {domain}: {count} candidates")

    # Query effectiveness analysis
    print(f"\\n🔍 Query Effectiveness:")
    query_counts = {}
    for candidate in competitors_discovered:
        query = candidate.query_used[:50] + "..." if len(candidate.query_used) > 50 else candidate.query_used
        query_counts[query] = query_counts.get(query, 0) + 1

    top_queries = sorted(query_counts.items(), key=lambda x: x[1], reverse=True)[:3]
    for query, count in top_queries:
        print(f"   • '{query}': {count} results")

    print(f"\\n💡 Stage 1 Discovery completed successfully!")
    print(f"   Ready to proceed to Stage 2 (AI Curation)")
    print(f"   Note: BigQuery table will be created in Stage 2 (Curation)")

else:
    print("❌ No discovery results found")
    print("   Make sure you ran Cell 10 (Stage 1 Discovery) first")
    print("   Check the output above for any errors")'''

    # Split into lines and add newlines
    lines = new_content.split('\n')
    cell['source'] = [line + '\n' for line in lines[:-1]] + [lines[-1]]

# Save the fixed notebook
with open(notebook_path, 'w') as f:
    json.dump(nb, f, indent=2)

print(f"✅ Fixed Cell 12: {notebook_path}")
print("   - Now analyzes in-memory discovery results instead of BigQuery")
print("   - Provides comprehensive Stage 1 analysis")
print("   - Explains that BigQuery table is created in Stage 2")
print("\\n📋 Cell 12 now works correctly after Stage 1 execution")