#!/usr/bin/env python3
"""
Script to check for capitalization inconsistencies in the funnel column
of the ads_with_dates table in BigQuery.
"""

import sys
import os

# Add src directory to path so we can import the bigquery_client module
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from utils.bigquery_client import run_query

def check_funnel_capitalization():
    """Check for capitalization inconsistencies in funnel column"""

    query = """
    SELECT
        funnel,
        COUNT(*) as count
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
    WHERE funnel IS NOT NULL
    GROUP BY funnel
    ORDER BY count DESC
    """

    print("Running BigQuery query to check funnel capitalization...")
    print("Query:")
    print(query)
    print("\n" + "="*60 + "\n")

    try:
        # Run the query using the existing bigquery_client module
        results = run_query(query, project_id="bigquery-ai-kaggle-469620")

        print("Results:")
        print(results.to_string(index=False))

        # Analyze the results for capitalization issues
        print("\n" + "="*60)
        print("ANALYSIS:")
        print("="*60)

        funnel_values = results['funnel'].tolist()

        # Check for Upper/upper inconsistency
        upper_variants = [f for f in funnel_values if f.lower() == 'upper']
        if len(upper_variants) > 1:
            print(f"⚠️  CAPITALIZATION ISSUE FOUND: Upper funnel variants: {upper_variants}")
        else:
            print(f"✅ Upper funnel: {upper_variants}")

        # Check for Mid/mid inconsistency
        mid_variants = [f for f in funnel_values if f.lower() == 'mid']
        if len(mid_variants) > 1:
            print(f"⚠️  CAPITALIZATION ISSUE FOUND: Mid funnel variants: {mid_variants}")
        else:
            print(f"✅ Mid funnel: {mid_variants}")

        # Check for Lower/lower inconsistency
        lower_variants = [f for f in funnel_values if f.lower() == 'lower']
        if len(lower_variants) > 1:
            print(f"⚠️  CAPITALIZATION ISSUE FOUND: Lower funnel variants: {lower_variants}")
        else:
            print(f"✅ Lower funnel: {lower_variants}")

        # Check for any unexpected values
        expected_values = {'upper', 'mid', 'lower'}
        unexpected = [f for f in funnel_values if f.lower() not in expected_values]
        if unexpected:
            print(f"⚠️  UNEXPECTED FUNNEL VALUES: {unexpected}")
        else:
            print("✅ No unexpected funnel values found")

        print(f"\nTotal distinct funnel values: {len(funnel_values)}")
        print(f"Total records with non-null funnel: {results['count'].sum()}")

    except Exception as e:
        print(f"Error running query: {e}")
        return False

    return True

if __name__ == "__main__":
    success = check_funnel_capitalization()
    sys.exit(0 if success else 1)