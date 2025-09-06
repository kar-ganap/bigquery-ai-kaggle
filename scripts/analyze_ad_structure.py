#!/usr/bin/env python3
"""
Analyze the complete structure of a Meta Ad Library response
to identify all available text fields for embeddings
"""

import os
import sys
import json
from pprint import pprint

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Add utils to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
from ads_fetcher import MetaAdsFetcher

def analyze_single_ad_structure():
    """Fetch one ad and analyze its complete structure"""
    
    print("🔍 ANALYZING META AD LIBRARY RESPONSE STRUCTURE")
    print("=" * 70)
    
    fetcher = MetaAdsFetcher()
    
    # Get one PayPal ad for detailed analysis
    print("📱 Fetching one PayPal ad for structural analysis...")
    
    ads_collected = []
    for ad_or_result in fetcher.fetch_company_ads_paginated(
        company_name="PayPal",
        max_ads=1,  # Just get one ad
        max_pages=1,
        delay_between_requests=0.1
    ):
        if hasattr(ad_or_result, 'success'):  # Final result
            result = ad_or_result
            break
        else:
            ads_collected.append(ad_or_result)
    
    if not ads_collected:
        print("❌ No ads retrieved")
        return
    
    ad = ads_collected[0]
    ad_id = ad.get('ad_archive_id', 'UNKNOWN')
    
    print(f"\n📋 ANALYZING AD: {ad_id}")
    print("=" * 70)
    
    # Function to extract all text fields recursively
    def extract_text_fields(obj, path="", text_fields=None):
        if text_fields is None:
            text_fields = {}
        
        if isinstance(obj, dict):
            for key, value in obj.items():
                new_path = f"{path}.{key}" if path else key
                if isinstance(value, str) and len(value.strip()) > 0:
                    # This is a text field with content
                    text_fields[new_path] = value
                elif isinstance(value, (dict, list)):
                    extract_text_fields(value, new_path, text_fields)
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                new_path = f"{path}[{i}]"
                if isinstance(item, str) and len(item.strip()) > 0:
                    text_fields[new_path] = item
                elif isinstance(item, (dict, list)):
                    extract_text_fields(item, new_path, text_fields)
        
        return text_fields
    
    # Extract all text fields
    all_text_fields = extract_text_fields(ad)
    
    print(f"📝 FOUND {len(all_text_fields)} TEXT FIELDS:")
    print("-" * 70)
    
    # Categorize and display text fields
    meaningful_fields = {}
    metadata_fields = {}
    
    for path, content in all_text_fields.items():
        # Classify as meaningful creative content vs metadata
        lower_path = path.lower()
        lower_content = content.lower()
        
        # Skip obvious metadata/technical fields
        if any(skip in lower_path for skip in ['id', 'url', 'date', 'time', 'status', 'currency', 'region']):
            metadata_fields[path] = content
        elif any(skip in lower_content for skip in ['http', 'www.', '.com', 'id:', 'status:']):
            metadata_fields[path] = content
        elif len(content) < 5:  # Very short strings likely metadata
            metadata_fields[path] = content
        else:
            meaningful_fields[path] = content
    
    print("🎯 MEANINGFUL CREATIVE TEXT FIELDS:")
    print("-" * 50)
    for i, (path, content) in enumerate(meaningful_fields.items(), 1):
        preview = content[:100] + "..." if len(content) > 100 else content
        print(f"{i:2d}. {path}")
        print(f"    Content: {preview}")
        print(f"    Length: {len(content)} chars")
        print()
    
    print("🔧 METADATA/TECHNICAL FIELDS:")
    print("-" * 50)
    for i, (path, content) in enumerate(metadata_fields.items(), 1):
        preview = content[:50] + "..." if len(content) > 50 else content
        print(f"{i:2d}. {path}: {preview}")
    
    # Recommendations for embedding enhancement
    print("\n💡 RECOMMENDATIONS FOR EMBEDDING ENHANCEMENT:")
    print("=" * 70)
    
    current_fields = ['snapshot.title', 'snapshot.body.text']
    available_fields = list(meaningful_fields.keys())
    new_fields = [f for f in available_fields if f not in current_fields]
    
    if new_fields:
        print("✅ ADDITIONAL FIELDS TO CONSIDER:")
        for field in new_fields[:5]:  # Show top 5
            content = meaningful_fields[field]
            print(f"   - {field}: {len(content)} chars")
            print(f"     Preview: {content[:80]}...")
            print()
    else:
        print("ℹ️  We're already using the main text fields available")
    
    # Show complete JSON structure (truncated for readability)
    print("\n🔍 COMPLETE AD STRUCTURE (truncated):")
    print("=" * 70)
    
    # Create a clean version for display
    clean_ad = {}
    for key, value in ad.items():
        if isinstance(value, str) and len(value) > 100:
            clean_ad[key] = value[:100] + "... (truncated)"
        elif isinstance(value, dict):
            clean_ad[key] = {k: (str(v)[:50] + "..." if isinstance(v, str) and len(str(v)) > 50 else v) for k, v in value.items()}
        else:
            clean_ad[key] = value
    
    pprint(clean_ad, width=100, depth=3)
    
    # Save full structure to file for detailed analysis
    with open('data/temp/sample_ad_structure.json', 'w') as f:
        json.dump(ad, f, indent=2, default=str)
    
    print(f"\n💾 Full ad structure saved to: data/temp/sample_ad_structure.json")
    
    return {
        'ad_id': ad_id,
        'total_text_fields': len(all_text_fields),
        'meaningful_fields': len(meaningful_fields),
        'metadata_fields': len(metadata_fields),
        'meaningful_field_names': list(meaningful_fields.keys()),
        'current_usage': current_fields,
        'recommended_additions': new_fields[:5]
    }

if __name__ == "__main__":
    analyze_single_ad_structure()