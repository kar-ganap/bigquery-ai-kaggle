#!/usr/bin/env python3
"""
Enhanced ad text extraction with all meaningful text fields
Based on our structural analysis of Meta Ad Library responses
"""

def extract_enhanced_ad_text(ad):
    """
    Extract all meaningful text fields from a Meta Ad Library ad
    Returns raw fields only - all processing logic moved to BigQuery
    
    Args:
        ad: Raw ad object from Meta Ad Library API
        
    Returns:
        dict: Raw text fields for BigQuery processing
    """
    
    snapshot = ad.get('snapshot', {})
    
    # Extract raw fields only - let BigQuery do the processing
    title = snapshot.get('title') or ''
    body_text = (snapshot.get('body', {}) or {}).get('text', '')
    cta_text = snapshot.get('cta_text') or ''
    page_name = ad.get('page_name') or snapshot.get('page_name') or ''
    
    # Page category (first one if multiple)
    page_categories = snapshot.get('page_categories', [])
    page_category = page_categories[0] if page_categories else ''
    
    # CTA type
    cta_type = snapshot.get('cta_type') or ''
    
    return {
        # Raw fields only - BigQuery handles all processing
        'page_name': page_name,
        'page_category': page_category,
        'title': title,
        'body_text': body_text,
        'cta_text': cta_text,
        'cta_type': cta_type,
        'ad_archive_id': ad.get('ad_archive_id', ''),
        'publisher_platforms': ad.get('publisher_platform', [])
    }

# NOTE: Text processing functions moved to BigQuery SQL
# All content structuring and quality scoring now happens in SQL
# This keeps Python focused on data extraction only

def test_enhanced_extraction():
    """Test the simplified extraction with sample data"""
    
    # Sample ad structure based on our analysis
    sample_ad = {
        'ad_archive_id': '784934174032998',
        'page_name': 'PayPal',
        'publisher_platform': ['facebook', 'instagram'],
        'snapshot': {
            'title': 'Search like a Pro on us',
            'body': {'text': 'Get a year of Perplexity Pro free ($200 value) & early access to their AI-powered browser Comet if you subscribe with PayPal.'},
            'cta_text': 'Download',
            'cta_type': 'DOWNLOAD',
            'page_name': 'PayPal',
            'page_categories': ['Financial Service']
        }
    }
    
    print("🧪 TESTING SIMPLIFIED AD TEXT EXTRACTION")
    print("=" * 60)
    
    result = extract_enhanced_ad_text(sample_ad)
    
    print("📊 RAW FIELD EXTRACTION:")
    print("-" * 40)
    for key, value in result.items():
        print(f"{key:20}: {value}")
    
    print(f"\n✅ SUCCESS: Raw fields extracted")
    print("💡 All text processing now happens in BigQuery SQL")
    
    return result

if __name__ == "__main__":
    test_enhanced_extraction()