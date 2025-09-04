#!/usr/bin/env python3
"""
Test script for competitor discovery functionality
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.search_utils import extract_company_names, score_search_result

def test_company_name_extraction():
    """Test company name extraction patterns"""
    print("🧪 Testing company name extraction...")
    
    test_cases = [
        "Top 10 Nike competitors: Adidas, Under Armour, Puma, and New Balance dominate athletic footwear",
        "Best alternatives to Stripe include PayPal, Square, and Braintree for payment processing",
        "Nike vs Adidas: Which athletic brand is better?",
        "Companies like Apple: Microsoft, Google, Amazon are major tech competitors",
        "Leading SaaS platforms: Salesforce, HubSpot, Zendesk Inc., and ServiceNow Corp.",
    ]
    
    for i, text in enumerate(test_cases, 1):
        companies = extract_company_names(text)
        print(f"\nTest {i}:")
        print(f"Input: {text[:80]}...")
        print(f"Found: {companies}")
    
def test_search_result_scoring():
    """Test search result relevance scoring"""
    print("\n🧪 Testing search result scoring...")
    
    test_results = [
        {
            "title": "Nike vs Adidas: Complete Comparison 2024",
            "snippet": "Compare Nike and Adidas athletic brands including performance, pricing, and market share",
            "url": "https://g2.com/compare/nike-vs-adidas",
            "query": "Nike competitors"
        },
        {
            "title": "Top 10 Athletic Apparel Companies",
            "snippet": "Leading sportswear brands including Nike, Adidas, Under Armour, Puma ranking by revenue",
            "url": "https://example-blog.com/top-athletic-brands",
            "query": "athletic apparel competitors"
        },
        {
            "title": "Nike Stock Price Today - Latest News",
            "snippet": "Nike (NKE) stock trading information and financial reports",
            "url": "https://finance.yahoo.com/quote/NKE",
            "query": "Nike competitors"
        }
    ]
    
    for i, result in enumerate(test_results, 1):
        score = score_search_result(
            result["title"], 
            result["snippet"], 
            result["url"], 
            result["query"]
        )
        print(f"\nResult {i} (Score: {score:.2f}):")
        print(f"Title: {result['title']}")
        print(f"URL: {result['url']}")

def test_discovery_logic():
    """Test discovery logic without API calls"""
    print("\n🧪 Testing discovery logic...")
    
    # Mock search results
    mock_results = [
        {
            "title": "Nike vs Adidas vs Under Armour: Athletic Brand Comparison",
            "snippet": "Compare the top athletic apparel companies Nike, Adidas, Under Armour, and Puma",
            "link": "https://g2.com/compare/athletic-brands",
            "query_context": "Nike competitors"
        },
        {
            "title": "Top 10 Sportswear Brands 2024 - Complete List",  
            "snippet": "Leading athletic companies: 1. Nike 2. Adidas 3. Puma 4. Under Armour 5. New Balance",
            "link": "https://example.com/top-sportswear",
            "query_context": "top athletic apparel brands"
        }
    ]
    
    print("Mock search results:")
    for result in mock_results:
        companies = extract_company_names(f"{result['title']} {result['snippet']}")
        score = score_search_result(
            result["title"], 
            result["snippet"], 
            result["link"], 
            result["query_context"]
        )
        print(f"\nQuery: {result['query_context']}")
        print(f"Found companies: {companies}")
        print(f"Relevance score: {score:.2f}")

if __name__ == "__main__":
    print("🔍 Testing Competitor Discovery Components")
    print("=" * 50)
    
    test_company_name_extraction()
    test_search_result_scoring()  
    test_discovery_logic()
    
    print(f"\n✅ All tests completed!")
    print("\nNext steps:")
    print("1. Set up Google Custom Search Engine API")
    print("2. Add API keys to .env file")
    print("3. Test with real brand: python discover_competitors_v2.py --brand Nike")