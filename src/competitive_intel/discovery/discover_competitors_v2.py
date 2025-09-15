#!/usr/bin/env python3
"""
Enhanced competitor discovery using Google Custom Search Engine API
with adaptive strategies for different brand types and footprints.
"""

import os
import argparse
import csv
import re
import time
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict

import requests
from googleapiclient.discovery import build
from bs4 import BeautifulSoup
import pandas as pd

try:
    # Import from src.utils for modern pipeline
    from src.utils.bigquery_client import get_bigquery_client, load_dataframe_to_bq
    from src.utils.search_utils import extract_company_names, score_search_result, dedupe_companies
except ImportError as e:
    print(f"Warning: Could not import utils modules: {e}")
    get_bigquery_client = None
    load_dataframe_to_bq = None
    extract_company_names = None
    score_search_result = None
    dedupe_companies = None

# Environment configuration
GOOGLE_CSE_API_KEY = os.environ.get("GOOGLE_CSE_API_KEY")
GOOGLE_CSE_CX = os.environ.get("GOOGLE_CSE_CX") 
BQ_PROJECT = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
BQ_DATASET = os.environ.get("BQ_DATASET", "ads_demo")

@dataclass
class CompetitorCandidate:
    """Structured competitor candidate with metadata"""
    company_name: str
    source_url: str
    source_title: str
    query_used: str
    raw_score: float
    found_in: str  # 'title' or 'snippet'
    discovery_method: str  # 'standard' or 'fallback'

class CompetitorDiscovery:
    """Enhanced competitor discovery with adaptive strategies"""
    
    def __init__(self):
        if not GOOGLE_CSE_API_KEY or not GOOGLE_CSE_CX:
            raise ValueError("Missing required environment variables: GOOGLE_CSE_API_KEY, GOOGLE_CSE_CX")
        
        self.cse_service = build("customsearch", "v1", developerKey=GOOGLE_CSE_API_KEY)
        self.results_cache = {}
        
    def generate_standard_queries(self, brand: str, vertical: str = None) -> List[str]:
        """Generate comprehensive competitor search queries"""
        queries = [
            # Direct competitor queries
            f"{brand} competitors",
            f"{brand} alternatives", 
            f"companies like {brand}",
            f"{brand} vs",
            f"alternatives to {brand}",
            f"{brand} competitor analysis",
        ]
        
        if vertical:
            # Category-based discovery
            queries.extend([
                f"top {vertical} brands",
                f"best {vertical} companies",
                f"{vertical} market leaders",
                f"leading {vertical} businesses",
                f"{vertical} competitive landscape",
                f"{brand} {vertical} competitors"
            ])
        
        return queries
    
    def generate_fallback_queries(self, brand: str, vertical: str = None) -> List[str]:
        """Fallback queries for brands with low online footprint"""
        queries = [
            f'"{brand}" industry',
            f'"{brand}" business type',
            f'what does {brand} do',
            f'{brand} company profile'
        ]
        
        if vertical:
            # Ecosystem-based discovery when brand-specific fails
            queries.extend([
                f'small {vertical} companies',
                f'{vertical} startups 2024',
                f'emerging {vertical} brands',
                f'local {vertical} businesses',
                f'{vertical} companies list'
            ])
        
        return queries
    
    def search_google_cse(self, query: str, num_results: int = 10) -> List[Dict]:
        """Execute Google Custom Search with caching and rate limiting"""
        if query in self.results_cache:
            return self.results_cache[query]
        
        try:
            # Rate limiting - free tier allows 100 queries/day
            time.sleep(0.1)
            
            result = self.cse_service.cse().list(
                q=query,
                cx=GOOGLE_CSE_CX,
                num=min(num_results, 10)  # Max 10 per request
            ).execute()
            
            search_results = []
            for item in result.get('items', []):
                search_results.append({
                    'title': item.get('title', ''),
                    'snippet': item.get('snippet', ''),
                    'link': item.get('link', ''),
                    'query_context': query
                })
            
            self.results_cache[query] = search_results
            return search_results
            
        except Exception as e:
            print(f"Search failed for query '{query}': {e}")
            return []
    
    def detect_brand_vertical(self, brand: str) -> Optional[str]:
        """Simple heuristic vertical detection using search results"""
        # Try to find industry context through search
        vertical_queries = [
            f'"{brand}" industry',
            f'what does {brand} do',
            f'{brand} business'
        ]
        
        industry_keywords = {
            'Athletic Apparel': ['athletic', 'sportswear', 'fitness', 'running', 'sports'],
            'Fintech': ['fintech', 'payments', 'financial', 'banking', 'finance'],
            'SaaS': ['software', 'saas', 'platform', 'cloud', 'enterprise'],
            'E-commerce': ['ecommerce', 'retail', 'online store', 'marketplace'],
            'Healthcare': ['healthcare', 'medical', 'health', 'pharmaceutical'],
            'Food & Beverage': ['food', 'beverage', 'restaurant', 'coffee', 'drink'],
            'Eyewear': ['glasses', 'eyewear', 'optical', 'vision', 'frames'],
            'Automotive': ['automotive', 'car', 'vehicle', 'auto'],
        }
        
        # Simple keyword matching from search results
        all_content = ""
        for query in vertical_queries[:2]:  # Limit to avoid quota issues
            results = self.search_google_cse(query, 3)
            for result in results:
                all_content += f" {result.get('title', '')} {result.get('snippet', '')}"
        
        content_lower = all_content.lower()
        
        # Score each vertical by keyword matches
        vertical_scores = {}
        for vertical, keywords in industry_keywords.items():
            score = sum(1 for keyword in keywords if keyword in content_lower)
            if score > 0:
                vertical_scores[vertical] = score
        
        # Return highest scoring vertical
        if vertical_scores:
            return max(vertical_scores, key=vertical_scores.get)
        
        return None
    
    def extract_candidates_from_results(self, results: List[Dict], brand: str, 
                                       discovery_method: str = 'standard') -> List[CompetitorCandidate]:
        """Extract competitor candidates from search results"""
        candidates = []
        
        for result in results:
            title = result.get('title', '')
            snippet = result.get('snippet', '')
            url = result.get('link', '')
            query = result.get('query_context', '')
            
            # Calculate relevance score
            raw_score = score_search_result(title, snippet, url, query) if score_search_result else 0.5
            
            # Extract company names from title and snippet
            title_companies = extract_company_names(title) if extract_company_names else [title.split()[0] if title else "Unknown"]
            snippet_companies = extract_company_names(snippet) if extract_company_names else []
            
            # Create candidates for each found company
            for company in title_companies:
                if company.lower() != brand.lower():  # Exclude self
                    candidates.append(CompetitorCandidate(
                        company_name=company,
                        source_url=url,
                        source_title=title,
                        query_used=query,
                        raw_score=raw_score + 0.2,  # Bonus for title
                        found_in='title',
                        discovery_method=discovery_method
                    ))
            
            for company in snippet_companies:
                if company.lower() != brand.lower():  # Exclude self
                    candidates.append(CompetitorCandidate(
                        company_name=company,
                        source_url=url,
                        source_title=title,
                        query_used=query,
                        raw_score=raw_score,
                        found_in='snippet',
                        discovery_method=discovery_method
                    ))
        
        return candidates
    
    def discover_competitors(self, brand: str, vertical: str = None, 
                           max_results_per_query: int = 10) -> List[CompetitorCandidate]:
        """Main competitor discovery with adaptive strategy"""
        
        print(f"🔍 Discovering competitors for '{brand}'...")
        
        # Step 1: Detect vertical if not provided
        if not vertical:
            print("📊 Detecting brand vertical...")
            vertical = self.detect_brand_vertical(brand)
            if vertical:
                print(f"✅ Detected vertical: {vertical}")
            else:
                print("⚠️  Could not detect vertical, using generic queries")
        
        # Step 2: Try standard discovery approach
        standard_queries = self.generate_standard_queries(brand, vertical)
        all_candidates = []
        
        print(f"🎯 Executing {len(standard_queries)} standard discovery queries...")
        for query in standard_queries:
            results = self.search_google_cse(query, max_results_per_query)
            candidates = self.extract_candidates_from_results(results, brand, 'standard')
            all_candidates.extend(candidates)
            print(f"   '{query[:50]}...' → {len(candidates)} candidates")
        
        # Step 3: Check if we have sufficient results
        unique_standard_candidates = len(set(c.company_name.lower() for c in all_candidates))
        print(f"📈 Standard discovery found {unique_standard_candidates} unique candidates")
        
        # Step 4: Use fallback strategy if insufficient results
        if unique_standard_candidates < 5:
            print("🔄 Insufficient results, trying fallback discovery...")
            fallback_queries = self.generate_fallback_queries(brand, vertical)
            
            for query in fallback_queries:
                results = self.search_google_cse(query, max_results_per_query)
                candidates = self.extract_candidates_from_results(results, brand, 'fallback')
                all_candidates.extend(candidates)
                print(f"   '{query[:50]}...' → {len(candidates)} candidates")
        
        # Step 5: Deduplicate and score
        dedupe_data = [
            {
                'company_name': c.company_name,
                'source_url': c.source_url,
                'source_title': c.source_title,
                'query_used': c.query_used,
                'raw_score': c.raw_score,
                'found_in': c.found_in,
                'discovery_method': c.discovery_method
            } for c in all_candidates
        ]
        
        # Dedupe companies if function is available, otherwise use simple deduplication
        if dedupe_companies:
            unique_candidates = dedupe_companies(dedupe_data, 'company_name')
        else:
            # Simple deduplication by company_name
            seen_names = set()
            unique_candidates = []
            for data in dedupe_data:
                name_lower = data['company_name'].lower()
                if name_lower not in seen_names:
                    unique_candidates.append(data)
                    seen_names.add(name_lower)
        
        # Convert back to CompetitorCandidate objects
        final_candidates = [
            CompetitorCandidate(**data) for data in unique_candidates
        ]
        
        # Sort by raw score descending
        final_candidates.sort(key=lambda x: x.raw_score, reverse=True)
        
        print(f"✅ Discovery complete: {len(final_candidates)} unique candidates found")
        return final_candidates
    
    def save_to_csv(self, candidates: List[CompetitorCandidate], 
                   output_path: str, brand: str, vertical: str = None):
        """Save candidates to CSV file"""
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'target_brand', 'target_vertical', 'company_name', 
                'source_url', 'source_title', 'query_used', 
                'raw_score', 'found_in', 'discovery_method'
            ])
            
            for candidate in candidates:
                writer.writerow([
                    brand, vertical or 'Unknown',
                    candidate.company_name, candidate.source_url,
                    candidate.source_title, candidate.query_used,
                    candidate.raw_score, candidate.found_in,
                    candidate.discovery_method
                ])
        
        print(f"💾 Saved {len(candidates)} candidates to {output_path}")
    
    def load_to_bigquery(self, candidates: List[CompetitorCandidate], 
                        brand: str, vertical: str = None):
        """Load candidates directly to BigQuery"""
        if not candidates:
            print("⚠️  No candidates to load to BigQuery")
            return
        
        # Convert to DataFrame
        data = []
        for candidate in candidates:
            data.append({
                'target_brand': brand,
                'target_vertical': vertical or 'Unknown',
                'company_name': candidate.company_name,
                'source_url': candidate.source_url,
                'source_title': candidate.source_title,
                'query_used': candidate.query_used,
                'raw_score': candidate.raw_score,
                'found_in': candidate.found_in,
                'discovery_method': candidate.discovery_method,
                'discovered_at': pd.Timestamp.now()
            })
        
        df = pd.DataFrame(data)
        table_id = f"{BQ_PROJECT}.{BQ_DATASET}.competitors_raw"
        
        try:
            load_dataframe_to_bq(df, table_id, write_disposition="WRITE_APPEND")
            print(f"✅ Loaded {len(df)} candidates to BigQuery table: {table_id}")
        except Exception as e:
            print(f"❌ Failed to load to BigQuery: {e}")

def main():
    parser = argparse.ArgumentParser(description="Discover competitors using Google Custom Search")
    parser.add_argument("--brand", required=True, help="Target brand name")
    parser.add_argument("--vertical", help="Industry vertical (auto-detected if not provided)")
    parser.add_argument("--output", default="data/temp/competitors_raw.csv", 
                       help="Output CSV file path")
    parser.add_argument("--load-bq", action="store_true", 
                       help="Load results directly to BigQuery")
    parser.add_argument("--max-results", type=int, default=10,
                       help="Maximum results per query")
    
    args = parser.parse_args()
    
    # Initialize discovery system
    discovery = CompetitorDiscovery()
    
    # Run discovery
    candidates = discovery.discover_competitors(
        brand=args.brand,
        vertical=args.vertical,
        max_results_per_query=args.max_results
    )
    
    if not candidates:
        print("❌ No competitors found!")
        return
    
    # Output results
    print(f"\n🎉 Top candidates for '{args.brand}':")
    for i, candidate in enumerate(candidates[:10], 1):
        print(f"{i:2d}. {candidate.company_name:<25} "
              f"(score: {candidate.raw_score:.2f}, method: {candidate.discovery_method})")
    
    # Save to CSV
    discovery.save_to_csv(candidates, args.output, args.brand, args.vertical)
    
    # Load to BigQuery if requested
    if args.load_bq:
        discovery.load_to_bigquery(candidates, args.brand, args.vertical)

if __name__ == "__main__":
    main()