"""
Search utilities for competitor discovery - company name extraction and scoring
"""
import re
import html
from typing import List, Dict, Set
from urllib.parse import urlparse

def extract_company_names(text: str, min_length: int = 2, max_length: int = 40) -> List[str]:
    """Extract potential company names from text using multiple patterns"""
    if not text:
        return []
    
    candidates = []
    
    # Pattern 1: Capitalized sequences (2-4 words) - simplified
    pattern1 = r'\b[A-Z][a-zA-Z&\'\-]*(?:\s+[A-Z][a-zA-Z&\'\-]*){0,3}\b'
    matches1 = re.findall(pattern1, text)
    candidates.extend(matches1)
    
    # Pattern 2: Common business suffixes
    pattern2 = r'\b[A-Z][a-zA-Z\s&\'\-]+(?:\s+(?:Inc|Corp|LLC|Ltd|Co|Company))\b'
    matches2 = re.findall(pattern2, text)
    candidates.extend(matches2)
    
    # Pattern 3: Brand name indicators in context
    pattern3 = r'(?:vs\s+|compared\s+to\s+|like\s+|alternative\s+to\s+)([A-Z][a-zA-Z\s&\'\-]+?)(?:\s|,|\.|\|)'
    matches3 = re.findall(pattern3, text, re.IGNORECASE)
    candidates.extend(matches3)
    
    # Pattern 4: List format extraction  
    pattern4 = r'(?:\d+\.\s+|•\s+|\-\s+)([A-Z][a-zA-Z\s&\'\-]+?)(?:\s*[\-\–\—]|\s*:|\s*\(|$)'
    matches4 = re.findall(pattern4, text)
    candidates.extend(matches4)
    
    # Clean and filter candidates
    cleaned_candidates = []
    for candidate in candidates:
        # Clean HTML entities and whitespace
        clean_name = html.unescape(candidate).strip()
        
        # Length filter
        if not (min_length <= len(clean_name) <= max_length):
            continue
            
        # Simple exclude list - only obvious non-companies
        exclude_list = {
            'Top', 'Best', 'Leading', 'Popular', 'Most', 'Latest', 'New', 'Old',
            'Companies', 'Brands', 'Products', 'Services', 'Solutions', 'Options', 'Alternatives',
            'How', 'Why', 'What', 'Where', 'When', 'Which', 'The', 'A', 'An',
            'List', 'Guide', 'Review', 'Article', 'Blog', 'Website'
        }
        
        if clean_name in exclude_list:
            continue
            
        # Must start with capital letter and contain only letters/spaces/common chars
        if not re.match(r'^[A-Z]', clean_name):
            continue
            
        # Additional business logic filters
        if _is_likely_company_name(clean_name):
            cleaned_candidates.append(clean_name)
    
    return list(set(cleaned_candidates))  # Remove duplicates

def _is_likely_company_name(name: str) -> bool:
    """Apply heuristics to determine if a string is likely a company name"""
    
    # Must contain at least one letter
    if not re.search(r'[a-zA-Z]', name):
        return False
    
    # Common non-company words to exclude
    non_companies = {
        'Google Search', 'Search Results', 'Web Search', 'Related Searches',
        'More Info', 'Click Here', 'Read More', 'Learn More', 'Find Out',
        'Sign Up', 'Log In', 'Contact Us', 'About Us', 'Privacy Policy',
        'Terms Service', 'Home Page', 'Main Page'
    }
    
    if name in non_companies:
        return False
        
    # Generic business terms that are not company names
    generic_terms = {
        'Business', 'Company', 'Corporation', 'Enterprise', 'Organization',
        'Industry', 'Market', 'Sector', 'Platform', 'Solution', 'Service'
    }
    
    if name in generic_terms:
        return False
    
    # Positive indicators of company names
    company_indicators = [
        r'\b(Inc|Corp|LLC|Ltd|Co|Company|Technologies|Tech|Solutions|Systems|Group|Partners)\b',
        r'\b[A-Z][a-z]*[A-Z]',  # CamelCase
        r'&',  # Company & Company
    ]
    
    # Boost confidence if it matches company patterns
    has_company_indicators = any(re.search(pattern, name) for pattern in company_indicators)
    
    # Length-based filtering with indicators
    if len(name.split()) == 1:
        # Single word: must be 3+ chars and have company indicators
        return len(name) >= 3 and (has_company_indicators or name[0].isupper())
    elif len(name.split()) <= 4:
        # 2-4 words: likely a company name
        return True
    else:
        # 5+ words: must have strong company indicators
        return has_company_indicators

def score_search_result(title: str, snippet: str, url: str, query: str) -> float:
    """Score a search result for competitor discovery relevance"""
    score = 1.0  # Base score
    
    # Domain authority weighting
    domain = urlparse(url).netloc.lower()
    
    domain_scores = {
        'g2.com': 3.0,
        'capterra.com': 3.0,
        'softwareadvice.com': 3.0, 
        'gartner.com': 2.5,
        'forrester.com': 2.5,
        'crunchbase.com': 2.0,
        'linkedin.com': 2.0,
        'techcrunch.com': 1.8,
        'forbes.com': 1.5,
        'businessinsider.com': 1.5,
        'reddit.com': 1.2,
        'quora.com': 1.1,
        'wikipedia.org': 1.8,
    }
    
    # Apply domain multiplier
    for domain_key, multiplier in domain_scores.items():
        if domain_key in domain:
            score *= multiplier
            break
    
    # Content pattern bonuses
    title_lower = title.lower()
    snippet_lower = snippet.lower()
    combined_text = f"{title_lower} {snippet_lower}"
    
    # High-value comparison signals
    if re.search(r'\bvs\b|\bversus\b|\bcompared?\s+to\b', combined_text):
        score += 2.0
        
    if re.search(r'\balternative[s]?\s+to\b|\bcompetitor[s]?\b|\brival[s]?\b', combined_text):
        score += 1.5
        
    # List and ranking signals  
    if re.search(r'\btop\s+\d+\b|\bbest\s+\d+\b|\btop\s+(ten|five|20)\b', title_lower):
        score += 1.5
        
    if re.search(r'\bleading\b|\bmarket\s+leader[s]?\b|\bdominant\b', combined_text):
        score += 1.0
    
    # Industry/category signals
    if re.search(r'\bmarket\b|\bindustry\b|\bsector\b|\blandscape\b', combined_text):
        score += 0.5
    
    # Query relevance bonus
    if query:
        query_terms = [term.lower().strip('"') for term in query.split() if len(term.strip('"')) > 2]
        matching_terms = sum(1 for term in query_terms if term in combined_text)
        if query_terms:
            relevance_ratio = matching_terms / len(query_terms)
            score += relevance_ratio * 0.5
    
    # Penalty for obviously irrelevant content
    irrelevant_patterns = [
        r'\bjob[s]?\b|\bcareers?\b|\bhiring\b',
        r'\bnews\b|\bpress\s+release\b|\bannouncement\b',
        r'\bstock\s+price\b|\bfinancial\s+report\b|\bearnings\b',
        r'\blogin\b|\bsign\s+up\b|\bregister\b',
    ]
    
    for pattern in irrelevant_patterns:
        if re.search(pattern, combined_text):
            score *= 0.7  # Penalty
            break
    
    return max(score, 0.1)  # Minimum score

def dedupe_companies(companies: List[Dict], key: str = "company_name") -> List[Dict]:
    """Remove duplicate companies while preserving highest-scored entries"""
    seen: Dict[str, Dict] = {}
    
    for company in companies:
        name_key = company[key].lower().strip()
        
        # Normalize common variations
        name_key = normalize_company_name(name_key)
        
        if name_key not in seen or company.get('raw_score', 0) > seen[name_key].get('raw_score', 0):
            seen[name_key] = company
    
    # Sort by raw_score descending
    unique_companies = sorted(seen.values(), key=lambda x: x.get('raw_score', 0), reverse=True)
    
    return unique_companies

def normalize_company_name(name: str) -> str:
    """Normalize company name variations for deduplication"""
    normalized = name.lower().strip()
    
    # Remove common suffixes for comparison
    suffixes = [
        r'\s+(inc|incorporated|corp|corporation|llc|ltd|limited|co|company)\.?$',
        r'\s+(technologies|tech|solutions|systems|group|partners)$'
    ]
    
    for suffix in suffixes:
        normalized = re.sub(suffix, '', normalized)
    
    # Remove extra whitespace
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    
    # Handle common abbreviations
    abbreviations = {
        'international': 'intl',
        'corporation': 'corp',
        'incorporated': 'inc',
        'limited': 'ltd',
        'company': 'co'
    }
    
    for full, abbr in abbreviations.items():
        normalized = normalized.replace(full, abbr)
    
    return normalized