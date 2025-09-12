#!/usr/bin/env python3
"""
Intelligent competitor name validation with multi-layered filtering
Designed to cast a wide net while minimizing false negatives
"""

import re
from typing import List, Dict, Tuple, Set
from dataclasses import dataclass

@dataclass
class ValidationResult:
    is_valid: bool
    confidence: float
    reasons: List[str]
    category: str  # 'valid', 'invalid', 'suspicious'

class CompetitorNameValidator:
    """Multi-layered validator for competitor names across all verticals"""
    
    def __init__(self):
        # Tier 1: Safe generic terms (never companies)
        self.generic_blacklist = {
            # Meta-analysis terms
            'competitors', 'competitor', 'analysis', 'insights', 'overview', 'report',
            'research', 'study', 'whitepaper', 'case study', 'blog post', 'article',
            
            # Market/business terms  
            'market share', 'market size', 'market analysis', 'market overview',
            'industry analysis', 'industry overview', 'industry report',
            'competitive landscape', 'competitive analysis',
            
            # Content descriptors
            'latest', 'recent', 'updated', 'new', 'top', 'best', 'leading',
            'key', 'major', 'main', 'primary', 'essential', 'critical',
            
            # Temporal terms
            '2024', '2025', '2023', 'q1', 'q2', 'q3', 'q4', 'january', 'february',
            'march', 'april', 'may', 'june', 'july', 'august', 'september',
            'october', 'november', 'december',
            
            # Generic business terms
            'small business', 'enterprise', 'startup', 'b2b', 'b2c', 'saas',
            'platform', 'solution', 'software', 'service', 'tool', 'app',
            'technology', 'tech', 'digital', 'online', 'web', 'mobile',
            
            # Business/industry descriptors
            'company', 'companies', 'business', 'businesses', 'firm', 'firms',
            'corporation', 'corp', 'inc', 'ltd', 'llc', 'organization', 'org',
            'group', 'groups', 'team', 'teams', 'department', 'division',
            'unit', 'center', 'centre', 'institute', 'foundation',
            
            # Market/competition terms
            'market', 'markets', 'industry', 'industries', 'sector', 'sectors',
            'vertical', 'verticals', 'space', 'landscape', 'ecosystem',
            'segment', 'segments', 'niche', 'category', 'categories',
            'field', 'fields', 'domain', 'domains', 'area', 'areas',
            
            # Performance/ranking terms
            'leader', 'leaders', 'leading', 'biggest', 'largest',
            'major', 'main', 'key', 'primary', 'dominant', 'successful',
            'popular', 'trending', 'growing', 'emerging', 'established',
            'recognized', 'known', 'famous', 'notable', 'prominent'
            
            # Roles/positions
            'ceo', 'cto', 'cfo', 'cmo', 'founder', 'executive', 'director',
            'manager', 'analyst', 'consultant', 'expert', 'specialist'
        }
        
        # Tier 2: Suspicious endings (context-dependent)
        self.suspicious_endings = {
            'management', 'solutions', 'services', 'consulting', 'analytics',
            'intelligence', 'research', 'insights', 'strategy', 'advisory'
        }
        
        # Tier 3: Suspicious patterns
        self.suspicious_patterns = [
            r'^top \d+',           # "Top 10", "Top 5"
            r'^\d+ best',          # "10 Best", "5 Best"  
            r'^leading \w+',       # "Leading SaaS"
            r'^best \w+ for',      # "Best CRM for"
            r'vs\.? ',             # "Company A vs Company B"
            r' vs\.? ',            # "Company A vs Company B"
            r'comparison$',        # "Salesforce Comparison"
            r'review$',            # "HubSpot Review"
            r'pricing$',           # "Stripe Pricing"
            r'alternatives?$',     # "Slack Alternative", "Zoom Alternatives"
            
            # Additional aggressive patterns from Phase 3
            r'^\w+\s+(companies|businesses|firms)$',  # "Eyewear Companies"
            r'^\w+\s+(market|industry|sector)$',      # "Eyewear Market"
            r'^(how|what|where|when|why|which)',      # Question-like titles
            r'\s+(guide|tutorial|tips|tricks)$',      # Content titles
            r'^\w+\s+(list|ranking|directory)$',      # "Eyewear List"
            r'complete\s+guide',                      # "Complete Guide to..."
            r'ultimate\s+(guide|list)',               # "Ultimate Guide/List"
            r'\d{4}\s+(outlook|forecast|trends)',     # "2024 Outlook"
            r'(inc|corp|ltd|llc)\.?$',               # Generic suffixes only
            r'^(the|a|an)\s+',                       # Articles at start
            r'\s+(startup|startups)$',               # Generic startup terms
        ]
        
        # Protected terms (known to be real companies despite suspicious patterns)
        self.protected_companies = {
            # Companies with suspicious words
            'accenture', 'deloitte', 'pwc', 'kpmg', 'mckinsey',  # Consulting/Management
            'tata consultancy services', 'ibm global services',   # Services
            'oracle', 'salesforce', 'servicenow',                # Enterprise
            
            # Tech companies with generic terms
            'meta', 'alphabet', 'apple', 'google', 'microsoft',
            'amazon', 'netflix', 'uber', 'airbnb', 'tesla',
            
            # Financial services
            'american express', 'goldman sachs', 'morgan stanley',
            'jp morgan', 'bank of america', 'wells fargo',
            
            # Single word companies
            'stripe', 'paypal', 'venmo', 'square', 'plaid',
            'shopify', 'spotify', 'slack', 'zoom', 'figma'
        }
    
    def validate_name(self, name: str) -> ValidationResult:
        """Validate a single competitor name"""
        
        if not name or not isinstance(name, str):
            return ValidationResult(False, 0.0, ["Empty or invalid name"], "invalid")
        
        name_clean = name.strip().lower()
        name_original = name.strip()
        reasons = []
        
        # Step 1: Length checks
        if len(name_clean) < 2:
            return ValidationResult(False, 0.0, ["Too short (< 2 characters)"], "invalid")
        
        if len(name_clean) > 60:
            reasons.append("Very long name (> 60 chars) - likely article title")
            return ValidationResult(False, 0.2, reasons, "invalid")
        
        # Step 2: Protected companies (override everything)
        if name_clean in self.protected_companies:
            return ValidationResult(True, 0.95, ["Protected company list"], "valid")
        
        # Step 3: Generic blacklist (hard filter)
        if name_clean in self.generic_blacklist:
            return ValidationResult(False, 0.0, [f"Generic term: '{name_clean}'"], "invalid")
        
        # Step 4: Pattern-based filtering
        for pattern in self.suspicious_patterns:
            if re.search(pattern, name_clean, re.IGNORECASE):
                reasons.append(f"Suspicious pattern: {pattern}")
                return ValidationResult(False, 0.1, reasons, "invalid")
        
        # Step 5: Suspicious endings (context-dependent scoring)
        confidence = 0.8  # Start with high confidence
        
        for ending in self.suspicious_endings:
            if name_clean.endswith(ending):
                # Check if it's ONLY the ending (e.g., "Management" vs "Risk Management Inc")
                if name_clean == ending:
                    return ValidationResult(False, 0.0, [f"Pure generic term: '{ending}'"], "invalid")
                else:
                    # Real company might have suspicious ending (e.g., "Accenture Management Consulting")
                    confidence -= 0.3
                    reasons.append(f"Suspicious ending: '{ending}' - needs validation")
                    break
        
        # Step 6: All caps check (usually acronyms or generic terms)
        if name_original.isupper() and len(name_original) <= 5:
            confidence -= 0.2
            reasons.append("All caps short form - likely acronym")
        
        # Step 7: Number-heavy names
        if re.search(r'\d{4}', name_clean):  # Contains year-like number
            confidence -= 0.3
            reasons.append("Contains year-like numbers")
        
        # Step 8: Too generic single words
        if ' ' not in name_clean and len(name_clean) <= 6:
            if name_clean not in self.protected_companies:
                confidence -= 0.4
                reasons.append("Very short single word - needs validation")
        
        # Final classification
        if confidence >= 0.7:
            category = "valid"
        elif confidence >= 0.4:
            category = "suspicious" 
        else:
            category = "invalid"
        
        if not reasons:
            reasons.append("Passed all validation checks")
        
        return ValidationResult(confidence >= 0.4, confidence, reasons, category)
    
    def validate_batch(self, names: List[str]) -> Dict[str, ValidationResult]:
        """Validate multiple competitor names"""
        return {name: self.validate_name(name) for name in names}
    
    def get_clean_competitors(self, names: List[str], min_confidence: float = 0.4) -> List[Tuple[str, ValidationResult]]:
        """Get filtered list of valid competitors"""
        results = self.validate_batch(names)
        return [(name, result) for name, result in results.items() 
                if result.is_valid and result.confidence >= min_confidence]
    
    def get_high_confidence_competitors(self, names: List[str], min_confidence: float = 0.7) -> List[Tuple[str, ValidationResult]]:
        """Get filtered list of valid competitors"""
        results = self.validate_batch(names)
        return [(name, result) for name, result in results.items() 
                if result.is_valid and result.confidence >= min_confidence]


def test_validator():
    """Test the validator with various cases"""
    
    validator = CompetitorNameValidator()
    
    test_cases = [
        # Should be VALID
        "PayPal", "Stripe", "Square", "Apple", "Google", "Microsoft",
        "Accenture", "Deloitte", "PwC", "Tata Consultancy Services",
        "JPMorgan Chase", "Goldman Sachs", "American Express",
        
        # Should be INVALID  
        "Payment Management", "Market Share", "Competitor Insights",
        "Analysis", "Overview", "2024", "Best CRM", "Top 10",
        "Leading SaaS", "Small Business", "CEO", "Technology",
        
        # Should be SUSPICIOUS (need human review)
        "Risk Management Solutions", "Digital Analytics Corp", 
        "Strategic Consulting Group", "ABC Management Inc",
        
        # Edge cases
        "", "A", "Very Long Company Name That Looks Like Article Title Here",
        "AI", "IBM", "3M", "H&R Block"
    ]
    
    print("🧪 COMPETITOR NAME VALIDATOR TEST")
    print("=" * 80)
    
    for name in test_cases:
        result = validator.validate_name(name)
        status_emoji = "✅" if result.is_valid else "❌"
        print(f"{status_emoji} {name:30} | {result.confidence:.2f} | {result.category:10} | {result.reasons[0][:50]}")
    
    return validator


if __name__ == "__main__":
    test_validator()