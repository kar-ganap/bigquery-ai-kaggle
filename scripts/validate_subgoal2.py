#!/usr/bin/env python3
"""
Validation script for Subgoal 2: Enhanced Google Custom Search Discovery
Checks all completion criteria without requiring actual API calls
"""

import os
import sys
import importlib.util
from pathlib import Path

def check_file_exists(file_path: str, description: str) -> bool:
    """Check if a required file exists"""
    if os.path.exists(file_path):
        print(f"✅ {description}: {file_path}")
        return True
    else:
        print(f"❌ {description}: {file_path} - NOT FOUND")
        return False

def check_imports() -> bool:
    """Check if all required imports work"""
    try:
        sys.path.append(os.path.join(os.path.dirname(__file__)))
        
        # Test core imports
        from utils.search_utils import extract_company_names, score_search_result, dedupe_companies
        from utils.bigquery_client import get_bigquery_client, load_dataframe_to_bq
        
        print("✅ All utility imports successful")
        return True
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False

def check_discovery_logic() -> bool:
    """Test discovery logic components"""
    try:
        from utils.search_utils import extract_company_names, score_search_result
        
        # Test company name extraction
        test_text = "Top Nike competitors include Adidas, Under Armour, and Puma"
        companies = extract_company_names(test_text)
        expected_companies = {'Adidas', 'Under Armour', 'Puma'}
        
        if not expected_companies.issubset(set(companies)):
            print(f"❌ Company extraction failed. Expected: {expected_companies}, Got: {companies}")
            return False
        
        # Test scoring
        score = score_search_result(
            "Nike vs Adidas: Athletic Brand Comparison",
            "Compare Nike and Adidas athletic brands",
            "https://g2.com/compare/nike-vs-adidas",
            "Nike competitors"
        )
        
        if score < 2.0:  # Should be high due to g2.com domain + vs pattern
            print(f"❌ Scoring too low: {score}")
            return False
            
        print("✅ Discovery logic components working")
        return True
    except Exception as e:
        print(f"❌ Discovery logic error: {e}")
        return False

def check_adaptive_strategies() -> bool:
    """Check if adaptive discovery strategies are implemented"""
    try:
        # Check if main discovery script can be imported
        spec = importlib.util.spec_from_file_location(
            "discover_competitors_v2", 
            "scripts/discover_competitors_v2.py"
        )
        
        if spec is None or spec.loader is None:
            print("❌ Cannot load discover_competitors_v2.py")
            return False
            
        module = importlib.util.module_from_spec(spec)
        
        # Read the source to check for adaptive strategies
        with open("scripts/discover_competitors_v2.py", 'r') as f:
            source = f.read()
        
        required_methods = [
            "generate_standard_queries",
            "generate_fallback_queries", 
            "detect_brand_vertical",
            "discover_competitors"
        ]
        
        for method in required_methods:
            if method not in source:
                print(f"❌ Missing method: {method}")
                return False
        
        print("✅ Adaptive discovery strategies implemented")
        return True
    except Exception as e:
        print(f"❌ Adaptive strategy check error: {e}")
        return False

def check_environment_config() -> bool:
    """Check environment configuration"""
    env_file = ".env"
    if not os.path.exists(env_file):
        print(f"❌ Environment file not found: {env_file}")
        return False
    
    with open(env_file, 'r') as f:
        content = f.read()
    
    required_vars = ["GOOGLE_CSE_API_KEY", "GOOGLE_CSE_CX"]
    missing_vars = []
    
    for var in required_vars:
        if var not in content:
            missing_vars.append(var)
    
    if missing_vars:
        print(f"❌ Missing environment variables: {missing_vars}")
        print("ℹ️  Add your Google CSE credentials to .env file")
        return False
    
    print("✅ Environment configuration ready")
    return True

def check_output_structure() -> bool:
    """Check if output directory structure exists"""
    required_dirs = [
        "data/temp",
        "docs"
    ]
    
    all_exist = True
    for dir_path in required_dirs:
        if os.path.exists(dir_path):
            print(f"✅ Directory exists: {dir_path}")
        else:
            print(f"❌ Directory missing: {dir_path}")
            all_exist = False
    
    return all_exist

def main():
    """Run all Subgoal 2 validation checks"""
    print("🔍 Validating Subgoal 2: Enhanced Google Custom Search Discovery")
    print("=" * 70)
    
    checks = [
        ("File Structure", lambda: all([
            check_file_exists("scripts/discover_competitors_v2.py", "Main discovery script"),
            check_file_exists("scripts/utils/search_utils.py", "Search utilities"),  
            check_file_exists("scripts/utils/bigquery_client.py", "BigQuery client"),
            check_file_exists("docs/google_cse_setup.md", "Setup documentation")
        ])),
        ("Import Dependencies", check_imports),
        ("Discovery Logic", check_discovery_logic),
        ("Adaptive Strategies", check_adaptive_strategies),
        ("Environment Config", check_environment_config),
        ("Output Structure", check_output_structure),
    ]
    
    results = {}
    for check_name, check_func in checks:
        print(f"\n🧪 {check_name}:")
        try:
            results[check_name] = check_func()
        except Exception as e:
            print(f"❌ {check_name} failed with error: {e}")
            results[check_name] = False
    
    # Summary
    print(f"\n📊 Validation Summary:")
    print("=" * 30)
    
    passed = sum(results.values())
    total = len(results)
    
    for check_name, passed_check in results.items():
        status = "✅ PASS" if passed_check else "❌ FAIL"
        print(f"{status} {check_name}")
    
    print(f"\nOverall: {passed}/{total} checks passed")
    
    if passed == total:
        print("🎉 Subgoal 2 validation: ALL CHECKS PASSED!")
        print("\nNext steps:")
        print("1. Set up Google Custom Search Engine API (see docs/google_cse_setup.md)")
        print("2. Test with real brand: python scripts/discover_competitors_v2.py --brand Nike")
        print("3. Proceed to Subgoal 3: BigQuery AI Competitor Curation")
        return True
    else:
        print("⚠️  Some checks failed. Please fix issues before proceeding.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)