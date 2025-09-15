#!/usr/bin/env python3
"""
Debug the SQL structure by showing line numbers
"""

from src.competitive_intel.analysis.enhanced_whitespace_detection import Enhanced3DWhiteSpaceDetector

def debug_sql_structure():
    """Show the SQL with line numbers to find the syntax error"""

    print("🔍 Debugging SQL Structure")
    print("=" * 60)

    try:
        # Initialize detector
        competitors = ['LensCrafters', 'EyeBuyDirect', 'Zenni Optical', 'GlassesUSA']
        detector = Enhanced3DWhiteSpaceDetector(
            project_id="bigquery-ai-kaggle-469620",
            dataset_id="ads_demo",
            brand="Warby Parker",
            competitors=competitors
        )

        # Generate the SQL
        whitespace_sql = detector.analyze_real_strategic_positions("stage4_test")

        # Split into lines and show with numbers
        lines = whitespace_sql.split('\n')

        print(f"📝 Total lines: {len(lines)}")
        print(f"🎯 Looking around line 47 where error occurred:")
        print("-" * 80)

        # Show lines 40-55 to see the problem area
        for i, line in enumerate(lines[39:56], start=40):  # Lines 40-55
            marker = ">>> " if i == 47 else "    "
            print(f"{marker}{i:3d}: {line}")

        print("-" * 80)

        # Also check the WITH clause structure
        print(f"\n🔍 WITH clause analysis:")
        with_sections = []
        current_section = ""
        in_with = False

        for line in lines:
            if "WITH " in line and not in_with:
                in_with = True
                current_section = line
            elif in_with:
                if " AS (" in line and current_section:
                    with_sections.append(current_section.strip())
                    current_section = line
                elif line.strip().startswith(")") and "," in line:
                    with_sections.append(current_section.strip())
                    current_section = ""
                elif not line.strip().startswith("--") and line.strip():
                    current_section += " " + line.strip()

        print(f"📊 Found {len(with_sections)} WITH sections:")
        for i, section in enumerate(with_sections, 1):
            print(f"   {i}. {section[:100]}...")

    except Exception as e:
        print(f"❌ Debug failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_sql_structure()