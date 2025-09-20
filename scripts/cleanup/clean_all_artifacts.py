#!/usr/bin/env python3
"""
Clean All BigQuery Artifacts - Complete Reset with Infrastructure Management

Enhanced clean slate solution that:
- Preserves base data and checkpoints (high watermark solutions)
- Only cleans analysis/pipeline result tables
- Verifies and sets up required infrastructure
- Optionally auto-runs pipeline after cleaning

Usage:
    python clean_all_artifacts.py                              # Clean only
    python clean_all_artifacts.py --run "Warby Parker"         # Clean + run pipeline
    python clean_all_artifacts.py --run "Warby Parker" --vertical "eyewear"
"""
import argparse
import subprocess
import sys
import os
from src.utils.bigquery_client import get_bigquery_client, run_query

def check_infrastructure():
    """Check and setup required BigQuery infrastructure - CREATE missing components"""

    print("🔧 INFRASTRUCTURE PRE-FLIGHT CHECK & AUTO-SETUP")
    print("=" * 50)

    try:
        client = get_bigquery_client()
        project_id = "bigquery-ai-kaggle-469620"
        dataset_id = f"{project_id}.ads_demo"

        # Check and create Vertex AI connection
        connection_exists = False
        try:
            # List existing connections to check if vertex-ai exists
            connections_query = f"""
            SELECT connection_name
            FROM `{project_id}.us.INFORMATION_SCHEMA.CONNECTIONS`
            WHERE connection_name = 'vertex-ai'
            """
            result = run_query(connections_query)
            connection_exists = len(result) > 0
        except Exception as e:
            # If we can't query connections, assume it doesn't exist
            connection_exists = False

        if connection_exists:
            print("   ✅ Vertex AI connection exists")
        else:
            print("   🔧 Creating Vertex AI connection...")
            try:
                # Use bq command line tool for connection creation (SQL DDL doesn't support this)
                import subprocess
                connection_cmd = [
                    "bq", "mk", "--connection",
                    "--location=us",
                    "--project_id=" + project_id,
                    "--connection_type=CLOUD_RESOURCE",
                    "vertex-ai"
                ]
                result = subprocess.run(connection_cmd, capture_output=True, text=True)
                if result.returncode == 0:
                    print("   ✅ Vertex AI connection created successfully")
                else:
                    print(f"   ⚠️  Connection creation via bq CLI failed: {result.stderr}")
                    print("   📄 Will attempt to use existing connection or fallback methods")
            except Exception as e:
                print(f"   ⚠️  Connection creation skipped: {e}")
                print("   📄 Will attempt to use existing connection or fallback methods")

        # Check and create text embedding model
        model_exists = False
        try:
            # Check if model exists
            models_query = f"""
            SELECT model_name
            FROM `{dataset_id}.INFORMATION_SCHEMA.MODELS`
            WHERE model_name = 'text_embedding_model'
            """
            result = run_query(models_query)
            model_exists = len(result) > 0
        except Exception as e:
            model_exists = False

        if model_exists:
            print("   ✅ Text embedding model exists")
        else:
            print("   🔧 Creating text embedding model...")
            try:
                # Create the text embedding model with updated endpoint
                model_sql = f"""
                CREATE OR REPLACE MODEL `{dataset_id}.text_embedding_model`
                REMOTE WITH CONNECTION `{project_id}.us.vertex-ai`
                OPTIONS (
                  endpoint = 'text-embedding-004'
                );
                """
                run_query(model_sql)
                print("   ✅ Text embedding model created successfully")
            except Exception as e:
                print(f"   ⚠️  Model creation skipped: {e}")
                print("   📄 Will use fallback embedding methods when needed")

        # Check for existing data tables (informational only - pipeline will create them)
        data_tables = ['ads_raw', 'ads_with_dates']
        print(f"   📊 Data tables status (pipeline will create if missing):")
        for table_name in data_tables:
            try:
                test_query = f"SELECT COUNT(*) as count FROM `{dataset_id}.{table_name}` LIMIT 1"
                result = run_query(test_query)
                row_count = result.iloc[0]['count'] if not result.empty else 0
                print(f"      ✅ {table_name}: {row_count:,} rows (existing)")
            except Exception as e:
                print(f"      📝 {table_name}: Will be created by pipeline")

        return True

    except Exception as e:
        print(f"   ❌ Infrastructure check failed: {e}")
        return False


def clean_pipeline_artifacts(clean_persistent: bool = False):
    """Delete only pipeline analysis tables, preserving base data and checkpoints"""

    if clean_persistent:
        print("\n🧹 COMPLETE CLEAN SLATE - Deleting ALL Tables Including Base Data")
        print("=" * 70)
        print("⚠️  WARNING: This will delete persistent base data tables!")
        print("📊 Full data re-ingestion will be required on next pipeline run")
    else:
        print("\n🧹 SMART CLEAN SLATE - Deleting Pipeline Analysis Tables Only")
        print("=" * 60)

    # Tables to preserve (base data and infrastructure)
    PRESERVE_TABLES = {
        'text_embedding_model',  # Always preserve infrastructure - will be recreated if needed
    }

    # Conditionally preserve base data tables
    if not clean_persistent:
        PRESERVE_TABLES.update({
            'ads_raw',           # Base Meta Ad Library data
            'ads_with_dates',    # Processed base data with timestamps
        })

    # Table patterns to clean (analysis results)
    CLEAN_PATTERNS = [
        'visual_intelligence_',      # Run-specific visual analysis
        'audience_intelligence_',    # Run-specific audience analysis
        'creative_intelligence_',    # Run-specific creative analysis
        'channel_intelligence_',     # Run-specific channel analysis
        'creative_themes_',          # Run-specific creative themes
        'timing_patterns_',          # Run-specific timing analysis
        'channel_performance_',      # Run-specific channel performance
        'visual_sampling_strategy',  # Static analysis table
        'cta_aggressiveness_analysis', # Static analysis table
        'ads_embeddings',            # Embeddings (will be regenerated)
        'competitors_raw_',          # Competitor discovery results
        'competitors_curated_',      # AI-curated competitors
        'competitors_batch_',        # Competitor batches
        'gemini_model',              # Gemini model instances (will be recreated)
        'v_intelligence_summary_',   # Intelligence summary views
    ]

    # Additional patterns to clean when --clean-persistent is used
    if clean_persistent:
        CLEAN_PATTERNS.extend([
            'ads_raw_',              # Raw Meta ads data (demo clean slate)
        ])

    try:
        client = get_bigquery_client()
        dataset_id = "bigquery-ai-kaggle-469620.ads_demo"

        # List all tables in the dataset
        tables = client.list_tables(dataset_id)
        all_tables = [table.table_id for table in tables]

        # Filter tables to clean
        tables_to_clean = []
        tables_to_preserve = []

        for table_id in all_tables:
            should_clean = False

            # Check if table matches any clean pattern
            for pattern in CLEAN_PATTERNS:
                if table_id.startswith(pattern) or table_id == pattern:
                    should_clean = True
                    break

            # Always preserve explicitly listed tables
            if table_id in PRESERVE_TABLES:
                should_clean = False

            if should_clean:
                tables_to_clean.append(table_id)
            else:
                tables_to_preserve.append(table_id)

        print(f"📋 Found {len(all_tables)} total tables:")
        print(f"   🗑️  Will clean: {len(tables_to_clean)} analysis tables")
        print(f"   💾 Will preserve: {len(tables_to_preserve)} base data tables")

        if tables_to_preserve:
            print(f"\n💾 PRESERVING (base data & infrastructure):")
            for table_id in sorted(tables_to_preserve):
                print(f"   • {table_id}")

        if tables_to_clean:
            print(f"\n🗑️  CLEANING (analysis results):")
            for table_id in sorted(tables_to_clean):
                print(f"   • {table_id}")

            # Delete analysis tables
            deleted_count = 0
            for table_id in tables_to_clean:
                try:
                    table_ref = f"{dataset_id}.{table_id}"
                    # Handle views differently from tables
                    if table_id.startswith('v_'):
                        delete_query = f"DROP VIEW IF EXISTS `{table_ref}`"
                    else:
                        delete_query = f"DROP TABLE IF EXISTS `{table_ref}`"
                    run_query(delete_query)
                    print(f"   ✅ Deleted: {table_id}")
                    deleted_count += 1
                except Exception as e:
                    print(f"   ⚠️  Failed to delete {table_id}: {e}")

            print(f"\n🗑️  Analysis cleanup completed!")
            print(f"   📊 Deleted {deleted_count}/{len(tables_to_clean)} analysis tables")
            print(f"   💾 Preserved {len(tables_to_preserve)} base data tables")
        else:
            print(f"\n✨ No analysis tables found to clean - already clean slate!")

        return True

    except Exception as e:
        print(f"❌ Clean up failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def clean_local_artifacts():
    """Clean up local output files (preserving checkpoints and latest sql_dashboards)"""
    import os
    import glob

    print(f"\n🧹 Cleaning Local Artifacts (preserving checkpoints and latest sql_dashboards)...")

    # Clean temporary files but preserve checkpoints and latest sql_dashboards
    patterns_to_clean = [
        "data/output/*.log",              # Log files
        "data/output/*.json",             # Temporary JSON files (not in clean_checkpoints)
    ]

    cleaned_count = 0
    preserved_count = 0

    # Handle non-dashboard files
    for pattern in patterns_to_clean:
        files = glob.glob(pattern, recursive=True)
        for file_path in files:
            # Skip if it's in clean_checkpoints directory (high watermark solutions)
            if 'clean_checkpoints' in file_path:
                preserved_count += 1
                continue

            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
                    print(f"   ✅ Deleted: {os.path.basename(file_path)}")
                    cleaned_count += 1
            except Exception as e:
                print(f"   ⚠️  Failed to delete {file_path}: {e}")

    # Handle sql_dashboards directories - preserve only the most recent one
    dashboard_dirs = glob.glob("data/output/sql_dashboards_*")
    if dashboard_dirs:
        # Sort by modification time (most recent last)
        dashboard_dirs.sort(key=lambda x: os.path.getmtime(x))

        if len(dashboard_dirs) > 1:
            # Keep the most recent, delete the rest
            dirs_to_delete = dashboard_dirs[:-1]  # All except the last (most recent)
            latest_dir = dashboard_dirs[-1]

            print(f"   💾 Preserving latest dashboard: {os.path.basename(latest_dir)}")
            preserved_count += 1

            for dir_path in dirs_to_delete:
                try:
                    import shutil
                    shutil.rmtree(dir_path)
                    print(f"   ✅ Deleted directory: {os.path.basename(dir_path)}")
                    cleaned_count += 1
                except Exception as e:
                    print(f"   ⚠️  Failed to delete {dir_path}: {e}")
        else:
            # Only one dashboard directory, preserve it
            print(f"   💾 Preserving only dashboard: {os.path.basename(dashboard_dirs[0])}")
            preserved_count += 1

    print(f"   🗑️  Cleaned {cleaned_count} temporary files")
    if preserved_count > 0:
        print(f"   💾 Preserved {preserved_count} files (checkpoints and latest dashboard)")


def auto_run_pipeline(brand: str, vertical: str = "", verbose: bool = False):
    """Automatically run the pipeline after cleaning"""

    print(f"\n🚀 AUTO-RUNNING PIPELINE")
    print("=" * 50)
    print(f"Brand: {brand}")
    print(f"Vertical: {vertical or 'Auto-detect'}")
    print(f"Verbose: {verbose}")
    print("=" * 50)

    # Build command
    cmd = ["uv", "run", "python", "-m", "src.pipeline.orchestrator", "--brand", brand]

    if vertical:
        cmd.extend(["--vertical", vertical])

    if verbose:
        cmd.append("--verbose")

    print(f"🔄 Running: {' '.join(cmd)}")

    try:
        # Run the pipeline
        result = subprocess.run(cmd, capture_output=False, text=True)

        if result.returncode == 0:
            print(f"\n✅ Pipeline completed successfully!")
            return True
        else:
            print(f"\n❌ Pipeline failed with exit code {result.returncode}")
            return False

    except Exception as e:
        print(f"\n❌ Failed to run pipeline: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(
        description="Enhanced Clean Slate BigQuery Artifacts Manager"
    )
    parser.add_argument("--run", help="Clean and auto-run pipeline for specified brand")
    parser.add_argument("--vertical", help="Brand vertical (optional)")
    parser.add_argument("--verbose", action="store_true", help="Verbose pipeline output")
    parser.add_argument("--check-only", action="store_true", help="Only check infrastructure, don't clean")
    parser.add_argument("--clean-persistent", action="store_true",
                       help="⚠️  DANGER: Also clean persistent base data tables (ads_raw, ads_with_dates). Use with caution - will require full data re-ingestion.")

    args = parser.parse_args()

    print("🚀 ENHANCED CLEAN SLATE BIGQUERY ARTIFACTS MANAGER")
    print("=" * 60)

    # Step 1: Infrastructure pre-flight check
    infrastructure_ok = check_infrastructure()

    if args.check_only:
        print(f"\n{'✅' if infrastructure_ok else '❌'} Infrastructure check complete")
        sys.exit(0 if infrastructure_ok else 1)

    if not infrastructure_ok:
        print("\n⚠️  Infrastructure issues detected - pipeline may use fallbacks")
        if args.run:
            print("   Auto-run mode: Continuing with fallbacks (some features may be limited)")
        else:
            print("   Continue anyway? Some features may be limited.")
            response = input("   Continue? (y/N): ")
            if response.lower() != 'y':
                print("   Aborted - please fix infrastructure first")
                sys.exit(1)

    # Step 2: Smart cleanup (preserving base data and checkpoints)
    cleanup_success = clean_pipeline_artifacts(args.clean_persistent)

    # Step 3: Clean local artifacts (preserving checkpoints)
    clean_local_artifacts()

    if not cleanup_success:
        print(f"\n❌ Cleanup had issues - check logs before proceeding")
        sys.exit(1)

    # Step 4: Auto-run pipeline if requested
    if args.run:
        print(f"\n🎯 CLEAN SLATE ACHIEVED - Auto-running pipeline...")
        pipeline_success = auto_run_pipeline(args.run, args.vertical or "", args.verbose)

        if pipeline_success:
            print(f"\n🎉 COMPLETE SUCCESS!")
            print(f"   ✅ Clean slate achieved")
            print(f"   ✅ Pipeline completed successfully")
            print(f"   📊 Check output files for results")
        else:
            print(f"\n⚠️  Clean slate achieved but pipeline had issues")
            print(f"   ✅ Clean slate achieved")
            print(f"   ❌ Pipeline failed - check logs")
            sys.exit(1)
    else:
        print(f"\n✅ CLEAN SLATE ACHIEVED!")
        if args.clean_persistent:
            print(f"   🧹 ALL tables cleaned (including base data)")
            print(f"   💾 Only checkpoints preserved")
            print(f"   ⚠️  Full data re-ingestion required on next run")
        else:
            print(f"   🧹 Analysis tables cleaned")
            print(f"   💾 Base data and checkpoints preserved")
            print(f"   🚀 Ready for fresh pipeline run")
        print(f"\n📋 To run pipeline:")
        print(f"   uv run python -m src.pipeline.orchestrator --brand 'Warby Parker' --vertical 'eyewear' --verbose")
        print(f"\n💡 Or use auto-run:")
        print(f"   uv run python clean_all_artifacts.py --run 'Warby Parker' --vertical 'eyewear' --verbose")
        if args.clean_persistent:
            print(f"\n⚠️  Note: Since --clean-persistent was used, the next pipeline run will take longer")
            print(f"   📊 All base data will need to be re-ingested from Meta Ad Library")


if __name__ == "__main__":
    main()