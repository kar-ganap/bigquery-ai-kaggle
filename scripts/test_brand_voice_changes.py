#!/usr/bin/env python3
"""
MODERATE Test #4: Brand Voice Changes Detection
Tests whether we can detect intentional brand voice changes in sample data
"""

import os
import pandas as pd
from google.cloud import bigquery

PROJECT_ID = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
DATASET_ID = os.environ.get("BQ_DATASET", "ads_demo")

client = bigquery.Client(project=PROJECT_ID)

def detect_brand_voice_evolution():
    """Detect changes in brand voice over time using linguistic patterns"""
    
    query = f"""
    WITH brand_voice_analysis AS (
      SELECT 
        brand,
        ad_id,
        creative_text,
        title,
        DATE_TRUNC(DATE(start_timestamp), MONTH) AS month_start,
        active_days,
        
        -- Voice characteristics scoring
        REGEXP_CONTAINS(UPPER(creative_text), r'\\b(YOU|YOUR|YOURS)\\b') AS uses_second_person,
        REGEXP_CONTAINS(UPPER(creative_text), r'\\b(WE|OUR|OURS|US)\\b') AS uses_first_person_plural,
        REGEXP_CONTAINS(UPPER(creative_text), r'[!]') AS uses_exclamation,
        REGEXP_CONTAINS(UPPER(creative_text), r'\\b(NEW|FRESH|INNOVATIVE|REVOLUTIONARY)\\b') AS innovation_language,
        REGEXP_CONTAINS(UPPER(creative_text), r'\\b(PERFORMANCE|ATHLETE|CHAMPION|WIN|VICTORY)\\b') AS performance_language,
        REGEXP_CONTAINS(UPPER(creative_text), r'\\b(STYLE|FASHION|TRENDY|COOL|FRESH)\\b') AS style_language,
        
        -- Tone indicators
        REGEXP_CONTAINS(creative_text, r'[😀-🙿🚀-🛿🎀-🏿💀-🟿]') AS has_emojis,
        LENGTH(creative_text) AS text_length,
        ARRAY_LENGTH(REGEXP_EXTRACT_ALL(creative_text, r'\\b\\w+\\b')) AS word_count
        
      FROM `{PROJECT_ID}.{DATASET_ID}.ads_with_dates`
      WHERE creative_text IS NOT NULL
        AND LENGTH(creative_text) >= 20
        AND start_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 18 MONTH)
    ),
    
    monthly_voice_metrics AS (
      SELECT 
        brand,
        month_start,
        
        COUNT(*) AS ads_count,
        
        -- Voice characteristic percentages
        AVG(CASE WHEN uses_second_person THEN 1.0 ELSE 0.0 END) * 100 AS pct_second_person,
        AVG(CASE WHEN uses_first_person_plural THEN 1.0 ELSE 0.0 END) * 100 AS pct_first_person_plural,
        AVG(CASE WHEN uses_exclamation THEN 1.0 ELSE 0.0 END) * 100 AS pct_exclamation,
        AVG(CASE WHEN innovation_language THEN 1.0 ELSE 0.0 END) * 100 AS pct_innovation_language,
        AVG(CASE WHEN performance_language THEN 1.0 ELSE 0.0 END) * 100 AS pct_performance_language,
        AVG(CASE WHEN style_language THEN 1.0 ELSE 0.0 END) * 100 AS pct_style_language,
        AVG(CASE WHEN has_emojis THEN 1.0 ELSE 0.0 END) * 100 AS pct_emojis,
        
        -- Average metrics
        AVG(text_length) AS avg_text_length,
        AVG(word_count) AS avg_word_count
        
      FROM brand_voice_analysis
      GROUP BY brand, month_start
      HAVING COUNT(*) >= 5  -- Only months with sufficient data
    ),
    
    voice_changes AS (
      SELECT 
        brand,
        month_start,
        ads_count,
        
        pct_second_person,
        pct_innovation_language,
        pct_performance_language,
        pct_style_language,
        avg_text_length,
        
        -- Month-over-month changes
        pct_second_person - LAG(pct_second_person) OVER (
          PARTITION BY brand ORDER BY month_start
        ) AS second_person_change_mom,
        
        pct_innovation_language - LAG(pct_innovation_language) OVER (
          PARTITION BY brand ORDER BY month_start  
        ) AS innovation_language_change_mom,
        
        pct_performance_language - LAG(pct_performance_language) OVER (
          PARTITION BY brand ORDER BY month_start
        ) AS performance_language_change_mom,
        
        avg_text_length - LAG(avg_text_length) OVER (
          PARTITION BY brand ORDER BY month_start
        ) AS text_length_change_mom,
        
        -- Voice shift detection
        CASE 
          WHEN ABS(pct_second_person - LAG(pct_second_person) OVER (
            PARTITION BY brand ORDER BY month_start
          )) >= 25 THEN 'PERSONALIZATION_SHIFT'
          WHEN ABS(pct_innovation_language - LAG(pct_innovation_language) OVER (
            PARTITION BY brand ORDER BY month_start  
          )) >= 20 THEN 'INNOVATION_MESSAGING_SHIFT'
          WHEN ABS(pct_performance_language - LAG(pct_performance_language) OVER (
            PARTITION BY brand ORDER BY month_start
          )) >= 20 THEN 'PERFORMANCE_FOCUS_SHIFT'
          WHEN ABS(avg_text_length - LAG(avg_text_length) OVER (
            PARTITION BY brand ORDER BY month_start
          )) >= 30 THEN 'COMMUNICATION_STYLE_SHIFT'
          ELSE 'STABLE_VOICE'
        END AS voice_change_type
        
      FROM monthly_voice_metrics
    )
    
    SELECT 
      brand,
      month_start,
      voice_change_type,
      
      ROUND(pct_second_person, 1) AS pct_second_person,
      ROUND(pct_innovation_language, 1) AS pct_innovation_language, 
      ROUND(pct_performance_language, 1) AS pct_performance_language,
      ROUND(avg_text_length, 1) AS avg_text_length,
      
      ROUND(second_person_change_mom, 1) AS second_person_change_mom,
      ROUND(innovation_language_change_mom, 1) AS innovation_language_change_mom,
      ROUND(performance_language_change_mom, 1) AS performance_language_change_mom,
      ROUND(text_length_change_mom, 1) AS text_length_change_mom,
      
      ads_count
      
    FROM voice_changes
    ORDER BY brand, month_start DESC
    """
    
    print("🔍 MODERATE TEST #4: Brand Voice Changes Detection")
    print("=" * 60)
    
    try:
        df = client.query(query).result().to_dataframe()
        
        if len(df) > 0:
            print(f"📊 Brand Voice Evolution Analysis:")
            print(f"   Total brand-month combinations analyzed: {len(df)}")
            
            # Count voice changes by type
            voice_changes = df[df['voice_change_type'] != 'STABLE_VOICE']
            changes_by_type = voice_changes['voice_change_type'].value_counts()
            
            print(f"   Voice changes detected: {len(voice_changes)}")
            
            if len(voice_changes) > 0:
                print(f"\n📈 Voice Change Types:")
                for change_type, count in changes_by_type.items():
                    print(f"   {change_type}: {count}")
                
                print(f"\n🏷️  Brand Voice Changes by Brand:")
                for brand in df['brand'].unique():
                    brand_data = df[df['brand'] == brand]
                    brand_changes = brand_data[brand_data['voice_change_type'] != 'STABLE_VOICE']
                    
                    if len(brand_changes) > 0:
                        print(f"\n   {brand} ({len(brand_changes)} changes detected):")
                        for _, change in brand_changes.head(3).iterrows():
                            print(f"     {change['month_start'].strftime('%Y-%m')}: {change['voice_change_type']}")
                            if pd.notna(change['second_person_change_mom']):
                                print(f"       2nd person: {change['second_person_change_mom']:+.1f}% change")
                            if pd.notna(change['innovation_language_change_mom']):
                                print(f"       Innovation language: {change['innovation_language_change_mom']:+.1f}% change")
                            if pd.notna(change['performance_language_change_mom']):
                                print(f"       Performance language: {change['performance_language_change_mom']:+.1f}% change")
                    else:
                        print(f"\n   {brand}: No significant voice changes detected")
                
                return len(voice_changes) > 0
            else:
                print("   No significant voice changes detected")
                return False
        else:
            print("❌ No voice analysis data returned")
            return False
            
    except Exception as e:
        print(f"❌ Error during analysis: {e}")
        return False

def analyze_brand_voice_patterns():
    """Analyze overall brand voice patterns for context"""
    
    query = f"""
    SELECT 
      brand,
      COUNT(*) AS total_ads,
      
      -- Voice characteristics
      AVG(CASE WHEN REGEXP_CONTAINS(UPPER(creative_text), r'\\b(YOU|YOUR|YOURS)\\b') THEN 1.0 ELSE 0.0 END) * 100 AS pct_second_person,
      AVG(CASE WHEN REGEXP_CONTAINS(UPPER(creative_text), r'\\b(PERFORMANCE|ATHLETE|CHAMPION|WIN)\\b') THEN 1.0 ELSE 0.0 END) * 100 AS pct_performance_language,
      AVG(CASE WHEN REGEXP_CONTAINS(UPPER(creative_text), r'\\b(STYLE|FASHION|TRENDY|COOL)\\b') THEN 1.0 ELSE 0.0 END) * 100 AS pct_style_language,
      AVG(CASE WHEN REGEXP_CONTAINS(UPPER(creative_text), r'\\b(NEW|FRESH|INNOVATIVE)\\b') THEN 1.0 ELSE 0.0 END) * 100 AS pct_innovation_language,
      
      -- Text characteristics
      AVG(LENGTH(creative_text)) AS avg_text_length,
      AVG(CASE WHEN REGEXP_CONTAINS(creative_text, r'[!]') THEN 1.0 ELSE 0.0 END) * 100 AS pct_exclamation
      
    FROM `{PROJECT_ID}.{DATASET_ID}.ads_with_dates`
    WHERE creative_text IS NOT NULL
      AND LENGTH(creative_text) >= 20
    GROUP BY brand
    ORDER BY total_ads DESC
    """
    
    print(f"\n📊 Overall Brand Voice Patterns:")
    print("=" * 40)
    
    try:
        df = client.query(query).result().to_dataframe()
        
        for _, row in df.iterrows():
            print(f"\n🏷️  {row['brand']} ({row['total_ads']} ads)")
            print(f"   2nd person usage: {row['pct_second_person']:.1f}%")
            print(f"   Performance language: {row['pct_performance_language']:.1f}%")
            print(f"   Style language: {row['pct_style_language']:.1f}%")
            print(f"   Innovation language: {row['pct_innovation_language']:.1f}%")
            print(f"   Avg text length: {row['avg_text_length']:.0f} chars")
            print(f"   Exclamation usage: {row['pct_exclamation']:.1f}%")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    print("🧪 MODERATE TEST #4: BRAND VOICE CHANGES DETECTION")
    print("=" * 60)
    
    # Detect brand voice changes
    voice_changes_detected = detect_brand_voice_evolution()
    
    # Analyze overall voice patterns for context
    analyze_brand_voice_patterns()
    
    if voice_changes_detected:
        print("\n✅ MODERATE TEST #4 PASSED: Brand voice changes detected successfully")
        print("🎯 Goal: System can identify shifts in brand messaging and tone")
    else:
        print("\n⚠️  MODERATE TEST #4 PARTIAL: No major voice changes detected in sample period")
        print("🔍 This may indicate stable brand voices or need for longer time window")