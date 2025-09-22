# === STAGE 8 DEEP DIVE: COMPETITIVE POSITIONING ANALYSIS ===

if 'stage8_results' in locals() and stage8_results is not None:
    print("🔍 === COMPREHENSIVE COMPETITIVE INTELLIGENCE ANALYSIS ===")
    print("=" * 70)

    # Import required libraries for analysis and visualization
    import pandas as pd
    import numpy as np
    from src.utils.bigquery_client import run_query
    import os

    BQ_PROJECT = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
    BQ_DATASET = os.environ.get("BQ_DATASET", "ads_demo")

    print(f"\n📊 1. COMPETITIVE CTA STRATEGY ANALYSIS")
    print("=" * 50)

    # Get comprehensive CTA analysis from the corrected table
    try:
        cta_positioning_query = f"""
        SELECT
            brand,
            total_ads,
            avg_cta_aggressiveness,
            cta_aggressiveness_stddev,
            urgency_driven_ctas,
            action_focused_ctas,
            exploratory_ctas,
            soft_sell_ctas,
            ultra_aggressive_count,
            aggressive_count,
            moderate_count,
            consultative_count,
            minimal_count,
            dominant_cta_strategy,
            -- Calculate meaningful percentages
            ROUND(urgency_driven_ctas * 100.0 / GREATEST(total_ads, 1), 1) as urgency_driven_pct,
            ROUND(action_focused_ctas * 100.0 / GREATEST(total_ads, 1), 1) as action_focused_pct,
            ROUND(exploratory_ctas * 100.0 / GREATEST(total_ads, 1), 1) as exploratory_pct,
            ROUND(soft_sell_ctas * 100.0 / GREATEST(total_ads, 1), 1) as soft_sell_pct,
            ROUND(ultra_aggressive_count * 100.0 / GREATEST(total_ads, 1), 1) as ultra_aggressive_pct,
            ROUND(aggressive_count * 100.0 / GREATEST(total_ads, 1), 1) as aggressive_pct,
            ROUND(moderate_count * 100.0 / GREATEST(total_ads, 1), 1) as moderate_pct,
            ROUND(consultative_count * 100.0 / GREATEST(total_ads, 1), 1) as consultative_pct,
            ROUND(minimal_count * 100.0 / GREATEST(total_ads, 1), 1) as minimal_pct,
            -- Market positioning metrics
            RANK() OVER (ORDER BY avg_cta_aggressiveness DESC) as aggressiveness_rank
        FROM `{BQ_PROJECT}.{BQ_DATASET}.cta_aggressiveness_analysis`
        ORDER BY avg_cta_aggressiveness DESC
        """

        cta_df = run_query(cta_positioning_query)

        if not cta_df.empty:
            print(f"   📈 Brand CTA Strategy Rankings (All {len(cta_df)} Brands):")
            for _, row in cta_df.iterrows():
                target_indicator = "🎯" if row['brand'] == context.brand else "🔸"
                print(f"   {target_indicator} #{row['aggressiveness_rank']:0.0f} {row['brand']}: {row['avg_cta_aggressiveness']:.1f}/10 ({row['dominant_cta_strategy']})")
                print(f"      📊 {row['total_ads']} ads | Urgency: {row['urgency_driven_pct']:.1f}% | Action: {row['action_focused_pct']:.1f}% | Exploratory: {row['exploratory_pct']:.1f}%")

            # Target brand detailed analysis
            target_row = cta_df[cta_df['brand'] == context.brand]
            if not target_row.empty:
                target_data = target_row.iloc[0]
                market_median = cta_df['avg_cta_aggressiveness'].median()

                print(f"\n   🎯 {context.brand} DETAILED CTA STRATEGY PROFILE:")
                print(f"      Overall Aggressiveness: {target_data['avg_cta_aggressiveness']:.2f}/10 (±{target_data['cta_aggressiveness_stddev']:.2f})")
                print(f"      Rank: #{target_data['aggressiveness_rank']:0.0f} of {len(cta_df)} brands")
                print(f"      Dominant Strategy: {target_data['dominant_cta_strategy']}")
                print(f"      Strategy Mix:")
                print(f"        • Urgency-Driven: {target_data['urgency_driven_pct']:.1f}% ({target_data['urgency_driven_ctas']} ads)")
                print(f"        • Action-Focused: {target_data['action_focused_pct']:.1f}% ({target_data['action_focused_ctas']} ads)")
                print(f"        • Exploratory: {target_data['exploratory_pct']:.1f}% ({target_data['exploratory_ctas']} ads)")
                print(f"        • Soft-Sell: {target_data['soft_sell_pct']:.1f}% ({target_data['soft_sell_ctas']} ads)")

                # Market comparison
                if target_data['avg_cta_aggressiveness'] > market_median + 1.0:
                    print(f"      📈 SIGNIFICANTLY MORE AGGRESSIVE than market median ({market_median:.2f})")
                elif target_data['avg_cta_aggressiveness'] > market_median:
                    print(f"      📊 ABOVE MARKET median aggressiveness ({market_median:.2f})")
                elif target_data['avg_cta_aggressiveness'] < market_median - 1.0:
                    print(f"      📉 SIGNIFICANTLY LESS AGGRESSIVE than market median ({market_median:.2f})")
                else:
                    print(f"      📍 ALIGNED WITH MARKET median ({market_median:.2f})")

            # Market overview
            print(f"\n   🌍 MARKET CTA STRATEGY OVERVIEW:")
            print(f"      Total Brands: {len(cta_df)}")
            print(f"      Market Median Aggressiveness: {cta_df['avg_cta_aggressiveness'].median():.2f}/10")
            print(f"      Most Aggressive: {cta_df.iloc[0]['brand']} ({cta_df.iloc[0]['avg_cta_aggressiveness']:.1f}/10)")
            print(f"      Most Conservative: {cta_df.iloc[-1]['brand']} ({cta_df.iloc[-1]['avg_cta_aggressiveness']:.1f}/10)")

    except Exception as e:
        print(f"   ⚠️ Error in CTA analysis: {e}")
        import traceback
        traceback.print_exc()

    print(f"\n📊 2. STRATEGIC RECOMMENDATIONS & INTERVENTIONS")
    print("=" * 50)

    # Generate strategic recommendations based on the analysis
    current_state = stage8_results.current_state
    influence = stage8_results.influence

    print(f"   🎯 IMMEDIATE TACTICAL RECOMMENDATIONS:")

    # CTA Strategy Recommendations using corrected data
    if 'target_data' in locals():
        cta_score = target_data['avg_cta_aggressiveness']
        cta_consistency = 10 - min(target_data['cta_aggressiveness_stddev'], 10)  # Higher consistency = better

        if cta_score > 8.0:
            print(f"   📈 CTA STRATEGY: Consider moderating ultra-aggressive approach")
            print(f"      Risk: Potential customer fatigue from high-pressure tactics")
            print(f"      Current score: {cta_score:.1f}/10 - Above ultra-aggressive threshold")
        elif cta_score < 4.0:
            print(f"   📈 CTA STRATEGY: Opportunity to increase call-to-action intensity")
            print(f"      Opportunity: More aggressive CTAs could drive higher conversion")
            print(f"      Current score: {cta_score:.1f}/10 - Below moderate threshold")
        else:
            print(f"   📈 CTA STRATEGY: Current aggressiveness level is well-positioned")
            print(f"      Current score: {cta_score:.1f}/10 - Optimal range")

        # Strategy mix recommendations
        if target_data['urgency_driven_pct'] > 50:
            print(f"   ⚠️ URGENCY OVERLOAD: {target_data['urgency_driven_pct']:.1f}% urgency-driven may cause fatigue")
        elif target_data['urgency_driven_pct'] < 10:
            print(f"   💡 URGENCY OPPORTUNITY: Only {target_data['urgency_driven_pct']:.1f}% urgency-driven - consider testing more")

        if target_data['exploratory_pct'] < 5:
            print(f"   🔍 EDUCATION GAP: Only {target_data['exploratory_pct']:.1f}% exploratory CTAs - missing nurture opportunities")

    # Fatigue and copying analysis (existing logic)
    fatigue_score = current_state.get('avg_fatigue_score', 0)
    if fatigue_score > 0.6:
        print(f"   🎨 CREATIVE STRATEGY: URGENT - Creative refresh needed")
        print(f"      Fatigue level: {fatigue_score:.3f} - HIGH risk")
    elif fatigue_score > 0.4:
        print(f"   🎨 CREATIVE STRATEGY: Monitor creative performance closely")
        print(f"      Fatigue level: {fatigue_score:.3f} - MEDIUM risk")
    else:
        print(f"   🎨 CREATIVE STRATEGY: Creative freshness is strong")
        print(f"      Fatigue level: {fatigue_score:.3f} - LOW risk")

    if influence.get('copying_detected', False):
        similarity_score = influence.get('similarity_score', 0)
        copier = influence.get('top_copier', 'Unknown')
        if similarity_score > 0.7:
            print(f"   ⚠️ COMPETITIVE THREAT: HIGH similarity with {copier} ({similarity_score:.3f})")
        else:
            print(f"   📊 COMPETITIVE MONITORING: Moderate similarity with {copier} ({similarity_score:.3f})")
    else:
        print(f"   ✅ COMPETITIVE POSITION: No significant copying detected")

    print(f"\n📊 3. ENHANCED CTA STRATEGY VISUALIZATIONS")
    print("=" * 50)

    # Import visualization libraries with enhanced configuration
    try:
        import matplotlib.pyplot as plt
        import seaborn as sns
        from matplotlib import rcParams
        import matplotlib.patches as patches

        # ENHANCED HIGH-QUALITY PLOTTING CONFIGURATION
        plt.style.use('default')
        rcParams['figure.dpi'] = 150  # High DPI for sharp plots
        rcParams['savefig.dpi'] = 300  # Even higher for saved figures
        rcParams['font.size'] = 12
        rcParams['axes.titlesize'] = 16
        rcParams['axes.labelsize'] = 13
        rcParams['xtick.labelsize'] = 11
        rcParams['ytick.labelsize'] = 11
        rcParams['legend.fontsize'] = 12
        rcParams['font.family'] = 'sans-serif'
        rcParams['font.sans-serif'] = ['Arial', 'DejaVu Sans', 'Liberation Sans']
        rcParams['axes.linewidth'] = 1.2
        rcParams['grid.linewidth'] = 0.8
        rcParams['lines.linewidth'] = 2.0

        # Enhanced professional color palette
        brand_colors = {
            'target': '#e74c3c',      # Red for target brand
            'competitor': '#3498db',   # Blue for competitors
            'palette': ['#3498db', '#e67e22', '#2ecc71', '#9b59b6', '#f39c12',
                       '#1abc9c', '#34495e', '#e91e63', '#ff9800', '#607d8b']
        }

        print(f"   📈 Generating enhanced CTA strategy visualizations...")

        if not cta_df.empty:
            # === NEW VISUALIZATION 1: CTA STRATEGY RADAR CHART ===
            print(f"\n   🎯 1. CTA Strategy Radar Chart (HIGH-RESOLUTION)")

            # Create the definitive CTA strategy radar chart
            fig, ax = plt.subplots(figsize=(14, 14), subplot_kw=dict(projection='polar'))

            # Define meaningful radar metrics (0-100 scale for readability)
            metrics = ['Aggressiveness\n(0-10)', 'Consistency\n(0-10)', 'Urgency Focus\n(%)',
                      'Action Focus\n(%)', 'Exploratory\n(%)', 'Soft Sell\n(%)']

            # Calculate radar data for each brand with proper normalization
            radar_data = []
            for _, row in cta_df.iterrows():
                # Calculate consistency score (inverse of standard deviation, scaled 0-10)
                consistency_score = max(0, 10 - row['cta_aggressiveness_stddev'])

                brand_values = [
                    row['avg_cta_aggressiveness'] * 10,  # Scale 0-100 for visibility
                    consistency_score * 10,               # Scale 0-100 for visibility
                    row['urgency_driven_pct'],            # Already in percentage
                    row['action_focused_pct'],            # Already in percentage
                    row['exploratory_pct'],               # Already in percentage
                    row['soft_sell_pct']                  # Already in percentage
                ]
                radar_data.append(brand_values)

            # Set up radar chart angles
            angles = np.linspace(0, 2 * np.pi, len(metrics), endpoint=False).tolist()
            angles += angles[:1]  # Complete the circle

            # Plot each brand with enhanced styling
            for i, (_, row) in enumerate(cta_df.iterrows()):
                brand_values = radar_data[i] + radar_data[i][:1]  # Complete the circle

                # Enhanced color and styling
                if row['brand'] == context.brand:
                    color = brand_colors['target']
                    alpha = 0.9
                    linewidth = 4
                    marker_size = 8
                    zorder = 10
                else:
                    color = brand_colors['palette'][i % len(brand_colors['palette'])]
                    alpha = 0.7
                    linewidth = 2.5
                    marker_size = 6
                    zorder = 5

                # Plot with enhanced markers and fill
                ax.plot(angles, brand_values, 'o-', linewidth=linewidth,
                       label=row['brand'], color=color, alpha=alpha,
                       markersize=marker_size, zorder=zorder)
                ax.fill(angles, brand_values, alpha=0.15, color=color, zorder=1)

            # Enhanced radar chart customization
            ax.set_xticks(angles[:-1])
            ax.set_xticklabels(metrics, fontsize=12, fontweight='bold')
            ax.set_ylim(0, 100)
            ax.set_yticks([20, 40, 60, 80, 100])
            ax.set_yticklabels(['20', '40', '60', '80', '100'], fontsize=10, alpha=0.7)
            ax.grid(True, alpha=0.4, linewidth=1)

            # Add reference rings for better readability
            for y in [25, 50, 75]:
                ax.plot(angles, [y] * len(angles), color='gray', linewidth=0.5, alpha=0.3, linestyle='--')

            # Enhanced title and legend
            ax.set_title(f'CTA Strategy Profile Radar\n{context.brand} vs {len(cta_df)-1} Competitors',
                        fontsize=16, fontweight='bold', pad=30)
            ax.legend(loc='upper right', bbox_to_anchor=(1.35, 1.0),
                     frameon=True, fancybox=True, shadow=True, fontsize=11)

            plt.tight_layout()
            plt.show()

            # === NEW VISUALIZATION 2: CTA STRATEGY MIX COMPARISON ===
            print(f"\n   📊 2. CTA Strategy Mix Comparison (HIGH-RESOLUTION)")

            # Create enhanced side-by-side comparison
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 10))

            # Enhanced color scheme for CTA strategies
            strategy_colors = {
                'Urgency Driven': '#e74c3c',    # Red for urgency
                'Action Focused': '#f39c12',    # Orange for action
                'Exploratory': '#3498db',       # Blue for exploration
                'Soft Sell': '#2ecc71'          # Green for soft sell
            }

            # Left plot: Strategy percentages (normalized and meaningful)
            brands = cta_df['brand']
            x_pos = range(len(brands))

            # Create stacked percentage bars
            urgency_pct = cta_df['urgency_driven_pct']
            action_pct = cta_df['action_focused_pct']
            exploratory_pct = cta_df['exploratory_pct']
            soft_pct = cta_df['soft_sell_pct']

            width = 0.7
            bars1 = ax1.bar(x_pos, urgency_pct, width, label='Urgency Driven',
                           color=strategy_colors['Urgency Driven'], alpha=0.9, edgecolor='white', linewidth=1.5)
            bars2 = ax1.bar(x_pos, action_pct, width, bottom=urgency_pct, label='Action Focused',
                           color=strategy_colors['Action Focused'], alpha=0.9, edgecolor='white', linewidth=1.5)
            bars3 = ax1.bar(x_pos, exploratory_pct, width, bottom=urgency_pct+action_pct, label='Exploratory',
                           color=strategy_colors['Exploratory'], alpha=0.9, edgecolor='white', linewidth=1.5)
            bars4 = ax1.bar(x_pos, soft_pct, width, bottom=urgency_pct+action_pct+exploratory_pct,
                           label='Soft Sell', color=strategy_colors['Soft Sell'], alpha=0.9, edgecolor='white', linewidth=1.5)

            # Enhanced labels for significant segments
            for i, brand in enumerate(brands):
                if urgency_pct.iloc[i] > 15:  # Show label if >15%
                    ax1.text(i, urgency_pct.iloc[i]/2, f"{urgency_pct.iloc[i]:.0f}%",
                            ha='center', va='center', fontweight='bold', color='white', fontsize=10)
                if action_pct.iloc[i] > 15:
                    ax1.text(i, urgency_pct.iloc[i] + action_pct.iloc[i]/2, f"{action_pct.iloc[i]:.0f}%",
                            ha='center', va='center', fontweight='bold', color='white', fontsize=10)

            # Enhanced styling for left plot
            ax1.set_xlabel('Brands', fontsize=14, fontweight='bold')
            ax1.set_ylabel('Strategy Distribution (%)', fontsize=14, fontweight='bold')
            ax1.set_title('CTA Strategy Mix by Brand\n(Percentage Distribution)', fontsize=15, fontweight='bold', pad=20)
            ax1.set_xticks(x_pos)
            enhanced_labels = [f"**{brand}**" if brand == context.brand else brand for brand in brands]
            ax1.set_xticklabels(enhanced_labels, rotation=45, ha='right', fontsize=12)
            ax1.legend(loc='upper right', frameon=True, fancybox=True, shadow=True)
            ax1.grid(axis='y', alpha=0.4, linestyle='--', linewidth=0.8)
            ax1.spines['top'].set_visible(False)
            ax1.spines['right'].set_visible(False)
            ax1.set_ylim(0, 100)

            # Right plot: Aggressiveness vs Consistency scatter
            colors = [brand_colors['target'] if brand == context.brand else brand_colors['competitor']
                     for brand in brands]
            sizes = [500 if brand == context.brand else 250 for brand in brands]

            # Calculate consistency scores
            consistency_scores = [max(0, 10 - row['cta_aggressiveness_stddev']) for _, row in cta_df.iterrows()]

            scatter = ax2.scatter(cta_df['avg_cta_aggressiveness'], consistency_scores,
                                c=colors, s=sizes, alpha=0.8,
                                edgecolors='white', linewidth=2.5, zorder=3)

            # Enhanced brand labels
            for i, (_, row) in enumerate(cta_df.iterrows()):
                label_color = 'white' if row['brand'] == context.brand else 'black'
                font_weight = 'bold' if row['brand'] == context.brand else 'normal'
                font_size = 12 if row['brand'] == context.brand else 10

                ax2.annotate(row['brand'],
                           (row['avg_cta_aggressiveness'], consistency_scores[i]),
                           xytext=(8, 8), textcoords='offset points',
                           fontsize=font_size, fontweight=font_weight,
                           color=label_color,
                           bbox=dict(boxstyle='round,pad=0.3',
                                   facecolor=colors[i], alpha=0.8, edgecolor='white'))

            # Enhanced quadrant lines
            median_aggr = cta_df['avg_cta_aggressiveness'].median()
            median_cons = np.median(consistency_scores)

            ax2.axhline(y=median_cons, color='#7f8c8d', linestyle='--', alpha=0.7, linewidth=2)
            ax2.axvline(x=median_aggr, color='#7f8c8d', linestyle='--', alpha=0.7, linewidth=2)

            # Quadrant labels
            ax2.text(0.02, 0.98, 'Low Aggression\nHigh Consistency', transform=ax2.transAxes,
                   fontsize=10, alpha=0.6, ha='left', va='top',
                   bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.4))
            ax2.text(0.98, 0.98, 'High Aggression\nHigh Consistency', transform=ax2.transAxes,
                   fontsize=10, alpha=0.6, ha='right', va='top',
                   bbox=dict(boxstyle='round', facecolor='gold', alpha=0.4))
            ax2.text(0.02, 0.02, 'Low Aggression\nLow Consistency', transform=ax2.transAxes,
                   fontsize=10, alpha=0.6, ha='left', va='bottom',
                   bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.4))
            ax2.text(0.98, 0.02, 'High Aggression\nLow Consistency', transform=ax2.transAxes,
                   fontsize=10, alpha=0.6, ha='right', va='bottom',
                   bbox=dict(boxstyle='round', facecolor='lightcoral', alpha=0.4))

            # Enhanced styling for right plot
            ax2.set_xlabel('CTA Aggressiveness Score (0-10)', fontsize=14, fontweight='bold')
            ax2.set_ylabel('CTA Consistency Score (0-10)', fontsize=14, fontweight='bold')
            ax2.set_title('Aggressiveness vs Consistency Matrix\n(Strategic Positioning)', fontsize=15, fontweight='bold', pad=20)
            ax2.grid(True, alpha=0.4, linestyle='-', linewidth=0.8)
            ax2.spines['top'].set_visible(False)
            ax2.spines['right'].set_visible(False)
            ax2.set_xlim(0, 10)
            ax2.set_ylim(0, 10)

            plt.tight_layout()
            plt.show()

            # === NEW VISUALIZATION 3: COMPETITIVE AGGRESSIVENESS RANKING ===
            print(f"\n   🏆 3. Competitive Aggressiveness Ranking (HIGH-RESOLUTION)")

            # Create enhanced ranking visualization
            fig, ax = plt.subplots(figsize=(16, 12))

            # Sort by aggressiveness for ranking display
            sorted_df = cta_df.sort_values('avg_cta_aggressiveness', ascending=True)

            # Create horizontal bars with gradient effect
            colors = []
            for brand in sorted_df['brand']:
                if brand == context.brand:
                    colors.append('#e74c3c')  # Target brand
                else:
                    colors.append('#3498db')  # Competitors

            y_pos = range(len(sorted_df))
            bars = ax.barh(y_pos, sorted_df['avg_cta_aggressiveness'],
                          color=colors, alpha=0.85, edgecolor='white', linewidth=2,
                          height=0.7)

            # Add aggressiveness score labels
            for i, (_, row) in enumerate(sorted_df.iterrows()):
                score = row['avg_cta_aggressiveness']
                consistency = max(0, 10 - row['cta_aggressiveness_stddev'])
                ax.text(score + 0.15, i, f"{score:.1f}/10",
                       va='center', ha='left', fontweight='bold', fontsize=11,
                       bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.9))

                # Add consistency indicator
                ax.text(score + 1.8, i, f"±{row['cta_aggressiveness_stddev']:.1f}",
                       va='center', ha='left', fontsize=9, alpha=0.7, style='italic')

            # Enhanced y-axis labels with strategy info
            enhanced_labels = []
            for _, row in sorted_df.iterrows():
                brand_name = f"**{row['brand']}**" if row['brand'] == context.brand else row['brand']
                strategy = row['dominant_cta_strategy'].replace('_', ' ').title()
                label = f"#{row['aggressiveness_rank']:.0f} {brand_name}\n({strategy})"
                enhanced_labels.append(label)

            ax.set_yticks(y_pos)
            ax.set_yticklabels(enhanced_labels, fontsize=11)

            # Enhanced styling with competitive zones
            ax.axvspan(0, 3, alpha=0.1, color='green', label='Conservative (0-3)')
            ax.axvspan(3, 5, alpha=0.1, color='yellow', label='Moderate (3-5)')
            ax.axvspan(5, 7, alpha=0.1, color='orange', label='Aggressive (5-7)')
            ax.axvspan(7, 10, alpha=0.1, color='red', label='Ultra-Aggressive (7-10)')

            # Market median line
            market_median = sorted_df['avg_cta_aggressiveness'].median()
            ax.axvline(x=market_median, color='#34495e', linestyle='--', alpha=0.8, linewidth=3,
                      label=f'Market Median ({market_median:.1f})')

            # Enhanced styling
            ax.set_xlabel('CTA Aggressiveness Score (0-10)', fontsize=14, fontweight='bold')
            ax.set_title(f'Competitive CTA Aggressiveness Rankings\n{context.brand} vs {len(sorted_df)-1} Competitors',
                        fontsize=16, fontweight='bold', pad=25)
            ax.legend(loc='lower right', frameon=True, fancybox=True, shadow=True, fontsize=10)
            ax.grid(axis='x', alpha=0.4, linestyle='--', linewidth=0.8)
            ax.spines['top'].set_visible(False)
            ax.spines['right'].set_visible(False)
            ax.set_xlim(0, max(sorted_df['avg_cta_aggressiveness']) * 1.2)

            plt.tight_layout()
            plt.show()

            print(f"   ✅ Generated 3 enhanced CTA strategy visualizations")
            print(f"   🎯 Radar chart with meaningful 0-100 scale metrics")
            print(f"   📊 Strategy mix with proper percentage normalization")
            print(f"   🏆 Competitive ranking with consistency indicators")
            print(f"   💡 All {len(cta_df)} brands properly analyzed with corrected CTA metrics")

    except ImportError as e:
        print(f"   ⚠️ Visualization libraries not available: {e}")
        print(f"   💡 Install with: pip install matplotlib seaborn")
    except Exception as e:
        print(f"   ⚠️ Error generating visualizations: {e}")
        import traceback
        traceback.print_exc()

    print(f"\n🎯 ENHANCED CTA COMPETITIVE INTELLIGENCE COMPLETE")
    print("=" * 70)
    print(f"✅ Complete CTA strategy analysis with meaningful metrics")
    print(f"📊 All {len(cta_df) if 'cta_df' in locals() and not cta_df.empty else 'available'} brands analyzed for aggressiveness, consistency, and strategy mix")
    print(f"🎯 Strategic recommendations based on corrected CTA scoring (0-10 scale)")
    print(f"📈 3 professional visualizations: radar chart, strategy mix, competitive ranking")
    print(f"💼 Executive-ready insights using proper percentage normalization and meaningful categories")

else:
    print("❌ Stage 8 results not available - run Stage 8 first")