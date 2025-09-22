# === FIXED STAGE 8 VISUALIZATIONS ===

# Fix 1: Consistent radar chart scales (all 0-100)
# Fix 2: Proper axis centering for scatter plots
# Fix 3: Correct category labels
# Fix 4: Move decisioning logic to SQL queries

if not cta_df.empty:
    # === FIXED VISUALIZATION 1: CTA STRATEGY RADAR CHART ===
    print(f"\n   🎯 1. FIXED CTA Strategy Radar Chart (Consistent 0-100 Scale)")

    # Create the radar chart with CONSISTENT 0-100 scale for all metrics
    fig, ax = plt.subplots(figsize=(14, 14), subplot_kw=dict(projection='polar'))

    # Define metrics with consistent 0-100 scale for readability
    metrics = ['Aggressiveness\n(0-100)', 'Consistency\n(0-100)', 'Urgency Focus\n(%)',
              'Action Focus\n(%)', 'Exploratory\n(%)', 'Soft Sell\n(%)']

    # Calculate radar data with CONSISTENT 0-100 scaling
    radar_data = []
    for _, row in cta_df.iterrows():
        # Calculate consistency score (inverse of standard deviation, scaled 0-100)
        consistency_score = max(0, (10 - row['cta_aggressiveness_stddev']) * 10)  # 0-100 scale

        brand_values = [
            row['avg_cta_aggressiveness'] * 10,  # 0-10 → 0-100 scale
            consistency_score,                    # 0-100 scale
            row['urgency_driven_pct'],           # Already 0-100 percentage
            row['action_focused_pct'],           # Already 0-100 percentage
            row['exploratory_pct'],              # Already 0-100 percentage
            row['soft_sell_pct']                 # Already 0-100 percentage
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

    # Enhanced radar chart customization with consistent 0-100 scale
    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(metrics, fontsize=12, fontweight='bold')
    ax.set_ylim(0, 100)  # Consistent 0-100 scale
    ax.set_yticks([20, 40, 60, 80, 100])
    ax.set_yticklabels(['20', '40', '60', '80', '100'], fontsize=10, alpha=0.7)
    ax.grid(True, alpha=0.4, linewidth=1)

    # Add reference rings for better readability
    for y in [25, 50, 75]:
        ax.plot(angles, [y] * len(angles), color='gray', linewidth=0.5, alpha=0.3, linestyle='--')

    # Enhanced title and legend
    ax.set_title(f'CTA Strategy Profile Radar (Consistent 0-100 Scale)\n{context.brand} vs {len(cta_df)-1} Competitors',
                fontsize=16, fontweight='bold', pad=30)
    ax.legend(loc='upper right', bbox_to_anchor=(1.35, 1.0),
             frameon=True, fancybox=True, shadow=True, fontsize=11)

    plt.tight_layout()
    plt.show()

    # === FIXED VISUALIZATION 2: CENTERED AGGRESSIVENESS VS CONSISTENCY ===
    print(f"\n   📊 2. FIXED Aggressiveness vs Consistency (Properly Centered)")

    fig, ax = plt.subplots(figsize=(12, 10))

    # Calculate consistency scores properly
    consistency_scores = [max(0, 10 - row['cta_aggressiveness_stddev']) for _, row in cta_df.iterrows()]

    # Enhanced colors and sizes
    colors = [brand_colors['target'] if brand == context.brand else brand_colors['competitor']
             for brand in cta_df['brand']]
    sizes = [500 if brand == context.brand else 250 for brand in cta_df['brand']]

    scatter = ax.scatter(cta_df['avg_cta_aggressiveness'], consistency_scores,
                        c=colors, s=sizes, alpha=0.8,
                        edgecolors='white', linewidth=2.5, zorder=3)

    # Enhanced brand labels
    for i, (_, row) in enumerate(cta_df.iterrows()):
        label_color = 'white' if row['brand'] == context.brand else 'black'
        font_weight = 'bold' if row['brand'] == context.brand else 'normal'
        font_size = 12 if row['brand'] == context.brand else 10

        ax.annotate(row['brand'],
                   (row['avg_cta_aggressiveness'], consistency_scores[i]),
                   xytext=(8, 8), textcoords='offset points',
                   fontsize=font_size, fontweight=font_weight,
                   color=label_color,
                   bbox=dict(boxstyle='round,pad=0.3',
                           facecolor=colors[i], alpha=0.8, edgecolor='white'))

    # FIXED: Properly center the plot based on actual data range
    min_aggr, max_aggr = cta_df['avg_cta_aggressiveness'].min(), cta_df['avg_cta_aggressiveness'].max()
    min_cons, max_cons = min(consistency_scores), max(consistency_scores)

    # Add reasonable padding and center the view
    aggr_padding = (max_aggr - min_aggr) * 0.2 if max_aggr > min_aggr else 1.0
    cons_padding = (max_cons - min_cons) * 0.2 if max_cons > min_cons else 1.0

    ax.set_xlim(max(0, min_aggr - aggr_padding), min(10, max_aggr + aggr_padding))
    ax.set_ylim(max(0, min_cons - cons_padding), min(10, max_cons + cons_padding))

    # Add meaningful reference lines based on data
    median_aggr = cta_df['avg_cta_aggressiveness'].median()
    median_cons = np.median(consistency_scores)

    ax.axhline(y=median_cons, color='#7f8c8d', linestyle='--', alpha=0.7, linewidth=2,
              label=f'Median Consistency ({median_cons:.1f})')
    ax.axvline(x=median_aggr, color='#7f8c8d', linestyle='--', alpha=0.7, linewidth=2,
              label=f'Median Aggressiveness ({median_aggr:.1f})')

    # Enhanced styling
    ax.set_xlabel('CTA Aggressiveness Score (0-10)', fontsize=14, fontweight='bold')
    ax.set_ylabel('CTA Consistency Score (0-10)', fontsize=14, fontweight='bold')
    ax.set_title('Aggressiveness vs Consistency Matrix\n(Properly Centered View)',
                fontsize=15, fontweight='bold', pad=20)
    ax.grid(True, alpha=0.4, linestyle='-', linewidth=0.8)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.legend(fontsize=10)

    plt.tight_layout()
    plt.show()

    # === FIXED VISUALIZATION 3: CORRECTED RANKING WITH PROPER CATEGORIES ===
    print(f"\n   🏆 3. FIXED Competitive Ranking (Corrected Categories)")

    fig, ax = plt.subplots(figsize=(16, 12))

    # Sort by aggressiveness for ranking display
    sorted_df = cta_df.sort_values('avg_cta_aggressiveness', ascending=True)

    # Create horizontal bars
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

        # FIXED: Correct category determination
        if score >= 7.0:
            category = 'ULTRA_AGGRESSIVE'
        elif score >= 5.0:
            category = 'AGGRESSIVE'
        elif score >= 3.0:
            category = 'MODERATE'
        elif score >= 1.0:
            category = 'CONSULTATIVE'
        else:
            category = 'MINIMAL'

        ax.text(score + 0.15, i, f"{score:.1f}/10",
               va='center', ha='left', fontweight='bold', fontsize=11,
               bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.9))

        # Add consistency indicator
        ax.text(score + 1.5, i, f"±{row['cta_aggressiveness_stddev']:.1f}",
               va='center', ha='left', fontsize=9, alpha=0.7, style='italic')

    # FIXED: Enhanced y-axis labels with CORRECT categories
    enhanced_labels = []
    for _, row in sorted_df.iterrows():
        brand_name = f"**{row['brand']}**" if row['brand'] == context.brand else row['brand']

        # Calculate correct category
        score = row['avg_cta_aggressiveness']
        if score >= 7.0:
            category = 'ULTRA_AGGRESSIVE'
        elif score >= 5.0:
            category = 'AGGRESSIVE'
        elif score >= 3.0:
            category = 'MODERATE'
        elif score >= 1.0:
            category = 'CONSULTATIVE'
        else:
            category = 'MINIMAL'

        label = f"#{row['aggressiveness_rank']:.0f} {brand_name}\n({category})"
        enhanced_labels.append(label)

    ax.set_yticks(y_pos)
    ax.set_yticklabels(enhanced_labels, fontsize=11)

    # FIXED: Enhanced styling with CORRECT competitive zones
    ax.axvspan(0, 1, alpha=0.1, color='lightblue', label='Minimal (0-1)')
    ax.axvspan(1, 3, alpha=0.1, color='lightgreen', label='Consultative (1-3)')
    ax.axvspan(3, 5, alpha=0.1, color='yellow', label='Moderate (3-5)')
    ax.axvspan(5, 7, alpha=0.1, color='orange', label='Aggressive (5-7)')
    ax.axvspan(7, 10, alpha=0.1, color='red', label='Ultra-Aggressive (7-10)')

    # Market median line
    market_median = sorted_df['avg_cta_aggressiveness'].median()
    ax.axvline(x=market_median, color='#34495e', linestyle='--', alpha=0.8, linewidth=3,
              label=f'Market Median ({market_median:.1f})')

    # Enhanced styling
    ax.set_xlabel('CTA Aggressiveness Score (0-10)', fontsize=14, fontweight='bold')
    ax.set_title(f'FIXED Competitive CTA Aggressiveness Rankings\n{context.brand} vs {len(sorted_df)-1} Competitors',
                fontsize=16, fontweight='bold', pad=25)
    ax.legend(loc='lower right', frameon=True, fancybox=True, shadow=True, fontsize=10)
    ax.grid(axis='x', alpha=0.4, linestyle='--', linewidth=0.8)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.set_xlim(0, max(sorted_df['avg_cta_aggressiveness']) * 1.1)

    plt.tight_layout()
    plt.show()

    print(f"   ✅ Generated 3 FIXED CTA strategy visualizations")
    print(f"   🎯 Consistent 0-100 scale radar chart")
    print(f"   📊 Properly centered aggressiveness vs consistency plot")
    print(f"   🏆 Corrected category labels and competitive zones")