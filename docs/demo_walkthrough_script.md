# 10-Minute Demo Walkthrough Script
## BigQuery AI Competitive Intelligence Pipeline

*Duration: 10 minutes | Tone: Matter-of-fact, clear explanation of results*

---

## **Opening: Problem Motivation (2 minutes)**

### **1. Show Meta Ad Library in Browser [30 seconds]**
*[Open browser to Meta Ad Library: https://www.facebook.com/ads/library/]*

"This is Meta's Ad Library—every ad running on Facebook and Instagram is publicly available here. For marketers, this represents an enormous competitive intelligence opportunity."

*[Search for "Warby Parker" and show results]*

"Here's Warby Parker's current ads. You can see dozens of active campaigns. Now imagine you're a growth marketer and need to monitor not just Warby Parker, but all their competitors. How do you track Zenni Optical, EyeBuyDirect, LensCrafters, and others? How do you detect when someone copies your creative? How do you find market gaps?"

*[Navigate through different competitor searches to show volume]*

"The problem is scale. Each competitor has dozens of ads. Manual analysis is impossible. You need systematic, automated competitive intelligence."

### **2. Solution Overview [30 seconds]**
"We built a BigQuery AI-native pipeline that solves this. It automatically discovers competitors, analyzes their creative strategies, detects copying patterns, and identifies market opportunities—all using only BigQuery's native AI primitives."

### **3. Architecture Overview [1 minute]**
*[Show pipeline flow diagram]*

"Here's how it works: 10 stages, from discovery to intelligence delivery."

*[Point to each major component]*
- "Stage 1-3: Find and validate real competitors using AI consensus"
- "Stage 4-6: Collect ads and generate semantic embeddings"
- "Stage 7-9: Analyze visual content and extract intelligence"
- "Stage 10: Progressive disclosure—filter insights by confidence and impact"

*[Show BigQuery AI primitives diagram]*

"The key innovation: we use only BigQuery's native AI functions. AI.GENERATE_TABLE for batch processing, ML.GENERATE_EMBEDDING for semantic analysis, AI.GENERATE for multimodal intelligence, ML.DISTANCE for copying detection, and ML.FORECAST for trend prediction."

---

## **Demo Execution: Real Results (6 minutes)**

### **4. Notebook Overview [30 seconds]**
*[Open demo notebook]*

"Let me walk you through actual results. We analyzed Warby Parker and discovered 7 real competitors from 466 initial candidates, then processed 582 ads to generate strategic intelligence."

### **5. L1 Executive Insights [1.5 minutes]**
*[Navigate to L1 results in notebook or show JSON]*

"First, L1 executive insights—5 critical findings with 82% confidence:"

**Critical Threat Detection:**
"We detected competitive copying from EyeBuyDirect with 72.9% similarity. This isn't just similar content—our temporal analysis proves the copying direction and timing."

*[Show the specific similarity score: 0.73]*

**Industry Context Issues:**
"Low eyewear industry relevance score of 0.15—Warby Parker's messaging doesn't emphasize eyewear-specific benefits enough."

**Emotional Intelligence Gaps:**
"Emotional intensity score of 0.36 and positive sentiment rate of just 2.07%—opportunities to strengthen emotional appeal."

**Platform Strategy:**
"Perfect platform diversification score of 1.0, but opportunity for better cross-platform synergy."

"Notice these aren't just metrics—they're actionable insights with confidence scores."

### **6. L2 Strategic Intelligence [1.5 minutes]**
*[Show L2 breakdown by intelligence type]*

"L2 provides strategic depth across five intelligence dimensions:"

**Competitive Intelligence:**
"The copying detection has 90% confidence and 80% business impact—this is a priority threat."

**CTA Intelligence:**
"CTA aggressiveness score of 9.77 out of 10—Warby Parker's calls-to-action are extremely aggressive, potentially hurting brand trust."

**Creative Intelligence:**
"Low industry relevance and emotional intensity confirmed through AI analysis of creative content."

**Audience Intelligence:**
"86.1% millennial targeting detected, with 362-character average message length—good efficiency."

**Channel Intelligence:**
"Maximum platform diversification achieved, but zero cross-platform synergy—missed integration opportunity."

### **7. L3 Actionable Interventions [1 minute]**
*[Show specific interventions]*

"L3 provides 16 specific, actionable interventions:"

- "Differentiate creative from EyeBuyDirect copying patterns"
- "Increase eyewear-specific messaging by 40%"
- "Implement cross-platform content synergy"
- "Moderate CTA aggressiveness for brand building"

"Each intervention includes confidence levels, business impact scores, and implementation guidance."

### **8. Technical Innovation Highlight [1.5 minutes]**
*[Show BigQuery console or SQL examples]*

"What makes this powerful: everything runs in BigQuery using native AI functions."

**Copying Detection:**
```sql
ML.DISTANCE(a.embedding, b.embedding, 'COSINE') < 0.3
```
"This mathematical proof shows 72.9% similarity with temporal lag analysis."

**Market Gap Analysis:**
"Our 3D whitespace detector found 6 untapped market opportunities by analyzing messaging angle × funnel stage × target persona combinations."

**Progressive Disclosure:**
"The system filtered 582 ads into 5 critical insights using composite scoring: confidence × business impact × actionability."

"No external vector databases, no separate ML infrastructure—pure BigQuery AI."

---

## **Value Demonstration (1.5 minutes)**

### **9. Business Impact [1 minute]**
"Let's talk real impact:"

**Cost Efficiency:**
"Traditional approaches require separate vector databases, ML serving infrastructure, and complex orchestration. Our BigQuery-native approach eliminates all that complexity."

**Speed to Insight:**
"From 466 competitor candidates to 5 critical insights in one automated run. Manual analysis would take weeks."

**Competitive Advantage:**
"72.9% copying detection with mathematical proof enables immediate competitive response. The 6 market gaps identified represent $150K-300K investment opportunities each."

**Scalability:**
"Adaptive sampling handles portfolios from 10 to 10,000 competitors while controlling costs."

### **10. Next Steps [30 seconds]**
"This demonstrates BigQuery AI's potential for competitive intelligence. The pipeline is modular—you can run individual stages, customize competitor lists, or adapt the progressive disclosure thresholds for your organization."

"The Warby Parker case study proves the concept. Your competitive intelligence transformation is just a BigQuery AI implementation away."

---

## **Closing**
"Questions about the technical implementation, business applications, or BigQuery AI architecture?"

---

## **Demo Flow Notes:**

### **Props Needed:**
- Browser with Meta Ad Library open
- Demo notebook ready to run
- Pipeline architecture diagram
- BigQuery AI primitives diagram
- Results JSON file accessible

### **Key Metrics to Emphasize:**
- **466 → 7 → 582 → 5**: Data funnel efficiency
- **72.9% similarity**: Precise copying detection
- **82% confidence**: Statistical rigor
- **6 market opportunities**: Business value
- **100% BigQuery native**: Technical innovation

### **Timing Checkpoints:**
- 2:00 - Problem established, solution introduced
- 4:00 - Architecture explained, demo started
- 7:00 - L1/L2 results demonstrated
- 9:00 - Technical innovation highlighted
- 10:00 - Value proposition concluded

### **Speaking Tips:**
- Use specific numbers from actual results
- Explain the "so what" for each insight
- Connect technical features to business value
- Maintain steady pace—avoid rushing through complex concepts
- Use the JSON structure to show progressive disclosure in action

This script balances technical depth with business relevance, using real results to demonstrate the pipeline's effectiveness while staying matter-of-fact and educational.