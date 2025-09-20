# BigQuery AI vs ML Functions - Complete Reference Guide (2025)

## Executive Summary

**AI.GENERATE is the newer, recommended approach** for BigQuery generative AI tasks as of 2025. While both AI.GENERATE and ML.GENERATE_TEXT can work with Gemini models, AI.GENERATE offers simpler integration and represents Google's current direction for embedding AI directly into SQL workflows.

## Function Comparison

### AI.GENERATE Functions (Newer - Recommended)

**Introduction Date**: 2025 (Latest update: 2025-08-26 UTC)
**Status**: Current recommended approach

#### Key Characteristics:
- **Scalar AI Functions**: Bring LLM power to each row of data
- **Direct Integration**: No remote model creation required
- **Modern Architecture**: "Building blocks for the even more intuitive and powerful AI Query Engine operators on the horizon"
- **Simplified Setup**: Works directly with Vertex AI endpoints
- **Full Gemini Support**: Supports any generally available or preview Gemini model including Gemini 2.5

#### Available Functions:
- `AI.GENERATE` - Generate text/analysis
- `AI.GENERATE_INT` - Generate integer values
- `AI.GENERATE_BOOL` - Generate boolean values
- `AI.GENERATE_DOUBLE` - Generate double values
- `AI.GENERATE_TABLE` - Generate structured tables

### ML.GENERATE_TEXT Function (Older)

**Introduction Date**: Earlier BigQuery ML era
**Status**: Legacy approach requiring remote models

#### Key Characteristics:
- **Remote Model Required**: Must create BigQuery ML remote model first
- **Complex Setup**: Requires connection + model creation workflow
- **Broader Compatibility**: Supports both Gemini and open models
- **Legacy Architecture**: Part of older BigQuery ML framework

## Correct AI.GENERATE Syntax (2025)

### Function Signature:
```sql
AI.GENERATE(
  [ prompt => ] 'PROMPT',
  connection_id => 'PROJECT_ID.LOCATION.CONNECTION_ID',
  [ endpoint => 'ENDPOINT' ],
  [ request_type => 'REQUEST_TYPE' ],
  [ model_params => MODEL_PARAMS ],
  [ output_schema => 'OUTPUT_SCHEMA' ]
)
```

### Critical Parameters:

#### Connection ID Format:
```sql
connection_id => 'PROJECT_ID.LOCATION.CONNECTION_ID'
```
**Example**: `'bigquery-ai-kaggle-469620.us.vertex-ai'`

**Components**:
- `PROJECT_ID`: Your Google Cloud project ID
- `LOCATION`: Connection location (must match dataset location)
- `CONNECTION_ID`: Your connection name

#### Endpoint Parameter (Optional):
```sql
endpoint => 'ENDPOINT'
```
**Options**:
- Specific model: `'gemini-2.0-flash'`, `'gemini-2.5-flash'`, etc.
- **Default**: If omitted, BigQuery ML selects a recent stable Gemini version

### Example Usage:
```sql
SELECT
  ad_text,
  AI.GENERATE(
    'Classify this ad by primary messaging angle. Return only: EMOTIONAL, FUNCTIONAL, ASPIRATIONAL, SOCIAL_PROOF, or PROBLEM_SOLUTION',
    connection_id => 'bigquery-ai-kaggle-469620.us.vertex-ai',
    endpoint => 'gemini-2.0-flash'
  ) as messaging_angle
FROM `project.dataset.ads_table`
```

## 2025 Updates & Features

### Gemini 2.5 Support:
- **New Feature**: Thinking process capabilities
- **Cost Consideration**: Gemini 2.5 models incur charges for thinking process
- **Budget Control**: Use `thinking_budget` parameter in `model_params`

### Enhanced Model Parameters:
```sql
model_params => STRUCT(
  0.2 AS temperature,
  100 AS max_output_tokens,
  50 AS thinking_budget,  -- For Gemini 2.5 models
  TRUE AS flatten_json_output
)
```

## Common Issues & Solutions

### Issue 1: Incorrect Connection Syntax
❌ **Wrong**: `connection_id => 'us.vertex-ai'`
✅ **Correct**: `connection_id => 'bigquery-ai-kaggle-469620.us.vertex-ai'`

### Issue 2: Using Deprecated Format
❌ **Wrong**: `AI.GENERATE(..., connection_id => @connection_id)`
✅ **Correct**: `AI.GENERATE(..., connection_id => 'project.location.connection')`

### Issue 3: SQL Syntax Errors
❌ **Wrong**: `connection_id => 'bigquery-ai-kaggle-469620.us.vertex-ai'` (quotes in middle of WITH clause)
✅ **Correct**: Ensure proper SQL escaping in generated queries

## Migration Guide

### From ML.GENERATE_TEXT to AI.GENERATE:

**Old Approach**:
```sql
-- 1. Create remote model (required)
CREATE OR REPLACE MODEL `dataset.model_name`
REMOTE WITH CONNECTION `location.connection_id`
OPTIONS (ENDPOINT = 'gemini-2.0-flash');

-- 2. Use ML.GENERATE_TEXT
ML.GENERATE_TEXT(
  MODEL `dataset.model_name`,
  (SELECT prompt, * FROM table),
  STRUCT(0.2 AS temperature)
)
```

**New Approach**:
```sql
-- Direct usage, no model creation needed
AI.GENERATE(
  'Your prompt here',
  connection_id => 'project.location.connection',
  endpoint => 'gemini-2.0-flash'
)
```

## Best Practices (2025)

1. **Use AI.GENERATE** for new implementations
2. **Always specify full connection ID** including project and location
3. **Consider endpoint parameter** for model version control
4. **Set temperature** for consistent results in production
5. **Monitor costs** especially with Gemini 2.5 thinking process
6. **Use budget controls** for Gemini 2.5 models

## Codebase Cleanup Actions

Based on our codebase analysis, we need to fix:

### Files with Incorrect Syntax:
1. `src/competitive_intel/analysis/enhanced_whitespace_detection.py`
2. `src/competitive_intel/intelligence/temporal_intelligence_module.py`
3. `src/competitive_intel/intelligence/channel_performance.py`

### Required Changes:
1. **Replace** `connection_id => 'bigquery-ai-kaggle-469620.us.gemini-connection'`
2. **With** `connection_id => 'bigquery-ai-kaggle-469620.us.vertex-ai'`
3. **Fix** any SQL escaping issues in generated queries
4. **Test** all AI.GENERATE calls for proper execution

## Connection Setup Verification

Our existing connection setup in `sql/setup_vertex_ai_connection.sql`:
```sql
CREATE OR REPLACE EXTERNAL CONNECTION `bigquery-ai-kaggle-469620.us.vertex-ai`
CONNECTION_TYPE = 'CLOUD_RESOURCE'
LOCATION = 'us'
```

This is **correct** and compatible with AI.GENERATE functions.

---

**Last Updated**: 2025-01-15
**BigQuery Documentation Reference**: 2025-08-26 UTC
**Recommended Approach**: Use AI.GENERATE for all new generative AI implementations