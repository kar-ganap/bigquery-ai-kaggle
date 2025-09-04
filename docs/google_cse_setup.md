# Google Custom Search Engine Setup

## Required for Subgoal 2: Competitor Discovery

To run the competitor discovery system, you need to set up Google Custom Search Engine API access.

## Step 1: Create Google Custom Search Engine

1. Go to [Google Custom Search Engine](https://cse.google.com/cse/)
2. Click "Add" to create a new search engine
3. Configuration:
   - **Sites to search**: Leave empty (we want to search the entire web)
   - **Name**: "Competitor Discovery Engine" 
   - **Language**: English
4. After creation, note your **Search engine ID (cx parameter)**

## Step 2: Enable Custom Search JSON API

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Select your project: `bigquery-ai-kaggle-469620`
3. Navigate to "APIs & Services" → "Library"
4. Search for "Custom Search JSON API"
5. Click "Enable"

## Step 3: Create API Key

1. Go to "APIs & Services" → "Credentials"
2. Click "Create Credentials" → "API Key"
3. Copy your **API Key**
4. (Optional) Restrict the key to Custom Search JSON API for security

## Step 4: Update Environment Variables

Add these to your `.env` file:
```bash
# Google Custom Search Engine (needed for competitor discovery)
GOOGLE_CSE_API_KEY=your_actual_api_key_here
GOOGLE_CSE_CX=your_actual_search_engine_id_here
```

## Step 5: Test the Setup

```bash
# Activate virtual environment
source .us-ads-radar/bin/activate

# Test with a well-known brand
python scripts/discover_competitors_v2.py --brand "Nike" --vertical "Athletic Apparel"

# Test with automatic vertical detection
python scripts/discover_competitors_v2.py --brand "Stripe"

# Test with small brand (fallback logic)
python scripts/discover_competitors_v2.py --brand "LocalCoffeeShop"
```

## Quotas and Limits

- **Free tier**: 100 search queries per day
- **Paid tier**: 10,000 queries per day ($5 per 1,000 queries)
- For development: Free tier should be sufficient
- For production: Consider paid tier

## Expected Output

```
🔍 Discovering competitors for 'Nike'...
📊 Detecting brand vertical...
✅ Detected vertical: Athletic Apparel
🎯 Executing 12 standard discovery queries...
   'Nike competitors' → 4 candidates
   'Nike alternatives' → 3 candidates
   ...
📈 Standard discovery found 15 unique candidates
✅ Discovery complete: 15 unique candidates found

🎉 Top candidates for 'Nike':
 1. Adidas                    (score: 5.20, method: standard)
 2. Under Armour              (score: 4.80, method: standard)
 3. Puma                      (score: 4.60, method: standard)
 ...

💾 Saved 15 candidates to data/temp/competitors_raw.csv
```

## Troubleshooting

**Error: "Missing required environment variables"**
- Ensure GOOGLE_CSE_API_KEY and GOOGLE_CSE_CX are set in .env
- Run `source .env` or restart terminal

**Error: "API key not valid"**
- Verify API key is correct
- Ensure Custom Search JSON API is enabled
- Check API key restrictions

**Error: "Quota exceeded"**
- You've hit the 100/day free limit
- Wait 24 hours or upgrade to paid tier
- Reduce --max-results parameter for testing