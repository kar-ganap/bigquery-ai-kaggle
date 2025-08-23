# US Ads Strategy Radar (BigQuery + Built-in AI)

A **SQL-first** pipeline that ingests public Meta Ad Library data (via ScrapeCreators), classifies each ad’s **funnel stage** and **message angles** using **BigQuery AI**, and generates a **weekly funnel mix**. Optionally add semantic **find-similar** with embeddings + `VECTOR_SEARCH`.

> Judges’ one-liner: *“Weekly, SQL-only readout of competitors’ funnel mix and message angles, plus semantic ‘find similar’ for any ad.”*

## What you get
- `scripts/ingest_fb_ads.py`: Fetch → flatten → load to BigQuery (1 row per ad card/variant).
- `sql/`: Paste-or-run SQL for schema, labeling (AI.GENERATE_TABLE), rollups, and optional embeddings.
- `docs/`: Architecture diagram (ASCII), demo runbook, blog draft.

## Prereqs
- Python 3.9+
- `gcloud` + access to BigQuery (enable **BigQuery Studio** features)
- A ScrapeCreators API key (Meta Ad Library wrapper)

## Quickstart

```bash
# 1) Clone and install
git clone .
cd us-ads-strategy-radar

python -m venv venv
source venv/bin/activate            # Windows: venv\Scripts\activate
pip install -r requirements.txt

# 2) Auth to BigQuery
gcloud auth application-default login

# 3) Set env
export SC_API_KEY="YOUR_SCRAPECREATORS_KEY"  # don't commit this!
export BQ_PROJECT="yourproj"                 # change to your GCP project id
export BQ_DATASET="ads_demo"                 # dataset will be created if missing

# 4) Ingest one or more Meta Page IDs
python scripts/ingest_fb_ads.py --page_id 242713675589446   # Youth Crews (example)
# repeat with other competitor page IDs to append more rows

# 5) Run SQL (creates labels + rollups)
bash scripts/run_all_sql.sh

# 6) Explore
bq query --project_id="$BQ_PROJECT" --use_legacy_sql=false   'SELECT * FROM `'"$BQ_PROJECT"'.'"$BQ_DATASET"'.brand_week_funnel_mix` ORDER BY week_start, brand, pct_of_activity DESC LIMIT 50'
```

If you prefer BigQuery Studio UI: open the `sql/*.sql` files, replace `yourproj` with your project id, and run in order.

## What’s in BigQuery
Tables created under `
`**`${BQ_PROJECT}.${BQ_DATASET}`**:

- `ads_raw` – flattened ad variants (card-level)
- `ads_labels` – `funnel STRING, angles ARRAY<STRING>` produced by `AI.GENERATE_TABLE`
- `ads_activity` – one row per ad per active day
- `brand_week_funnel_mix` – weekly % distribution by funnel stage
- *(optional)* `ads_emb` and vector index for semantic search

## Cost hygiene
- `ads_raw` is **partitioned by `first_seen`** and cluster-keyed (`brand, media_type`).
- Iterate on a few brands first. These queries are light on scanned bytes.

## Optional: embeddings + VECTOR_SEARCH
See `sql/03_embeddings.sql` and `sql/04_search_examples.sql`. You’ll need a Vertex AI remote embedding model + connection (commented instructions in the SQL).

## Don’t commit secrets
Use `.env` (ignored) or environment variables. See `.env.example` for reference.

## License
MIT (see `LICENSE`).
