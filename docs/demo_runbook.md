# Demo runbook

1) Ingest a brand
```bash
python scripts/ingest_fb_ads.py --page_id 242713675589446
```

2) Label ads (funnel + angles) and build weekly rollups
```bash
bash scripts/run_all_sql.sh
```

3) Readout queries
```sql
-- Funnel distribution counts
SELECT funnel, COUNT(*) AS n FROM `PROJECT.DATASET.ads_labels` GROUP BY 1 ORDER BY n DESC;

-- Weekly funnel %
SELECT * FROM `PROJECT.DATASET.brand_week_funnel_mix` ORDER BY week_start, brand, pct_of_activity DESC LIMIT 100;
```

4) (Optional) Build embeddings + run semantic search
- Open `sql/03_embeddings.sql` and follow the comments to create a Vertex connection and remote model.
- Then run `sql/04_search_examples.sql`.
