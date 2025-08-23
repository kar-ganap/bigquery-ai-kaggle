# SQL-first Competitive Creative Intelligence with BigQuery AI

**Problem.** It’s hard to see competitors’ weekly **funnel mix** and **message angles** across hundreds of Meta ads without manual review.

**Approach (SQL-first).** We ingest public Meta ads, then use `AI.GENERATE_TABLE` to produce typed labels (funnel, angles) from the ad text/title. We roll up weekly funnel % and (optionally) add semantic “find similar” with embeddings + `VECTOR_SEARCH`—all inside BigQuery.

**Impact.** Cuts 4–6 hours/week of analyst time to <10 minutes; unlocks instant lookalikes for creative testing and brief writing.

**How to reproduce.** See README, run `scripts/ingest_fb_ads.py`, then `scripts/run_all_sql.sh`.
