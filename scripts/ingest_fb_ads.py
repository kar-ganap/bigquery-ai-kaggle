import os, requests, pandas as pd
from datetime import datetime
from dateutil import parser as dtp
from google.cloud import bigquery

SC_API_KEY   = os.environ["SC_API_KEY"]
BQ_PROJECT   = os.environ.get("BQ_PROJECT", "yourproj")
BQ_DATASET   = os.environ.get("BQ_DATASET", "ads_demo")
TABLE_ID     = f"{BQ_PROJECT}.{BQ_DATASET}.ads_raw"

BASE_URL = "https://api.scrapecreators.com/v1/facebook/adLibrary/company/ads"

def to_date(s_or_epoch):
    if s_or_epoch is None:
        return None
    if isinstance(s_or_epoch, str):
        try:
            return dtp.parse(s_or_epoch).date()
        except Exception:
            pass
    try:
        return datetime.utcfromtimestamp(int(s_or_epoch)).date()
    except Exception:
        return None

def normalize_result(r):
    snapshot = r.get("snapshot", {}) or {}
    cards = snapshot.get("cards", []) or []
    page_name = r.get("page_name") or snapshot.get("page_name")
    snapshot_url = r.get("url")
    publisher = r.get("publisher_platform") or []
    pub_str = ",".join(publisher) if isinstance(publisher, list) else str(publisher or "")
    base = {
        "ad_id": r.get("ad_archive_id"),
        "brand": page_name,
        "page_name": page_name,
        "snapshot_url": snapshot_url,
        "display_format": snapshot.get("display_format"),
        "publisher_platforms": pub_str,
        "first_seen": to_date(r.get("start_date_string") or r.get("start_date")),
        "last_seen":  to_date(r.get("end_date_string") or r.get("end_date")),
        "landing_url": snapshot.get("link_url"),
        "cta_type": snapshot.get("cta_type"),
    }

    rows = []
    if cards:
        for idx, c in enumerate(cards):
            image_url = c.get("resized_image_url") or c.get("original_image_url")
            video_url = c.get("video_sd_url") or c.get("video_hd_url")
            creative_text = c.get("body") or (snapshot.get("body") or {}).get("text")
            title = c.get("title") or snapshot.get("title")
            cta_type = c.get("cta_type") or base["cta_type"]
            landing_url = c.get("link_url") or base["landing_url"]

            if video_url:
                media_type = "VIDEO"
            elif image_url:
                media_type = "IMAGE"
            else:
                media_type = (base["display_format"] or "UNKNOWN").upper()

            rows.append({
                **base,
                "card_index": idx,
                "creative_text": creative_text,
                "title": title,
                "media_type": media_type,
                "image_url": image_url,
                "video_url": video_url,
                "landing_url": landing_url,
                "cta_type": cta_type,
            })
    else:
        vids = snapshot.get("videos") or []
        video_url = (vids[0] or {}).get("video_sd_url") if vids else None
        creative_text = (snapshot.get("body") or {}).get("text")
        title = snapshot.get("title")
        media_type = "VIDEO" if video_url else (base["display_format"] or "UNKNOWN").upper()
        rows.append({
            **base,
            "card_index": None,
            "creative_text": creative_text,
            "title": title,
            "media_type": media_type,
            "image_url": None,
            "video_url": video_url,
        })
    return rows

def fetch_company_ads(page_id=None, company_name=None, country="US", status="ACTIVE", trim="false"):
    """
    Fetch all ads for either a page_id OR a company_name, following the `cursor`
    for pagination until there are no more pages.
    """
    if not page_id and not company_name:
        raise ValueError("Provide either page_id or company_name")

    params = {
        "country": country,
        "status": status,
        "trim": trim,
    }
    if page_id:
        params["pageId"] = page_id
    else:
        params["companyName"] = company_name

    headers = {"x-api-key": SC_API_KEY}
    all_results = []
    seen = set()

    while True:
        resp = requests.get(BASE_URL, headers=headers, params=params, timeout=60)
        resp.raise_for_status()
        js = resp.json() or {}
        results = js.get("results", []) or []

        # simple dedupe by ad id
        for r in results:
            ad_id = r.get("ad_archive_id") or r.get("id")
            if ad_id and ad_id in seen:
                continue
            if ad_id:
                seen.add(ad_id)
            all_results.append(r)

        # grab next page cursor if present
        cursor = js.get("cursor") or js.get("nextCursor") or js.get("next_page_cursor") or js.get("nextPageCursor")
        if cursor:
            params = {**params, "cursor": cursor}
        else:
            break

    return all_results

def ensure_table(client: bigquery.Client):
    client.create_dataset(BQ_DATASET, exists_ok=True)
    schema = [
        bigquery.SchemaField("ad_id", "STRING"),
        bigquery.SchemaField("brand", "STRING"),
        bigquery.SchemaField("page_name", "STRING"),
        bigquery.SchemaField("creative_text", "STRING"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("media_type", "STRING"),
        bigquery.SchemaField("first_seen", "DATE"),
        bigquery.SchemaField("last_seen", "DATE"),
        bigquery.SchemaField("snapshot_url", "STRING"),
        bigquery.SchemaField("landing_url", "STRING"),
        bigquery.SchemaField("card_index", "INT64"),
        bigquery.SchemaField("image_url", "STRING"),
        bigquery.SchemaField("video_url", "STRING"),
        bigquery.SchemaField("display_format", "STRING"),
        bigquery.SchemaField("publisher_platforms", "STRING"),
    ]
    table = bigquery.Table(TABLE_ID, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(field="first_seen")
    table.clustering_fields = ["brand","media_type"]
    try:
        client.create_table(table)
    except Exception:
        pass

def load_dataframe(df: pd.DataFrame):
    client = bigquery.Client(project=BQ_PROJECT)
    ensure_table(client)
    job = client.load_table_from_dataframe(
        df, TABLE_ID,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    )
    job.result()
    print(f"Loaded {len(df)} rows into {TABLE_ID}")

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--page_id", help="Meta page id")
    group.add_argument("--company_name", help="Company/Page name search")
    ap.add_argument("--country", default="US")
    ap.add_argument("--status", default="ACTIVE")
    ap.add_argument("--trim", default="false", choices=["true","false"])
    args = ap.parse_args()

    results = fetch_company_ads(
        page_id=args.page_id,
        company_name=args.company_name,
        country=args.country,
        status=args.status,
        trim=args.trim,
    )

    rows = []
    for r in results:
        rows.extend(normalize_result(r))
    df = pd.DataFrame(rows)
    if df.empty:
        print("No rows to load.")
    else:
        load_dataframe(df)
        print(df.head(3))
