#!/usr/bin/env python3
import argparse, subprocess, sys, pandas as pd

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="competitors_final.csv with column: canonical_name")
    ap.add_argument("--max", type=int, default=25, help="top N to pull")
    ap.add_argument("--country", default="US")
    ap.add_argument("--status", default="ACTIVE")
    args = ap.parse_args()

    df = pd.read_csv(args.csv)
    df = df[df["is_competitor"]==True].copy()
    df = df.sort_values(["segment"], ascending=True).head(args.max)

    for name in df["canonical_name"].tolist():
        print(f"==> Pulling ads for {name}")
        # try company_name first; your script paginates & loads BQ
        cmd = [
            sys.executable, "scripts/ingest_fb_ads.py",
            "--company_name", name,
            "--country", args.country,
            "--status", args.status
        ]
        subprocess.run(cmd, check=False)

if __name__ == "__main__":
    main()
