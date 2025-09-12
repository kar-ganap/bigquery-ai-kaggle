#!/usr/bin/env python3
import argparse, requests, re, html
from urllib.parse import quote
from collections import defaultdict

WIKI_API = "https://en.wikipedia.org/w/api.php"
USER_AGENT = "ads-radar/0.1 (competitor discovery)"

def wikipedia_search(q, limit=8):
    params = {
        "action": "query", "list":"search", "srsearch": q,
        "format":"json", "srlimit": limit
    }
    r = requests.get(WIKI_API, params=params, headers={"User-Agent":USER_AGENT}, timeout=30)
    r.raise_for_status()
    return [hit["title"] for hit in r.json().get("query",{}).get("search",[])]

def wikipedia_outlinks(title):
    # fetch sections + links; we’ll scan section titles for “Competitors/See also/Brands”
    plcontinue = None
    links = []
    while True:
        params = {
            "action":"query", "prop":"links|sections", "titles": title,
            "format":"json", "pllimit": "max"
        }
        if plcontinue: params["plcontinue"]=plcontinue
        r = requests.get(WIKI_API, params=params, headers={"User-Agent":USER_AGENT}, timeout=30)
        r.raise_for_status()
        js = r.json()
        pages = js.get("query",{}).get("pages",{})
        page = next(iter(pages.values()), {})
        links.extend([l["title"] for l in page.get("links",[]) if l.get("ns")==0])
        plcontinue = js.get("continue",{}).get("plcontinue")
        if not plcontinue: break
    return links

def simple_web_listicles(brand, vertical, n=10):
    # extremely light-weight: use Bing/Google manually if you have keys,
    # here we just probe a few common listicle patterns.
    seeds = [
        f"https://duckduckgo.com/html/?q=top+{quote(vertical)}+brands",
        f"https://duckduckgo.com/html/?q=best+{quote(vertical)}+companies",
        f"https://duckduckgo.com/html/?q=alternatives+to+{quote(brand)}",
    ]
    names = []
    for u in seeds:
        try:
            t = requests.get(u, timeout=20, headers={"User-Agent":USER_AGENT}).text
            # crude brand candidates: capitalized words 2–3 tokens
            for m in re.findall(r">([A-Z][\w&’'-]+(?:\s+[A-Z][\w&’'-]+){0,2})<", t):
                s = html.unescape(m).strip()
                if 2 <= len(s) <= 40 and not s.lower().startswith(("top ", "best ", "alternatives")):
                    names.append(s)
        except Exception:
            pass
    # keep rough top-N uniques preserving order
    out, seen = [], set()
    for x in names:
        k = x.lower()
        if k not in seen:
            out.append(x); seen.add(k)
        if len(out)>=n: break
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--brand", required=True)
    ap.add_argument("--domain", default=None)
    ap.add_argument("--vertical", default="")
    ap.add_argument("--out_csv", default="data/tmp/competitor_candidates.csv")
    args = ap.parse_args()

    brand = args.brand.strip()
    vertical = args.vertical or brand

    # 1) wiki seeds
    wiki_titles = wikipedia_search(brand, limit=5)
    wiki_links = []
    for t in wiki_titles:
        wiki_links += wikipedia_outlinks(t)

    # filter likely company pages
    likely = [x for x in wiki_links if not re.search(r":", x) and len(x) < 60]

    # 2) listicles / alternatives
    alt = simple_web_listicles(brand, vertical, n=40)

    # score
    score = defaultdict(lambda: {"company_name":"", "sources":set()})
    for name in likely:
        score[name]["company_name"]=name; score[name]["sources"].add("wikipedia_outlink")
    for name in alt:
        score[name]["company_name"]=name; score[name]["sources"].add("listicle")

    rows = []
    for v in score.values():
        rows.append({
            "company_name": v["company_name"],
            "sources": ",".join(sorted(v["sources"])),
            "raw_score": len(v["sources"]),
        })

    import os, csv
    os.makedirs(os.path.dirname(args.out_csv), exist_ok=True)
    with open(args.out_csv, "w", newline="", encoding="utf-8") as f:
        w=csv.DictWriter(f, fieldnames=["company_name","sources","raw_score"])
        w.writeheader(); w.writerows(rows)

    print(f"wrote {len(rows)} candidates -> {args.out_csv}")

if __name__ == "__main__":
    main()
