import requests
from bs4 import BeautifulSoup
import json
import os
import re
from datetime import datetime

SOURCES = [
    {"name": "Rock Products",            "url": "https://www.rockproducts.com/latest-news/"},
    {"name": "Concrete Products",        "url": "https://www.concreteproducts.com/news/"},
    {"name": "Pit & Quarry",             "url": "https://www.pitandquarry.com/news/"},
    {"name": "Martin Marietta",          "url": "https://www.martinmarietta.com/investors/press-releases/"},
    {"name": "Arcosa",                   "url": "https://investors.arcosa.com/press-releases"},
    {"name": "Amrize (AMRZ)",            "url": "https://www.amrize.com/news/"},
    {"name": "Construction Partners",    "url": "https://ir.constructionpartners.net/press-releases"},
    {"name": "Granite Construction",     "url": "https://www.graniteconstruction.com/newsroom"},
    {"name": "CRH",                      "url": "https://www.crh.com/media/press-releases/"},
    {"name": "Knife River",              "url": "https://www.kniferivercorp.com/news"},
    {"name": "Eagle Materials",          "url": "https://www.eaglematerials.com/press-releases"},
    {"name": "Heidelberg Materials",     "url": "https://www.heidelbergmaterials.com/en/news"},
    {"name": "Cemex",                    "url": "https://www.cemex.com/media/newsroom"},
    {"name": "GCC",                      "url": "https://ir.gcc.com/news-events/news-releases"},
    {"name": "PR Newswire Construction", "url": "https://www.prnewswire.com/news-releases/construction-materials/"},
]

MA_KEYWORDS = [
    "acqui", "merger", "merging", "merged", "divest", "divestiture",
    "joint venture", "takeover", "buyout", "purchase", "consolidat",
    "transaction", "invest", " sold ", "sale of", "strategic combination",
    "strategic acquisition", "acquisition agreement", "definitive agreement",
    "purchase agreement", "to acquire", "has acquired", "will acquire",
    "completes acquisition", "completed acquisition", "announces acquisition",
    "announced acquisition",
]

SECTOR_MAP = [
    {"label": "Aggregates", "keys": ["aggregate", "quarry", "crushed stone", "gravel", "sand and gravel", "rock product"]},
    {"label": "Cement",     "keys": ["cement", "clinker", "portland", "grinding station", "blended cement"]},
    {"label": "Asphalt",    "keys": ["asphalt", "hot mix", "hma", "bitumen", "bituminous", "tarmac"]},
    {"label": "Ready-Mix",  "keys": ["ready mix", "ready-mix", "readymix", "concrete plant", "batch plant"]},
    {"label": "Paving",     "keys": ["paving", "pavement", "road surfac", "highway material", "road building"]},
    {"label": "Precast",    "keys": ["precast", "prestressed", "concrete pipe", "prefabricated concrete"]},
]

TYPE_MAP = [
    {"type": "Divestiture", "keys": ["divest", "divestiture", " sold ", "sale of", "dispose", "selling its"]},
    {"type": "Merger",      "keys": ["merger", "merging", "merged", "combine", "strategic combination"]},
    {"type": "Investment",  "keys": ["investment", "invested", "investing", "minority stake", "equity stake"]},
    {"type": "Acquisition", "keys": ["acqui", "takeover", "buyout", "purchase", "to acquire", "has acquired", "definitive agreement"]},
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

def lc(s):
    return (s or "").lower()

def contains_ma(text):
    t = lc(text)
    return any(k in t for k in MA_KEYWORDS)

def detect_sector(text):
    t = lc(text)
    found = [s["label"] for s in SECTOR_MAP if any(k in t for k in s["keys"])]
    if not found:
        return "General"
    if len(found) > 1:
        return "Multiple"
    return found[0]

def detect_type(text):
    t = lc(text)
    for m in TYPE_MAP:
        if any(k in t for k in m["keys"]):
            return m["type"]
    return "Acquisition"

def matched_keywords(text):
    t = lc(text)
    labels = {
        "acqui": "Acquired", "merger": "Merger", "divest": "Divest",
        "joint venture": "Joint Venture", "takeover": "Takeover",
        "buyout": "Buyout", "purchase": "Purchase", "invest": "Investment",
        "to acquire": "To Acquire", "definitive agreement": "Definitive Agreement",
        "consolidat": "Consolidation", "transaction": "Transaction",
    }
    found = []
    for k, label in labels.items():
        if k in t and label not in found:
            found.append(label)
        if len(found) >= 4:
            break
    return ", ".join(found)

def extract_value(text):
    t = text or ""
    m = re.search(r'\$\s*([\d,]+\.?\d*)\s*billion', t, re.I)
    if m:
        return f"${float(m.group(1).replace(',', '')):.2f}B"
    m = re.search(r'\$\s*([\d,]+\.?\d*)\s*B\b', t, re.I)
    if m:
        return f"${float(m.group(1).replace(',', '')):.2f}B"
    m = re.search(r'\$\s*([\d,]+\.?\d*)\s*million', t, re.I)
    if m:
        return f"${float(m.group(1).replace(',', '')):.0f}M"
    m = re.search(r'\$\s*([\d,]+\.?\d*)\s*M\b', t, re.I)
    if m:
        return f"${float(m.group(1).replace(',', '')):.0f}M"
    return ""

def scrape_source(source):
    results = []
    try:
        session = requests.Session()
        session.headers.update(HEADERS)
        resp = session.get(source["url"], timeout=20, allow_redirects=True)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        blocks = []
        for tag in soup.find_all(["p", "h1", "h2", "h3", "li"]):
            text = tag.get_text(" ", strip=True)
            if len(text) > 20:
                blocks.append(text)
        seen = set()
        base_url = f"https://{requests.utils.urlparse(source['url']).netloc}"
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"].strip()
            anchor_text = a_tag.get_text(" ", strip=True)
            if not anchor_text or len(anchor_text) < 10:
                continue
            if href.startswith(("javascript", "mailto", "#")):
                continue
            if href.startswith("//"):
                href = "https:" + href
            elif href.startswith("/"):
                href = base_url + href
            elif not href.startswith("http"):
                continue
            norm_href = href.split("?")[0].rstrip("/")
            if norm_href in seen:
                continue
            if re.search(r'/(tag|category|author|search|feed|login|contact|about)/', href, re.I):
                continue
            if re.search(r'\.(jpg|jpeg|png|gif|svg|pdf|zip|css|js)$', href, re.I):
                continue
            title_words = [w for w in lc(anchor_text).split() if len(w) > 4]
            best_context = ""
            for block in blocks:
                match_count = sum(1 for w in title_words if w in lc(block))
                if match_count >= 2 and len(block) > len(best_context):
                    best_context = block
            combined = anchor_text + " " + best_context
            if not contains_ma(combined):
                continue
            seen.add(norm_href)
            summary = best_context if len(best_context) > 30 else anchor_text
            if len(summary) > 400:
                summary = summary[:397] + "..."
            results.append({
                "title":    anchor_text,
                "summary":  summary,
                "url":      href,
                "source":   source["name"],
                "keywords": matched_keywords(combined),
                "sector":   detect_sector(combined),
                "dealType": detect_type(combined),
                "value":    extract_value(combined),
                "date":     datetime.now().strftime("%b %d, %Y"),
            })
            if len(results) >= 10:
                break
        print(f"  {source['name']}: {len(results)} M&A articles found")
    except Exception as e:
        print(f"  {source['name']}: ERROR — {e}")
    return results

def main():
    print(f"Starting M&A scrape — {datetime.now()}")
    all_articles = []
    existing_urls = set()
    output_path = "docs/articles.json"
    if os.path.exists(output_path):
        with open(output_path, "r") as f:
            try:
                existing = json.load(f)
                for a in existing.get("articles", []):
                    existing_urls.add(a.get("url", ""))
                all_articles = existing.get("articles", [])
                print(f"Loaded {len(all_articles)} existing articles")
            except Exception:
                pass
    new_count = 0
    for source in SOURCES:
        print(f"Scraping: {source['name']}")
        articles = scrape_source(source)
        for a in articles:
            if a["url"] not in existing_urls:
                all_articles.insert(0, a)
                existing_urls.add(a["url"])
                new_count += 1
    all_articles = all_articles[:500]
    os.makedirs("docs", exist_ok=True)
    with open(output_path, "w") as f:
        json.dump({
            "fetchedAt": datetime.now().isoformat(),
            "totalArticles": len(all_articles),
            "newToday": new_count,
            "articles": all_articles,
        }, f, indent=2)
    print(f"Done. {new_count} new articles added. {len(all_articles)} total saved.")

if __name__ == "__main__":
    main()
