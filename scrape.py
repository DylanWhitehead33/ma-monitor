import requests
from bs4 import BeautifulSoup
import json
import os
import re
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse
import time

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

# Keep articles from the last 30 days
ARCHIVE_DAYS = 30

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

def extract_date(text, soup_tag=None):
    """Try to extract an article date from text or nearby HTML."""
    if not text:
        return None
    # Common formats: "January 15, 2025", "Jan 15, 2025", "2025-01-15", "01/15/2025"
    patterns = [
        r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\.?\s+(\d{1,2}),?\s+(\d{4})',
        r'(\d{4})-(\d{2})-(\d{2})',
        r'(\d{1,2})/(\d{1,2})/(\d{4})',
    ]
    months = {'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,'jul':7,'aug':8,'sep':9,'sept':9,'oct':10,'nov':11,'dec':12}
    for pattern in patterns:
        m = re.search(pattern, text, re.I)
        if m:
            try:
                g = m.groups()
                if len(g) == 3 and g[0].lower()[:3] in months:
                    return datetime(int(g[2]), months[g[0].lower()[:3]], int(g[1]))
                elif len(g) == 3 and len(g[0]) == 4:
                    return datetime(int(g[0]), int(g[1]), int(g[2]))
                elif len(g) == 3:
                    return datetime(int(g[2]), int(g[0]), int(g[1]))
            except (ValueError, KeyError):
                continue
    return None

def fetch_with_retry(session, url, retries=2):
    """Fetch a URL with retry logic."""
    for attempt in range(retries + 1):
        try:
            resp = session.get(url, timeout=20, allow_redirects=True)
            if resp.status_code == 200:
                return resp
        except Exception as e:
            if attempt == retries:
                raise
            time.sleep(1)
    return None

def scrape_source(source, cutoff_date):
    """Scrape a source, following pagination if available, collecting articles newer than cutoff_date."""
    results = []
    session = requests.Session()
    session.headers.update(HEADERS)

    # Try the main URL and common pagination patterns
    urls_to_try = [source["url"]]
    # Add common pagination URLs for first-run deep scan
    base = source["url"].rstrip("/")
    for page in range(2, 5):
        urls_to_try.append(f"{base}/page/{page}/")
        urls_to_try.append(f"{base}?page={page}")

    seen_urls = set()

    for page_url in urls_to_try:
        try:
            resp = fetch_with_retry(session, page_url)
            if not resp or resp.status_code != 200:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            # Collect all text blocks for context
            blocks = []
            for tag in soup.find_all(["p", "h1", "h2", "h3", "h4", "li", "article", "time"]):
                text = tag.get_text(" ", strip=True)
                if len(text) > 15:
                    blocks.append(text)

            base_url = f"https://{urlparse(source['url']).netloc}"

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
                if norm_href in seen_urls:
                    continue
                if re.search(r'/(tag|category|author|search|feed|login|contact|about)/', href, re.I):
                    continue
                if re.search(r'\.(jpg|jpeg|png|gif|svg|pdf|zip|css|js)$', href, re.I):
                    continue

                # Find nearby context
                title_words = [w for w in lc(anchor_text).split() if len(w) > 4]
                best_context = ""
                article_date_text = ""
                for block in blocks:
                    match_count = sum(1 for w in title_words if w in lc(block))
                    if match_count >= 2 and len(block) > len(best_context):
                        best_context = block
                    # Also look for date strings near the title
                    if match_count >= 2:
                        date_match = re.search(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2},?\s+\d{4}|\d{4}-\d{2}-\d{2}', block, re.I)
                        if date_match and not article_date_text:
                            article_date_text = date_match.group(0)

                combined = anchor_text + " " + best_context
                if not contains_ma(combined):
                    continue

                # Try to get the article date
                article_date = extract_date(article_date_text) or extract_date(best_context)

                # Skip articles older than cutoff (if we have a date)
                if article_date and article_date < cutoff_date:
                    continue

                # Use article date if we have it, otherwise mark as "recent"
                if article_date:
                    date_str = article_date.strftime("%b %d, %Y")
                    date_iso = article_date.isoformat()
                else:
                    date_str = datetime.now().strftime("%b %d, %Y")
                    date_iso = datetime.now().isoformat()

                seen_urls.add(norm_href)
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
                    "date":     date_str,
                    "dateISO":  date_iso,
                })

                if len(results) >= 30:  # cap per source
                    break

            if len(results) >= 30:
                break

            # Brief pause between pagination requests
            time.sleep(0.5)

        except Exception as e:
            print(f"  Error on page {page_url}: {e}")
            continue

    print(f"  {source['name']}: {len(results)} M&A articles found")
    return results


def main():
    print(f"Starting M&A scrape — {datetime.now()}")
    cutoff_date = datetime.now() - timedelta(days=ARCHIVE_DAYS)

    # Load existing articles
    all_articles = []
    existing_urls = set()
    output_path = "docs/articles.json"

    if os.path.exists(output_path):
        with open(output_path, "r") as f:
            try:
                existing = json.load(f)
                for a in existing.get("articles", []):
                    # Keep only articles within our archive window
                    try:
                        art_date = datetime.fromisoformat(a.get("dateISO", ""))
                        if art_date >= cutoff_date:
                            all_articles.append(a)
                            existing_urls.add(a.get("url", ""))
                    except (ValueError, TypeError):
                        # Keep articles without proper dates too (recent adds)
                        all_articles.append(a)
                        existing_urls.add(a.get("url", ""))
                print(f"Loaded {len(all_articles)} existing articles within {ARCHIVE_DAYS}-day window")
            except Exception as e:
                print(f"Could not load existing articles: {e}")

    new_count = 0
    for source in SOURCES:
        print(f"Scraping: {source['name']}")
        try:
            articles = scrape_source(source, cutoff_date)
            for a in articles:
                if a["url"] not in existing_urls:
                    all_articles.append(a)
                    existing_urls.add(a["url"])
                    new_count += 1
        except Exception as e:
            print(f"  ERROR scraping {source['name']}: {e}")

    # Sort newest first by dateISO
    def sort_key(a):
        try:
            return datetime.fromisoformat(a.get("dateISO", "1970-01-01"))
        except (ValueError, TypeError):
            return datetime(1970, 1, 1)

    all_articles.sort(key=sort_key, reverse=True)

    # Cap total archive size as a safety
    all_articles = all_articles[:1000]

    os.makedirs("docs", exist_ok=True)
    with open(output_path, "w") as f:
        json.dump({
            "fetchedAt": datetime.now().isoformat(),
            "totalArticles": len(all_articles),
            "newToday": new_count,
            "archiveDays": ARCHIVE_DAYS,
            "articles": all_articles,
        }, f, indent=2)

    print(f"Done. {new_count} new articles added. {len(all_articles)} total in archive.")


if __name__ == "__main__":
    main()
