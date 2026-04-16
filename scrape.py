import requests
from bs4 import BeautifulSoup
import json
import os
import re
from datetime import datetime, timedelta
import time
from xml.etree import ElementTree as ET

# Google News RSS searches — reliable, no blocking, covers all press coverage
# Each query targets specific companies or keyword combinations
GOOGLE_NEWS_QUERIES = [
    # Industry trade publications (these cover deals across the whole sector)
    {"name": "Aggregates Industry",  "query": '"aggregates" (acquisition OR merger OR acquired) construction'},
    {"name": "Cement Industry",      "query": '"cement" (acquisition OR merger OR acquired)'},
    {"name": "Asphalt Industry",     "query": '"asphalt" (acquisition OR merger OR acquired)'},
    {"name": "Ready-Mix Industry",   "query": '"ready-mix" OR "ready mix" (acquisition OR merger OR acquired)'},
    {"name": "Paving Industry",      "query": '"paving" (acquisition OR merger OR acquired) concrete OR asphalt'},
    {"name": "Precast Industry",     "query": '"precast concrete" (acquisition OR merger OR acquired)'},

    # Rock Products trade publication — targeted by sector
    {"name": "Rock Products · Aggregates", "query": '"Rock Products" aggregate acquisition'},
    {"name": "Rock Products · Ready-Mix",  "query": '"Rock Products" "ready mix" acquisition'},
    {"name": "Rock Products · Asphalt",    "query": '"Rock Products" asphalt acquisition'},
    {"name": "Rock Products · Cement",     "query": '"Rock Products" cement acquisition'},

    # Pit & Quarry trade publication — targeted by sector
    {"name": "Pit & Quarry · Aggregates",  "query": '"Pit & Quarry" aggregate acquisition'},
    {"name": "Pit & Quarry · Ready-Mix",   "query": '"Pit & Quarry" "ready mix" acquisition'},
    {"name": "Pit & Quarry · Asphalt",     "query": '"Pit & Quarry" asphalt acquisition'},
    {"name": "Pit & Quarry · Cement",      "query": '"Pit & Quarry" cement acquisition'},

    # Specific public companies — catches their M&A press releases across outlets
    {"name": "Martin Marietta",      "query": '"Martin Marietta" (acquisition OR acquired OR merger OR divest)'},
    {"name": "Vulcan Materials",     "query": '"Vulcan Materials" (acquisition OR acquired OR merger OR divest)'},
    {"name": "CRH",                  "query": '"CRH" (acquisition OR acquired OR merger) construction'},
    {"name": "Heidelberg Materials", "query": '"Heidelberg Materials" (acquisition OR acquired OR merger)'},
    {"name": "Cemex",                "query": '"Cemex" (acquisition OR acquired OR merger OR divest)'},
    {"name": "Eagle Materials",      "query": '"Eagle Materials" (acquisition OR acquired OR merger)'},
    {"name": "Knife River",          "query": '"Knife River" (acquisition OR acquired OR merger)'},
    {"name": "Arcosa",               "query": '"Arcosa" (acquisition OR acquired OR merger OR divest) construction'},
    {"name": "Amrize",               "query": '"Amrize" (acquisition OR acquired OR merger)'},
    {"name": "Summit Materials",     "query": '"Summit Materials" (acquisition OR acquired OR merger)'},
    {"name": "Granite Construction", "query": '"Granite Construction" (acquisition OR acquired OR merger)'},
    {"name": "Construction Partners","query": '"Construction Partners" (acquisition OR acquired OR merger) paving'},
    {"name": "GCC Cement",           "query": '"GCC" cement (acquisition OR acquired OR merger)'},
    {"name": "US Concrete",          "query": '"US Concrete" (acquisition OR acquired OR merger)'},
    {"name": "Holcim",               "query": '"Holcim" (acquisition OR acquired OR merger OR divest)'},
    {"name": "Lehigh Hanson",        "query": '"Lehigh Hanson" (acquisition OR acquired OR merger)'},
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
    "Accept": "application/xml,text/xml,application/rss+xml,text/html,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}

ARCHIVE_DAYS = 30

def lc(s):
    return (s or "").lower()

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

# ── DEDUPLICATION ─────────────────────────────────────────────────────────────
# Source quality tiers. Lower number = better. Used to pick the "best" article
# when multiple sources cover the same deal.
SOURCE_TIERS = {
    # Tier 1: industry trade press + primary wires (most authoritative)
    "pit & quarry":            1,
    "rock products":           1,
    "concrete products":       1,
    "world cement":            1,
    "cemnet.com":              1,
    "quarrymagazine.com":      1,
    "construction & demolition recycling": 1,
    "business wire":           1,
    "globenewswire":           1,
    "pr newswire":             1,
    "prnewswire":              1,
    # Tier 2: major general/financial press
    "reuters":                 2,
    "bloomberg":               2,
    "wall street journal":     2,
    "financial times":         2,
    "the globe and mail":      2,
    "stock titan":             2,
    "cnbc":                    2,
    "forbes":                  2,
    "yahoo finance":           2,
    # Tier 3: solid secondary coverage
    "marketwatch":             3,
    "seeking alpha":           3,
    "the economic times":      3,
    "pulse 2.0":               3,
    "tipranks":                3,
    "investing.com":           3,
    "moneycontrol.com":        3,
    "indexbox":                3,
    "ndtv profit":             3,
    # Tier 4: aggregators, algorithmic posts, SEO/bot-ish feeds
    "msn":                     4,
    "marketbeat":              4,
    "simplywall.st":           4,
    "tradingview":             4,
    "bitget":                  4,
    "whalesbook":              4,
    "tikr.com":                4,
    "scanx.trade":             4,
    "marketscreener.com":      4,
    "ad hoc news":             4,
    "citybiz":                 4,
    "minichart":               4,
    "finansavisen":            4,
    "tracxn":                  4,
}
DEFAULT_TIER = 3

# Stopwords / filler to strip when building a story key from the title
_STORY_STOPWORDS = set("""
a an and or but of for from to in on at by with as is are was were be been being
the this that these those its it about into over under after before
up down off out new nyse nasdaq tsx lse jse asx lon otc inc corp
co ltd plc group holdings holding company
completes completed complete announces announced announce finalizes finalized
begins began trading deal deals transaction transactions acquire acquires
acquired acquisition acquisitions merger mergers merges merged merge buy buys
bought buyout buyouts takeover takeovers divest divests divested divestiture
stake stakes shares share investment invest invests invested purchase
purchases purchased expands expand expanding expansion continues continued
closes closed closing provides provided official clarification key implications
via how why what can does will may would could should indirect
""".split())

def _normalize_story_key(article):
    """Produce a short signature that identifies a deal, independent of source.

    Strategy: strip the trailing " - Source Name" suffix Google News adds,
    lowercase, remove punctuation, drop stopwords and common deal jargon,
    and keep the top 5 remaining content tokens (sorted for order-stability).
    """
    title = article.get("title", "") or ""
    if " - " in title:
        head, _, tail = title.rpartition(" - ")
        if head and len(tail) < 60:
            title = head

    t = lc(title)
    t = re.sub(r"[^a-z0-9\s]", " ", t)
    tokens = [w for w in t.split() if w and w not in _STORY_STOPWORDS and len(w) > 2]

    if not tokens:
        return title.strip().lower()

    signature = sorted(set(tokens))[:5]
    return "|".join(signature)

def _source_rank(article):
    src = lc(article.get("source", ""))
    for known, tier in SOURCE_TIERS.items():
        if known in src:
            return tier
    return DEFAULT_TIER

def _summary_score(article):
    """Longer, more informative summaries beat headline-only summaries."""
    summary = article.get("summary", "") or ""
    title = article.get("title", "") or ""
    if summary.strip().lower().startswith(title.strip().lower()[:30]):
        return max(0, len(summary) - len(title))
    return len(summary)

def _date_score(article):
    try:
        return datetime.fromisoformat(article.get("dateISO", "1970-01-01")).timestamp()
    except (ValueError, TypeError):
        return 0

def dedupe_articles(articles):
    """Group articles by story signature and keep the single best per group.

    "Best" = lowest source tier, then longest informative summary, then newest.
    """
    groups = {}
    for a in articles:
        key = _normalize_story_key(a)
        if not key:
            key = a.get("url", "")
        groups.setdefault(key, []).append(a)

    winners = []
    dropped = 0
    for key, group in groups.items():
        if len(group) == 1:
            winners.append(group[0])
            continue
        group.sort(key=lambda a: (_source_rank(a), -_summary_score(a), -_date_score(a)))
        winners.append(group[0])
        dropped += len(group) - 1

    return winners, dropped

def fetch_google_news(query_obj):
    """Fetch a Google News RSS feed for a given search query."""
    results = []
    try:
        url = f"https://news.google.com/rss/search?q={requests.utils.quote(query_obj['query'] + ' when:30d')}&hl=en-US&gl=US&ceid=US:en"
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()

        root = ET.fromstring(resp.content)
        channel = root.find("channel")
        if channel is None:
            return results

        for item in channel.findall("item"):
            title_el = item.find("title")
            link_el = item.find("link")
            desc_el = item.find("description")
            date_el = item.find("pubDate")
            source_el = item.find("source")

            title = title_el.text if title_el is not None else ""
            link = link_el.text if link_el is not None else ""
            desc = desc_el.text if desc_el is not None else ""
            pub_date_str = date_el.text if date_el is not None else ""
            actual_source = source_el.text if source_el is not None else query_obj["name"]

            if not title or not link:
                continue

            try:
                pub_date = datetime.strptime(pub_date_str, "%a, %d %b %Y %H:%M:%S %Z")
            except ValueError:
                try:
                    pub_date = datetime.strptime(pub_date_str[:25], "%a, %d %b %Y %H:%M:%S")
                except ValueError:
                    pub_date = datetime.now()

            desc_clean = BeautifulSoup(desc, "html.parser").get_text(" ", strip=True)
            if desc_clean.startswith(title):
                desc_clean = desc_clean[len(title):].strip(" -—")
            if len(desc_clean) < 20:
                desc_clean = title

            combined = title + " " + desc_clean

            ma_check = lc(combined)
            if not any(k in ma_check for k in ["acqui", "merger", "merged", "divest", "buyout", "takeover", "deal", "purchas", "sold"]):
                continue

            if len(desc_clean) > 400:
                desc_clean = desc_clean[:397] + "..."

            results.append({
                "title":    title,
                "summary":  desc_clean,
                "url":      link,
                "source":   actual_source,
                "category": query_obj["name"],
                "keywords": matched_keywords(combined),
                "sector":   detect_sector(combined),
                "dealType": detect_type(combined),
                "value":    extract_value(combined),
                "date":     pub_date.strftime("%b %d, %Y"),
                "dateISO":  pub_date.isoformat(),
            })

        print(f"  {query_obj['name']}: {len(results)} M&A articles")
    except Exception as e:
        print(f"  {query_obj['name']}: ERROR — {e}")
    return results


def main():
    print(f"Starting M&A scrape — {datetime.now()}")
    cutoff_date = datetime.now() - timedelta(days=ARCHIVE_DAYS)

    all_articles = []
    existing_urls = set()
    output_path = "docs/articles.json"

    if os.path.exists(output_path):
        with open(output_path, "r") as f:
            try:
                existing = json.load(f)
                for a in existing.get("articles", []):
                    try:
                        art_date = datetime.fromisoformat(a.get("dateISO", ""))
                        if art_date >= cutoff_date:
                            all_articles.append(a)
                            existing_urls.add(a.get("url", ""))
                    except (ValueError, TypeError):
                        all_articles.append(a)
                        existing_urls.add(a.get("url", ""))
                print(f"Loaded {len(all_articles)} existing articles within {ARCHIVE_DAYS}-day window")
            except Exception as e:
                print(f"Could not load existing articles: {e}")

    new_count = 0
    for query in GOOGLE_NEWS_QUERIES:
        print(f"Querying: {query['name']}")
        try:
            articles = fetch_google_news(query)
            for a in articles:
                if a["url"] not in existing_urls:
                    all_articles.append(a)
                    existing_urls.add(a["url"])
                    new_count += 1
        except Exception as e:
            print(f"  ERROR on {query['name']}: {e}")
        time.sleep(0.3)

    # Strip any leftover region tags from earlier versions so the JSON stays clean
    for a in all_articles:
        a.pop("region", None)

    # Deduplicate across sources — keep best article per deal
    before = len(all_articles)
    all_articles, dropped = dedupe_articles(all_articles)
    print(f"Deduplication: {before} → {len(all_articles)} ({dropped} duplicates removed)")

    def sort_key(a):
        try:
            return datetime.fromisoformat(a.get("dateISO", "1970-01-01"))
        except (ValueError, TypeError):
            return datetime(1970, 1, 1)

    all_articles.sort(key=sort_key, reverse=True)
    all_articles = all_articles[:1000]

    os.makedirs("docs", exist_ok=True)
    with open(output_path, "w") as f:
        json.dump({
            "fetchedAt": datetime.now().isoformat(),
            "totalArticles": len(all_articles),
            "newToday": new_count,
            "duplicatesRemoved": dropped,
            "archiveDays": ARCHIVE_DAYS,
            "articles": all_articles,
        }, f, indent=2)

    print(f"Done. {new_count} new articles added. {len(all_articles)} total in archive.")


if __name__ == "__main__":
    main()
