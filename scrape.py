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

# ── REGION DETECTION ──────────────────────────────────────────────────────────
# Classifies an article by WHERE THE DEAL HAPPENS, not where the acquirer is
# headquartered. A Heidelberg acquisition in Texas is "US"; a Vulcan acquisition
# in Mexico is "Global". Company identity is ignored — only target geography.
#
# Default: ambiguous articles go to US. Only articles with clear non-US signals
# (and no US signals to counterbalance) are tagged Global.

US_STATES = [
    "alabama","alaska","arizona","arkansas","california","colorado","connecticut","delaware",
    "florida","georgia","hawaii","idaho","illinois","indiana","iowa","kansas","kentucky",
    "louisiana","maine","maryland","massachusetts","michigan","minnesota","mississippi",
    "missouri","montana","nebraska","nevada","new hampshire","new jersey","new mexico",
    "new york","north carolina","north dakota","ohio","oklahoma","oregon","pennsylvania",
    "rhode island","south carolina","south dakota","tennessee","texas","utah","vermont",
    "virginia","washington","west virginia","wisconsin","wyoming",
]
# Regional phrases that clearly mean "inside the US"
US_REGIONAL_PHRASES = [
    "u.s.","united states","us-based","us based","american","stateside",
    "west texas","east texas","south texas","north texas","central texas",
    "northeast","northwest","southeast","southwest","midwest","mid-atlantic",
    "new england","gulf coast","west coast","east coast","pacific northwest",
    "appalachia","rocky mountain","great lakes","great plains",
]
# Major US cities — used as deal-location signal
US_CITIES = [
    "new york city","los angeles","chicago","houston","phoenix","philadelphia","san antonio",
    "san diego","dallas","austin","nashville","denver","seattle","boston","atlanta","miami",
    "minneapolis","cincinnati","indianapolis","charlotte","pittsburgh","cleveland",
    "kansas city","st. louis","detroit","las vegas","baltimore","milwaukee","albuquerque",
    "tucson","fresno","sacramento","omaha","raleigh","tampa","orlando","jacksonville",
    "oklahoma city","memphis","louisville","richmond","birmingham","salt lake city",
    "fort worth","el paso","long beach","mesa","virginia beach","colorado springs",
    "tulsa","wichita","arlington","bakersfield","aurora","anaheim","santa ana",
    "corpus christi","riverside","lexington","stockton","henderson","saint paul",
    "greensboro","plano","newark","toledo","lincoln","chandler",
    "fort wayne","jersey city","st. petersburg","chula vista","laredo","madison",
    "lubbock","winston-salem","garland","glendale","hialeah","reno","chesapeake",
    "gilbert","irving","scottsdale","north las vegas","fremont","baton rouge",
    "boise","san bernardino","spokane","des moines","modesto",
    "fayetteville","tacoma","oxnard","fontana","columbus","montgomery","shreveport",
    "yonkers","akron","huntington beach","little rock","augusta","amarillo",
    "mobile","grand rapids","salt lake","tallahassee","huntsville","grand prairie",
    "knoxville","worcester","newport news","brownsville","santa clarita","overland park",
    "providence","garden grove","chattanooga","oceanside","jackson","fort lauderdale",
    "rancho cucamonga","santa rosa","port st. lucie","tempe",
    "springfield","pembroke pines","salem","cape coral","peoria","sioux falls",
    "eugene","rockford","palm bay","savannah","bridgeport","torrance","joliet",
    "paterson","naperville","alexandria","pasadena","hollywood","lancaster","hayward",
    "salinas","hampton","macon","pomona",
]
# Countries and non-US regions
NON_US_COUNTRIES = [
    "india","indian","pakistan","pakistani","bangladesh","sri lanka","nepal",
    "china","chinese","japan","japanese","korea","korean","vietnam","thailand",
    "malaysia","indonesia","philippines","singapore",
    "australia","australian","new zealand",
    "canada","canadian","mexico","mexican",
    "brazil","brazilian","argentina","chile","peru","peruvian","colombia","venezuela",
    "united kingdom","britain","british","england","scotland","wales","ireland","irish",
    "france","french","germany","german","spain","spanish","italy","italian","portugal",
    "netherlands","dutch","belgium","belgian","switzerland","swiss","austria","poland",
    "czech","slovakia","hungary","romania","bulgaria","greece","turkey","turkish",
    "russia","russian","ukraine","ukrainian","saudi","uae","emirates","qatar","kuwait",
    "egypt","nigeria","nigerian","kenya","south africa","ghana","morocco","algeria",
    "africa","african","europe","european","asia","asian","middle east","latin america",
    "south america","central america","north america",
]
# Foreign cities + international currency/market/regulator signals
NON_US_CITIES_AND_MARKETS = [
    # Major foreign cities
    "london","paris","berlin","madrid","rome","milan","amsterdam","brussels","vienna",
    "warsaw","prague","athens","istanbul","moscow","dubai","mumbai","delhi","bangalore",
    "kolkata","chennai","hyderabad","karachi","lahore","beijing","shanghai","hong kong",
    "tokyo","seoul","sydney","melbourne","toronto","montreal","edmonton","calgary",
    "vancouver","ottawa","winnipeg","halifax",
    "são paulo","sao paulo","buenos aires","mexico city","bogotá","bogota","lima",
    "santiago","caracas","quito","guadalajara","monterrey","tijuana",
    "lagos","cairo","nairobi","johannesburg","cape town","casablanca",
    "auckland","wellington","perth","brisbane","adelaide","christchurch",
    "coffs harbour","coffs",
    # Currencies / financial units
    "₹","crore","lakh","rupee","rupees","inr","euro","eur","gbp","pound sterling",
    "yen","yuan","renminbi","rmb","peso","ringgit","rand","naira",
    # Foreign exchanges / regulators
    "nse:","bse:","lse:","asx:","tsx:","jse:","nclt","sebi","fca",
    # Non-US regional/administrative phrases
    "nsw","new south wales","queensland","victoria","ontario","quebec","alberta",
    "british columbia","maharashtra","gujarat","tamil nadu","karnataka","punjab",
    "latin american","european union","eu-based","eurozone","eea",
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

def _strip_source_suffix(text):
    """Remove Google News " - Publisher" suffix so publisher names don't
    pollute region detection. Example:
      "Construction Partners acquires Four Star Paving in Nashville By Investing.com - Investing.com South Africa"
    becomes:
      "Construction Partners acquires Four Star Paving in Nashville By Investing.com"

    Also strips "By Publisher" mid-sentence markers that Google News adds when
    republishing (e.g. "... in Nashville By Investing.com").
    """
    if not text:
        return ""
    # Strip trailing " - Publisher Name" (last occurrence, publisher name capped)
    if " - " in text:
        head, _, tail = text.rpartition(" - ")
        if head and len(tail) < 80:
            text = head
    # Strip " By Publisher" patterns Google News inserts
    # e.g. "headline text By Investing.com" → "headline text"
    text = re.sub(r'\s+By\s+[A-Z][A-Za-z0-9\.\s]{0,40}$', '', text)
    return text

def detect_region(article_or_text):
    """Classify article as 'US' or 'Global' based on WHERE THE DEAL HAPPENS.

    Company HQ is ignored — the question is purely "is the target / asset /
    operation being acquired located in the United States?"

    Default rule: ambiguous articles go to US. Only articles with clear non-US
    location signals AND no counterbalancing US signals are tagged Global.
    """
    # Accept either a full article dict (so we can clean title separately)
    # or a raw text blob (back-compat).
    if isinstance(article_or_text, dict):
        title = _strip_source_suffix(article_or_text.get("title", "") or "")
        summary = _strip_source_suffix(article_or_text.get("summary", "") or "")
        text = title + " " + summary
    else:
        text = _strip_source_suffix(article_or_text or "")

    t = " " + lc(text) + " "

    us_score = 0
    non_us_score = 0

    # US deal-location signals
    for s in US_STATES:
        if re.search(r'\b' + re.escape(s) + r'\b', t):
            us_score += 3
    for c in US_CITIES:
        if re.search(r'\b' + re.escape(c) + r'\b', t):
            us_score += 3
    for p in US_REGIONAL_PHRASES:
        # Dotted phrases like "u.s." can't rely on \b (dot is non-word char)
        if "." in p:
            if (" " + p + " ") in t or (" " + p + ".") in t or (" " + p + ",") in t:
                us_score += 2
        else:
            if re.search(r'\b' + re.escape(p) + r'\b', t):
                us_score += 2

    # Non-US deal-location signals
    for c in NON_US_COUNTRIES:
        if re.search(r'\b' + re.escape(c) + r'\b', t):
            non_us_score += 3
    for c in NON_US_CITIES_AND_MARKETS:
        if "." in c or ":" in c or "₹" in c:
            if c in t:
                non_us_score += 3
        else:
            if re.search(r'\b' + re.escape(c) + r'\b', t):
                non_us_score += 3

    # Decision: Global ONLY when non-US signals clearly dominate AND no US
    # signal is present. Any US signal, or a tie, or no signal at all, → US.
    if non_us_score > 0 and us_score == 0:
        return "Global"
    if non_us_score > us_score and us_score < 3:
        # Non-US has a real lead and US only has weak signals (e.g. a single
        # "american" mention without city/state). Still Global.
        return "Global"
    return "US"

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

            article_record = {
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
            }
            # Region detection runs on the cleaned title+summary (not raw combined
            # string with publisher suffix) — pass the record so it can strip the
            # Google News " - Publisher" tail before scoring.
            article_record["region"] = detect_region(article_record)
            results.append(article_record)

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

    # Always re-tag region for every article using the cleaned detection so
    # updates to rules take effect on historical articles too.
    for a in all_articles:
        a["region"] = detect_region(a)

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

    us_count = sum(1 for a in all_articles if a.get("region") == "US")
    gl_count = len(all_articles) - us_count
    print(f"Region split: {us_count} US · {gl_count} Global")

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
