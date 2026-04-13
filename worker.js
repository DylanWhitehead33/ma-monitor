const SOURCES = [
  { name: 'Rock Products',             url: 'https://www.rockproducts.com/latest-news/' },
  { name: 'Concrete Products',         url: 'https://www.concreteproducts.com/news/' },
  { name: 'Pit & Quarry',              url: 'https://www.pitandquarry.com/news/' },
  { name: 'Martin Marietta',           url: 'https://www.martinmarietta.com/investors/press-releases/' },
  { name: 'Arcosa',                    url: 'https://investors.arcosa.com/press-releases' },
  { name: 'Amrize (AMRZ)',             url: 'https://www.amrize.com/news/' },
  { name: 'Construction Partners',     url: 'https://ir.constructionpartners.net/press-releases' },
  { name: 'Granite Construction',      url: 'https://www.graniteconstruction.com/newsroom' },
  { name: 'CRH',                       url: 'https://www.crh.com/media/press-releases/' },
  { name: 'Knife River',               url: 'https://www.kniferivercorp.com/news' },
  { name: 'Eagle Materials',           url: 'https://www.eaglematerials.com/press-releases' },
  { name: 'Heidelberg Materials',      url: 'https://www.heidelbergmaterials.com/en/news' },
  { name: 'Cemex',                     url: 'https://www.cemex.com/media/newsroom' },
  { name: 'GCC',                       url: 'https://ir.gcc.com/news-events/news-releases' },
  { name: 'PR Newswire Construction',  url: 'https://www.prnewswire.com/news-releases/construction-materials/' },
];

const MA_KEYWORDS = [
  'acqui', 'merger', 'merging', 'merged', 'divest', 'divestiture',
  'joint venture', ' jv ', 'takeover', 'buyout', 'purchase',
  'consolidat', ' deal ', 'transaction', 'invest', ' sold ',
  'sale of', 'strategic combination', 'strategic acquisition',
  'acquisition agreement', 'definitive agreement', 'purchase agreement',
  'enter into agreement', 'entered into agreement', 'signs agreement',
  'completes acquisition', 'completed acquisition', 'announces acquisition',
  'announced acquisition', 'to acquire', 'has acquired', 'will acquire',
];

const SECTOR_MAP = [
  { label: 'Aggregates', keys: ['aggregate', 'quarry', 'crushed stone', 'gravel', 'sand and gravel', 'limestone quarr', 'granite quarr', 'rock product', 'pit and quarry', 'pit & quarry'] },
  { label: 'Cement',     keys: ['cement', 'clinker', 'portland', 'grinding station', 'blended cement', 'cemex', 'heidelberg', 'gcc cement'] },
  { label: 'Asphalt',    keys: ['asphalt', 'hot mix', 'hma', 'bitumen', 'bituminous', 'tarmac', 'liquid asphalt'] },
  { label: 'Ready-Mix',  keys: ['ready mix', 'ready-mix', 'readymix', 'concrete plant', 'batch plant', 'transit mix', 'concrete delivery'] },
  { label: 'Paving',     keys: ['paving', 'pavement', 'road surfac', 'highway material', 'road building', 'road construction', 'pave contractor'] },
  { label: 'Precast',    keys: ['precast', 'prestressed', 'concrete pipe', 'prefabricated concrete', 'tilt-up', 'precast concrete'] },
];

const TYPE_MAP = [
  { type: 'divestiture', keys: ['divest', 'divestiture', ' sold ', 'sale of', 'dispose', 'disposal', 'selling its', 'sell its'] },
  { type: 'merger',      keys: ['merger', 'merging', 'merged', 'combine', 'strategic combination', 'business combination'] },
  { type: 'investment',  keys: ['investment', 'invested', 'investing', 'minority stake', 'equity stake', 'funding round', 'equity investment'] },
  { type: 'acquisition', keys: ['acqui', 'takeover', 'buyout', 'purchase', 'to acquire', 'has acquired', 'will acquire', 'completes acquisition', 'definitive agreement', 'purchase agreement'] },
];

function lc(s) { return (s || '').toLowerCase(); }

function containsMA(text) {
  const t = lc(text);
  return MA_KEYWORDS.some(k => t.includes(k));
}

function matchedKeywords(text) {
  const t = lc(text);
  const found = [];
  const display = {
    'acqui': 'Acquired', 'merger': 'Merger', 'merging': 'Merging', 'merged': 'Merged',
    'divest': 'Divest', 'divestiture': 'Divestiture', 'joint venture': 'Joint Venture',
    ' jv ': 'JV', 'takeover': 'Takeover', 'buyout': 'Buyout', 'purchase': 'Purchase',
    'consolidat': 'Consolidation', ' deal ': 'Deal', 'transaction': 'Transaction',
    'invest': 'Investment', ' sold ': 'Sold', 'sale of': 'Sale of',
    'to acquire': 'To Acquire', 'has acquired': 'Has Acquired',
    'definitive agreement': 'Definitive Agreement', 'purchase agreement': 'Purchase Agreement',
  };
  for (const [k, label] of Object.entries(display)) {
    if (t.includes(k) && !found.includes(label)) found.push(label);
    if (found.length >= 5) break;
  }
  return found;
}

function detectSector(text) {
  const t = lc(text);
  const found = SECTOR_MAP.filter(s => s.keys.some(k => t.includes(k))).map(s => s.label);
  if (found.length === 0) return null;
  if (found.length > 1)   return 'Multiple';
  return found[0];
}

function detectType(text) {
  const t = lc(text);
  for (const m of TYPE_MAP) {
    if (m.keys.some(k => t.includes(k))) return m.type;
  }
  return 'acquisition';
}

function extractValue(text) {
  const t = text || '';
  let m;
  m = t.match(/\$\s*([\d,]+\.?\d*)\s*billion/i);
  if (m) return parseFloat(m[1].replace(/,/g, ''));
  m = t.match(/\$\s*([\d,]+\.?\d*)\s*B\b/i);
  if (m) return parseFloat(m[1].replace(/,/g, ''));
  m = t.match(/\$\s*([\d,]+\.?\d*)\s*million/i);
  if (m) return +(parseFloat(m[1].replace(/,/g, '')) / 1000).toFixed(4);
  m = t.match(/\$\s*([\d,]+\.?\d*)\s*M\b/i);
  if (m) return +(parseFloat(m[1].replace(/,/g, '')) / 1000).toFixed(4);
  return null;
}

function stripTags(html) {
  return (html || '').replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();
}

function parseArticles(html, baseUrl, sourceName) {
  const results = [];
  const seen = new Set();
  const anchorRe = /<a[^>]+href=["']([^"'#?][^"']*)["'][^>]*>([\s\S]{0,200}?)<\/a>/gi;
  let m;
  const blockRe = /<(?:p|h[123456]|li)[^>]*>([\s\S]{20,800}?)<\/(?:p|h[123456]|li)>/gi;
  const blocks = [];
  let bm;
  while ((bm = blockRe.exec(html)) !== null) {
    const text = stripTags(bm[1]);
    if (text.length > 20) blocks.push(text);
  }
  while ((m = anchorRe.exec(html)) !== null) {
    let href = m[1].trim();
    const anchorText = stripTags(m[2]).trim();
    if (!href || href.startsWith('javascript') || href.startsWith('mailto') || href.length < 5) continue;
    if (!anchorText || anchorText.length < 10) continue;
    if (href.startsWith('//')) href = 'https:' + href;
    else if (href.startsWith('/')) {
      try { href = new URL(href, baseUrl).toString(); } catch(e) { continue; }
    } else if (!href.startsWith('http')) continue;
    const normHref = href.split('?')[0].replace(/\/$/, '');
    if (seen.has(normHref)) continue;
    if (/\/(tag|category|author|page|search|feed|login|contact|about)\//i.test(href)) continue;
    if (/\.(jpg|jpeg|png|gif|svg|pdf|zip|css|js)$/i.test(href)) continue;
    const titleLower = lc(anchorText);
    let bestContext = '';
    for (const block of blocks) {
      const words = titleLower.split(/\s+/).filter(w => w.length > 4);
      const matchCount = words.filter(w => lc(block).includes(w)).length;
      if (matchCount >= 2 && block.length > bestContext.length) bestContext = block;
    }
    const combined = anchorText + ' ' + bestContext;
    if (!containsMA(combined)) continue;
    seen.add(normHref);
    const summary = bestContext.length > 30 ? bestContext : anchorText;
    const trimmedSummary = summary.length > 320 ? summary.slice(0, 317) + '…' : summary;
    results.push({
      title: anchorText, summary: trimmedSummary, url: href,
      pubDate: new Date().toISOString(), source: sourceName,
      keywords: matchedKeywords(combined), sector: detectSector(combined),
      dealType: detectType(combined), dealValue: extractValue(combined),
    });
    if (results.length >= 15) break;
  }
  return results;
}

async function fetchSource(source) {
  try {
    const resp = await fetch(source.url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; MA-Monitor/1.0)',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      },
      cf: { cacheTtl: 3600, cacheEverything: true },
      signal: AbortSignal.timeout(12000),
    });
    if (!resp.ok) return { source: source.name, articles: [], error: `HTTP ${resp.status}` };
    const html = await resp.text();
    const articles = parseArticles(html, source.url, source.name);
    return { source: source.name, articles, error: null };
  } catch (e) {
    return { source: source.name, articles: [], error: e.message };
  }
}

export default {
  async fetch(request, env, ctx) {
    const corsHeaders = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
      'Content-Type': 'application/json',
    };
    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }
    const url = new URL(request.url);
    if (url.pathname === '/ping') {
      return new Response(JSON.stringify({ ok: true, sources: SOURCES.length }), { headers: corsHeaders });
    }
    const results = await Promise.all(SOURCES.map(fetchSource));
    const allArticles = [];
    const sourceStatuses = {};
    for (const r of results) {
      sourceStatuses[r.source] = r.error ? { ok: false, error: r.error } : { ok: true, count: r.articles.length };
