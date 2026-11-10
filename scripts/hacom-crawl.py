#!/usr/bin/env python3
"""Fast, cacheable public-site crawler for HACOM-style functional surveys.

HTTP is the default path. A browser is deliberately not required: sitemap/links,
JSON-LD, forms and visible text are stored in SQLite for later analysis. Dynamic
pages can be revisited separately with a browser after their public endpoints are
identified. The crawler is rate-limited, same-origin, and robots-aware.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import gzip
import json
import re
import sqlite3
import time
import urllib.parse
import urllib.robotparser
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path

import httpx


SKIP_EXT = re.compile(r"\.(?:jpg|jpeg|png|gif|webp|svg|ico|css|js|woff2?|ttf|pdf|zip|mp4)(?:$|\?)", re.I)
INTERESTING = re.compile(r"(?:san-pham|product|laptop|pc|linh-kien|build|buildpc|chinh-sach|bao-hanh|thanh-toan|giao-hang|tra-don|showroom|dich-vu|khuyen-mai|doanh-nghiep|blog|tin-tuc)", re.I)


class PageParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.links: list[str] = []
        self.text: list[str] = []
        self.title: list[str] = []
        self.meta: dict[str, str] = {}
        self.jsonld: list[str] = []
        self.forms: list[dict[str, str]] = []
        self._tag = ""
        self._in_title = False
        self._in_script = False
        self._script_buf: list[str] = []
        self._form: dict[str, str] | None = None
        self._excluded: str | None = None
        self.scripts: list[str] = []

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        self._tag = tag
        if tag in {"script", "style"}:
            self._excluded = tag
        if tag == "script" and a.get("src"):
            self.scripts.append(a["src"])
        if tag == "link" and a.get("rel") == "canonical":
            self.meta["canonical"] = a.get("href", "")
        if tag == "title": self._in_title = True
        if tag == "script" and a.get("type", "").lower() == "application/ld+json":
            self._in_script, self._script_buf = True, []
        if tag == "a" and a.get("href"): self.links.append(a["href"])
        if tag == "meta" and a.get("content"):
            key = (a.get("property") or a.get("name") or a.get("itemprop") or "").lower()
            if key: self.meta[key] = a["content"]
        if tag == "form": self._form = {"action": a.get("action", ""), "method": a.get("method", "get")}
        if tag == "input" and self._form is not None and a.get("name"):
            self._form.setdefault("inputs", "")
            self._form["inputs"] += (a.get("name", "") + "=" + a.get("type", "text") + ";")

    def handle_endtag(self, tag):
        if tag == self._excluded: self._excluded = None
        if tag == "title": self._in_title = False
        if tag == "script" and self._in_script:
            self.jsonld.append("".join(self._script_buf)); self._in_script = False
        if tag == "form" and self._form is not None:
            self.forms.append(self._form); self._form = None
        self._tag = ""

    def handle_data(self, data):
        if self._in_title: self.title.append(data)
        if self._in_script: self._script_buf.append(data)
        elif not self._excluded and data.strip(): self.text.append(data.strip())


def canon(base: str, raw: str) -> str | None:
    u = urllib.parse.urljoin(base, raw.strip())
    p = urllib.parse.urlsplit(u)
    if p.scheme not in {"http", "https"} or p.netloc != urllib.parse.urlsplit(base).netloc: return None
    path = re.sub(r"/{2,}", "/", p.path or "/")
    # Tracking parameters create enormous duplicate crawls.
    qs = [(k, v) for k, v in urllib.parse.parse_qsl(p.query, keep_blank_values=True)
          if not k.lower().startswith(("utm_", "fbclid", "gclid"))]
    return urllib.parse.urlunsplit((p.scheme, p.netloc.lower(), path, urllib.parse.urlencode(qs), ""))


def json_objects(values: list[str]) -> list[dict]:
    out = []
    for raw in values:
        try:
            obj = json.loads(raw)
            out.extend(obj if isinstance(obj, list) else [obj])
        except Exception: pass
    return [x for x in out if isinstance(x, dict)]


@dataclass
class Config:
    root: str
    max_pages: int
    concurrency: int
    delay: float
    out: Path
    store_html: bool = False
    cache_dir: Path = Path(".runtime/hacom-crawl-cache")
    refresh: bool = False


class Crawler:
    def __init__(self, cfg: Config):
        self.cfg, self.host = cfg, urllib.parse.urlsplit(cfg.root).netloc
        self.sem = asyncio.Semaphore(cfg.concurrency)
        self.rate_lock = asyncio.Lock()
        self.last_request = 0.0
        self.cache_hits = 0
        self.requests = 0
        cfg.cache_dir.mkdir(parents=True, exist_ok=True)
        self.client = httpx.AsyncClient(follow_redirects=True, timeout=25, headers={"User-Agent": "NovaTech-research-crawler/1.0 (+public survey; contact site owner before production use)"})
        self.db = sqlite3.connect(cfg.out)
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS pages (url TEXT PRIMARY KEY, status INTEGER, content_type TEXT, title TEXT, canonical TEXT, kind TEXT, fetched_at REAL, sha256 TEXT, text TEXT, html TEXT, error TEXT);
        CREATE TABLE IF NOT EXISTS links (from_url TEXT, to_url TEXT, PRIMARY KEY(from_url,to_url));
        CREATE TABLE IF NOT EXISTS jsonld (url TEXT, payload TEXT);
        CREATE TABLE IF NOT EXISTS forms (url TEXT, action TEXT, method TEXT, inputs TEXT);
        CREATE TABLE IF NOT EXISTS crawl_meta (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE IF NOT EXISTS scripts (url TEXT, src TEXT, PRIMARY KEY(url,src));
        CREATE TABLE IF NOT EXISTS discovered (url TEXT PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS skipped (url TEXT PRIMARY KEY, reason TEXT);
        """)
        self.rp = urllib.robotparser.RobotFileParser()
        self.rp.set_url(urllib.parse.urljoin(cfg.root, "/robots.txt"))
        self.allowed: set[str] = set()

    async def setup(self):
        try:
            r = await self.request(self.rp.url)
            if r.status_code not in {200, 404}: raise RuntimeError(f"robots.txt: HTTP {r.status_code}")
            self.rp.parse(r.text.splitlines())
        except Exception as exc:
            raise RuntimeError(f"Cannot determine robots rules: {exc}") from exc
        seeds = {self.cfg.root.rstrip("/") + "/"}
        sitemap_urls = set()
        sitemap_pending = {urllib.parse.urljoin(self.cfg.root, path) for path in ("/sitemap.xml", "/sitemap_index.xml")}
        sitemap_pending.update(self.rp.site_maps() or [])
        processed = set()
        while sitemap_pending and len(processed) < 300:
            sm = sorted(sitemap_pending)[0]
            sitemap_pending.remove(sm)
            if sm in processed or not canon(self.cfg.root, sm): continue
            processed.add(sm)
            try:
                r = await self.fetch(sm)
                if isinstance(r, httpx.Response) and r.status_code < 400:
                    xml = ET.fromstring(r.text)
                    locations = self.parse_sitemap(r.text)
                    if xml.tag.split('}')[-1] == 'sitemapindex': sitemap_pending.update(locations - processed)
                    else: seeds.update(locations)
            except (ET.ParseError, httpx.HTTPError): pass
        # Include operational routes explicitly so alphabetical product sampling
        # cannot crowd out the software-survey pages.
        seeds.update(urllib.parse.urljoin(self.cfg.root, path) for path in (
            '/buildpc', '/cart', '/tra-don-hang', '/showroom',
            '/chinh-sach-bao-hanh', '/chinh-sach-giao-hang',
            '/chinh-sach-doanh-nghiep', '/chinh-sach-nhap-lai-tinh-phi',
            '/chinh-sach-quy-dinh-chung', '/huong-dan-thanh-toan',
            '/tra-bao-hanh-tu-2025', '/tra-bao-hanh-truoc-2025',
            '/hacom-huong-dan-mua-hang-truc-tuyen', '/thong-tin-lien-he'))
        self.allowed = {x for x in seeds if canon(self.cfg.root, x) and not SKIP_EXT.search(x)}
        self.db.executemany('INSERT OR IGNORE INTO discovered VALUES(?)', [(u,) for u in self.allowed])
        print(f"Discovered {len(self.allowed):,} page URLs across {len(processed)} sitemaps", flush=True)
        self.db.execute("INSERT OR REPLACE INTO crawl_meta VALUES (?,?)", ("root", self.cfg.root)); self.db.commit()

    def parse_sitemap(self, raw: str) -> set[str]:
        out = set()
        try:
            root = ET.fromstring(raw)
            for el in root.iter():
                if el.tag.endswith("loc") and el.text:
                    u = canon(self.cfg.root, el.text)
                    if u: out.add(u)
        except ET.ParseError: pass
        return out

    async def fetch(self, url: str):
        if not self.rp.can_fetch("NovaTech-research-crawler", url):
            self.db.execute('INSERT OR REPLACE INTO skipped VALUES(?,?)', (url, 'robots.txt'))
            return None
        async with self.sem:
            try: return await self.request(url)
            except Exception as e: return e

    async def request(self, url: str):
        path = self.cfg.cache_dir / (hashlib.sha256(url.encode()).hexdigest() + '.json.gz')
        if path.exists() and not self.cfg.refresh:
            with gzip.open(path, 'rt') as f: cached = json.load(f)
            self.cache_hits += 1
            return httpx.Response(cached['status'], headers=cached['headers'],
                                  content=cached['body'].encode(), request=httpx.Request('GET', cached['url']))
        async with self.rate_lock:
            await asyncio.sleep(max(0, self.cfg.delay - (time.monotonic() - self.last_request)))
            self.last_request = time.monotonic()
        self.requests += 1
        response = await self.client.get(url)
        if urllib.parse.urlsplit(str(response.url)).netloc != self.host:
            raise RuntimeError('Redirect left the configured origin')
        if response.status_code in {200, 404}:
            headers = {'content-type': response.headers.get('content-type', '')}
            with gzip.open(path, 'wt') as f:
                json.dump({'url': str(response.url), 'status': response.status_code,
                           'headers': headers, 'body': response.text}, f, ensure_ascii=False)
        return response

    async def run(self):
        await self.setup()
        queue = list(self.allowed)
        # Prefer useful pages first; cap sitemap-only sites deterministically.
        important = re.compile(r'chinh-sach|huong-dan|buildpc|tra-don|tra-bao|showroom|thong-tin-lien-he')
        queue.sort(key=lambda u: (0 if u == self.cfg.root or important.search(u) else 1,
                                 hashlib.sha256(u.encode()).hexdigest()))
        seen = set()
        while queue and len(seen) < self.cfg.max_pages:
            batch = []
            while queue and len(batch) < self.cfg.concurrency and len(seen) < self.cfg.max_pages:
                u = queue.pop(0)
                if u not in seen and not SKIP_EXT.search(u): seen.add(u); batch.append(u)
            results = await asyncio.gather(*(self.fetch(u) for u in batch))
            for url, response in zip(batch, results):
                new = self.store(url, response)
                for u in new:
                    if u not in seen and u not in queue and len(seen) + len(queue) < self.cfg.max_pages * 3: queue.append(u)
            self.db.commit()
            if len(seen) % 100 < self.cfg.concurrency:
                print(f"Processed {len(seen):,}/{self.cfg.max_pages:,}; HTTP {self.requests}, cache {self.cache_hits}", flush=True)
        self.db.executemany('INSERT OR REPLACE INTO crawl_meta VALUES (?,?)',
                            [('http_requests', str(self.requests)), ('cache_hits', str(self.cache_hits)),
                             ('remaining_queue', str(len(queue)))])
        self.db.execute("INSERT OR REPLACE INTO crawl_meta VALUES (?,?)", ("pages_seen", str(len(seen)))); self.db.commit()
        await self.client.aclose(); self.db.close()

    def store(self, url: str, response) -> set[str]:
        if response is None:
            return set()
        if isinstance(response, Exception):
            self.db.execute("INSERT OR REPLACE INTO pages(url,error,fetched_at) VALUES(?,?,?)", (url, str(response), time.time())); return set()
        content_type = response.headers.get("content-type", "")
        body = response.text if "text" in content_type or "html" in content_type or "xml" in content_type else ""
        if ("xml" in content_type or url.endswith(".xml")) and body.lstrip().startswith("<"):
            return {u for u in self.parse_sitemap(body) if u}
        p = PageParser();
        try: p.feed(body)
        except Exception: pass
        title = " ".join("".join(p.title).split())
        canonical = p.meta.get('canonical') or p.meta.get("og:url", "")
        objects = json_objects(p.jsonld)
        kinds = {str(x.get("@type", "")).lower() for x in objects}
        kind = "product" if "product" in kinds or p.meta.get("og:type") == "product" else ("policy" if re.search(r"chinh-sach|huong-dan|bao-hanh|giao-hang|thanh-toan", url, re.I) else "page")
        saved_html = body if self.cfg.store_html else ""
        self.db.execute("INSERT OR REPLACE INTO pages VALUES(?,?,?,?,?,?,?,?,?,?,?)", (url, response.status_code, response.headers.get("content-type", ""), title, canonical, kind, time.time(), hashlib.sha256(body.encode()).hexdigest(), " ".join(p.text), saved_html, ""))
        self.db.execute("DELETE FROM jsonld WHERE url=?", (url,)); self.db.executemany("INSERT INTO jsonld VALUES(?,?)", [(url, json.dumps(x, ensure_ascii=False)) for x in objects])
        self.db.execute("DELETE FROM forms WHERE url=?", (url,)); self.db.executemany("INSERT INTO forms VALUES(?,?,?,?)", [(url, f.get("action", ""), f.get("method", ""), f.get("inputs", "")) for f in p.forms])
        links = {x for raw in p.links if (x := canon(url, raw))}
        self.db.executemany('INSERT OR IGNORE INTO discovered VALUES(?)', [(u,) for u in links])
        self.db.executemany('INSERT OR IGNORE INTO scripts VALUES(?,?)',
                            [(url, urllib.parse.urljoin(url, src)) for src in p.scripts])
        self.db.executemany("INSERT OR IGNORE INTO links VALUES(?,?)", [(url, x) for x in links]); return links


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--root", default="https://hacom.vn/"); ap.add_argument("--max-pages", type=int, default=250); ap.add_argument("--concurrency", type=int, default=6); ap.add_argument("--delay", type=float, default=.25); ap.add_argument("--out", type=Path, default=Path("docs/research/hacom-2026-10-07/hacom-crawl.sqlite")); ap.add_argument("--store-html", action="store_true", help="retain raw HTML; off by default to keep large crawls compact")
    ap.add_argument('--cache-dir', type=Path, default=Path('.runtime/hacom-crawl-cache'))
    ap.add_argument('--refresh', action='store_true', help='ignore cached HTTP responses')
    args = ap.parse_args()
    if args.max_pages < 1 or args.concurrency < 1 or args.delay < 0: ap.error('invalid crawl limits')
    args.out.parent.mkdir(parents=True, exist_ok=True); asyncio.run(Crawler(Config(args.root, args.max_pages, args.concurrency, args.delay, args.out, args.store_html, args.cache_dir, args.refresh)).run()); print(f"Wrote {args.out}")

if __name__ == "__main__": main()
