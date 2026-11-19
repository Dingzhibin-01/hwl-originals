#!/usr/bin/env python3
"""各国政府/智库官网 feed 自动发现。

输入 sites/gov_sites.csv（site_url,domain,name,kind,country）
输出 results/discovered.csv（site_url,feed_url,http_status,title + 附加列）

发现顺序：
  1) 首页 <link rel="alternate" type="application/rss+xml|atom+xml">
  2) 依次试 /feed /rss /rss.xml /atom.xml /feed.xml /index.xml
每个候选 feed 都真拉一次，返回 200 且能被 feedparser 解析才算命中。
"""
from __future__ import annotations

import argparse
import calendar
import csv
import datetime as dt
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, urlparse

import feedparser
import requests

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}
TIMEOUT = 12
FEED_TIMEOUT = 10
WORKERS = 24
MAX_BYTES = 600_000
SUFFIXES = ["/feed", "/rss", "/rss.xml", "/atom.xml", "/feed.xml", "/index.xml"]
FEED_MARKERS = (b"<rss", b"<feed", b"<rdf:rdf")
LINK_RE = re.compile(r"<link\b[^>]*>", re.I)
ATTR_RE = re.compile(r"""([\w:-]+)\s*=\s*["']([^"']*)["']""", re.I)

OUT_FIELDS = ["site_url", "feed_url", "http_status", "title", "kind", "country",
              "method", "items", "last_item_date", "elapsed_ms", "error"]

_local = threading.local()


def sess() -> requests.Session:
    s = getattr(_local, "s", None)
    if s is None:
        s = requests.Session()
        s.headers.update(HEADERS)
        _local.s = s
    return s


def fetch(url: str, timeout: float = TIMEOUT, limit: int = MAX_BYTES):
    try:
        with sess().get(url, timeout=timeout, allow_redirects=True, stream=True) as r:
            buf = bytearray()
            for chunk in r.iter_content(32768):
                if not chunk:
                    continue
                buf.extend(chunk)
                if len(buf) >= limit:
                    break
            return r.status_code, r.url or url, (r.headers.get("Content-Type") or ""), bytes(buf), ""
    except requests.exceptions.Timeout:
        return 0, url, "", b"", "timeout"
    except Exception as exc:  # noqa: BLE001
        return 0, url, "", b"", "%s: %s" % (type(exc).__name__, str(exc)[:120])


def decode(body: bytes, ctype: str) -> str:
    m = re.search(r"charset=([\w-]+)", ctype or "", re.I)
    encs = ([m.group(1)] if m else []) + ["utf-8", "gb18030", "latin-1"]
    for enc in encs:
        try:
            return body.decode(enc, "replace")
        except Exception:  # noqa: BLE001
            continue
    return body.decode("utf-8", "replace")


def page_title(html: str) -> str:
    m = re.search(r"<title[^>]*>(.*?)</title>", html, re.S | re.I)
    if not m:
        return ""
    return re.sub(r"\s+", " ", m.group(1)).strip()[:120]


def autodiscover(html: str, base: str) -> str:
    for tag in LINK_RE.findall(html):
        attrs = {k.lower(): v for k, v in ATTR_RE.findall(tag)}
        rel = (attrs.get("rel") or "").lower()
        typ = (attrs.get("type") or "").lower()
        href = attrs.get("href") or ""
        if "alternate" in rel and ("rss" in typ or "atom" in typ) and href:
            return urljoin(base, href.strip())
    # 退一步：任何 type 含 rss/atom 的 link
    for tag in LINK_RE.findall(html):
        attrs = {k.lower(): v for k, v in ATTR_RE.findall(tag)}
        typ = (attrs.get("type") or "").lower()
        href = attrs.get("href") or ""
        if ("rss" in typ or "atom" in typ) and href:
            return urljoin(base, href.strip())
    return ""


def last_item_date(parsed) -> str:
    best = None
    for entry in parsed.entries[:200]:
        for key in ("published_parsed", "updated_parsed"):
            t = entry.get(key)
            if not t:
                continue
            try:
                if best is None or t > best:
                    best = t
            except TypeError:
                pass
            break
    if not best or not best[0] or best[0] < 1990:
        return ""
    return dt.datetime.fromtimestamp(calendar.timegm(best), dt.timezone.utc).strftime("%Y-%m-%d")


def probe_feed(url: str):
    """真拉一次 feed；返回 (ok, items, last_date, status, note, ms)。"""
    t0 = time.monotonic()
    status, final, ctype, body, err = fetch(url, timeout=FEED_TIMEOUT, limit=1_500_000)
    ms = int((time.monotonic() - t0) * 1000)
    if err or not body:
        return False, 0, "", status, err or "empty", ms
    head = body[:8192].lower()
    if not any(m in head for m in FEED_MARKERS) and "xml" not in (ctype or "").lower():
        return False, 0, "", status, "not xml", ms
    parsed = feedparser.parse(body)
    n = len(parsed.entries)
    if parsed.version or n:
        return True, n, last_item_date(parsed), status, "", ms
    return False, 0, "", status, "xml but not feed", ms


def discover(row: dict) -> dict:
    site = (row.get("site_url") or "").strip()
    rec = {f: "" for f in OUT_FIELDS}
    rec.update(site_url=site, kind=row.get("kind", ""), country=row.get("country", ""))
    t0 = time.monotonic()
    if not site.startswith(("http://", "https://")):
        rec["error"] = "invalid url"
        return rec

    status, final, ctype, body, err = fetch(site)
    rec["http_status"] = status
    html = decode(body, ctype) if body else ""
    rec["title"] = page_title(html) if html else ""
    if not body:
        rec["error"] = err or "empty"
        rec["elapsed_ms"] = int((time.monotonic() - t0) * 1000)
        return rec

    cand = autodiscover(html, final)
    if cand:
        ok, n, last, st, note, _ = probe_feed(cand)
        if ok:
            rec.update(feed_url=cand, method="autodiscover", items=n,
                       last_item_date=last, http_status=st)
            rec["elapsed_ms"] = int((time.monotonic() - t0) * 1000)
            return rec
        rec["error"] = "autodiscover invalid: %s" % note

    root = "{0.scheme}://{0.netloc}".format(urlparse(final))
    for suffix in SUFFIXES:
        cand = root + suffix
        ok, n, last, st, note, _ = probe_feed(cand)
        if ok:
            rec.update(feed_url=cand, method="suffix" + suffix, items=n,
                       last_item_date=last, http_status=st, error="")
            rec["elapsed_ms"] = int((time.monotonic() - t0) * 1000)
            return rec
    if not rec["error"]:
        rec["error"] = "no feed found"
    rec["elapsed_ms"] = int((time.monotonic() - t0) * 1000)
    return rec


def load_sites(path: str, offset: int, limit: int):
    with open(path, newline="", encoding="utf-8-sig") as f:
        rows = [{(k or "").strip(): (v or "").strip() for k, v in r.items()} for r in csv.DictReader(f)]
    rows = [r for r in rows if r.get("site_url")]
    return rows[offset:offset + limit] if limit else rows[offset:]


def write_csv(path, rows, fields):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    os.replace(tmp, path)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sites", default="sites/gov_sites.csv")
    ap.add_argument("--offset", type=int, default=0)
    ap.add_argument("--limit", type=int, default=400)
    ap.add_argument("--workers", type=int, default=WORKERS)
    ap.add_argument("--out", default="results/discovered.csv")
    ap.add_argument("--out-all", default="results/discovered_all.csv")
    args = ap.parse_args(argv)

    sites = load_sites(args.sites, args.offset, args.limit)
    print("sites: %d (offset=%d limit=%d)" % (len(sites), args.offset, args.limit))
    if not sites:
        print("本批没有站点")
        return 0

    results = [None] * len(sites)
    done = 0
    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futs = {pool.submit(discover, row): i for i, row in enumerate(sites)}
        for fut in as_completed(futs):
            i = futs[fut]
            try:
                results[i] = fut.result()
            except Exception as exc:  # noqa: BLE001
                results[i] = {"site_url": sites[i].get("site_url", ""), "error": "worker: %s" % exc}
            done += 1
            if done % 25 == 0 or done == len(sites):
                rate = done / max(time.monotonic() - t0, 1e-6)
                print("  %d/%d  %.1f/s" % (done, len(sites), rate), flush=True)

    live = [r for r in results if r.get("feed_url")]
    write_csv(args.out_all, results, OUT_FIELDS)
    write_csv(args.out, live, OUT_FIELDS)
    kinds = {}
    for r in live:
        kinds[r.get("kind") or "?"] = kinds.get(r.get("kind") or "?", 0) + 1
    print("=" * 60)
    print("站点 %d  发现 feed %d  命中率 %.1f%%" % (len(results), len(live),
                                                100.0 * len(live) / max(len(results), 1)))
    print("按 kind: %s" % kinds)
    print("=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
