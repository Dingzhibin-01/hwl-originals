#!/usr/bin/env python3
"""批量探测 RSS/Atom 信息源是否可用（供 .github/workflows/verify-feeds.yml 调用）。

用法::

    python .github/scripts/verify_feeds.py --batch 1 --chunk 500

从 candidates/candidates.csv（表头 url,domain,kind,country,lang）中取
第 (batch-1)*chunk 行到 batch*chunk 行（1 起算、不含表头），并发探测后写入
results/batch_<batch>.csv。同一 batch 重跑会覆盖该文件，不会产生重复行。
"""

from __future__ import annotations

import argparse
import calendar
import csv
import datetime as dt
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import feedparser
import requests

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)
HEADERS = {
    "User-Agent": UA,
    "Accept": "application/rss+xml, application/atom+xml, application/xml;q=0.9, "
    "text/xml;q=0.9, */*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8",
}

# 前 12 列为约定字段，content_type / bytes 追加在末尾用于排查（不影响按列名汇总）。
FIELDS = [
    "url", "domain", "kind", "country", "lang",
    "status", "final_url", "is_feed", "items", "last_item_date",
    "elapsed_ms", "error",
    "content_type", "bytes",
]

TIMEOUT = 15          # 单条超时（秒）
WORKERS = 40          # 并发数
MAX_BYTES = 2_000_000  # 只读前 2 MB，足够判定 feed 且避免大文件拖垮内存
FEED_MARKERS = (b"<rss", b"<feed", b"<rdf:rdf")
MAX_BATCH = 800       # 单批上限，避免 Actions 6 小时上限被打爆

_local = threading.local()


def _session() -> requests.Session:
    s = getattr(_local, "s", None)
    if s is None:
        s = requests.Session()
        _local.s = s
    return s


def _host(url: str) -> str:
    try:
        host = (urlparse(url).hostname or "").lower()
    except ValueError:
        return ""
    return host[4:] if host.startswith("www.") else host


def _read_body(resp: requests.Response, limit: int = MAX_BYTES) -> bytes:
    buf = bytearray()
    for chunk in resp.iter_content(65536):
        if not chunk:
            continue
        buf.extend(chunk)
        if len(buf) >= limit:
            return bytes(buf[:limit])
    return bytes(buf)


def _last_item_date(parsed) -> str:
    """取 entries 里最新的 published/updated，输出 UTC 日期 YYYY-MM-DD。"""
    best = None
    for entry in parsed.entries[:300]:
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
    return dt.datetime.fromtimestamp(
        calendar.timegm(best), dt.timezone.utc
    ).strftime("%Y-%m-%d")


def probe(row: dict) -> dict:
    """探测单条源，返回一行结果（字段见 FIELDS）。"""
    url = (row.get("url") or "").strip()
    rec = {f: "" for f in FIELDS}
    rec.update(
        url=url,
        domain=(row.get("domain") or "").strip() or _host(url),
        kind=(row.get("kind") or "").strip(),
        country=(row.get("country") or "").strip(),
        lang=(row.get("lang") or "").strip(),
        status=0,
        is_feed=0,
        items=0,
        bytes=0,
    )
    if not url.lower().startswith(("http://", "https://")):
        rec["error"] = "invalid url"
        rec["elapsed_ms"] = 0
        return rec

    started = time.monotonic()
    try:
        with _session().get(
            url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True, stream=True
        ) as resp:
            body = _read_body(resp)
            ctype = (resp.headers.get("Content-Type") or "").strip()
            rec["status"] = resp.status_code
            rec["final_url"] = resp.url or ""
            rec["content_type"] = ctype
            rec["bytes"] = len(body)

            head = body[:8192].lower()
            marker = any(m in head for m in FEED_MARKERS)
            looks_xml = marker or "xml" in ctype.lower()
            if looks_xml and body:
                parsed = feedparser.parse(body)
                items = len(parsed.entries)
                if parsed.version or items:
                    rec["is_feed"] = 1
                    rec["items"] = items
                    rec["last_item_date"] = _last_item_date(parsed)
                else:
                    rec["error"] = "xml but not a feed"
            elif not body:
                rec["error"] = f"empty body (status {rec['status']})"
    except requests.exceptions.Timeout:
        rec["error"] = "timeout"
    except requests.exceptions.TooManyRedirects:
        rec["error"] = "too many redirects"
    except Exception as exc:  # noqa: BLE001 - 单条失败不应中断整批
        rec["error"] = f"{type(exc).__name__}: {str(exc)[:180]}"
    finally:
        rec["elapsed_ms"] = int((time.monotonic() - started) * 1000)
    return rec


def load_rows(path: str) -> list[dict]:
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        headers = [(h or "").strip().lower() for h in (reader.fieldnames or [])]
        if "url" not in headers:
            sys.exit(f"[fatal] {path} 缺少 url 表头，实际表头：{reader.fieldnames}")
        rows = []
        for row in reader:
            clean = {
                (k or "").strip().lower(): (v or "").strip()
                for k, v in row.items()
                if isinstance(k, str)
            }
            if clean.get("url"):
                rows.append(clean)
    return rows


def write_csv(path: str, rows: list[dict]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp, path)


def summarize(rows: list[dict]) -> str:
    total = len(rows)
    http_200 = sum(1 for r in rows if r["status"] == 200)
    feeds = [r for r in rows if r["is_feed"] == 1]
    usable = [r for r in feeds if r["status"] == 200 and r["items"] > 0]
    errors: dict[str, int] = {}
    for r in rows:
        if r["error"]:
            key = r["error"].split(":")[0][:40]
            errors[key] = errors.get(key, 0) + 1
    lines = [
        "=" * 60,
        f"总数 {total}   HTTP 200 {http_200}   合法 feed {len(feeds)}   有条目且 200 {len(usable)}",
        f"合法 feed 占比 {len(feeds) / total:.1%}" if total else "本批为空",
    ]
    if errors:
        top = sorted(errors.items(), key=lambda kv: -kv[1])[:8]
        lines.append("错误分布：" + "，".join(f"{k}×{v}" for k, v in top))
    lines.append("=" * 60)
    return "\n".join(lines)


def append_step_summary(text: str) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write("```\n" + text + "\n```\n")
    except OSError:
        pass


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="批量探测 RSS/Atom 源")
    ap.add_argument("--batch", type=int, required=True, help="批次号，1-20")
    ap.add_argument("--chunk", type=int, default=500, help=f"每批条数，1-{MAX_BATCH}")
    ap.add_argument("--candidates", default="candidates/candidates.csv")
    ap.add_argument("--out-dir", default="results")
    ap.add_argument("--workers", type=int, default=WORKERS)
    args = ap.parse_args(argv)

    if not 1 <= args.batch <= 20:
        sys.exit(f"[fatal] batch 必须在 1-20 之间，收到 {args.batch}")
    if not 1 <= args.chunk <= MAX_BATCH:
        sys.exit(f"[fatal] chunk 必须在 1-{MAX_BATCH} 之间，收到 {args.chunk}")
    if not os.path.exists(args.candidates):
        sys.exit(f"[fatal] 找不到 {args.candidates}，请先按 candidates/README.md 放入候选清单")

    rows = load_rows(args.candidates)
    start = (args.batch - 1) * args.chunk
    batch_rows = rows[start:start + args.chunk]
    out_path = os.path.join(args.out_dir, f"batch_{args.batch}.csv")

    print(f"candidates: {args.candidates} 共 {len(rows)} 条")
    print(f"batch={args.batch} chunk={args.chunk} 切片=[{start + 1}, {start + len(batch_rows)}]")
    if not batch_rows:
        print("本批没有数据（超出清单末尾），不写结果文件。")
        return 0

    results: list[dict | None] = [None] * len(batch_rows)
    done = 0
    began = time.monotonic()
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = {
            pool.submit(probe, row): i for i, row in enumerate(batch_rows)
        }
        for fut in as_completed(futures):
            i = futures[fut]
            try:
                results[i] = fut.result()
            except Exception as exc:  # 理论上 probe 不会抛，兜底
                results[i] = {
                    **{f: "" for f in FIELDS},
                    "url": batch_rows[i].get("url", ""),
                    "domain": _host(batch_rows[i].get("url", "")),
                    "status": 0,
                    "is_feed": 0,
                    "items": 0,
                    "error": f"worker error: {exc}",
                }
            done += 1
            if done % 25 == 0 or done == len(batch_rows):
                rate = done / max(time.monotonic() - began, 1e-6)
                print(f"  {done}/{len(batch_rows)}  {rate:.1f} 条/秒", flush=True)

    write_csv(out_path, results)  # type: ignore[arg-type]
    report = summarize(results)  # type: ignore[arg-type]
    print(report)
    print(f"已写入 {out_path}")
    append_step_summary(f"batch {args.batch} chunk {args.chunk}\n{report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
