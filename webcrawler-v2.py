#!/usr/bin/env python3
"""
Robust web crawler to download PDFs from websites, with JavaScript rendering via Playwright.

Install requirements:
    pip install requests beautifulsoup4 playwright
    playwright install  # install browser binaries

Usage:
    python robust_crawler.py \
        --start-url https://papers.nips.cc \
        --domains papers.nips.cc openreview.net \
        --exclude 'logout|login' \
        --output-dir pdfs \
        --max-depth 3 \
        --delay 1.0 \
        --overwrite \
        --render-delay 2 \
        --infinite-scroll \
        --trace
"""
import argparse
import os
import re
import time
from urllib.parse import urljoin, urlparse, parse_qs
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


def parse_args():
    parser = argparse.ArgumentParser(
        description='Robust crawler: JS rendering + PDF downloading.'
    )
    parser.add_argument('--start-url', required=True, help='Initial URL to start crawling')
    parser.add_argument('--domains', nargs='+', required=True,
                        help='List of allowed domains (e.g., papers.nips.cc openreview.net)')
    parser.add_argument('--exclude', default=None,
                        help='Regex pattern to exclude URLs')
    parser.add_argument('--output-dir', default='pdfs', help='Directory to save PDFs')
    parser.add_argument('--max-depth', type=int, default=3, help='Maximum crawl depth')
    parser.add_argument('--delay', type=float, default=1.0, help='Delay between requests')
    parser.add_argument('--overwrite', action='store_true', help='Overwrite existing files')
    parser.add_argument('--render-delay', type=float, default=2.0,
                        help='Time in seconds to wait after page load for JS')
    parser.add_argument('--infinite-scroll', action='store_true',
                        help='Enable scrolling to bottom until no new content loads')
    parser.add_argument('--trace', action='store_true',
                        help='Enable tracing of link paths to each downloaded PDF')
    return parser.parse_args()


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def is_allowed_domain(url, domains):
    return urlparse(url).netloc in domains


def log_warning(output_dir, url, error):
    with open(os.path.join(output_dir, 'WARNINGS.txt'), 'a') as wf:
        wf.write(f"Failed: {url} -> {error}\n")


def log_trace(output_dir, path_list):
    trace_file = os.path.join(output_dir, 'trace.txt')
    with open(trace_file, 'a') as tf:
        tf.write(" -> ".join(path_list) + "\n")


def is_pdf_link(url):
    parsed = urlparse(url)
    if parsed.path.lower().endswith('.pdf'):
        return True
    if parsed.path == '/pdf' and 'id' in parse_qs(parsed.query):
        return True
    if parsed.path == '/attachment' and 'id' in parse_qs(parsed.query):
        return True
    return False


def get_pdf_filename(url):
    parsed = urlparse(url)
    if parsed.path.lower().endswith('.pdf'):
        return os.path.basename(parsed.path)
    qs = parse_qs(parsed.query)
    pdf_id = qs.get('id', [''])[0]
    name = qs.get('name', [None])[0]
    if name and name.lower().endswith('.pdf'):
        return name
    if pdf_id:
        return f"{pdf_id}.pdf"
    return os.path.basename(parsed.path) or 'download.pdf'


def download_pdf(url, output_dir, overwrite, trace, path_list):
    filename = get_pdf_filename(url)
    filepath = os.path.join(output_dir, filename)
    if os.path.exists(filepath) and not overwrite:
        print(f"[~] Skip existing: {filename}")
        return
    for attempt in range(1, 3):
        try:
            print(f"[+] Downloading (try {attempt}): {url}")
            resp = requests.get(url, stream=True)
            resp.raise_for_status()
            with open(filepath, 'wb') as f:
                for chunk in resp.iter_content(8192):
                    f.write(chunk)
            if trace:
                log_trace(output_dir, path_list + [url])
            return
        except Exception as e:
            print(f"[!] Download error (try {attempt}) {url}: {e}")
            time.sleep(1)
    log_warning(output_dir, url, 'Download failed')


def render_page(url, render_delay, infinite_scroll):
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url)
        time.sleep(render_delay)
        if infinite_scroll:
            last_height = None
            while True:
                curr_height = page.evaluate('document.body.scrollHeight')
                if curr_height == last_height:
                    break
                last_height = curr_height
                page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                time.sleep(render_delay)
        content = page.content()
        browser.close()
    return content


def extract_links(html, base_url):
    soup = BeautifulSoup(html, 'html.parser')
    for tag in soup.find_all('a', href=True):
        href = tag['href']
        full = urljoin(base_url, href)
        yield full


def crawl(args):
    visited = set()
    queue = [(args.start_url, 0, [args.start_url])]
    exclude_re = re.compile(args.exclude) if args.exclude else None
    ensure_dir(args.output_dir)

    while queue:
        url, depth, path_list = queue.pop(0)
        if url in visited or depth > args.max_depth:
            continue
        visited.add(url)
        print(f"[*] Depth {depth}: {url}")

        try:
            html = render_page(url, args.render_delay, args.infinite_scroll)
        except Exception as e:
            print(f"[!] Render error: {e}")
            log_warning(args.output_dir, url, e)
            continue

        for link in extract_links(html, url):
            norm = urlparse(link)._replace(fragment='').geturl()
            if not is_allowed_domain(norm, args.domains):
                continue
            if exclude_re and exclude_re.search(norm):
                print(f"[-] Excluded: {norm}")
                continue
            if is_pdf_link(norm):
                download_pdf(norm, args.output_dir, args.overwrite, args.trace, path_list)
            else:
                queue.append((norm, depth + 1, path_list + [norm]))
        time.sleep(args.delay)


def main():
    args = parse_args()
    crawl(args)


if __name__ == '__main__':
    main()
